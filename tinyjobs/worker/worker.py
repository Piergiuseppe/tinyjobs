from __future__ import annotations

import asyncio
import contextvars
import logging
import math
import threading
import time
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Sequence

from ..exceptions import (
    AppClosedError,
    BackendNotReadyError,
    ExecutorClosedError,
    UnknownTaskError,
)
from ..ids import new_worker_id
from ..models import Job
from ..retention import RetentionPolicy
from ..retry import RetryPolicy
from ..runtime import inside_running_loop
from ..deadlines import Deadline, bounded, current, under
from ..duration import format_seconds
from ..executors import ExecutorResolver
from ..polling import AdaptiveInterval
from ..services.execution import current_runner
from .signals import ShutdownWatcher

if TYPE_CHECKING:
    from ..app import TinyJobs

RELEASE_SECONDS = 1.0
RELEASE_SHARE = 4

logger = logging.getLogger("tinyjobs.worker")


class Worker:
    def __init__(
        self,
        app: "TinyJobs",
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 8,
        async_concurrency: int | None = None,
        lease_seconds: float = 60.0,
        reap_interval: float = 30.0,
        shutdown_timeout: float = 30.0,
        unknown_task_delay: float = 60.0,
        unknown_task_max_delay: float = 3600.0,
        retention: RetentionPolicy | None = None,
        scheduler: bool = True,
        misfire_grace: float = 60.0,
        poll_minimum: float = 0.005,
        poll_maximum: float = 0.5,
        worker_id: str | None = None,
        handle_signals: bool = True,
    ) -> None:
        self._app = app
        self._queues = tuple(queues)
        self._concurrency = concurrency
        self._lease_seconds = lease_seconds
        self._reap_interval = reap_interval
        self._shutdown_timeout = shutdown_timeout
        self._configured_shutdown_timeout = shutdown_timeout
        self._unknown_task_backoff = RetryPolicy(
            base=unknown_task_delay,
            maximum=max(unknown_task_max_delay, unknown_task_delay),
            jitter=False,
        )
        self._retention = retention or RetentionPolicy()
        self._scheduler = (
            app.scheduler(misfire_grace=misfire_grace) if scheduler else None
        )
        self._interval = AdaptiveInterval(minimum=poll_minimum, maximum=poll_maximum)
        self._async_concurrency = async_concurrency
        self._id = worker_id or new_worker_id()
        self._handle_signals = handle_signals
        self._inflight: dict[tuple[str, int], Job] = {}
        self._lost: set[tuple[str, int]] = set()
        self._settling: dict[tuple[str, int], bool] = {}
        self._running: dict[asyncio.Task, Deadline] = {}
        self._releases: set[asyncio.Future] = set()
        self._unreleased: list[Job] = []
        self._finished = threading.Event()
        self._prepare_run()

    def _prepare_run(self) -> None:
        self._executors = ExecutorResolver(
            concurrency=self._concurrency, async_concurrency=self._async_concurrency
        )
        self._stopping = asyncio.Event()
        self._forced = asyncio.Event()
        self._capacity = asyncio.Event()
        self._capacity.set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._shutdown_timeout = self._configured_shutdown_timeout
        self._ending = Deadline()
        self._admitting = Deadline()
        self._beating = Deadline()

    @property
    def id(self) -> str:
        return self._id

    @property
    def shutdown_timeout(self) -> float:
        return self._shutdown_timeout

    @property
    def finished(self) -> bool:
        return self._finished.is_set()

    def run(self) -> None:
        asyncio.run(self.start())

    def stop(self) -> None:
        stopping, ending, loop = self._stopping, self._ending, self._loop
        if loop is None or (
            inside_running_loop() and loop is asyncio.get_running_loop()
        ):
            self._request_stop(stopping, ending)
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(self._request_stop, stopping, ending)

    async def start(self) -> None:
        await self._app.ready()
        self._loop = asyncio.get_running_loop()
        self._finished.clear()
        self._app.attach(self)
        try:
            await self._serve()
        finally:
            closing = self._app.detach(self)
            self._prepare_run()
            self._finished.set()
            if closing:
                await self._app.aclose()

    async def _serve(self) -> None:
        watcher = ShutdownWatcher(self._request_stop, self._request_force)
        background: list[asyncio.Task] = []
        heartbeat: list[asyncio.Task] = []
        with under(self._ending):
            await self._served(watcher, background, heartbeat)

    async def _served(
        self,
        watcher: ShutdownWatcher,
        background: list[asyncio.Task],
        heartbeat: list[asyncio.Task],
    ) -> None:
        try:
            if self._handle_signals:
                watcher.install()
            logger.info(
                "worker %s listening on %s with concurrency %d",
                self._id,
                ",".join(self._queues),
                self._concurrency,
            )
            beating = contextvars.copy_context()
            beating.run(current.set, self._beating)
            heartbeat.append(
                asyncio.create_task(
                    self._heartbeat_loop(), name="tinyjobs-heartbeat", context=beating
                )
            )
            background.extend(self._start_background())
            await self._claim_loop()
        finally:
            for task in background:
                task.cancel()
            try:
                await asyncio.gather(*background, return_exceptions=True)
                await self._drain()
            finally:
                self._beating.end(time.monotonic())
                for task in heartbeat:
                    task.cancel()
                await asyncio.gather(*heartbeat, return_exceptions=True)
                watcher.uninstall()
                self._executors.shutdown()
                logger.info("worker %s stopped", self._id)

    def _start_background(self) -> list[asyncio.Task]:
        background = [
            asyncio.create_task(self._reaper_loop(), name="tinyjobs-reaper"),
            asyncio.create_task(
                self._housekeeping_loop(), name="tinyjobs-housekeeping"
            ),
        ]
        if self._scheduler is not None:
            background.append(
                asyncio.create_task(self._scheduler.start(), name="tinyjobs-scheduler")
            )
        return background

    async def _claim_loop(self) -> None:
        while not self._stopping.is_set():
            capacity = self._concurrency - len(self._inflight)
            if capacity <= 0:
                self._capacity.clear()
                await self._wait_for(self._capacity)
                continue
            jobs = await self._claim(capacity)
            if not jobs:
                await self._idle(self._interval.next())
                continue
            self._interval.reset()
            for job in jobs:
                self._launch(job)

    async def _claim(self, capacity: int) -> list[Job]:
        try:
            return await self._claimed(capacity)
        except AppClosedError:
            logger.info("the app was closed; worker %s is stopping", self._id)
            self._request_stop()
            return []
        except Exception:
            logger.exception("claim failed; backing off")
            await self._pause(self._interval.next())
            return []

    async def _claimed(self, capacity: int) -> list[Job]:
        with under(Deadline(math.inf, self._admitting)):
            claiming = asyncio.ensure_future(
                self._app.backend.claim(
                    worker_id=self._id,
                    queues=self._queues,
                    limit=capacity,
                    lease_seconds=self._lease_seconds,
                )
            )
        stopping = asyncio.ensure_future(self._stopping.wait())
        forced = asyncio.ensure_future(self._forced.wait())
        try:
            await asyncio.wait({claiming, stopping}, return_when=asyncio.FIRST_COMPLETED)
            if not claiming.done():
                await asyncio.wait(
                    {claiming, forced},
                    timeout=max(0.0, self._admitting.left()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            if not claiming.done():
                logger.warning(
                    "a claim was still waiting on the backend at shutdown; giving it up"
                )
                return []
            return claiming.result()
        finally:
            for task in (claiming, stopping, forced):
                task.cancel()
            await asyncio.gather(claiming, stopping, forced, return_exceptions=True)

    def _launch(self, job: Job) -> None:
        self._inflight[job.id, job.claims] = job
        deadline = Deadline(math.inf, self._ending)
        context = contextvars.copy_context()
        context.run(current.set, deadline)
        task = asyncio.create_task(
            self._run_job(job), name=f"tinyjobs-{job.id}", context=context
        )
        self._running[task] = deadline
        task.add_done_callback(self._finalize(job))

    def _finalize(self, job: Job):
        def done(task: asyncio.Task) -> None:
            self._running.pop(task, None)
            self._inflight.pop((job.id, job.claims), None)
            self._lost.discard((job.id, job.claims))
            self._capacity.set()
            settled = self._settling.pop((job.id, job.claims), None)
            if task.cancelled() and settled:
                return
            if task.cancelled() and settled is False:
                logger.warning(
                    "job %s ended while its outcome was being recorded; if it was not, "
                    "the reaper takes it back once its lease expires",
                    job.id,
                )
                return
            if task.cancelled():
                self._release_later(job)
                return
            error = task.exception()
            if error is not None:
                logger.exception(
                    "job %s crashed the worker loop", job.id, exc_info=error
                )

        return done

    def _release_later(self, job: Job) -> None:
        self._unreleased.append(job)
        if len(self._unreleased) == 1:
            asyncio.get_running_loop().call_soon(self._release_all)

    def _release_all(self) -> None:
        jobs, self._unreleased = self._unreleased, []
        if not jobs:
            return
        release = asyncio.ensure_future(self._releasing(jobs))
        self._releases.add(release)
        release.add_done_callback(self._released(jobs))

    async def _releasing(self, jobs: list[Job]) -> None:
        with under(self._ending):
            async with bounded(self._ending.at):
                await self._app.backend.release_many(jobs)

    def _released(self, jobs: list[Job]):
        def done(release: asyncio.Future) -> None:
            self._releases.discard(release)
            failure = None if release.cancelled() else release.exception()
            if not release.cancelled() and failure is None:
                return
            logger.warning(
                "could not release %s in time; the reaper takes %s back once %s "
                "lease expires%s",
                _named(jobs),
                "it" if len(jobs) == 1 else "them",
                "its" if len(jobs) == 1 else "their",
                "" if failure is None else f" ({str(failure) or type(failure).__name__})",
            )

        return done

    async def _run_job(self, job: Job) -> None:
        current_runner.set(self)
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._release_unknown(job)
            return
        try:
            attempt = await self._app.execution.execute(
                self._app.bind(job),
                task,
                self._executors,
                held_until=self._answer_by,
                settling=partial(self._settling.__setitem__, (job.id, job.claims), False),
                settled=partial(self._settling.__setitem__, (job.id, job.claims), True),
            )
        except ExecutorClosedError:
            await self._hand_back_unstarted(job)
            return
        except Exception:
            logger.exception(
                "could not record the outcome of job %s; the reaper takes it back "
                "once its lease expires",
                job.id,
            )
            return
        if not attempt.recorded:
            logger.warning(
                "job %s attempt %d is no longer held by worker %s; its outcome "
                "was not recorded",
                job.id,
                job.attempts,
                self._id,
            )
            return
        if attempt.error is None:
            logger.info("job %s succeeded on attempt %d", job.id, job.attempts)
            return
        logger.warning(
            "job %s failed on attempt %d/%d (%s)%s",
            job.id,
            job.attempts,
            job.max_attempts,
            type(attempt.error).__name__,
            "; will retry" if attempt.will_retry else "",
        )

    async def _hand_back_unstarted(self, job: Job) -> None:
        logger.info(
            "the job threads are shut down; worker %s hands job %s back and stops",
            self._id,
            job.id,
        )
        self._request_stop()
        try:
            async with bounded(self._answer_by()):
                await self._app.backend.hand_back(job)
        except Exception:
            logger.warning(
                "could not hand back job %s; the reaper takes it back once its "
                "lease expires",
                job.id,
                exc_info=True,
            )

    async def _release_unknown(self, job: Job) -> None:
        delay = self._unknown_task_backoff.delay_for(job.hand_backs + 1)
        logger.error(
            "task %r is not registered in this worker; leaving job %s queued "
            "and retrying it in %s",
            job.task,
            job.id,
            format_seconds(delay),
        )
        async with bounded(self._answer_by()):
            await self._app.backend.hand_back(job, delay)

    def _answer_by(self) -> float:
        return time.monotonic() + self._lease_seconds

    @property
    def _beat_seconds(self) -> float:
        return max(self._lease_seconds / 3.0, 1.0)

    async def _heartbeat_loop(self) -> None:
        asked = time.monotonic()
        while True:
            await asyncio.sleep(max(0.0, asked + self._beat_seconds - time.monotonic()))
            asked = time.monotonic()
            beaten = [key for key in self._inflight if key not in self._lost]
            job_ids = list({job_id for job_id, _ in beaten})
            if not job_ids:
                continue
            try:
                async with bounded(asked + self._lease_seconds / 4):
                    renewed = await self._app.backend.heartbeat(
                        job_ids, self._lease_seconds, worker_id=self._id
                    )
            except AppClosedError:
                return
            except (TimeoutError, *self._transient) as error:
                logger.warning(
                    "heartbeat for %d job(s) got no answer in time (%s); the next one "
                    "tries again",
                    len(job_ids),
                    str(error) or type(error).__name__,
                )
                continue
            except Exception:
                logger.exception("heartbeat failed for %d jobs", len(job_ids))
                continue
            if renewed is not None:
                self._forget_lost(beaten, set(renewed))

    @property
    def _transient(self) -> tuple[type[BaseException], ...]:
        return tuple(getattr(self._app.backend, "transient_errors", ()))

    def _forget_lost(self, beaten: list[tuple[str, int]], renewed: set[str]) -> None:
        for key in beaten:
            if key[0] in renewed or key not in self._inflight or key in self._settling:
                continue
            self._lost.add(key)
            logger.warning(
                "worker %s no longer holds job %s: its lease lapsed and another worker may "
                "run it; its outcome will not be recorded here",
                self._id,
                key[0],
            )

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reap_interval)
            try:
                reaped = await self._app.backend.reap_expired()
            except AppClosedError:
                return
            except Exception:
                logger.exception("reaper pass failed")
                continue
            if reaped:
                logger.warning("reclaimed %d job(s) with an expired lease", reaped)

    async def _housekeeping_loop(self) -> None:
        if not self._retention.enabled:
            logger.info("retention disabled, nothing will be pruned")
            return
        if not self._app.backend.capabilities.retention:
            logger.info("this backend prunes on its own, skipping housekeeping")
            return
        logger.info("retention: %s", self._retention.describe())
        while True:
            await asyncio.sleep(self._retention.check_interval)
            try:
                removed = await self._app.backend.purge(
                    self._retention, self._app.clock.now()
                )
            except AppClosedError:
                return
            except Exception:
                logger.exception("retention sweep failed")
                continue
            if removed:
                logger.info("retention: pruned %d job(s)", removed)

    async def _drain(self) -> None:
        await self._wait_for_jobs()
        self._release_all()
        if not self._releases:
            return
        left = self._ending.left()
        _, late = await asyncio.wait(
            tuple(self._releases), timeout=None if left == math.inf else max(0.0, left)
        )
        for release in late:
            release.cancel()
        await asyncio.gather(*late, return_exceptions=True)

    async def _wait_for_jobs(self) -> None:
        if not self._running:
            return
        waited = max(0.0, min(self._admitting.at, self._ending.at) - time.monotonic())
        if waited == math.inf:
            waited = self._shutdown_timeout
        logger.info(
            "waiting up to %s for %d job(s)", format_seconds(waited), len(self._running)
        )
        _, still_running = await asyncio.wait(tuple(self._running), timeout=waited)
        if not still_running:
            return
        stuck = {task for task in still_running if self._executors.thread_of(task)}
        stopped = still_running - stuck
        if stopped:
            logger.warning("releasing %d unfinished job(s) back to the queue", len(stopped))
            for task in stopped:
                task.cancel()
            await asyncio.gather(*stopped, return_exceptions=True)
        if stuck:
            await self._outlived(stuck)

    async def _outlived(self, stuck: set[asyncio.Task]) -> None:
        logger.warning(
            "%d job(s) run in threads that cannot be stopped; worker %s keeps their leases "
            "and records their outcome when they end",
            len(stuck),
            self._id,
        )
        loop = asyncio.get_running_loop()
        window = _release_window(self._configured_shutdown_timeout)
        for task in stuck:
            deadline, thread = self._running.get(task), self._executors.thread_of(task)
            if deadline is None or thread is None:
                continue
            deadline.outer = None
            thread.add_done_callback(partial(_ended_by, loop, deadline, window))
        await asyncio.gather(*stuck, return_exceptions=True)

    async def _wait_for(self, event: asyncio.Event) -> None:
        waiters = [
            asyncio.create_task(event.wait()),
            asyncio.create_task(self._stopping.wait()),
        ]
        done, pending = await asyncio.wait(
            waiters, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

    async def _idle(self, seconds: float) -> None:
        try:
            backend = self._app.backend
        except BackendNotReadyError:
            backend = None
        if not getattr(backend, "wakes_workers", False):
            await self._pause(seconds)
            return
        waiting = asyncio.create_task(
            backend.wait_for_jobs(worker_id=self._id, queues=self._queues, timeout=seconds)
        )
        stopping = asyncio.create_task(self._stopping.wait())
        try:
            await asyncio.wait({waiting, stopping}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (waiting, stopping):
                task.cancel()
            await asyncio.gather(waiting, stopping, return_exceptions=True)
        failure = None if waiting.cancelled() else waiting.exception()
        if failure is not None and not isinstance(failure, AppClosedError):
            logger.warning("waiting for new jobs failed: %s", failure)
            await self._pause(seconds)

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    def _request_stop(
        self, stopping: asyncio.Event | None = None, ending: Deadline | None = None
    ) -> None:
        logger.info("shutdown requested; finishing in-flight jobs")
        now, timeout = time.monotonic(), self._shutdown_timeout
        if ending is None or ending is self._ending:
            self._admitting.end(now + timeout - _release_window(timeout))
        (self._ending if ending is None else ending).end(now + timeout)
        (self._stopping if stopping is None else stopping).set()

    def _request_force(self) -> None:
        logger.warning("second signal received; exiting now")
        self._forced.set()
        self._shutdown_timeout = 0.0
        now = time.monotonic()
        self._admitting.end(now)
        self._ending.end(now + RELEASE_SECONDS)
        for task in tuple(self._running):
            if not self._executors.thread_of(task):
                task.cancel()


def _release_window(timeout: float) -> float:
    return min(timeout, max(RELEASE_SECONDS, timeout / RELEASE_SHARE))


def _ended_by(
    loop: asyncio.AbstractEventLoop, deadline: Deadline, window: float, _: object
) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(lambda: deadline.end(time.monotonic() + window))


def _named(jobs: Sequence[Job]) -> str:
    if len(jobs) == 1:
        return f"job {jobs[0].id}"
    return f"{len(jobs)} jobs ({', '.join(job.id for job in jobs)})"
