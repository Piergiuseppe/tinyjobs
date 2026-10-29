from __future__ import annotations

import asyncio
import contextvars
import logging
import math
import threading
import time
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Any, Collection, Sequence

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
from ..deadlines import GRACE_SECONDS, Deadline, bounded, current, final, under
from ..duration import format_seconds
from ..executors import ExecutorResolver
from ..polling import AdaptiveInterval
from ..services.execution import current_runner
from ..services.lifecycle import VERIFY_SECONDS
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
        self._abandoned: set[tuple[str, int]] = set()
        self._misnamed = False
        self._running: dict[asyncio.Task, _Run] = {}
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
        self._writing = Deadline()
        self._admitting = Deadline()
        self._beating = Deadline()
        self._sparing: asyncio.Task | None = None

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
        expected = self._expect(self._concurrency)
        try:
            await self._serve()
        finally:
            expected()
            closing = self._app.detach(self)
            self._prepare_run()
            self._finished.set()
            if closing:
                await self._app.aclose()

    def _expect(self, count: int):
        expect = getattr(self._app.backend, "expect", None)
        if expect is None:
            return lambda: None
        expect(count)
        return partial(expect, -count)

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
                if self._sparing is not None:
                    self._sparing.cancel()
                    await asyncio.gather(self._sparing, return_exceptions=True)
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
        except (TimeoutError, *self._transient) as error:
            logger.warning(
                "claim got no answer in time (%s); backing off", str(error) or type(error).__name__
            )
            await self._pause(self._interval.next())
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
        key = (job.id, job.claims)
        self._inflight[key] = job
        run = _Run(key, Deadline(math.inf, self._writing), Deadline(math.inf, self._ending))
        context = contextvars.copy_context()
        context.run(current.set, run.deadline)
        context.run(final.set, run.last)
        task = asyncio.create_task(
            self._run_job(job), name=f"tinyjobs-{job.id}", context=context
        )
        self._running[task] = run
        task.add_done_callback(self._finalize(job))

    def _finalize(self, job: Job):
        def done(task: asyncio.Task) -> None:
            key = (job.id, job.claims)
            self._running.pop(task, None)
            self._inflight.pop(key, None)
            self._lost.discard(key)
            self._capacity.set()
            settled = self._settling.pop(key, None)
            if key in self._abandoned:
                self._abandoned.discard(key)
                return
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
        asked = renewed = time.monotonic()
        while True:
            await asyncio.sleep(max(0.0, asked + self._beat_seconds - time.monotonic()))
            asked = time.monotonic()
            releasing = self._admitting.left() <= 0
            kept = {run.key for run in self._running.values() if run.kept}
            beaten = [
                key
                for key in self._inflight
                if key not in self._lost and (not releasing or key in kept)
            ]
            job_ids = list({job_id for job_id, _ in beaten})
            if not job_ids:
                renewed = asked
                continue
            try:
                self._forget_lost(beaten, await self._beat(job_ids, asked, renewed))
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
            renewed = asked

    async def _beat(self, job_ids: list[str], asked: float, renewed: float) -> Any:
        hedge = asked + self._lease_seconds / 4
        async with bounded(max(hedge, renewed + self._lease_seconds)):
            attempts = {asyncio.ensure_future(self._renew(job_ids))}
            try:
                hedged, failure = False, None
                while attempts:
                    done, attempts = await asyncio.wait(
                        attempts,
                        timeout=None if hedged else max(0.0, hedge - time.monotonic()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for attempt in done:
                        if attempt.exception() is None:
                            return attempt.result()
                        failure = attempt.exception()
                    if not hedged and not done:
                        hedged = True
                        attempts.add(asyncio.ensure_future(self._renew(job_ids)))
                raise failure or TimeoutError()
            finally:
                for attempt in attempts:
                    attempt.cancel()
                await asyncio.gather(*attempts, return_exceptions=True)

    async def _renew(self, job_ids: list[str]) -> Any:
        return await self._app.backend.heartbeat(
            job_ids, self._lease_seconds, worker_id=self._id
        )

    @property
    def _transient(self) -> tuple[type[BaseException], ...]:
        return tuple(getattr(self._app.backend, "transient_errors", ()))

    def _forget_lost(self, beaten: list[tuple[str, int]], answer: Any) -> None:
        if answer is None:
            return
        if (
            isinstance(answer, (str, bytes))
            or not isinstance(answer, Collection)
            or not all(isinstance(job_id, str) for job_id in answer)
        ):
            if not self._misnamed:
                self._misnamed = True
                logger.warning(
                    "the backend's heartbeat returned %s rather than the ids of the leases it "
                    "renewed, so worker %s cannot tell when it loses a lease",
                    type(answer).__name__,
                    self._id,
                )
            return
        renewed = set(answer)
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
        if self._ending.at == math.inf:
            self._request_stop()
        logger.info(
            "waiting up to %s for %d job(s)",
            format_seconds(max(0.0, self._admitting.left())),
            len(self._running),
        )
        await self._until(self._running, self._admitting)
        stuck: dict[asyncio.Task, Any] = {}
        settling: set[asyncio.Task] = set()
        stopped: set[asyncio.Task] = set()
        for task, run in tuple(self._running.items()):
            thread = self._executors.keeps(task)
            if thread is not None:
                stuck[task] = thread
                self._keep(task, thread)
            elif self._settling.get(run.key) is False:
                settling.add(task)
            else:
                stopped.add(task)
        if stopped:
            logger.warning("releasing %d unfinished job(s) back to the queue", len(stopped))
            for task in stopped:
                task.cancel()
        if stopped or settling:
            await self._until(stopped | settling, self._ending)
            await self._until(settling, self._ending, GRACE_SECONDS)
            self._leave_behind(stopped | settling)
        if stuck:
            await self._outlived(stuck)

    async def _until(
        self, tasks: Collection[asyncio.Task], deadline: Deadline, grace: float = 0.0
    ) -> None:
        forced = asyncio.ensure_future(self._forced.wait())
        try:
            while pending := {task for task in tuple(tasks) if not task.done()}:
                left = deadline.left() + grace
                if left <= 0:
                    return
                await asyncio.wait(
                    pending if forced.done() else {*pending, forced},
                    timeout=None if left == math.inf else left,
                    return_when=asyncio.FIRST_COMPLETED,
                )
        finally:
            forced.cancel()
            await asyncio.gather(forced, return_exceptions=True)

    def _leave_behind(self, tasks: set[asyncio.Task]) -> None:
        behind = [task for task in tasks if not task.done()]
        if not behind:
            return
        for task in behind:
            run = self._running.get(task)
            if run is not None:
                self._abandoned.add(run.key)
            task.cancel()
        logger.warning(
            "%d job(s) did not end within the shutdown timeout although cancelled; worker %s "
            "leaves them behind and the reaper takes them back once their lease expires: %s",
            len(behind),
            self._id,
            ", ".join(sorted(task.get_name() for task in behind)),
        )

    async def _outlived(self, stuck: dict[asyncio.Task, Any]) -> None:
        logger.warning(
            "%d job(s) run in threads that cannot be stopped; worker %s keeps their leases "
            "and records their outcome when they end",
            len(stuck),
            self._id,
        )
        await asyncio.gather(*stuck, return_exceptions=True)

    def _keep(self, task: asyncio.Task, thread: Any) -> None:
        run = self._running.get(task)
        if run is None or run.kept:
            return
        run.kept = True
        run.deadline.outer = None
        run.last.outer = None
        window = _release_window(self._configured_shutdown_timeout)
        thread.add_done_callback(partial(_ended_by, asyncio.get_running_loop(), run, window))

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
            window = _release_window(timeout)
            self._admitting.end(now + timeout - window)
            self._writing.end(now + timeout - min(VERIFY_SECONDS, window / 2))
        (self._ending if ending is None else ending).end(now + timeout)
        (self._stopping if stopping is None else stopping).set()
        if (ending is None or ending is self._ending) and self._sparing is None:
            self._spare()

    def _spare(self) -> None:
        try:
            backend = self._app.backend
            asyncio.get_running_loop()
        except (BackendNotReadyError, RuntimeError):
            return
        spare = getattr(backend, "spare", None)
        if spare is None:
            return
        context = contextvars.copy_context()
        context.run(current.set, self._ending)
        self._sparing = asyncio.create_task(
            self._spared(spare), name="tinyjobs-spare", context=context
        )

    async def _spared(self, spare: Any) -> None:
        try:
            async with bounded(self._admitting.at):
                await spare()
        except Exception as error:
            logger.debug("could not open a spare connection for the releases: %s", error)

    def _request_force(self) -> None:
        logger.warning("second signal received; exiting now")
        self._forced.set()
        self._shutdown_timeout = 0.0
        now = time.monotonic()
        self._admitting.end(now)
        self._writing.end(now + RELEASE_SECONDS / 2)
        self._ending.end(now + RELEASE_SECONDS)
        for task, run in tuple(self._running.items()):
            if self._settling.get(run.key) is False:
                continue
            thread = self._executors.keeps(task)
            if thread is not None:
                self._keep(task, thread)
                continue
            task.cancel()


def _release_window(timeout: float) -> float:
    return min(timeout, max(RELEASE_SECONDS, timeout / RELEASE_SHARE))


class _Run:
    __slots__ = ("key", "deadline", "last", "kept")

    def __init__(self, key: tuple[str, int], deadline: Deadline, last: Deadline) -> None:
        self.key = key
        self.deadline = deadline
        self.last = last
        self.kept = False

    def close(self, window: float) -> None:
        now = time.monotonic()
        self.deadline.end(now + window)
        self.last.end(now + window + VERIFY_SECONDS)


def _ended_by(loop: asyncio.AbstractEventLoop, run: _Run, window: float, _: object) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(run.close, window)


def _named(jobs: Sequence[Job]) -> str:
    if len(jobs) == 1:
        return f"job {jobs[0].id}"
    return f"{len(jobs)} jobs ({', '.join(job.id for job in jobs)})"
