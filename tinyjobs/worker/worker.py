from __future__ import annotations

import asyncio
import logging
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
from ..duration import format_seconds
from ..executors import ExecutorResolver
from ..polling import AdaptiveInterval
from ..services.execution import current_runner
from .signals import ShutdownWatcher

if TYPE_CHECKING:
    from ..app import TinyJobs

LEASE_MARGIN_SECONDS = 1.0

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
        self._held_until: dict[tuple[str, int], float] = {}
        self._settling: set[tuple[str, int]] = set()
        self._running: set[asyncio.Task] = set()
        self._releases: set[asyncio.Future] = set()
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
        stopping, loop = self._stopping, self._loop
        if loop is None or (
            inside_running_loop() and loop is asyncio.get_running_loop()
        ):
            self._request_stop(stopping)
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(self._request_stop, stopping)

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
        try:
            if self._handle_signals:
                watcher.install()
            logger.info(
                "worker %s listening on %s with concurrency %d",
                self._id,
                ",".join(self._queues),
                self._concurrency,
            )
            heartbeat.append(
                asyncio.create_task(self._heartbeat_loop(), name="tinyjobs-heartbeat")
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
            asked = time.monotonic()
            jobs = await self._claim(capacity)
            if not jobs:
                await self._idle(self._interval.next())
                continue
            self._interval.reset()
            for job in jobs:
                self._held_until[job.id, job.claims] = asked + self._lease_seconds
                self._launch(job)

    async def _claim(self, capacity: int) -> list[Job]:
        try:
            return await self._app.backend.claim(
                worker_id=self._id,
                queues=self._queues,
                limit=capacity,
                lease_seconds=self._lease_seconds,
            )
        except AppClosedError:
            logger.info("the app was closed; worker %s is stopping", self._id)
            self._request_stop()
            return []
        except Exception:
            logger.exception("claim failed; backing off")
            await self._pause(self._interval.next())
            return []

    def _launch(self, job: Job) -> None:
        self._inflight[job.id, job.claims] = job
        task = asyncio.create_task(self._run_job(job), name=f"tinyjobs-{job.id}")
        self._running.add(task)
        task.add_done_callback(self._finalize(job))

    def _finalize(self, job: Job):
        def done(task: asyncio.Task) -> None:
            self._running.discard(task)
            self._inflight.pop((job.id, job.claims), None)
            self._held_until.pop((job.id, job.claims), None)
            self._capacity.set()
            settled = (job.id, job.claims) in self._settling
            self._settling.discard((job.id, job.claims))
            if task.cancelled() and settled:
                logger.warning(
                    "job %s ended before its outcome was recorded; the reaper takes "
                    "it back once its lease expires",
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
        release = asyncio.ensure_future(self._app.backend.release(job))
        self._releases.add(release)
        release.add_done_callback(self._released(job))

    def _released(self, job: Job):
        def done(release: asyncio.Future) -> None:
            self._releases.discard(release)
            if not release.cancelled() and release.exception() is not None:
                logger.warning(
                    "could not release job %s; the reaper takes it back once its "
                    "lease expires",
                    job.id,
                    exc_info=release.exception(),
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
                held_until=lambda: self._held_until.get((job.id, job.claims), 0.0)
                - LEASE_MARGIN_SECONDS,
                settling=partial(self._settling.add, (job.id, job.claims)),
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
        await self._app.backend.hand_back(job, delay)

    @property
    def _beat_seconds(self) -> float:
        return max(self._lease_seconds / 3.0, 1.0)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._beat_seconds)
            beaten = list(self._inflight)
            job_ids = list({job_id for job_id, _ in beaten})
            if not job_ids:
                continue
            asked = time.monotonic()
            try:
                await self._app.backend.heartbeat(
                    job_ids, self._lease_seconds, worker_id=self._id
                )
                for key in beaten:
                    if key in self._held_until:
                        self._held_until[key] = asked + self._lease_seconds
            except AppClosedError:
                return
            except Exception:
                logger.exception("heartbeat failed for %d jobs", len(job_ids))

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
        await asyncio.gather(*self._releases, return_exceptions=True)

    async def _wait_for_jobs(self) -> None:
        if not self._running:
            return
        logger.info(
            "waiting up to %s for %d job(s)",
            format_seconds(self._shutdown_timeout),
            len(self._running),
        )
        pending = tuple(self._running)
        _, still_running = await asyncio.wait(
            pending, timeout=self._shutdown_timeout
        )
        if not still_running:
            return
        logger.warning("releasing %d unfinished job(s) back to the queue", len(still_running))
        for task in still_running:
            task.cancel()
        await asyncio.gather(*still_running, return_exceptions=True)

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

    def _request_stop(self, stopping: asyncio.Event | None = None) -> None:
        logger.info("shutdown requested; finishing in-flight jobs")
        (self._stopping if stopping is None else stopping).set()

    def _request_force(self) -> None:
        logger.warning("second signal received; exiting now")
        self._forced.set()
        self._shutdown_timeout = 0.0
        for task in tuple(self._running):
            task.cancel()
