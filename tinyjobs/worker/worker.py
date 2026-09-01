from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Sequence

from ..exceptions import UnknownTaskError
from ..ids import new_worker_id
from ..models import Job
from ..executors import ExecutorResolver
from .polling import AdaptiveInterval
from .signals import ShutdownWatcher

if TYPE_CHECKING:
    from ..app import TinyJobs

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
        poll_minimum: float = 0.005,
        poll_maximum: float = 0.5,
        worker_id: str | None = None,
    ) -> None:
        self._app = app
        self._queues = tuple(queues)
        self._concurrency = concurrency
        self._lease_seconds = lease_seconds
        self._reap_interval = reap_interval
        self._shutdown_timeout = shutdown_timeout
        self._unknown_task_delay = unknown_task_delay
        self._interval = AdaptiveInterval(minimum=poll_minimum, maximum=poll_maximum)
        self._executors = ExecutorResolver(
            concurrency=concurrency, async_concurrency=async_concurrency
        )
        self._id = worker_id or new_worker_id()
        self._inflight: dict[str, Job] = {}
        self._running: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()
        self._forced = asyncio.Event()
        self._capacity = asyncio.Event()
        self._capacity.set()

    @property
    def id(self) -> str:
        return self._id

    def run(self) -> None:
        asyncio.run(self.start())

    def stop(self) -> None:
        self._request_stop()

    async def start(self) -> None:
        watcher = ShutdownWatcher(self._request_stop, self._request_force)
        watcher.install()
        logger.info(
            "worker %s listening on %s with concurrency %d",
            self._id,
            ",".join(self._queues),
            self._concurrency,
        )
        background = [
            asyncio.create_task(self._heartbeat_loop(), name="tinyjobs-heartbeat"),
            asyncio.create_task(self._reaper_loop(), name="tinyjobs-reaper"),
        ]
        try:
            await self._claim_loop()
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._drain()
            watcher.uninstall()
            self._executors.shutdown()
            logger.info("worker %s stopped", self._id)

    async def _claim_loop(self) -> None:
        while not self._stopping.is_set():
            capacity = self._concurrency - len(self._inflight)
            if capacity <= 0:
                self._capacity.clear()
                await self._wait_for(self._capacity)
                continue
            jobs = await self._claim(capacity)
            if not jobs:
                await self._pause(self._interval.next())
                continue
            self._interval.reset()
            for job in jobs:
                self._launch(job)

    async def _claim(self, capacity: int) -> list[Job]:
        try:
            return await asyncio.to_thread(
                self._app.backend.claim,
                worker_id=self._id,
                queues=self._queues,
                limit=capacity,
                lease_seconds=self._lease_seconds,
            )
        except Exception:
            logger.exception("claim failed; backing off")
            await self._pause(self._interval.next())
            return []

    def _launch(self, job: Job) -> None:
        self._inflight[job.id] = job
        task = asyncio.create_task(self._run_job(job), name=f"tinyjobs-{job.id}")
        self._running.add(task)
        task.add_done_callback(self._finalize(job.id))

    def _finalize(self, job_id: str):
        def done(task: asyncio.Task) -> None:
            self._running.discard(task)
            self._inflight.pop(job_id, None)
            self._capacity.set()
            if task.cancelled():
                return
            error = task.exception()
            if error is not None:
                logger.exception(
                    "job %s crashed the worker loop", job_id, exc_info=error
                )

        return done

    async def _run_job(self, job: Job) -> None:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._release_unknown(job)
            return
        try:
            payload = self._app.serialization.decode_payload(job)
            result = await self._executors.resolve(task).execute(task, payload)
            await asyncio.to_thread(self._app.lifecycle.succeed, job, task, result)
        except asyncio.CancelledError:
            await asyncio.shield(
                asyncio.to_thread(self._app.backend.release, job)
            )
            raise
        except BaseException as error:
            await self._record_failure(job, task, error)
            return
        logger.info("job %s succeeded on attempt %d", job.id, job.attempts)

    async def _record_failure(self, job: Job, task, error: BaseException) -> None:
        _, will_retry = await asyncio.to_thread(
            self._app.lifecycle.fail, job, task, error
        )
        logger.warning(
            "job %s failed on attempt %d/%d (%s)%s",
            job.id,
            job.attempts,
            job.max_attempts,
            type(error).__name__,
            "; will retry" if will_retry else "",
        )

    async def _release_unknown(self, job: Job) -> None:
        logger.error(
            "task %r is not registered in this worker; leaving job %s queued "
            "and retrying it in %.0fs",
            job.task,
            job.id,
            self._unknown_task_delay,
        )
        await asyncio.to_thread(
            self._app.backend.release, job, self._unknown_task_delay
        )

    async def _heartbeat_loop(self) -> None:
        interval = max(self._lease_seconds / 3.0, 1.0)
        while True:
            await asyncio.sleep(interval)
            job_ids = list(self._inflight)
            if not job_ids:
                continue
            try:
                await asyncio.to_thread(
                    self._app.backend.heartbeat, job_ids, self._lease_seconds
                )
            except Exception:
                logger.exception("heartbeat failed for %d jobs", len(job_ids))

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reap_interval)
            try:
                reaped = await asyncio.to_thread(self._app.backend.reap_expired)
            except Exception:
                logger.exception("reaper pass failed")
                continue
            if reaped:
                logger.warning("reclaimed %d job(s) with an expired lease", reaped)

    async def _drain(self) -> None:
        if not self._running:
            return
        logger.info("waiting up to %.0fs for %d job(s)", self._shutdown_timeout, len(self._running))
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

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    def _request_stop(self) -> None:
        logger.info("shutdown requested; finishing in-flight jobs")
        self._stopping.set()

    def _request_force(self) -> None:
        logger.warning("second signal received; exiting now")
        self._forced.set()
        self._shutdown_timeout = 0.0
        for task in tuple(self._running):
            task.cancel()
