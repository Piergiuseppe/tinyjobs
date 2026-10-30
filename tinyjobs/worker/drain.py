from __future__ import annotations

import asyncio
import logging
import threading
import time
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Awaitable, Callable, Sequence, TypeVar

from ..exceptions import AppClosedError, ExecutorClosedError, UnknownTaskError
from ..executors import ExecutorResolver
from ..models import Job
from ..runtime import CLOSED_MESSAGE, inside_running_loop
from ..services.execution import current_runner
from ..sheltered import sheltered

if TYPE_CHECKING:
    from ..app import TinyJobs
    from ..task import Task

DRAIN_WORKER_ID = "drain"
DRAIN_LEASE_SECONDS = 60.0
DRAIN_RENEW_SECONDS = 20.0
DRAIN_SHUTDOWN_SECONDS = 30.0

logger = logging.getLogger("tinyjobs.worker")

T = TypeVar("T")


class Drain:
    def __init__(
        self,
        app: "TinyJobs",
        queues: Sequence[str] | None = None,
        *,
        shutdown_timeout: float = DRAIN_SHUTDOWN_SECONDS,
    ) -> None:
        self._app = app
        self._queues = tuple(queues) if queues else None
        self._shutdown_timeout = shutdown_timeout
        self._finished = threading.Event()
        self._prepare_run()

    def _prepare_run(self) -> None:
        self._executors = ExecutorResolver(concurrency=1, widens=True)
        self._stopping = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def id(self) -> str:
        return DRAIN_WORKER_ID

    @property
    def shutdown_timeout(self) -> float:
        return self._shutdown_timeout

    @property
    def finished(self) -> bool:
        return self._finished.is_set()

    def stop(self) -> None:
        stopping, loop = self._stopping, self._loop
        if loop is None or (
            inside_running_loop() and loop is asyncio.get_running_loop()
        ):
            stopping.set()
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(stopping.set)

    async def run(self) -> int:
        return await self._attached(self._drain)

    async def run_job(self, job_id: str) -> bool:
        return await self._attached(partial(self._run_job, job_id))

    async def _attached(self, running: Callable[[], Awaitable[T]]) -> T:
        await self._app.ensure_connected()
        self._app.attach(self)
        self._loop = asyncio.get_running_loop()
        self._finished.clear()
        try:
            return await running()
        finally:
            closing = self._app.detach(self)
            self._executors.shutdown()
            self._prepare_run()
            self._finished.set()
            if closing:
                await self._app.aclose()

    async def _drain(self) -> int:
        ran = 0
        while not self._stopping.is_set():
            queues = self._queues or await self._known_queues()
            drained = 0
            while not self._stopping.is_set() and (claimed := await self._claim(queues)):
                drained += await self._run(claimed)
            ran += drained
            if not drained or self._queues:
                return ran
        return ran

    async def _claim(self, queues: Sequence[str]) -> Job | None:
        jobs = await self._app.backend.claim(
            worker_id=DRAIN_WORKER_ID,
            queues=queues,
            limit=1,
            lease_seconds=DRAIN_LEASE_SECONDS,
        )
        return jobs[0] if jobs else None

    async def _run_job(self, job_id: str) -> bool:
        job = await self._app.backend.claim_job(
            job_id, worker_id=DRAIN_WORKER_ID, lease_seconds=DRAIN_LEASE_SECONDS
        )
        if job is None:
            return False
        if await self._run(job):
            return True
        raise AppClosedError(CLOSED_MESSAGE)

    async def _known_queues(self) -> tuple[str, ...]:
        stats = await self._app.astats()
        return tuple(queue.queue for queue in stats.queues)

    async def _run(self, job: Job) -> bool:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._app.backend.hand_back(job)
            raise
        running = asyncio.ensure_future(self._attempt(job, task))
        renewing = _Renewal(self._app, job)
        try:
            return await self._settle(running, job)
        except ExecutorClosedError:
            await self._app.backend.hand_back(job)
            raise
        except asyncio.CancelledError:
            await self._abandon(running, job)
            raise
        finally:
            if running.done():
                renewing.stop()
            else:
                running.add_done_callback(renewing.stop)

    async def _settle(self, running: asyncio.Future, job: Job) -> bool:
        stopping = asyncio.ensure_future(self._stopping.wait())
        try:
            await asyncio.wait(
                (running, stopping), return_when=asyncio.FIRST_COMPLETED
            )
            if not running.done():
                await asyncio.wait((running,), timeout=self._shutdown_timeout)
            if not running.done() and self._executors.keeps(running) is not None:
                logger.warning(
                    "job %s runs in a thread that cannot be stopped; the drain waits for it "
                    "and records its outcome",
                    job.id,
                )
                await asyncio.wait((running,))
        finally:
            stopping.cancel()
        if running.done():
            running.result()
            return True
        await self._abandon(running, job)
        return False

    async def _abandon(self, running: asyncio.Future, job: Job) -> None:
        kept = self._executors.keeps(running)
        if kept is not None:
            logger.warning(
                "job %s still runs in a thread that cannot be stopped; it is not released, and "
                "its outcome is recorded if it returns while the loop runs, else its lease ends",
                job.id,
            )
            return
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await sheltered(self._app.backend.release(job))

    async def _attempt(self, job: Job, task: "Task") -> None:
        current_runner.set(self)
        await self._app.execution.execute(
            self._app.bind(job), task, self._executors, held_until=_lease_end
        )


class _Renewal:
    def __init__(self, app: "TinyJobs", job: Job) -> None:
        self._app = app
        self._job = job
        self._loop = asyncio.get_running_loop()
        self._beating: asyncio.Task | None = None
        self._stopped = False
        self._handle = self._loop.call_later(DRAIN_RENEW_SECONDS, self._due)

    def stop(self, _: object = None) -> None:
        self._stopped = True
        self._handle.cancel()
        if self._beating is not None:
            self._beating.cancel()

    def _due(self) -> None:
        self._beating = self._loop.create_task(self._beat())

    async def _beat(self) -> None:
        try:
            await self._app.backend.heartbeat(
                [self._job.id], DRAIN_LEASE_SECONDS, worker_id=DRAIN_WORKER_ID
            )
        except Exception as error:
            logger.warning(
                "could not renew the lease of job %s, trying again in %.0f seconds: %s",
                self._job.id,
                DRAIN_RENEW_SECONDS,
                error,
            )
        finally:
            self._beating = None
            if not self._stopped:
                self._handle = self._loop.call_later(DRAIN_RENEW_SECONDS, self._due)


def _lease_end() -> float:
    return time.monotonic() + DRAIN_LEASE_SECONDS
