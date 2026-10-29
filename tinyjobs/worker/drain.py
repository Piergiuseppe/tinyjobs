from __future__ import annotations

import asyncio
import logging
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Sequence

from ..exceptions import ExecutorClosedError, UnknownTaskError
from ..executors import ExecutorResolver
from ..models import Job
from ..runtime import inside_running_loop
from ..services.execution import current_runner
from ..sheltered import sheltered

if TYPE_CHECKING:
    from ..app import TinyJobs
    from ..task import Task

DRAIN_WORKER_ID = "drain"
DRAIN_LEASE_SECONDS = 3600.0
DRAIN_SHUTDOWN_SECONDS = 30.0

logger = logging.getLogger("tinyjobs.worker")


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
        self._executors = ExecutorResolver(concurrency=1, replacing=True)
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
        await self._app.ensure_connected()
        self._loop = asyncio.get_running_loop()
        self._finished.clear()
        self._app.attach(self)
        try:
            return await self._drain()
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
            while not self._stopping.is_set() and (job := await self._claim(queues)):
                drained += await self._execute(job)
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

    async def _known_queues(self) -> tuple[str, ...]:
        stats = await self._app.astats()
        return tuple(queue.queue for queue in stats.queues)

    async def _execute(self, job: Job) -> bool:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._app.backend.hand_back(job)
            raise
        running = asyncio.ensure_future(self._attempt(job, task))
        try:
            return await self._settle(running, job)
        except ExecutorClosedError:
            await self._app.backend.hand_back(job)
            raise
        except asyncio.CancelledError:
            await self._abandon(running, job)
            raise

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
        if self._executors.keeps(running) is not None:
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
        await self._app.execution.execute(self._app.bind(job), task, self._executors)
