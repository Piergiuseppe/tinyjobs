from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Sequence

from ..exceptions import ExecutorClosedError, UnknownTaskError
from ..executors import ExecutorResolver
from ..executors.thread import Spare, holds_spare
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
DRAIN_SPARE_THREADS = 8

logger = logging.getLogger("tinyjobs.worker")


class Drain:
    def __init__(
        self,
        app: "TinyJobs",
        queues: Sequence[str] | None = None,
        *,
        shutdown_timeout: float = DRAIN_SHUTDOWN_SECONDS,
        quiet: bool = False,
    ) -> None:
        self._app = app
        self._queues = tuple(queues) if queues else None
        self._shutdown_timeout = shutdown_timeout
        self._quiet = quiet
        self._finished = threading.Event()
        self._prepare_run()

    def _prepare_run(self) -> None:
        self._executors = ExecutorResolver(concurrency=1)
        self._stopping = asyncio.Event()
        self._warned = self._quiet
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
            while not self._stopping.is_set() and (job := await self._next(queues)):
                drained += await self._execute(job)
            ran += drained
            if not drained or self._queues:
                return ran
        return ran

    async def _next(self, queues: Sequence[str]) -> Job | None:
        spares = self._app.thread_spares
        exhausted = spares.exhausted
        if exhausted and not self._warned:
            self._warned = True
            logger.warning(
                "%d job thread(s) still run past their timeout, so the drain leaves the jobs "
                "of sync tasks with a timeout queued until one returns",
                spares.limit,
            )
        if not exhausted:
            return await self._claim(queues)
        if not getattr(self._app.backend, "claims_by_task", False):
            return None
        unthreaded = [task.name for task in self._app.registry if not _threaded(task)]
        return await self._claim(queues, tasks=unthreaded)

    async def _claim(
        self, queues: Sequence[str], tasks: Sequence[str] | None = None
    ) -> Job | None:
        jobs = await self._app.backend.claim(
            worker_id=DRAIN_WORKER_ID,
            queues=queues,
            limit=1,
            lease_seconds=DRAIN_LEASE_SECONDS,
            **({} if tasks is None else {"tasks": tasks}),
        )
        return jobs[0] if jobs else None

    async def _known_queues(self) -> tuple[str, ...]:
        stats = await self._app.astats()
        return tuple(queue.queue for queue in stats.queues)

    async def _execute(self, job: Job) -> bool:
        draining = self._app.in_drains.enter(job.id)
        try:
            return await self._run(job)
        finally:
            self._app.in_drains.leave(job.id, draining)

    async def _run(self, job: Job) -> bool:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._app.backend.hand_back(job)
            raise
        spare = None
        if _threaded(task) and not holds_spare():
            try:
                spare = await self._reserved()
            except asyncio.CancelledError:
                await sheltered(self._app.backend.hand_back(job, in_place=True))
                raise
            if spare is None:
                await sheltered(self._app.backend.hand_back(job, in_place=True))
                return False
        running = asyncio.ensure_future(self._attempt(job, task, spare))
        try:
            return await self._settle(running, job, spare)
        except ExecutorClosedError:
            await self._app.backend.hand_back(job)
            raise
        except asyncio.CancelledError:
            await self._abandon(running, job, spare)
            raise

    async def _reserved(self) -> Spare | None:
        reserving = asyncio.ensure_future(self._app.thread_spares.reserve())
        stopping = asyncio.ensure_future(self._stopping.wait())
        try:
            await asyncio.wait((reserving, stopping), return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            reserving.cancel()
            reserving.add_done_callback(_unused)
            raise
        finally:
            stopping.cancel()
        if not reserving.done():
            reserving.cancel()
            await asyncio.gather(reserving, return_exceptions=True)
        spare = None if reserving.cancelled() else reserving.result()
        if spare is not None and self._stopping.is_set():
            spare.end()
            return None
        return spare

    async def _settle(self, running: asyncio.Future, job: Job, spare: Spare | None) -> bool:
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
        await self._abandon(running, job, spare)
        return False

    async def _abandon(self, running: asyncio.Future, job: Job, spare: Spare | None) -> None:
        kept = self._executors.keeps(running)
        if kept is not None:
            if spare is not None:
                spare.keep(kept)
            logger.warning(
                "job %s still runs in a thread that cannot be stopped; it is not released, and "
                "its outcome is recorded if it returns while the loop runs, else its lease ends",
                job.id,
            )
            return
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await sheltered(self._app.backend.release(job))

    async def _attempt(self, job: Job, task: "Task", spare: Spare | None) -> None:
        current_runner.set(self)
        owner = asyncio.current_task()
        self._executors.lend(owner, spare)
        try:
            await self._app.execution.execute(self._app.bind(job), task, self._executors)
        finally:
            self._executors.lend(owner, None)
            if spare is not None:
                spare.end()


def _threaded(task: "Task") -> bool:
    return not task.is_coroutine and task.timeout is not None


def _unused(reserving: asyncio.Future) -> None:
    if not reserving.cancelled() and reserving.exception() is None and reserving.result():
        reserving.result().end()


class InDrains:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, concurrent.futures.Future] = {}

    def enter(self, job_id: str) -> concurrent.futures.Future:
        draining: concurrent.futures.Future = concurrent.futures.Future()
        with self._lock:
            self._jobs[job_id] = draining
        return draining

    def leave(self, job_id: str, draining: concurrent.futures.Future) -> None:
        with self._lock:
            if self._jobs.get(job_id) is draining:
                del self._jobs[job_id]
        draining.set_result(None)

    def running(self, job_id: str) -> concurrent.futures.Future | None:
        with self._lock:
            return self._jobs.get(job_id)
