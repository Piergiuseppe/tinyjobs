from __future__ import annotations

import asyncio
import logging
import threading
import time
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Awaitable, Callable, Sequence, TypeVar

from ..exceptions import ExecutorClosedError, UnknownTaskError
from ..executors import ExecutorResolver
from ..executors.thread import Spare, held_spares, wake
from ..models import Job
from ..runtime import inside_running_loop
from ..services.execution import current_runner
from ..sheltered import sheltered

if TYPE_CHECKING:
    from ..app import TinyJobs
    from ..task import Task

DRAIN_WORKER_ID = "drain"
DRAIN_LEASE_SECONDS = 60.0
DRAIN_RENEW_SECONDS = 20.0
DRAIN_SHUTDOWN_SECONDS = 30.0
DRAIN_SPARE_THREADS = 8

logger = logging.getLogger("tinyjobs.worker")

T = TypeVar("T")

Waiter = tuple[asyncio.AbstractEventLoop, asyncio.Future]
Waiters = list[Waiter]


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
        self._kept: Spare | None = None
        self._executors = ExecutorResolver(concurrency=1)
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
            self._drop_kept()
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
                drained += await self._execute(*claimed)
            ran += drained
            if not drained or self._queues:
                return ran
        return ran

    async def _claim(self, queues: Sequence[str]) -> tuple[Job, Waiters] | None:
        return await self._claimed(
            partial(
                self._app.backend.claim,
                worker_id=DRAIN_WORKER_ID,
                queues=queues,
                limit=1,
                lease_seconds=DRAIN_LEASE_SECONDS,
            )
        )

    async def _run_job(self, job_id: str) -> bool:
        claimed = await self._claimed(partial(self._claimed_by_id, job_id))
        if claimed is None:
            return False
        return await self._execute(*claimed)

    async def _claimed_by_id(self, job_id: str) -> list[Job]:
        job = await self._app.backend.claim_job(
            job_id, worker_id=DRAIN_WORKER_ID, lease_seconds=DRAIN_LEASE_SECONDS
        )
        return [] if job is None else [job]

    async def _claimed(
        self, claiming: Callable[[], Awaitable[list[Job]]]
    ) -> tuple[Job, Waiters] | None:
        in_drains = self._app.in_drains
        claim = in_drains.begin()
        try:
            jobs = await claiming()
            return (jobs[0], in_drains.enter(jobs[0].id)) if jobs else None
        finally:
            in_drains.end(claim)

    async def _known_queues(self) -> tuple[str, ...]:
        stats = await self._app.astats()
        return tuple(queue.queue for queue in stats.queues)

    async def _execute(self, job: Job, waiting: Waiters) -> bool:
        try:
            return await self._run(job)
        finally:
            self._app.in_drains.leave(job.id, waiting)

    async def _run(self, job: Job) -> bool:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._app.backend.hand_back(job)
            raise
        spare, self._kept = self._kept, None
        if not needs_spare(task):
            if spare is not None:
                spare.end()
            spare = None
        elif spare is None:
            spares = self._app.thread_spares
            spare = await spares.reserve(wait=False)
            if spare is None and spares in held_spares():
                spare = spares.borrow()
            if spare is None:
                await sheltered(self._app.backend.hand_back(job, in_place=True))
                if spares not in held_spares() and not self._stopping.is_set():
                    self._kept = await self._reserved()
                    if self._kept is not None:
                        return False
                if not self._stopping.is_set():
                    logger.warning(
                        "job %s and the jobs behind it stay queued: all %d job threads for "
                        "sync tasks with a timeout are taken, %d of them by threads past their "
                        "timeout; a later drain or eager call runs them once one is free",
                        job.id,
                        spares.limit,
                        spares.strayed,
                    )
                    self._stopping.set()
                return False
        running = asyncio.ensure_future(self._attempt(job, task, spare))
        renewing = asyncio.ensure_future(self._renewing(job))
        try:
            return await self._settle(running, job, spare)
        except ExecutorClosedError:
            await self._app.backend.hand_back(job)
            raise
        except asyncio.CancelledError:
            await self._abandon(running, job, spare)
            raise
        finally:
            if running.done():
                renewing.cancel()
                await asyncio.gather(renewing, return_exceptions=True)
            else:
                running.add_done_callback(partial(_stop_renewing, renewing))

    async def _renewing(self, job: Job) -> None:
        while True:
            await asyncio.sleep(DRAIN_RENEW_SECONDS)
            try:
                await self._app.backend.heartbeat(
                    [job.id], DRAIN_LEASE_SECONDS, worker_id=DRAIN_WORKER_ID
                )
            except Exception as error:
                logger.warning(
                    "could not renew the lease of job %s, trying again in %.0f seconds: %s",
                    job.id,
                    DRAIN_RENEW_SECONDS,
                    error,
                )

    def _drop_kept(self) -> None:
        kept, self._kept = self._kept, None
        if kept is not None:
            kept.end()

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
            await self._app.execution.execute(
                self._app.bind(job), task, self._executors, held_until=_lease_end
            )
        finally:
            self._executors.lend(owner, None)
            if spare is not None:
                spare.end()


class InDrains:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, Waiters] = {}
        self._claims: set[object] = set()
        self._after_claims: list[tuple[set[object], Waiter]] = []

    def begin(self) -> object:
        claim = object()
        with self._lock:
            self._claims.add(claim)
        return claim

    def end(self, claim: object) -> None:
        with self._lock:
            self._claims.discard(claim)
            woken = [waiter for pending, waiter in self._after_claims if not pending & self._claims]
        wake(woken)

    def enter(self, job_id: str) -> Waiters:
        waiting: Waiters = []
        with self._lock:
            self._jobs[job_id] = waiting
        return waiting

    def leave(self, job_id: str, waiting: Waiters) -> None:
        with self._lock:
            if self._jobs.get(job_id) is waiting:
                del self._jobs[job_id]
            woken = list(waiting)
        wake(woken)

    async def finished(self, job_id: str) -> bool:
        loop = asyncio.get_running_loop()
        with self._lock:
            waiting = self._jobs.get(job_id)
            if waiting is None:
                return False
            waiter = (loop, loop.create_future())
            waiting.append(waiter)
        try:
            await waiter[1]
        finally:
            with self._lock:
                if waiter in waiting:
                    waiting.remove(waiter)
        return True

    async def claimed(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if not self._claims:
                return
            entry = (set(self._claims), (loop, loop.create_future()))
            self._after_claims.append(entry)
        try:
            await entry[1][1]
        finally:
            with self._lock:
                if entry in self._after_claims:
                    self._after_claims.remove(entry)


def _stop_renewing(renewing: asyncio.Future, _: object) -> None:
    renewing.cancel()


def _lease_end() -> float:
    return time.monotonic() + DRAIN_LEASE_SECONDS


def needs_spare(task: "Task") -> bool:
    return not task.is_coroutine and task.timeout is not None


def _unused(reserving: asyncio.Future) -> None:
    if not reserving.cancelled() and reserving.exception() is None and reserving.result():
        reserving.result().end()
