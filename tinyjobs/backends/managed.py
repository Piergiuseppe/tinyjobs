from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import time
from contextlib import suppress
from functools import partial
from typing import Any, Awaitable, Callable, Collection, Coroutine, Sequence

from ..deadlines import Deadline
from ..deadlines import current as current_deadline
from ..exceptions import AppClosedError, EventLoopStoppedError
from ..models import Execution, ExecutionError, Job, Page, ScheduleState, Stats
from ..retention import RetentionPolicy
from ..runtime import (
    CLOSED_MESSAGE,
    LoopRunner,
    heal_this_thread,
    run_kept,
    run_on,
    submit,
    tracked_lock,
)
from ..status import ExecutionStatus, JobStatus
from .base import Backend

CLOSE_GRACE_SECONDS = 5.0
CLOSE_POLL_SECONDS = 0.01
DELIVERED = "delivered"
ABANDONED = "abandoned"


class ManagedBackend:
    def __init__(self, backend: Backend, runner: LoopRunner) -> None:
        self._inner = backend
        self._runner = runner
        self._loop = runner.own_loop() if backend.capabilities.loop_bound else None
        if self._loop is not None:
            runner.keep(self._loop, backend)
        self._active = 0
        self._returning: dict[concurrent.futures.Future, Deadline | None] = {}
        self._closing = False
        self._gate = tracked_lock()
        self.capabilities = backend.capabilities
        self._knows_holders = _accepts(getattr(backend, "heartbeat", None), "worker_id")
        self.hands_back_in_place = _accepts(getattr(backend, "hand_back", None), "in_place")

    @property
    def alive(self) -> bool:
        return self._loop is None or self._runner.serves(self._loop)

    def __getattr__(self, name: str) -> Any:
        if "_inner" not in self.__dict__:
            raise AttributeError(name)
        return getattr(self._inner, name)

    async def setup(self) -> None:
        await self._forward(self._inner.setup)

    async def close(self) -> None:
        with self._gate:
            self._closing = True
        deadline = time.monotonic() + CLOSE_GRACE_SECONDS
        while (self._active or self._awaited()) and time.monotonic() < deadline:
            await asyncio.sleep(CLOSE_POLL_SECONDS)
        if not self.alive:
            return
        try:
            await self._routed(self._inner.close(), "close")
        except (AppClosedError, EventLoopStoppedError):
            return
        self._runner.release(self._inner)

    async def enqueue(self, job: Job) -> Job:
        return await self._forward(self._inner.enqueue, job)

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        return await self._forward(self._inner.enqueue_many, jobs)

    async def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]:
        return await self._claimed(
            partial(
                self._inner.claim,
                worker_id=worker_id,
                queues=queues,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        )

    async def claim_job(
        self, job_id: str, *, worker_id: str, lease_seconds: float
    ) -> Job | None:
        method = getattr(self._inner, "claim_job", None)
        if method is None:
            return None
        jobs = await self._claimed(
            partial(_listed, method, job_id, worker_id=worker_id, lease_seconds=lease_seconds)
        )
        return jobs[0] if jobs else None

    async def _claimed(self, claiming: Callable[[], Awaitable[list[Job]]]) -> list[Job]:
        outcome = self._track()
        if outcome is None:
            raise AppClosedError(CLOSED_MESSAGE)
        decided: dict[int, object] = {}
        try:
            home = self._loop or self._runner.own_loop()
            coroutine = self._claim_kept(decided, claiming)
        except BaseException:
            outcome.cancel()
            raise
        try:
            return await run_kept(home, coroutine, "claim", outcome)
        except BaseException:
            self._abandon_claim(decided, outcome, home)
            raise

    def _track(self) -> concurrent.futures.Future | None:
        with self._gate:
            if self._closing:
                return None
            return self._pending()

    def _awaited(self) -> bool:
        return any(
            deadline is None or deadline.left() > 0
            for deadline in list(self._returning.values())
        )

    def _pending(self) -> concurrent.futures.Future:
        outcome: concurrent.futures.Future = concurrent.futures.Future()
        self._returning[outcome] = current_deadline.get()
        outcome.add_done_callback(partial(self._returned, outcome))
        return outcome

    async def _claim_kept(
        self, decided: dict[int, object], claiming: Callable[[], Awaitable[list[Job]]]
    ) -> list[Job]:
        if decided.get(0) is ABANDONED:
            return []
        jobs = await claiming()
        if decided.setdefault(0, DELIVERED) is DELIVERED:
            return jobs
        await self._hand_back_all(jobs)
        return []

    def _returned(self, outcome: concurrent.futures.Future, _: object) -> None:
        self._returning.pop(outcome, None)

    def _abandon_claim(
        self,
        decided: dict[int, object],
        outcome: concurrent.futures.Future,
        home: asyncio.AbstractEventLoop,
    ) -> None:
        if decided.setdefault(0, ABANDONED) is not ABANDONED:
            returning = self._pending()
            outcome.add_done_callback(partial(self._return_delivered, home, returning))
        elif home.is_closed():
            outcome.cancel()

    def _return_delivered(
        self,
        home: asyncio.AbstractEventLoop,
        returning: concurrent.futures.Future,
        outcome: concurrent.futures.Future,
    ) -> None:
        if outcome.cancelled() or outcome.exception() is not None or not outcome.result():
            returning.cancel()
            return
        try:
            submit(
                home,
                self._hand_back_all(outcome.result()),
                "hand_back",
                outcome=returning,
                returning=True,
            )
        except Exception:
            returning.cancel()

    async def _hand_back_all(self, jobs: Sequence[Job]) -> None:
        method = getattr(self._inner, "hand_back", self._inner.release)
        for job in jobs:
            with suppress(Exception):
                await self._routed(method(job, 0.0), method.__name__)

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> Collection[str] | None:
        if worker_id is not None and self._knows_holders:
            return await self._forward(
                self._inner.heartbeat, job_ids, lease_seconds, worker_id=worker_id
            )
        await self._forward(self._inner.heartbeat, job_ids, lease_seconds)
        return None

    async def complete(self, job: Job, result: bytes | None) -> bool:
        return await self._forward(self._inner.complete, job, result)

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        return await self._forward(
            self._inner.fail, job, error, retry_at, execution_status
        )

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        return await self._forward(self._inner.release, job, delay)

    async def release_many(self, jobs: Sequence[Job]) -> int:
        method = getattr(self._inner, "release_many", None)
        if method is not None:
            return await self._forward(method, jobs)
        released = await asyncio.gather(*(self.release(job) for job in jobs))
        return sum(map(bool, released))

    async def hand_back(self, job: Job, delay: float = 0.0, *, in_place: bool = False) -> bool:
        if in_place and self.hands_back_in_place:
            return await self._forward(self._inner.hand_back, job, delay, in_place=True)
        method = getattr(self._inner, "hand_back", self._inner.release)
        return await self._forward(method, job, delay)

    async def follow_clock(self) -> None:
        method = getattr(self._inner, "follow_clock", None)
        if method is not None:
            await self._forward(method)

    async def spare(self) -> None:
        method = getattr(self._inner, "spare", None)
        if method is not None:
            await self._forward(method)

    def reserve(self, owner: object, jobs: int) -> int:
        method = getattr(self._inner, "reserve", None)
        return 1 if method is None else method(owner, jobs)

    @property
    def wakes_workers(self) -> bool:
        return hasattr(self._inner, "wait_for_jobs")

    async def wait_for_jobs(
        self, *, worker_id: str, queues: Sequence[str], timeout: float
    ) -> None:
        await self._forward(
            self._inner.wait_for_jobs, worker_id=worker_id, queues=queues, timeout=timeout
        )

    async def cancel(self, job_id: str) -> bool:
        return await self._forward(self._inner.cancel, job_id)

    async def requeue(self, job_id: str) -> bool:
        return await self._forward(self._inner.requeue, job_id)

    async def get(self, job_id: str) -> Job | None:
        return await self._forward(self._inner.get, job_id)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        return await self._forward(
            self._inner.list, status=status, queue=queue, limit=limit, cursor=cursor
        )

    async def executions(self, job_id: str) -> list[Execution]:
        return await self._forward(self._inner.executions, job_id)

    async def reap_expired(self) -> int:
        return await self._forward(self._inner.reap_expired)

    async def counts_by_status(self) -> dict[str, int]:
        return await self._forward(self._inner.counts_by_status)

    async def stats(self, now: float) -> Stats:
        return await self._forward(self._inner.stats, now)

    async def purge(self, policy: RetentionPolicy, now: float) -> int | None:
        return await self._forward(self._inner.purge, policy, now)

    async def schedules(self) -> list[ScheduleState]:
        return await self._forward(self._inner.schedules)

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None:
        await self._forward(self._inner.register_schedules, states)

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        return await self._forward(
            self._inner.fire_schedule,
            name,
            expected=expected,
            next_run_at=next_run_at,
            jobs=jobs,
        )

    async def _forward(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        heal_this_thread()
        entered = False
        try:
            with self._gate:
                if self._closing:
                    raise AppClosedError(CLOSED_MESSAGE)
                self._active += 1
                entered = True
            return await self._routed(method(*args, **kwargs), method.__name__)
        finally:
            if entered:
                with self._gate:
                    self._active -= 1

    async def _routed(self, coroutine: Coroutine[Any, Any, Any], name: str) -> Any:
        if self._loop is None:
            return await coroutine
        return await run_on(self._loop, coroutine, name)


async def _listed(method: Any, *arguments: Any, **options: Any) -> list[Job]:
    job = await method(*arguments, **options)
    return [] if job is None else [job]


def _accepts(method: Any, name: str) -> bool:
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == name
        and parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        for parameter in parameters
    )
