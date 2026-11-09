from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ..clock import Clock
from ..current import current_job
from ..exceptions import ExecutorClosedError, ForcedInterrupt, ResultError
from ..executors import ExecutorResolver
from ..models import Job
from .hooks import Hooks
from .lifecycle import JobLifecycleService
from .serialization import SerializationService

if TYPE_CHECKING:
    from ..task import Task

current_runner: ContextVar[object] = ContextVar("tinyjobs_current_runner")


@dataclass(frozen=True, slots=True)
class Attempt:
    error: BaseException | None = None
    will_retry: bool = False
    recorded: bool = True
    duration: float = 0.0


class ExecutionService:
    def __init__(
        self,
        serialization: SerializationService,
        lifecycle: JobLifecycleService,
        hooks: Hooks,
        clock: Clock,
    ) -> None:
        self._serialization = serialization
        self._lifecycle = lifecycle
        self._hooks = hooks
        self._clock = clock

    async def execute(
        self,
        job: Job,
        task: "Task",
        executors: ExecutorResolver,
        *,
        held_until: Callable[[], float] | None = None,
        settling: Callable[[], None] | None = None,
        settled: Callable[[], None] | None = None,
    ) -> Attempt:
        token = current_job.set(job)
        try:
            return await self._attempt(job, task, executors, held_until, settling, settled)
        finally:
            current_job.reset(token)

    async def _attempt(
        self,
        job: Job,
        task: "Task",
        executors: ExecutorResolver,
        held_until: Callable[[], float] | None,
        settling: Callable[[], None] | None,
        settled: Callable[[], None] | None,
    ) -> Attempt:
        try:
            await self._hooks.started(job)
            return await self._run(job, task, executors, held_until, settling, settled)
        finally:
            self._hooks.ended(job)

    async def _run(
        self,
        job: Job,
        task: "Task",
        executors: ExecutorResolver,
        held_until: Callable[[], float] | None,
        settling: Callable[[], None] | None,
        settled: Callable[[], None] | None,
    ) -> Attempt:
        started = self._clock.monotonic()
        try:
            payload = self._serialization.decode_payload(job)
            result = await executors.resolve(task).execute(task, payload, job=job)
            encoded = self._lifecycle.encode_result(job, task, result)
        except (asyncio.CancelledError, ExecutorClosedError, ForcedInterrupt):
            raise
        except BaseException as error:
            duration = self._clock.monotonic() - started
            if settling is not None:
                settling()
            recorded, will_retry = await self._lifecycle.fail(
                job,
                task,
                error,
                held_until=held_until,
                final=isinstance(error, ResultError),
            )
            if settled is not None:
                settled()
            if not recorded:
                return Attempt(error, will_retry, recorded=False, duration=duration)
            await self._hooks.failed(job, error, will_retry)
            return Attempt(error, will_retry, duration=duration)
        duration = self._clock.monotonic() - started
        if settling is not None:
            settling()
        recorded = await self._lifecycle.complete(job, encoded, held_until=held_until)
        if settled is not None:
            settled()
        if not recorded:
            return Attempt(recorded=False, duration=duration)
        await self._hooks.succeeded(job, result, duration)
        return Attempt(duration=duration)
