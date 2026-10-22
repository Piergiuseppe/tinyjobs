from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..clock import Clock
from ..executors import ExecutorResolver
from ..models import Job
from .hooks import Hooks
from .lifecycle import JobLifecycleService
from .serialization import SerializationService

if TYPE_CHECKING:
    from ..task import Task

current_job: ContextVar[Job] = ContextVar("tinyjobs_current_job")


@dataclass(frozen=True, slots=True)
class Attempt:
    error: BaseException | None = None
    will_retry: bool = False


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
        self, job: Job, task: "Task", executors: ExecutorResolver
    ) -> Attempt:
        current_job.set(job)
        await self._hooks.started(job)
        started = self._clock.monotonic()
        try:
            payload = self._serialization.decode_payload(job)
            result = await executors.resolve(task).execute(task, payload)
            encoded = self._lifecycle.encode_result(job, task, result)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            _, will_retry = await self._lifecycle.fail(job, task, error)
            await self._hooks.failed(job, error, will_retry)
            return Attempt(error, will_retry)
        duration = self._clock.monotonic() - started
        await self._lifecycle.complete(job, encoded)
        await self._hooks.succeeded(job, result, duration)
        return Attempt()
