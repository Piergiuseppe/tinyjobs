from __future__ import annotations

import asyncio
import logging
import time
import traceback as traceback_module
from functools import partial
from typing import Any, Awaitable, Callable, TYPE_CHECKING

from ..backends.base import Backend
from ..clock import Clock, epoch, from_wall
from ..exceptions import Retry
from ..models import ExecutionError, Job
from ..status import ExecutionStatus
from .serialization import SerializationService

if TYPE_CHECKING:
    from ..task import Task

FIRST_RETRY_SECONDS = 0.25
LONGEST_RETRY_SECONDS = 4.0

logger = logging.getLogger("tinyjobs.worker")


class JobLifecycleService:
    def __init__(
        self,
        backend: Backend,
        serialization: SerializationService,
        clock: Clock,
    ) -> None:
        self._backend = backend
        self._serialization = serialization
        self._clock = clock

    async def succeed(self, job: Job, task: "Task", result: Any) -> bool:
        return await self.complete(job, self.encode_result(job, task, result))

    async def complete(
        self, job: Job, encoded: bytes | None, *, patience: float = 0.0
    ) -> bool:
        return await self._persisting(
            job, partial(self._backend.complete, job, encoded), patience
        )

    async def fail(
        self, job: Job, task: "Task", error: BaseException, *, patience: float = 0.0
    ) -> tuple[bool, bool]:
        details = self._describe(error)
        retry_at = self._retry_at(job, task, error)
        write = partial(
            self._backend.fail, job, details, retry_at, self._execution_status(error)
        )
        return await self._persisting(job, write, patience), retry_at is not None

    async def _persisting(
        self, job: Job, write: Callable[[], Awaitable[bool]], patience: float
    ) -> bool:
        transient = getattr(self._backend, "transient_errors", ())
        deadline = time.monotonic() + patience
        delay = FIRST_RETRY_SECONDS
        interrupted = False
        while True:
            try:
                applied = await write()
            except transient as error:
                if time.monotonic() + delay > deadline:
                    raise
                logger.warning(
                    "could not record the outcome of job %s (%s); trying again in %gs",
                    job.id,
                    error,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, LONGEST_RETRY_SECONDS)
                interrupted = True
                continue
            if applied or not interrupted:
                return applied
            return await self._recorded_before(job)

    async def _recorded_before(self, job: Job) -> bool:
        for execution in await self._backend.executions(job.id):
            if (
                execution.attempt == job.attempts
                and execution.worker_id == job.worker_id
                and execution.status is not ExecutionStatus.RUNNING
            ):
                return True
        return False

    def _retry_at(
        self, job: Job, task: "Task", error: BaseException
    ) -> float | None:
        if not task.retry_policy.should_retry(
            job.attempts, job.max_attempts, error
        ):
            return None
        if isinstance(error, Retry) and error.at is not None:
            return from_wall(self._clock, epoch(error.at))
        delay = task.retry_policy.delay_for(job.attempts, error)
        return self._clock.now() + delay

    def encode_result(self, job: Job, task: "Task", result: Any) -> bytes | None:
        if not task.store_result or result is None:
            return None
        return self._serialization.encode_result(result, job.codec)

    def _execution_status(self, error: BaseException) -> ExecutionStatus:
        if isinstance(error, TimeoutError):
            return ExecutionStatus.TIMEOUT
        return ExecutionStatus.FAILED

    def _describe(self, error: BaseException) -> ExecutionError:
        return ExecutionError(
            type=type(error).__name__,
            message=str(error),
            traceback="".join(
                traceback_module.format_exception(
                    type(error), error, error.__traceback__
                )
            ),
        )
