from __future__ import annotations

import asyncio
import logging
import time
import traceback as traceback_module
from functools import partial
from contextlib import nullcontext
from typing import Any, AsyncContextManager, Awaitable, Callable, TYPE_CHECKING

from ..backends.base import Backend
from ..clock import Clock, epoch, from_wall
from ..deadlines import bounded
from ..exceptions import Retry
from ..models import ExecutionError, Job
from ..status import ExecutionStatus
from .serialization import SerializationService

if TYPE_CHECKING:
    from ..task import Task

FIRST_RETRY_SECONDS = 0.25
LAST_WORD_SECONDS = 1.0
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
        self,
        job: Job,
        encoded: bytes | None,
        *,
        held_until: Callable[[], float] | None = None,
    ) -> bool:
        return await self._persisting(
            job,
            partial(self._backend.complete, job, encoded),
            ExecutionStatus.SUCCESS,
            held_until,
        )

    async def fail(
        self,
        job: Job,
        task: "Task",
        error: BaseException,
        *,
        held_until: Callable[[], float] | None = None,
    ) -> tuple[bool, bool]:
        details = self._describe(error)
        retry_at = self._retry_at(job, task, error)
        status = self._execution_status(error)
        write = partial(self._backend.fail, job, details, retry_at, status)
        return (
            await self._persisting(job, write, status, held_until),
            retry_at is not None,
        )

    async def _persisting(
        self,
        job: Job,
        write: Callable[[], Awaitable[bool]],
        status: ExecutionStatus,
        held_until: Callable[[], float] | None,
    ) -> bool:
        transient = getattr(self._backend, "transient_errors", ())
        delay = FIRST_RETRY_SECONDS
        deadline: float | None = None
        interrupted = False
        while True:
            try:
                async with self._held(held_until):
                    if interrupted:
                        return await write() or await self._recorded_before(job, status)
                    return await write()
            except transient as error:
                if deadline is None:
                    deadline = held_until() if held_until else 0.0
                left = deadline - time.monotonic()
                if left <= 0:
                    raise
                pause = min(delay, left)
                logger.warning(
                    "could not record the outcome of job %s (%s); trying again in %.2gs",
                    job.id,
                    error,
                    pause,
                )
                await asyncio.sleep(pause)
                delay = min(delay * 2, LONGEST_RETRY_SECONDS)
                interrupted = True

    def _held(self, held_until: Callable[[], float] | None) -> AsyncContextManager[Any]:
        if held_until is None:
            return nullcontext()
        return bounded(max(held_until(), time.monotonic() + LAST_WORD_SECONDS))

    async def _recorded_before(self, job: Job, status: ExecutionStatus) -> bool:
        history = await self._backend.executions(job.id)
        if all(execution.claim is not None for execution in history):
            history = [execution for execution in history if execution.claim == job.claims]
        if not history:
            return False
        mine = history[-1]
        return (
            mine.attempt == job.attempts
            and mine.worker_id == job.worker_id
            and mine.status is status
        )

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
