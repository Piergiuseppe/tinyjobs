from __future__ import annotations

import traceback as traceback_module
from typing import Any, TYPE_CHECKING

from ..backends.base import Backend
from ..clock import Clock
from ..exceptions import Retry
from ..models import ExecutionError, Job
from ..status import ExecutionStatus
from .serialization import SerializationService

if TYPE_CHECKING:
    from ..task import Task


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

    def succeed(self, job: Job, task: "Task", result: Any) -> bool:
        encoded = self._encode_result(job, task, result)
        return self._backend.complete(job, encoded)

    def fail(self, job: Job, task: "Task", error: BaseException) -> tuple[bool, bool]:
        details = self._describe(error)
        retry_at = self._retry_at(job, task, error)
        applied = self._backend.fail(
            job, details, retry_at, self._execution_status(error)
        )
        return applied, retry_at is not None

    def _retry_at(
        self, job: Job, task: "Task", error: BaseException
    ) -> float | None:
        if not task.retry_policy.should_retry(
            job.attempts, job.max_attempts, error
        ):
            return None
        if isinstance(error, Retry) and error.at is not None:
            return error.at
        delay = task.retry_policy.delay_for(job.attempts, error)
        return self._clock.now() + delay

    def _encode_result(self, job: Job, task: "Task", result: Any) -> bytes | None:
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
