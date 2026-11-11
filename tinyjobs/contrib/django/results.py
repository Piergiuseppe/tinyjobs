from __future__ import annotations

import traceback as tracebacks
from datetime import UTC, datetime
from typing import Any, Sequence

from django.conf import settings
from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.utils import timezone

from ...following import PARENT_ENDED_MESSAGE, PARENT_GONE_MESSAGE
from ...models import Execution, Job, JobPayload
from ...status import ExecutionStatus, JobStatus
from .discovery import stored
from .exceptions import JobCancelled, WorkerLost

STATUSES = {
    JobStatus.QUEUED: TaskResultStatus.READY,
    JobStatus.HELD: TaskResultStatus.READY,
    JobStatus.WAITING: TaskResultStatus.READY,
    JobStatus.RUNNING: TaskResultStatus.RUNNING,
    JobStatus.SUCCESS: TaskResultStatus.SUCCESSFUL,
    JobStatus.FAILED: TaskResultStatus.FAILED,
    JobStatus.CANCELLED: TaskResultStatus.FAILED,
}

FAILED_EXECUTIONS = frozenset({ExecutionStatus.FAILED, ExecutionStatus.TIMEOUT})


def build_result(
    task: Task,
    job: Job,
    payload: JobPayload,
    executions: Sequence[Execution],
    *,
    alias: str,
    value: Any = None,
) -> TaskResult:
    result = TaskResult(
        task=stored(
            task,
            priority=job.priority,
            queue_name=job.queue,
            run_after=(
                moment(job.run_at)
                if job.claims == 0
                and job.run_at != job.created_at
                and (job.after is None or job.status is JobStatus.WAITING)
                else None
            ),
            follows=job.after,
        ),
        id=job.id,
        status=STATUSES[job.status],
        enqueued_at=moment(job.created_at),
        started_at=moment(executions[0].started_at) if executions else None,
        finished_at=moment(job.finished_at),
        last_attempted_at=moment(executions[-1].started_at) if executions else None,
        args=list(payload.args),
        kwargs=dict(payload.kwargs),
        backend=alias,
        errors=errors_of(job, executions),
        worker_ids=[execution.worker_id for execution in executions],
    )
    object.__setattr__(result, "_return_value", value)
    return result


def raised(error: BaseException) -> TaskError:
    kind = type(error)
    return TaskError(
        exception_class_path=f"{kind.__module__}.{kind.__qualname__}",
        traceback="".join(tracebacks.format_exception(error)),
    )


def errors_of(job: Job, executions: Sequence[Execution]) -> list[TaskError]:
    errors = [error_of(execution) for execution in executions if failed(execution)]
    if errors or STATUSES[job.status] is not TaskResultStatus.FAILED:
        return errors
    if job.status is JobStatus.CANCELLED:
        followed = job.last_error in (PARENT_ENDED_MESSAGE, PARENT_GONE_MESSAGE)
        return [synthetic(JobCancelled(job.last_error if followed else "the job was cancelled"))]
    return [synthetic(WorkerLost("the worker running the job was lost"))]


def synthetic(error: Exception) -> TaskError:
    kind = type(error)
    return TaskError(
        exception_class_path=f"{kind.__module__}.{kind.__qualname__}",
        traceback="".join(tracebacks.format_exception_only(error)),
    )


def failed(execution: Execution) -> bool:
    return execution.status in FAILED_EXECUTIONS and bool(execution.traceback)


def error_of(execution: Execution) -> TaskError:
    return TaskError(
        exception_class_path=exception_path(
            execution.error_type or "", execution.traceback
        ),
        traceback=execution.traceback,
    )


def exception_path(name: str, traceback: str) -> str:
    for line in reversed(traceback.splitlines()):
        qualified = line.split(":", 1)[0].strip(" |")
        if qualified == name or qualified.endswith(f".{name}"):
            return qualified if "." in qualified else f"builtins.{qualified}"
    return f"builtins.{name}"


def moment(timestamp: float | None) -> datetime | None:
    if timestamp is None:
        return None
    value = datetime.fromtimestamp(timestamp, tz=UTC)
    return value if settings.USE_TZ else timezone.make_naive(value)
