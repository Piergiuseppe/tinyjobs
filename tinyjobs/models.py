from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .status import ExecutionStatus, JobStatus


@dataclass(slots=True)
class JobPayload:
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Job:
    id: str
    task: str
    payload: bytes
    codec: str
    queue: str
    status: JobStatus
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 1
    run_at: float = 0.0
    created_at: float = 0.0
    finished_at: float | None = None
    lease_expires_at: float | None = None
    worker_id: str | None = None
    dedup_key: str | None = None
    result: bytes | None = None
    last_error: str | None = None

    @property
    def attempts_remaining(self) -> int:
        return max(self.max_attempts - self.attempts, 0)


@dataclass(slots=True)
class Execution:
    job_id: str
    attempt: int
    status: ExecutionStatus
    worker_id: str
    started_at: float
    id: int | None = None
    finished_at: float | None = None
    error_type: str | None = None
    traceback: str | None = None

    @property
    def duration(self) -> float | None:
        if self.finished_at is None:
            return None
        return self.finished_at - self.started_at


@dataclass(frozen=True, slots=True)
class ExecutionError:
    type: str
    message: str
    traceback: str

    @property
    def summary(self) -> str:
        return f"{self.type}: {self.message}" if self.message else self.type


@dataclass(frozen=True, slots=True)
class Page:
    items: list[Job]
    cursor: str | None = None
