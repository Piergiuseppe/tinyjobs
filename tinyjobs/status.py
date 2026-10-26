from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_JOB_STATUSES

    @property
    def is_claimable(self) -> bool:
        return self is JobStatus.QUEUED


class ExecutionStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    LOST = "lost"
    CANCELLED = "cancelled"


_TERMINAL_JOB_STATUSES = frozenset(
    {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED}
)
