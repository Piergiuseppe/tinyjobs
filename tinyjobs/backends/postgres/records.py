from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence

from ...models import Execution, Job, ScheduleState
from ...status import ExecutionStatus, JobStatus

JOB_COLUMNS = (
    "id",
    "task",
    "payload",
    "codec",
    "queue",
    "status",
    "priority",
    "attempts",
    "max_attempts",
    "hand_backs",
    "claims",
    "run_at",
    "created_at",
    "finished_at",
    "lease_expires_at",
    "worker_id",
    "dedup_key",
    "dedup_digest",
    "result",
    "last_error",
)

JOB_TYPES = (
    "text",
    "text",
    "bytea",
    "text",
    "text",
    "text",
    "integer",
    "integer",
    "integer",
    "integer",
    "integer",
    "double precision",
    "double precision",
    "double precision",
    "double precision",
    "text",
    "text",
    "bytea",
    "bytea",
    "text",
)


def job_columns(alias: str | None = None) -> str:
    if alias is None:
        return ", ".join(JOB_COLUMNS)
    return ", ".join(f"{alias}.{column}" for column in JOB_COLUMNS)


def job_arrays(jobs: Sequence[Job]) -> list[list[Any]]:
    rows = [job_to_row(job) for job in jobs]
    return [list(column) for column in zip(*rows)] if rows else [[] for _ in JOB_COLUMNS]


def job_to_row(job: Job) -> tuple:
    return (
        job.id,
        job.task,
        job.payload,
        job.codec,
        job.queue,
        str(job.status),
        job.priority,
        job.attempts,
        job.max_attempts,
        job.hand_backs,
        job.claims,
        job.run_at,
        job.created_at,
        job.finished_at,
        job.lease_expires_at,
        job.worker_id,
        job.dedup_key,
        dedup_digest(job.dedup_key),
        job.result_payload,
        job.last_error,
    )


def dedup_digest(key: str | None) -> bytes | None:
    return None if key is None else hashlib.sha256(key.encode()).digest()


def job_from_row(row: Mapping[str, Any]) -> Job:
    return Job(
        id=row["id"],
        task=row["task"],
        payload=bytes(row["payload"]),
        codec=row["codec"],
        queue=row["queue"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        hand_backs=row["hand_backs"],
        claims=row["claims"],
        run_at=row["run_at"],
        created_at=row["created_at"],
        finished_at=row["finished_at"],
        lease_expires_at=row["lease_expires_at"],
        worker_id=row["worker_id"],
        dedup_key=row["dedup_key"],
        result_payload=None if row["result"] is None else bytes(row["result"]),
        last_error=row["last_error"],
    )


def execution_from_row(row: Mapping[str, Any]) -> Execution:
    return Execution(
        id=row["id"],
        job_id=row["job_id"],
        attempt=row["attempt"],
        status=ExecutionStatus(row["status"]),
        worker_id=row["worker_id"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        error_type=row["error_type"],
        traceback=row["traceback"],
        claim=row["claim"],
    )


def schedule_from_row(row: Mapping[str, Any]) -> ScheduleState:
    return ScheduleState(
        name=row["name"],
        cron=row["cron"],
        timezone=row["timezone"],
        next_run_at=row["next_run_at"],
        last_run_at=row["last_run_at"],
        last_job_id=row["last_job_id"],
    )
