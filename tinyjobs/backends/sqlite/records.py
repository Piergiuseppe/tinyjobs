from __future__ import annotations

import sqlite3

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
    "run_at",
    "created_at",
    "finished_at",
    "lease_expires_at",
    "worker_id",
    "dedup_key",
    "result",
    "last_error",
)

JOB_COLUMN_LIST = ", ".join(JOB_COLUMNS)
JOB_PLACEHOLDERS = ", ".join("?" for _ in JOB_COLUMNS)


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
        job.run_at,
        job.created_at,
        job.finished_at,
        job.lease_expires_at,
        job.worker_id,
        job.dedup_key,
        job.result_payload,
        job.last_error,
    )


def job_from_row(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"],
        task=row["task"],
        payload=row["payload"],
        codec=row["codec"],
        queue=row["queue"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        run_at=row["run_at"],
        created_at=row["created_at"],
        finished_at=row["finished_at"],
        lease_expires_at=row["lease_expires_at"],
        worker_id=row["worker_id"],
        dedup_key=row["dedup_key"],
        result_payload=row["result"],
        last_error=row["last_error"],
    )


def execution_from_row(row: sqlite3.Row) -> Execution:
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
    )


def schedule_from_row(row: sqlite3.Row) -> ScheduleState:
    return ScheduleState(
        name=row["name"],
        cron=row["cron"],
        timezone=row["timezone"],
        next_run_at=row["next_run_at"],
        last_run_at=row["last_run_at"],
        last_job_id=row["last_job_id"],
    )
