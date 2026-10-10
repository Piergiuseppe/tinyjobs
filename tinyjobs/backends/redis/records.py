from __future__ import annotations

import json
from typing import Any, Sequence

from ...models import Execution, Job, ScheduleState
from ...status import ExecutionStatus, JobStatus

BINARY_FIELDS = ("payload", "result")
INTEGER_FIELDS = ("priority", "attempts", "max_attempts")
TIME_FIELDS = ("run_at", "created_at", "finished_at", "lease_expires_at")
TEXT_FIELDS = ("id", "task", "codec", "queue", "status", "worker_id", "dedup_key", "last_error")


def number(value: float) -> str:
    return repr(float(value))


def job_to_arguments(job: Job) -> list[Any]:
    values = {
        "id": job.id,
        "task": job.task,
        "payload": job.payload,
        "codec": job.codec,
        "queue": job.queue,
        "status": str(job.status),
        "priority": str(job.priority),
        "attempts": str(job.attempts),
        "max_attempts": str(job.max_attempts),
        "run_at": number(job.run_at),
        "created_at": number(job.created_at),
        "finished_at": None if job.finished_at is None else number(job.finished_at),
        "lease_expires_at": None
        if job.lease_expires_at is None
        else number(job.lease_expires_at),
        "worker_id": job.worker_id,
        "dedup_key": job.dedup_key,
        "result": job.result_payload,
        "last_error": job.last_error,
    }
    flat: list[Any] = []
    for field, value in values.items():
        if value is not None:
            flat.extend((field, value))
    return [len(flat), *flat]


def jobs_to_arguments(jobs: Sequence[Job]) -> list[Any]:
    arguments: list[Any] = [len(jobs)]
    for job in jobs:
        arguments.extend(job_to_arguments(job))
    return arguments


def job_from_flat(flat: Sequence[bytes]) -> Job:
    return job_from_fields(dict(zip(flat[::2], flat[1::2])))


def job_from_fields(raw: dict[bytes, bytes]) -> Job:
    fields = {key.decode(): value for key, value in raw.items()}

    def text(name: str) -> str | None:
        value = fields.get(name)
        return None if value is None else value.decode()

    def moment(name: str) -> float | None:
        value = fields.get(name)
        return None if value is None else float(value)

    return Job(
        id=text("id"),
        task=text("task"),
        payload=fields["payload"],
        codec=text("codec"),
        queue=text("queue"),
        status=JobStatus(text("status")),
        priority=int(fields["priority"]),
        attempts=int(fields["attempts"]),
        max_attempts=int(fields["max_attempts"]),
        run_at=moment("run_at"),
        created_at=moment("created_at"),
        finished_at=moment("finished_at"),
        lease_expires_at=moment("lease_expires_at"),
        worker_id=text("worker_id"),
        dedup_key=text("dedup_key"),
        result_payload=fields.get("result"),
        last_error=text("last_error"),
    )


def execution_from(job_id: str, attempt: bytes, encoded: bytes) -> Execution:
    data = json.loads(encoded)
    finished_at = data.get("finished_at")
    return Execution(
        job_id=job_id,
        attempt=int(attempt),
        status=ExecutionStatus(data["status"]),
        worker_id=data["worker_id"],
        started_at=float(data["started_at"]),
        finished_at=None if finished_at is None else float(finished_at),
        error_type=data.get("error_type"),
        traceback=data.get("traceback"),
    )


def schedule_from(raw: dict[bytes, bytes]) -> ScheduleState:
    fields = {key.decode(): value.decode() for key, value in raw.items()}
    last_run_at = fields.get("last_run_at")
    return ScheduleState(
        name=fields["name"],
        cron=fields["cron"],
        timezone=fields["timezone"],
        next_run_at=float(fields["next_run_at"]),
        last_run_at=None if last_run_at is None else float(last_run_at),
        last_job_id=fields.get("last_job_id"),
    )
