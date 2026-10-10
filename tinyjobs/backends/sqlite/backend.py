from __future__ import annotations

import asyncio
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from functools import partial
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import urlsplit

from ...clock import Clock
from ...models import Execution, ExecutionError, Job, Page, ScheduleState
from ...retention import RetentionPolicy
from ...status import ExecutionStatus, JobStatus
from ..base import BaseBackend, Capabilities
from .claim import SUPPORTS_RETURNING, build_claim_strategy
from .connection import ConnectionPool
from .queries import (
    ADVANCE_SCHEDULE,
    CANCEL_JOB,
    CLAIM_HOUSEKEEPING,
    COMPLETE_JOB,
    FAIL_JOB,
    FINISH_EXECUTION,
    HEARTBEAT,
    INSERT_EXECUTION,
    INSERT_JOB,
    MARK_EXECUTIONS_LOST,
    PURGE_BATCH,
    REAP_EXPIRED,
    RECORD_SCHEDULE_RUN,
    REGISTER_SCHEDULE,
    RELEASE_JOB,
    REQUEUE_JOB,
    RESCHEDULE_JOB,
    SELECT_EXECUTIONS,
    SELECT_ACTIVE_BY_DEDUP,
    SELECT_EXPIRED_IDS,
    SELECT_JOB,
    SELECT_SCHEDULES,
)
from .records import (
    JOB_COLUMN_LIST,
    execution_from_row,
    job_from_row,
    job_to_row,
    schedule_from_row,
)
from .schema import DDL, MIGRATIONS, SCHEMA_VERSION

MEMORY_TEMPLATE = "file:tinyjobs-{token}?mode=memory&cache=shared"
LEASE_EXPIRED_MESSAGE = (
    "lease expired: the worker holding this job stopped reporting"
)


class SQLiteBackend(BaseBackend):
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=True,
        retention=True,
        schedules=True,
        max_payload_bytes=None,
    )

    def __init__(
        self,
        database: str,
        *,
        uri: bool = False,
        busy_timeout: float = 5.0,
        clock: Clock | None = None,
    ) -> None:
        self._clock = clock or Clock()
        self._connections = ConnectionPool(
            database, uri=uri, busy_timeout=busy_timeout
        )
        self._claim_strategy = build_claim_strategy()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="tinyjobs-sqlite"
        )


    async def _call(self, function, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, partial(function, *args))

    async def setup(self) -> None:
        await self._call(self._setup_sync)

    async def close(self) -> None:
        await self._call(self._close_sync)
        self._executor.shutdown(wait=True)

    async def enqueue(self, job: Job) -> Job:
        return await self._call(self._enqueue_sync, job)

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        return await self._call(self._enqueue_many_sync, jobs)

    async def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]:
        return await self._call(
            self._claim_sync, worker_id, tuple(queues), limit, lease_seconds
        )

    async def heartbeat(self, job_ids: Sequence[str], lease_seconds: float) -> None:
        await self._call(self._heartbeat_sync, tuple(job_ids), lease_seconds)

    async def complete(self, job: Job, result: bytes | None) -> bool:
        return await self._call(self._complete_sync, job, result)

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        return await self._call(
            self._fail_sync, job, error, retry_at, execution_status
        )

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        return await self._call(self._release_sync, job, delay)

    async def cancel(self, job_id: str) -> bool:
        return await self._call(self._cancel_sync, job_id)

    async def requeue(self, job_id: str) -> bool:
        return await self._call(self._requeue_sync, job_id)

    async def get(self, job_id: str) -> Job | None:
        return await self._call(self._get_sync, job_id)

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        return await self._call(self._list_sync, status, queue, limit, cursor)

    async def executions(self, job_id: str) -> list[Execution]:
        return await self._call(self._executions_sync, job_id)

    async def reap_expired(self) -> int:
        return await self._call(self._reap_expired_sync)

    async def purge(self, policy: RetentionPolicy, now: float) -> int | None:
        return await self._call(self._purge_sync, policy, now)

    async def counts_by_status(self) -> dict[str, int]:
        return await self._call(self._counts_by_status_sync)

    async def schedules(self) -> list[ScheduleState]:
        return await self._call(self._schedules_sync)

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None:
        await self._call(self._register_schedules_sync, tuple(states))

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        return await self._call(
            self._fire_schedule_sync, name, expected, next_run_at, tuple(jobs)
        )

    @property
    def database(self) -> str:
        return self._connections.database

    @classmethod
    def from_url(cls, url: str, **options: Any) -> "SQLiteBackend":
        database = urlsplit(url).path[1:]
        if not database or database == ":memory:":
            token = uuid.uuid4().hex
            return cls(MEMORY_TEMPLATE.format(token=token), uri=True, **options)
        path = Path(database).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(str(path), **options)

    def _setup_sync(self) -> None:
        with self._connections.write() as connection:
            existing = self._stored_version(connection)
            for statement in DDL:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
                "ON CONFLICT (key) DO NOTHING",
                (str(SCHEMA_VERSION),),
            )
            connection.execute(
                "INSERT INTO meta (key, value) VALUES ('last_purge_at', '0') "
                "ON CONFLICT (key) DO NOTHING"
            )
            if existing is not None:
                self._migrate(connection, existing)
        self._assert_schema_supported()

    def _stored_version(self, connection: sqlite3.Connection) -> int | None:
        present = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
        ).fetchone()
        if present is None:
            return None
        row = connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else None

    def _migrate(self, connection: sqlite3.Connection, current: int) -> None:
        if current >= SCHEMA_VERSION:
            return
        for version in range(current + 1, SCHEMA_VERSION + 1):
            for statement in MIGRATIONS.get(version, ()):
                connection.execute(statement)
        connection.execute(
            "UPDATE meta SET value = ? WHERE key = 'schema_version'",
            (str(SCHEMA_VERSION),),
        )

    def _close_sync(self) -> None:
        self._connections.close()

    def transaction(self) -> AbstractContextManager[sqlite3.Connection]:
        return self._connections.write()

    def _enqueue_sync(self, job: Job) -> Job:
        with self._connections.write() as connection:
            return self._insert(connection, job)

    def _enqueue_many_sync(self, jobs: Sequence[Job]) -> list[Job]:
        with self._connections.write() as connection:
            if any(job.dedup_key is not None for job in jobs):
                return [self._insert(connection, job) for job in jobs]
            connection.executemany(INSERT_JOB, [job_to_row(job) for job in jobs])
            return list(jobs)

    def _insert(self, connection: sqlite3.Connection, job: Job) -> Job:
        if job.dedup_key is None:
            connection.execute(INSERT_JOB, job_to_row(job))
            return job
        existing = self._active_duplicate(connection, job.dedup_key)
        if existing is not None:
            return existing
        try:
            connection.execute(INSERT_JOB, job_to_row(job))
        except sqlite3.IntegrityError:
            existing = self._active_duplicate(connection, job.dedup_key)
            if existing is None:
                raise
            return existing
        return job

    def _active_duplicate(
        self, connection: sqlite3.Connection, dedup_key: str
    ) -> Job | None:
        row = connection.execute(SELECT_ACTIVE_BY_DEDUP, (dedup_key,)).fetchone()
        return job_from_row(row) if row else None

    def _claim_sync(
        self,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]:
        if limit <= 0 or not queues:
            return []
        now = self._clock.now()
        with self._connections.write() as connection:
            jobs = self._claim_strategy.claim(
                connection,
                worker_id=worker_id,
                queues=tuple(queues),
                limit=limit,
                lease_expires_at=now + lease_seconds,
                now=now,
            )
            for job in jobs:
                connection.execute(
                    INSERT_EXECUTION, (job.id, job.attempts, worker_id, now)
                )
        return jobs

    def _heartbeat_sync(self, job_ids: Sequence[str], lease_seconds: float) -> None:
        if not job_ids:
            return
        placeholders = ", ".join("?" for _ in job_ids)
        expiry = self._clock.now() + lease_seconds
        with self._connections.write() as connection:
            connection.execute(
                HEARTBEAT.format(ids=placeholders), (expiry, *job_ids)
            )

    def _complete_sync(self, job: Job, result: bytes | None) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                COMPLETE_JOB, (now, result, job.id, job.worker_id)
            )
            if cursor.rowcount != 1:
                return False
            self._finish_execution(
                connection, job, ExecutionStatus.SUCCESS, None, now
            )
        return True

    def _fail_sync(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus,
    ) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            if retry_at is None:
                cursor = connection.execute(
                    FAIL_JOB, (now, error.summary, job.id, job.worker_id)
                )
            else:
                cursor = connection.execute(
                    RESCHEDULE_JOB, (retry_at, error.summary, job.id, job.worker_id)
                )
            if cursor.rowcount != 1:
                return False
            self._finish_execution(connection, job, execution_status, error, now)
        return True

    def _release_sync(self, job: Job, delay: float) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                RELEASE_JOB, (now + delay, job.id, job.worker_id)
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(
                MARK_EXECUTIONS_LOST.format(ids="?"), (now, job.id)
            )
        return True

    def _cancel_sync(self, job_id: str) -> bool:
        with self._connections.write() as connection:
            cursor = connection.execute(CANCEL_JOB, (self._clock.now(), job_id))
        return cursor.rowcount == 1

    def _requeue_sync(self, job_id: str) -> bool:
        with self._connections.write() as connection:
            cursor = connection.execute(REQUEUE_JOB, (self._clock.now(), job_id))
        return cursor.rowcount == 1

    def _get_sync(self, job_id: str) -> Job | None:
        row = self._connections.connection.execute(SELECT_JOB, (job_id,)).fetchone()
        return job_from_row(row) if row else None

    def _list_sync(
        self,
        status: JobStatus | None,
        queue: str | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        conditions: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            conditions.append("status = ?")
            parameters.append(str(status))
        if queue is not None:
            conditions.append("queue = ?")
            parameters.append(queue)
        if cursor is not None:
            conditions.append("id < ?")
            parameters.append(cursor)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = (
            f"SELECT {JOB_COLUMN_LIST} FROM jobs {where} ORDER BY id DESC LIMIT ?"
        )
        rows = self._connections.connection.execute(
            sql, (*parameters, limit + 1)
        ).fetchall()
        jobs = [job_from_row(row) for row in rows[:limit]]
        next_cursor = jobs[-1].id if len(rows) > limit and jobs else None
        return Page(items=jobs, cursor=next_cursor)

    def _executions_sync(self, job_id: str) -> list[Execution]:
        rows = self._connections.connection.execute(
            SELECT_EXECUTIONS, (job_id,)
        ).fetchall()
        return [execution_from_row(row) for row in rows]

    def _reap_expired_sync(self) -> int:
        now = self._clock.now()
        with self._connections.write() as connection:
            expired = self._select_expired(connection, now)
            if not expired:
                return 0
            placeholders = ", ".join("?" for _ in expired)
            connection.execute(
                MARK_EXECUTIONS_LOST.format(ids=placeholders), (now, *expired)
            )
        return len(expired)

    def _purge_sync(self, policy: RetentionPolicy, now: float) -> int | None:
        if not policy.enabled:
            return None
        with self._connections.write() as connection:
            won = connection.execute(
                CLAIM_HOUSEKEEPING, (str(now), now - policy.interval)
            ).fetchone()
        if won is None:
            return None
        removed = 0
        for status, cutoff in policy.horizons(now).items():
            while removed < policy.maximum_per_sweep:
                batch = min(policy.batch, policy.maximum_per_sweep - removed)
                with self._connections.write() as connection:
                    gone = connection.execute(
                        PURGE_BATCH, (str(status), cutoff, batch)
                    ).fetchall()
                removed += len(gone)
                if len(gone) < batch:
                    break
        return removed

    def _counts_by_status_sync(self) -> dict[str, int]:
        rows = self._connections.connection.execute(
            "SELECT status, count(*) AS total FROM jobs GROUP BY status"
        ).fetchall()
        return {row["status"]: row["total"] for row in rows}

    def _schedules_sync(self) -> list[ScheduleState]:
        rows = self._connections.connection.execute(SELECT_SCHEDULES).fetchall()
        return [schedule_from_row(row) for row in rows]

    def _register_schedules_sync(self, states: Sequence[ScheduleState]) -> None:
        with self._connections.write() as connection:
            connection.executemany(
                REGISTER_SCHEDULE,
                [
                    (state.name, state.cron, state.timezone, state.next_run_at)
                    for state in states
                ],
            )

    def _fire_schedule_sync(
        self,
        name: str,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        with self._connections.write() as connection:
            cursor = connection.execute(ADVANCE_SCHEDULE, (next_run_at, name, expected))
            if cursor.rowcount != 1:
                return None
            stored = [self._insert(connection, job) for job in jobs]
            if stored:
                connection.execute(
                    RECORD_SCHEDULE_RUN, (self._clock.now(), stored[-1].id, name)
                )
        return stored

    def _select_expired(
        self, connection: sqlite3.Connection, now: float
    ) -> list[str]:
        if SUPPORTS_RETURNING:
            rows = connection.execute(REAP_EXPIRED, (now, now, now)).fetchall()
            return [row["id"] for row in rows]
        rows = connection.execute(SELECT_EXPIRED_IDS, (now,)).fetchall()
        for row in rows:
            exhausted = row["attempts"] >= row["max_attempts"]
            connection.execute(
                "UPDATE jobs SET status = ?, run_at = ?, finished_at = ?, "
                "lease_expires_at = NULL, worker_id = NULL, last_error = ? "
                "WHERE id = ?",
                (
                    "failed" if exhausted else "queued",
                    now,
                    now if exhausted else None,
                    LEASE_EXPIRED_MESSAGE,
                    row["id"],
                ),
            )
        return [row["id"] for row in rows]

    def _finish_execution(
        self,
        connection: sqlite3.Connection,
        job: Job,
        status: ExecutionStatus,
        error: ExecutionError | None,
        now: float,
    ) -> None:
        connection.execute(
            FINISH_EXECUTION,
            (
                str(status),
                now,
                error.type if error else None,
                error.traceback if error else None,
                job.id,
                job.attempts,
            ),
        )

    def _assert_schema_supported(self) -> None:
        row = self._connections.connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        stored = int(row["value"]) if row else SCHEMA_VERSION
        if stored > SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema version {stored} is newer than this tinyjobs "
                f"build understands ({SCHEMA_VERSION}); upgrade the package"
            )
