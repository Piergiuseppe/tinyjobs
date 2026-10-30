from __future__ import annotations

import asyncio
import sqlite3
import uuid
from contextlib import AbstractContextManager, suppress
from functools import partial
from pathlib import Path
from typing import Any, Callable, Sequence, TypeVar
from urllib.parse import urlsplit

from ...clock import Clock
from ...models import (
    Execution,
    ExecutionError,
    Job,
    Page,
    QueueStats,
    ScheduleState,
    Stats,
)
from ...retention import RetentionPolicy
from ...exceptions import AppClosedError
from ...runtime import CountingPool, pause_threads_at_fork, refuse_interrupted
from ...sheltered import sheltered
from ...status import ExecutionStatus, JobStatus
from ..base import LEASE_EXPIRED_MESSAGE, BaseBackend, Capabilities
from .claim import SUPPORTS_RETURNING, build_claim_strategy
from .connection import FORK_GATE, ConnectionPool, StepAside
from .queries import (
    ADVANCE_SCHEDULE,
    CANCEL_EXECUTIONS,
    CANCEL_JOB,
    CLAIM_DUE_ONE,
    CLAIM_HOUSEKEEPING,
    COMPLETE_JOB,
    COUNT_BY_QUEUE,
    COUNT_BY_STATUS,
    COUNT_EXPIRED_LEASES,
    DELETE_EXECUTION,
    FAIL_JOB,
    FINISH_EXECUTION,
    HAND_BACK_JOB,
    HEARTBEAT,
    RENEWED,
    INSERT_EXECUTION,
    INSERT_JOB,
    MARK_EXECUTIONS_LOST,
    OLDEST_DUE_BY_QUEUE,
    PURGE_BATCH,
    QUEUED_BY_TASK,
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
from .schema import (
    DDL,
    DROP_LEGACY_INDEX,
    LEGACY_INDEX_IS_UNIQUE,
    MIGRATIONS,
    SCHEMA_VERSION,
)

T = TypeVar("T")

MEMORY_TEMPLATE = "file:tinyjobs-{token}?mode=memory&cache=shared"

EXECUTOR_GONE_MESSAGE = (
    "this SQLite backend was closed or the interpreter is shutting down, so it "
    "takes no more calls"
)

HELD_TRANSACTION_MESSAGE = (
    "this tinyjobs call writes to the queue, whose write lock your own open "
    "backend.transaction() holds, so it could only wait for that lock and fail; "
    "make the call after the transaction ends"
)

RETIRE_SECONDS = 1.0
CANCELLED_CALL_SECONDS = 0.5

STEPPED_ASIDE = object()

RETURNING_CALLS = frozenset(
    {
        "_complete_sync",
        "_fail_sync",
        "_release_sync",
        "_release_many_sync",
        "_hand_back_sync",
        "_heartbeat_sync",
        "_close_sync",
    }
)

LOCK_FREE_CALLS = frozenset(
    {
        "_get_sync",
        "_list_sync",
        "_executions_sync",
        "_counts_by_status_sync",
        "_stats_sync",
        "_schedules_sync",
        "_close_sync",
    }
)


class SQLiteBackend(BaseBackend):
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=False,
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
        self._executor = _executor()
        self._closed = False
        pause_threads_at_fork(self)

    async def _call(self, function, *args):
        refuse_interrupted(function.__name__.strip("_"))
        if function.__name__ not in LOCK_FREE_CALLS:
            self.refuse_held_transaction()
        while True:
            executor = self._executor
            try:
                running = executor.submit(_gated, executor, function, *args)
            except RuntimeError as error:
                if executor.retiring:
                    continue
                if self._closed or function.__name__ not in RETURNING_CALLS:
                    raise AppClosedError(EXECUTOR_GONE_MESSAGE) from error
                return _gated(None, function, *args)
            pending = asyncio.wrap_future(running)
            try:
                result = await sheltered(pending)
            except asyncio.CancelledError:
                pending.add_done_callback(_retrieved)
                if not running.cancel():
                    await asyncio.wait({pending}, timeout=CANCELLED_CALL_SECONDS)
                raise
            if result is not STEPPED_ASIDE:
                return result

    async def setup(self) -> None:
        await self._call(self._setup_sync)

    async def close(self) -> None:
        try:
            await self._call(self._close_sync)
        except BaseException:
            self._closed = True
            with suppress(RuntimeError):
                self._executor.submit(_gated, None, self._close_sync)
            self._executor.shutdown(wait=False)
            raise
        self._closed = True
        self._executor.shutdown(wait=True)

    def pause_for_fork(self) -> None:
        if self._executor.idle:
            self.step_aside_for_fork()

    def step_aside_for_fork(self) -> None:
        if self._closed or not self._executor.started:
            return
        retired, self._executor = self._executor, _executor()
        retired.retiring = True
        retired.submit(self._connections.hand_over)
        retired.shutdown(wait=False)
        retired.join(RETIRE_SECONDS)

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

    async def claim_job(
        self, job_id: str, *, worker_id: str, lease_seconds: float
    ) -> Job | None:
        return await self._call(self._claim_job_sync, job_id, worker_id, lease_seconds)

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> list[str]:
        return await self._call(
            self._heartbeat_sync, tuple(job_ids), lease_seconds, worker_id
        )

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

    async def release_many(self, jobs: Sequence[Job]) -> int:
        if not jobs:
            return 0
        return await self._call(self._release_many_sync, tuple(jobs))

    async def hand_back(self, job: Job, delay: float = 0.0, *, in_place: bool = False) -> bool:
        return await self._call(self._hand_back_sync, job, delay, in_place)

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

    async def stats(self, now: float) -> Stats:
        return await self._call(self._stats_sync, now)

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
            if existing is not None:
                _refuse_newer(existing)
                self._migrate(connection, existing)
            _drop_unique_legacy_index(connection)
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

    def refuse_held_transaction(self) -> None:
        if self._connections.held_here():
            raise RuntimeError(HELD_TRANSACTION_MESSAGE)

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
                    INSERT_EXECUTION, (job.id, job.attempts, job.claims, worker_id, now)
                )
        return jobs

    def _claim_job_sync(
        self, job_id: str, worker_id: str, lease_seconds: float
    ) -> Job | None:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                CLAIM_DUE_ONE, (worker_id, now + lease_seconds, job_id, now)
            )
            if cursor.rowcount != 1:
                return None
            job = job_from_row(connection.execute(SELECT_JOB, (job_id,)).fetchone())
            connection.execute(
                INSERT_EXECUTION, (job.id, job.attempts, job.claims, worker_id, now)
            )
        return job

    def _heartbeat_sync(
        self, job_ids: Sequence[str], lease_seconds: float, worker_id: str | None
    ) -> list[str]:
        if not job_ids:
            return []
        placeholders = ", ".join("?" for _ in job_ids)
        expiry = self._clock.now() + lease_seconds
        with self._connections.write() as connection:
            connection.execute(
                HEARTBEAT.format(ids=placeholders),
                (expiry, worker_id, worker_id, *job_ids),
            )
            return [
                row[0]
                for row in connection.execute(
                    RENEWED.format(ids=placeholders),
                    (expiry, worker_id, worker_id, *job_ids),
                )
            ]

    def _complete_sync(self, job: Job, result: bytes | None) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                COMPLETE_JOB, (now, result, job.id, job.worker_id, job.claims)
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
                    FAIL_JOB, (now, error.summary, job.id, job.worker_id, job.claims)
                )
            else:
                cursor = connection.execute(
                    RESCHEDULE_JOB,
                    (retry_at, error.summary, job.id, job.worker_id, job.claims),
                )
            if cursor.rowcount != 1:
                return False
            self._finish_execution(connection, job, execution_status, error, now)
        return True

    def _release_sync(self, job: Job, delay: float) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                RELEASE_JOB, (now + delay, job.id, job.worker_id, job.claims)
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(
                MARK_EXECUTIONS_LOST.format(ids="?"), (now, job.id)
            )
        return True

    def _release_many_sync(self, jobs: Sequence[Job]) -> int:
        now = self._clock.now()
        released = 0
        with self._connections.write() as connection:
            for job in jobs:
                cursor = connection.execute(
                    RELEASE_JOB, (now, job.id, job.worker_id, job.claims)
                )
                if cursor.rowcount == 1:
                    connection.execute(MARK_EXECUTIONS_LOST.format(ids="?"), (now, job.id))
                    released += 1
        return released

    def _hand_back_sync(self, job: Job, delay: float, in_place: bool) -> bool:
        now = self._clock.now()
        run_at = min(job.run_at, now) if in_place else now + delay
        with self._connections.write() as connection:
            cursor = connection.execute(
                HAND_BACK_JOB, (run_at, job.id, job.worker_id, job.claims)
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(DELETE_EXECUTION, (job.id, job.claims))
        return True

    def _cancel_sync(self, job_id: str) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(CANCEL_JOB, (now, job_id))
            if cursor.rowcount != 1:
                return False
            connection.execute(CANCEL_EXECUTIONS, (now, job_id))
        return True

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
        rows = self._connections.connection.execute(COUNT_BY_STATUS).fetchall()
        return {row["status"]: row["total"] for row in rows}

    def _stats_sync(self, now: float) -> Stats:
        connection = self._connections.connection
        counts: dict[str, dict[str, int]] = {}
        for row in connection.execute(COUNT_BY_QUEUE).fetchall():
            counts.setdefault(row["queue"], {})[row["status"]] = row["total"]
        oldest = {
            row["queue"]: row["oldest"]
            for row in connection.execute(OLDEST_DUE_BY_QUEUE, (now,)).fetchall()
        }
        expired = connection.execute(COUNT_EXPIRED_LEASES, (now,)).fetchone()
        queued = connection.execute(QUEUED_BY_TASK).fetchall()
        return Stats(
            queues=[
                QueueStats(queue, counts[queue], oldest.get(queue))
                for queue in sorted(counts)
            ],
            expired_leases=expired["total"],
            queued_tasks={row["task"]: row["total"] for row in queued},
        )

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
                "lease_expires_at = NULL, worker_id = NULL, hand_backs = 0, "
                "last_error = ? "
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
                job.claims,
            ),
        )

    def _assert_schema_supported(self) -> None:
        row = self._connections.connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        _refuse_newer(int(row["value"]) if row else SCHEMA_VERSION)


def _refuse_newer(stored: int) -> None:
    if stored > SCHEMA_VERSION:
        raise RuntimeError(
            f"database schema version {stored} is newer than this tinyjobs "
            f"build understands ({SCHEMA_VERSION}); upgrade the package"
        )


def _drop_unique_legacy_index(connection: sqlite3.Connection) -> None:
    if connection.execute(LEGACY_INDEX_IS_UNIQUE).fetchone() is not None:
        connection.execute(DROP_LEGACY_INDEX)


def _executor() -> CountingPool:
    return CountingPool(max_workers=1, thread_name_prefix="tinyjobs-sqlite")


def _retrieved(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()


def _gated(pool: CountingPool | None, function: Callable[..., T], *args: Any) -> T:
    if pool is None:
        with FORK_GATE.using():
            return function(*args)
    if pool.retiring:
        return STEPPED_ASIDE
    try:
        with FORK_GATE.using(partial(getattr, pool, "retiring")):
            return function(*args)
    except StepAside:
        return STEPPED_ASIDE
