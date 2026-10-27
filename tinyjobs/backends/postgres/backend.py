from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Sequence, TypeVar

import psycopg
from psycopg import errors
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from ...clock import Clock, FollowingClock, elapsed
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
from ...status import ExecutionStatus, JobStatus
from ..base import LEASE_EXPIRED_MESSAGE, BaseBackend, Capabilities
from .queries import Queries
from .records import (
    JOB_COLUMNS,
    execution_from_row,
    job_arrays,
    job_from_row,
    schedule_from_row,
)
from .schema import SCHEMA_VERSION, Tables

DEFAULT_PREFIX = "tinyjobs"
DEFAULT_MIN_CONNECTIONS = 1
DEFAULT_MAX_CONNECTIONS = 10
DEFAULT_POOL_TIMEOUT = 20.0
ENQUEUE_BATCH = 1000
INSERT_ROUNDS = 5
RETRIED_ERRORS = (errors.DeadlockDetected, errors.SerializationFailure)
RETRIES = 3

T = TypeVar("T")


class PostgresBackend(BaseBackend):
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=False,
        retention=True,
        schedules=True,
        max_payload_bytes=None,
        loop_bound=True,
    )

    def __init__(
        self,
        conninfo: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        schema: str | None = None,
        clock: Clock | None = None,
        min_connections: int = DEFAULT_MIN_CONNECTIONS,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        pool_timeout: float = DEFAULT_POOL_TIMEOUT,
        notify: bool = True,
        **connection_options: Any,
    ) -> None:
        self._conninfo = conninfo
        self._tables = Tables(prefix, schema)
        self._queries = Queries(self._tables)
        self._clock = FollowingClock() if clock is None else clock
        self._following = isinstance(self._clock, FollowingClock)
        self._reading = asyncio.Lock()
        self._read = False
        self._notify = notify
        self._pool_timeout = pool_timeout
        self._pool = AsyncConnectionPool(
            conninfo,
            min_size=min(min_connections, max_connections),
            max_size=max_connections,
            timeout=pool_timeout,
            open=False,
            kwargs={"autocommit": True, "row_factory": dict_row, **connection_options},
            name="tinyjobs",
        )

    @classmethod
    def from_url(
        cls, url: str, *, clock: Clock | None = None, **options: Any
    ) -> "PostgresBackend":
        return cls(url, clock=clock, **options)

    async def setup(self) -> None:
        await self._forgetting(self._pool.open(wait=True, timeout=self._pool_timeout))
        async with self._transaction() as connection:
            await connection.execute(
                "SELECT pg_advisory_xact_lock(%(key)s)", {"key": self._tables.lock}
            )
            stored = await self._stored_version(connection)
            if stored is not None and stored > SCHEMA_VERSION:
                raise RuntimeError(
                    f"postgres schema version {stored} is newer than this tinyjobs "
                    f"build understands ({SCHEMA_VERSION}); upgrade the package"
                )
            for statement in self._tables.ddl():
                await connection.execute(statement)

    async def close(self) -> None:
        try:
            await self._pool.close()
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise

    async def follow_clock(self) -> None:
        await self._now()

    async def enqueue(self, job: Job) -> Job:
        return (await self.enqueue_many([job]))[0]

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        if not jobs:
            return []
        now = await self._now()
        if len(jobs) <= ENQUEUE_BATCH:
            async with self._connection() as connection:
                return await self._insert(connection, jobs, now)
        async with self._transaction() as connection:
            stored: list[Job] = []
            for start in range(0, len(jobs), ENQUEUE_BATCH):
                batch = jobs[start : start + ENQUEUE_BATCH]
                stored.extend(await self._insert(connection, batch, now))
            return stored

    async def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]:
        if limit <= 0 or not queues:
            return []
        now = await self._now()
        rows = await self._retrying(
            self._queries.claim,
            self._at(
                now,
                queues=list(dict.fromkeys(queues)),
                limit=limit,
                worker=worker_id,
                lease=now + lease_seconds,
                seconds=lease_seconds,
            ),
        )
        return [job_from_row(row) for row in rows]

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> None:
        if not job_ids:
            return
        now = await self._now()
        await self._retrying(
            self._queries.heartbeat,
            self._at(
                now,
                ids=list(job_ids),
                worker=worker_id,
                lease=now + lease_seconds,
                seconds=lease_seconds,
            ),
            rows=False,
        )

    async def complete(self, job: Job, result: bytes | None) -> bool:
        return await self._changed(
            self._queries.complete,
            await self._finishing(job, ExecutionStatus.SUCCESS, None, result=result),
        )

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        parameters = await self._finishing(
            job, execution_status, error, summary=error.summary, run_at=retry_at
        )
        query = self._queries.fail if retry_at is None else self._queries.reschedule
        return await self._changed(query, parameters)

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        now = await self._now()
        return await self._changed(
            self._queries.release, self._held(job, now, run_at=now + delay)
        )

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        now = await self._now()
        return await self._changed(
            self._queries.hand_back, self._held(job, now, run_at=now + delay)
        )

    async def cancel(self, job_id: str) -> bool:
        now = await self._now()
        return await self._changed(self._queries.cancel, self._at(now, id=job_id))

    async def requeue(self, job_id: str) -> bool:
        now = await self._now()
        try:
            async with self._transaction() as connection:
                cursor = await connection.execute(
                    self._queries.requeue, self._at(now, id=job_id)
                )
                row = await cursor.fetchone()
                if row is not None and self._notify and row["run_at"] <= now:
                    await connection.execute(
                        self._queries.notify,
                        {"channel": self._tables.channel, "queue": row["queue"]},
                    )
        except errors.UniqueViolation:
            return False
        return row is not None

    async def get(self, job_id: str) -> Job | None:
        rows = await self._rows(self._queries.get, {"id": job_id})
        return job_from_row(rows[0]) if rows else None

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        conditions: list[str] = []
        parameters: dict[str, Any] = {"limit": limit + 1}
        if status is not None:
            conditions.append("status = %(status)s")
            parameters["status"] = str(status)
        if queue is not None:
            conditions.append("queue = %(queue)s")
            parameters["queue"] = queue
        if cursor is not None:
            conditions.append("id < %(cursor)s")
            parameters["cursor"] = cursor
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = await self._rows(self._queries.list.format(where=where), parameters)
        jobs = [job_from_row(row) for row in rows[:limit]]
        next_cursor = jobs[-1].id if len(rows) > limit and jobs else None
        return Page(items=jobs, cursor=next_cursor)

    async def executions(self, job_id: str) -> list[Execution]:
        rows = await self._rows(self._queries.executions, {"id": job_id})
        return [execution_from_row(row) for row in rows]

    async def reap_expired(self) -> int:
        now = await self._now()
        rows = await self._retrying(
            self._queries.reap, self._at(now, message=LEASE_EXPIRED_MESSAGE)
        )
        return rows[0]["reaped"]

    async def counts_by_status(self) -> dict[str, int]:
        rows = await self._rows(self._queries.count_by_status, {})
        return {row["status"]: row["total"] for row in rows}

    async def stats(self, now: float) -> Stats:
        queries = self._queries
        async with self._connection() as connection:
            counted = await _fetch(connection, queries.count_by_queue, {})
            oldest = await _fetch(connection, queries.oldest_due_by_queue, {"now": now})
            expired = await _fetch(connection, queries.count_expired, {"now": now})
            queued = await _fetch(connection, queries.queued_by_task, {})
        counts: dict[str, dict[str, int]] = {}
        for row in counted:
            counts.setdefault(row["queue"], {})[row["status"]] = row["total"]
        due = {row["queue"]: row["oldest"] for row in oldest}
        return Stats(
            queues=[
                QueueStats(queue, counts[queue], due.get(queue)) for queue in sorted(counts)
            ],
            expired_leases=expired[0]["total"],
            queued_tasks={row["task"]: row["total"] for row in queued},
        )

    async def purge(self, policy: RetentionPolicy, now: float) -> int | None:
        if not policy.enabled:
            return None
        won = await self._rows(
            self._queries.claim_sweep,
            {"now": repr(float(now)), "cutoff": now - policy.interval},
        )
        if not won:
            return None
        removed = 0
        for status, cutoff in policy.horizons(now).items():
            while removed < policy.maximum_per_sweep:
                batch = min(policy.batch, policy.maximum_per_sweep - removed)
                rows = await self._retrying(
                    self._queries.purge_batch,
                    {"status": str(status), "cutoff": cutoff, "batch": batch},
                )
                gone = rows[0]["removed"]
                removed += gone
                if gone < batch:
                    break
        return removed

    async def schedules(self) -> list[ScheduleState]:
        await self._now()
        rows = await self._rows(self._queries.schedules, {})
        return [schedule_from_row(row) for row in rows]

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None:
        if not states:
            return
        await self._rows(
            self._queries.register_schedules,
            {
                "names": [state.name for state in states],
                "crons": [state.cron for state in states],
                "timezones": [state.timezone for state in states],
                "next_runs": [state.next_run_at for state in states],
            },
            rows=False,
        )

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        now = await self._now()
        async with self._transaction() as connection:
            cursor = await connection.execute(
                self._queries.advance_schedule,
                {"name": name, "expected": expected, "next_run_at": next_run_at},
            )
            if cursor.rowcount != 1:
                return None
            stored = await self._insert(connection, jobs, now) if jobs else []
            if stored:
                await connection.execute(
                    self._queries.record_schedule_run,
                    {"now": now, "job_id": stored[-1].id, "name": name},
                )
        return stored

    async def _insert(
        self, connection: psycopg.AsyncConnection, jobs: Sequence[Job], now: float
    ) -> list[Job]:
        resolved: dict[int, Job] = {}
        pending = list(enumerate(jobs))
        for _ in range(INSERT_ROUNDS):
            batch = [job for _, job in pending]
            parameters = dict(zip(JOB_COLUMNS, job_arrays(batch)))
            inserted = {
                row["id"]
                for row in await _fetch(
                    connection,
                    self._queries.insert,
                    {
                        **parameters,
                        "channel": self._tables.channel,
                        "notify": self._notify,
                        "now": now,
                    },
                )
            }
            missing = [(index, job) for index, job in pending if job.id not in inserted]
            for index, job in pending:
                if job.id in inserted:
                    resolved[index] = job
            if not missing:
                break
            pending = await self._holders(connection, missing, resolved)
            if not pending:
                break
        else:
            raise RuntimeError(
                "enqueue kept meeting dedup holders that finished before they could "
                "be read; try again"
            )
        return [resolved[index] for index in range(len(jobs))]

    async def _holders(
        self,
        connection: psycopg.AsyncConnection,
        missing: list[tuple[int, Job]],
        resolved: dict[int, Job],
    ) -> list[tuple[int, Job]]:
        existing = {
            row["id"]: job_from_row(row)
            for row in await _fetch(
                connection, self._queries.by_ids, {"ids": [job.id for _, job in missing]}
            )
        }
        keys = [job.dedup_key for _, job in missing if job.id not in existing and job.dedup_key]
        holders = (
            {
                row["dedup_key"]: job_from_row(row)
                for row in await _fetch(
                    connection, self._queries.active_by_keys, {"keys": keys}
                )
            }
            if keys
            else {}
        )
        retry = []
        for index, job in missing:
            found = existing.get(job.id) or holders.get(job.dedup_key)
            if found is None:
                retry.append((index, job))
            else:
                resolved[index] = found
        return retry

    async def _finishing(
        self,
        job: Job,
        execution_status: ExecutionStatus,
        error: ExecutionError | None,
        **extra: Any,
    ) -> dict[str, Any]:
        now = await self._now()
        return self._held(
            job,
            now,
            execution_status=str(execution_status),
            error_type=error.type if error else None,
            traceback=error.traceback if error else None,
            **extra,
        )

    def _held(self, job: Job, now: float, **extra: Any) -> dict[str, Any]:
        return self._at(
            now, id=job.id, worker=job.worker_id, claims=job.claims, **extra
        )

    def _at(self, now: float, **extra: Any) -> dict[str, Any]:
        return {"now": now, "following": self._following, **extra}

    async def _changed(self, query: str, parameters: dict[str, Any]) -> bool:
        rows = await self._retrying(query, parameters)
        return rows[0]["changed"] == 1

    async def _retrying(
        self, query: str, parameters: dict[str, Any], *, rows: bool = True
    ) -> list[dict[str, Any]]:
        for attempt in range(RETRIES):
            try:
                return await self._rows(query, parameters, rows=rows)
            except RETRIED_ERRORS:
                if attempt == RETRIES - 1:
                    raise
        raise AssertionError("unreachable")

    async def _rows(
        self, query: str, parameters: dict[str, Any], *, rows: bool = True
    ) -> list[dict[str, Any]]:
        async with self._connection() as connection:
            if not rows:
                await connection.execute(query, parameters)
                return []
            return await _fetch(connection, query, parameters)

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[psycopg.AsyncConnection]:
        try:
            async with self._pool.connection() as connection:
                yield connection
        except (psycopg.OperationalError, PoolTimeout):
            self._forget()
            raise

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[psycopg.AsyncConnection]:
        async with self._connection() as connection:
            async with connection.transaction():
                yield connection

    async def _stored_version(self, connection: psycopg.AsyncConnection) -> int | None:
        present = await _fetch(
            connection, self._queries.meta_exists, {"name": self._tables.meta}
        )
        if not present[0]["present"]:
            return None
        rows = await _fetch(connection, self._queries.schema_version, {})
        return int(rows[0]["value"]) if rows else None

    async def _now(self) -> float:
        clock = self._clock
        if isinstance(clock, FollowingClock) and (not self._read or clock.due()):
            async with self._reading:
                if not self._read or clock.due():
                    clock.follow(*await self._forgetting(self._timed()), self._server())
                    self._read = True
        return clock.now()

    async def _timed(self) -> tuple[float, float, float]:
        async with self._connection() as connection:
            started = elapsed()
            cursor = await connection.execute(self._queries.server_time)
            row = await cursor.fetchone()
            return row["now"], started, elapsed()

    async def _forgetting(self, call: Awaitable[T]) -> T:
        try:
            return await call
        except (psycopg.OperationalError, PoolTimeout):
            self._forget()
            raise

    def _forget(self) -> None:
        if isinstance(self._clock, FollowingClock):
            self._clock.forget()

    def _server(self) -> str:
        try:
            settings = conninfo_to_dict(self._conninfo)
        except psycopg.ProgrammingError:
            settings = {}
        place = settings.get("host") or "local socket"
        return f"postgres {place}:{settings.get('port', 5432)} db {settings.get('dbname', '')}"


async def _fetch(
    connection: psycopg.AsyncConnection, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    cursor = await connection.execute(query, parameters)
    return await cursor.fetchall()
