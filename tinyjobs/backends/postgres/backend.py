from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import weakref
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from typing import Any, AsyncIterator, Awaitable, Generator, Sequence, TypeVar

import psycopg
from psycopg import errors
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolClosed, PoolTimeout

from ...clock import Clock, FollowingClock, elapsed
from ...exceptions import ConfigurationError
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
from ...runtime import spared_until_close
from ...status import ExecutionStatus, JobStatus
from ..base import LEASE_EXPIRED_MESSAGE, BaseBackend, Capabilities
from .queries import NOTICE_CHARACTERS, Queries
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
DEFAULT_STATEMENT_TIMEOUT = 30.0
SHORTEST_STATEMENT_SECONDS = 0.001
LONGEST_STATEMENT_SECONDS = (2**31 - 1) / 1000
IDLE_IN_TRANSACTION_SECONDS = 60
SETUP_LOCK_SECONDS = 5
MAX_IDLE_SECONDS = 300.0
MAX_LIFETIME_SECONDS = 1800.0
NOTIFY_SECONDS = 0.05
LISTEN_RETRY_SECONDS = 1.0
KEEPALIVES = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}
ENQUEUE_BATCH = 1000
INSERT_ROUNDS = 50
RETRIED_ERRORS = (errors.DeadlockDetected, errors.SerializationFailure)
RETRIES = 3
WAKE_ERRORS = (psycopg.Error, PoolTimeout)

logger = logging.getLogger("tinyjobs.postgres")

T = TypeVar("T")
Steps = Generator[tuple[str, dict[str, Any]], list[dict[str, Any]], T]


class PostgresBackend(BaseBackend):
    transient_errors = (psycopg.OperationalError, PoolTimeout)
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=True,
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
        statement_timeout: float | None = DEFAULT_STATEMENT_TIMEOUT,
        notify: bool = True,
        **connection_options: Any,
    ) -> None:
        if statement_timeout is not None and not (
            SHORTEST_STATEMENT_SECONDS <= statement_timeout <= LONGEST_STATEMENT_SECONDS
        ):
            raise ConfigurationError(
                f"statement_timeout must be None or between {SHORTEST_STATEMENT_SECONDS} "
                f"and {LONGEST_STATEMENT_SECONDS:g} seconds, not {statement_timeout!r}"
            )
        self._conninfo = conninfo
        self._tables = Tables(prefix, schema)
        self._clock = FollowingClock() if clock is None else clock
        self._following = isinstance(self._clock, FollowingClock)
        self._queries = Queries(self._tables, self._following)
        self._reading = asyncio.Lock()
        self._read = False
        self._notify = notify
        self._woken: dict[str, float] = {}
        self._waiting: set[str] = set()
        self._waking: asyncio.Task | None = None
        self._heard: dict[str, int] = {}
        self._listened: set[str] = set()
        self._hearing = 0
        self._marks: dict[str, int] = {}
        self._waiters: dict[asyncio.Event, frozenset[str]] = {}
        self._listening: asyncio.Task | None = None
        self._connection_options = connection_options
        self._joined: weakref.WeakKeyDictionary[Any, bool] = weakref.WeakKeyDictionary()
        self._unplaced = self._queries
        self._identity: tuple[Any, ...] | None = None
        self._channel: str | None = None
        self._statement_timeout = statement_timeout
        self._pool_timeout = pool_timeout
        self._opening: asyncio.Future[None] | None = None
        self._closing: asyncio.Future[None] | None = None
        self._closed = False
        self._spent = False
        self._sizes = (min(min_connections, max_connections), max_connections)
        self._pool = self._new_pool()

    def _new_pool(self) -> AsyncConnectionPool:
        smallest, largest = self._sizes
        return AsyncConnectionPool(
            self._conninfo,
            min_size=smallest,
            max_size=largest,
            timeout=self._pool_timeout,
            max_idle=MAX_IDLE_SECONDS,
            max_lifetime=MAX_LIFETIME_SECONDS,
            open=False,
            kwargs=_connection_kwargs(self._conninfo, self._connection_options),
            configure=self._configure,
            name="tinyjobs",
        )

    @classmethod
    def from_url(
        cls, url: str, *, clock: Clock | None = None, **options: Any
    ) -> "PostgresBackend":
        return cls(url, clock=clock, **options)

    async def setup(self) -> None:
        await self._open()
        async with self._connection() as connection:
            stored = await self._stored_version(connection)
        if stored != SCHEMA_VERSION:
            await self._create()
        await self._place()
        await self._now()

    async def _place(self) -> None:
        tables = self._tables
        names = {
            "jobs": tables.jobs,
            "executions": tables.executions,
            "schedules": tables.schedules,
            "meta": tables.meta,
        }
        async with self._connection() as connection:
            placed = await _fetch(connection, self._unplaced.placement, names)
            if len(placed) > 1:
                raise ConfigurationError(
                    f"the tinyjobs tables for prefix {tables.prefix!r} resolve to more "
                    "than one schema on this connection's search_path; pass schema= "
                    "to name the one to use"
                )
            if not placed or placed[0]["tables"] != len(names):
                raise ConfigurationError(
                    f"some tinyjobs tables for prefix {tables.prefix!r} are missing "
                    f"although {tables.meta} records schema version {SCHEMA_VERSION}; "
                    "restore them or drop the rest so setup creates them again"
                )
            if not placed[0]["owned"]:
                raise ConfigurationError(
                    f"the tinyjobs tables for prefix {tables.prefix!r} or their schema "
                    "belong to a role that does not hold the connecting user's "
                    "privileges; tinyjobs only uses tables owned by its own role, a role "
                    "that has its privileges, or the database owner"
                )
            queries = Queries(tables.within(placed[0]["schema"]), self._following)
            rows = await _fetch(connection, queries.identity, {})
            if not rows:
                await connection.execute(tables.instance())
                rows = await _fetch(connection, queries.identity, {})
        self._identity = _identity_of(rows[0])
        self._channel = f"tinyjobs_{hashlib.sha256(rows[0]['instance'].encode()).hexdigest()[:24]}"
        self._queries = queries

    async def _create(self) -> None:
        async with self._transaction() as connection:
            await connection.execute(
                f"SET LOCAL lock_timeout = '{SETUP_LOCK_SECONDS}s'"
            )
            await connection.execute(
                "SELECT pg_advisory_xact_lock(%(key)s)", {"key": self._tables.lock}
            )
            stored = await self._stored_version(connection)
            if stored is not None and stored > SCHEMA_VERSION:
                raise RuntimeError(
                    f"postgres schema version {stored} is newer than this tinyjobs "
                    f"build understands ({SCHEMA_VERSION}); upgrade the package"
                )
            if stored == SCHEMA_VERSION:
                return
            for statement in self._tables.ddl():
                await connection.execute(statement)

    async def _open(self) -> None:
        failed = self._opening
        if failed is not None and failed.done() and not self._closed and (
            failed.cancelled() or failed.exception() is not None
        ):
            self._opening, self._spent = None, self._pool.closed
        if self._opening is None:
            if self._closed:
                raise PoolClosed(f"the pool {self._pool.name!r} is closed")
            if self._spent:
                self._pool, self._spent = self._new_pool(), False
            with spared_until_close():
                self._opening = asyncio.ensure_future(
                    self._pool.open(wait=True, timeout=self._pool_timeout)
                )
            self._opening.add_done_callback(_settled)
        opening = self._opening
        try:
            await self._forgetting(asyncio.shield(opening))
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if self._closed and opening.cancelled() and not (current and current.cancelling()):
                raise PoolClosed(f"the pool {self._pool.name!r} is closed") from None
            raise
        except BaseException:
            if self._opening is opening:
                self._opening = None
                self._spent = self._pool.closed
            raise

    async def close(self) -> None:
        self._closed = True
        if self._closing is None:
            with spared_until_close():
                self._closing = asyncio.ensure_future(self._shut())
            self._closing.add_done_callback(_settled)
        await asyncio.shield(self._closing)

    async def _shut(self) -> None:
        waking, self._waking = self._waking, None
        if waking is not None:
            waking.cancel()
        opening = self._opening
        if opening is not None and not opening.done():
            opening.cancel()
            await asyncio.wait({opening})
        listening, self._listening = self._listening, None
        if listening is not None:
            listening.cancel()
            await asyncio.wait({listening})
        for event in self._waiters:
            event.set()
        await self._pool.close()

    async def wait_for_jobs(
        self, *, worker_id: str, queues: Sequence[str], timeout: float
    ) -> None:
        if not self._notify or self._closed or self._channel is None:
            await asyncio.sleep(timeout)
            return
        if self._listening is None:
            with spared_until_close():
                self._listening = asyncio.create_task(
                    self._listen(), name="tinyjobs-postgres-listen"
                )
        wanted = frozenset(_noticed(queue) for queue in queues)
        self._listened.update(wanted)
        mark = self._marks.get(worker_id, 0)
        if any(self._heard.get(queue, 0) > mark for queue in wanted):
            return
        event = asyncio.Event()
        self._waiters[event] = wanted
        try:
            with suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout)
        finally:
            self._waiters.pop(event, None)

    async def _listen(self) -> None:
        options = {
            key: value
            for key, value in _connection_kwargs(self._conninfo, self._connection_options).items()
            if key != "row_factory"
        }
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(
                    self._conninfo, **options
                ) as connection:
                    await connection.execute(f'LISTEN "{self._channel}"')
                    self._hear(None)
                    async for notice in connection.notifies():
                        self._hear(notice.payload)
            except psycopg.Error as error:
                logger.debug("listening for new jobs failed: %s", error)
            self._hear(None)
            await asyncio.sleep(LISTEN_RETRY_SECONDS)

    def _hear(self, queue: str | None) -> None:
        self._hearing += 1
        for event, wanted in self._waiters.items():
            if queue is None or queue in wanted:
                event.set()
        if queue is not None and queue in self._listened:
            self._heard[queue] = self._hearing

    async def follow_clock(self) -> None:
        await self._now()

    async def enqueue(self, job: Job) -> Job:
        return (await self.enqueue_many([job]))[0]

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        if not jobs:
            return []
        now = await self._now()
        due: set[str] = set()
        ordered, places, copies = _lock_order(jobs)
        for attempt in range(RETRIES):
            try:
                if len(ordered) <= ENQUEUE_BATCH:
                    async with self._connection() as connection:
                        stored = await self._insert(connection, ordered, now, due)
                else:
                    async with self._transaction() as connection:
                        stored = []
                        for start in range(0, len(ordered), ENQUEUE_BATCH):
                            batch = ordered[start : start + ENQUEUE_BATCH]
                            stored.extend(await self._insert(connection, batch, now, due))
                break
            except RETRIED_ERRORS as error:
                if attempt == RETRIES - 1:
                    raise
                logger.warning("retrying a postgres enqueue after %s", error)
        await self._wake(due)
        return _in_order(places, copies, len(jobs), stored)

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
        self._marks[worker_id] = self._hearing
        self._listened.update(_noticed(queue) for queue in queues)
        wanted = list(dict.fromkeys(queues))
        now = await self._now()
        rows = await self._retrying(
            *self._claiming(now, wanted, worker_id, limit, lease_seconds)
        )
        return [job_from_row(row) for row in rows]

    def _claiming(
        self,
        now: float,
        wanted: list[str],
        worker_id: str,
        limit: int,
        lease_seconds: float,
    ) -> tuple[str, dict[str, Any]]:
        query = (
            self._queries.claim_one_queue
            if len(wanted) == 1
            else self._queries.claim_many_queues
        )
        return query, self._at(
            now,
            queues=wanted,
            queue=wanted[0],
            limit=limit,
            worker=worker_id,
            lease=now + lease_seconds,
            seconds=lease_seconds,
        )

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
            job, execution_status, error, summary=_storable(error.summary), run_at=retry_at
        )
        if retry_at is None:
            return await self._changed(self._queries.fail, parameters)
        return await self._requeued(job, self._queries.reschedule, parameters)

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        now = await self._now()
        return await self._requeued(
            job, self._queries.release, self._held(job, now, run_at=now + delay)
        )

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        now = await self._now()
        return await self._requeued(
            job, self._queries.hand_back, self._held(job, now, run_at=now + delay)
        )

    async def _requeued(self, job: Job, query: str, parameters: dict[str, Any]) -> bool:
        changed = await self._changed(query, parameters)
        if changed and parameters["run_at"] <= parameters["now"]:
            await self._wake({job.queue})
        return changed

    async def cancel(self, job_id: str) -> bool:
        now = await self._now()
        return await self._changed(self._queries.cancel, self._at(now, id=job_id))

    async def requeue(self, job_id: str) -> bool:
        now = await self._now()
        try:
            rows = await self._rows(self._queries.requeue, self._at(now, id=job_id))
        except errors.UniqueViolation:
            return False
        await self._wake({row["queue"] for row in rows if row["due"]})
        return bool(rows)

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
        rows = await self._rows(
            f"{self._queries.listed}{where} ORDER BY id DESC LIMIT %(limit)s", parameters
        )
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
        await self._wake(set(rows[0]["queues"]))
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
                    self._at(now, status=str(status), cutoff=cutoff, batch=batch),
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
        ordered, places, copies = _lock_order(jobs)
        for attempt in range(RETRIES):
            due: set[str] = set()
            try:
                async with self._transaction() as connection:
                    cursor = await connection.execute(
                        self._queries.advance_schedule,
                        {"name": name, "expected": expected, "next_run_at": next_run_at},
                    )
                    if cursor.rowcount != 1:
                        return None
                    stored = _in_order(
                        places,
                        copies,
                        len(jobs),
                        await self._insert(connection, ordered, now, due) if jobs else [],
                    )
                    if stored:
                        await connection.execute(
                            self._queries.record_schedule_run,
                            {"now": now, "job_id": stored[-1].id, "name": name},
                        )
                break
            except RETRIED_ERRORS as error:
                if attempt == RETRIES - 1:
                    raise
                logger.warning("retrying a postgres schedule firing after %s", error)
        await self._wake(due)
        return stored

    async def _insert(
        self,
        connection: psycopg.AsyncConnection,
        jobs: Sequence[Job],
        now: float,
        due: set[str],
    ) -> list[Job]:
        return await _driven(connection, self._inserting(jobs, now, due))

    def enqueue_in(self, tx: Any, jobs: Sequence[Job]) -> list[Job]:
        connection = _connection_of(tx)
        if not isinstance(connection, psycopg.Connection):
            raise TypeError(
                "tx must be a psycopg connection or cursor; pass an async one to "
                "aenqueue or asend"
            )
        steps = self._joining(connection, jobs)
        rows: list[dict[str, Any]] | None = None
        while True:
            try:
                query, parameters = steps.send(rows)
            except StopIteration as finished:
                return finished.value
            with connection.cursor(row_factory=dict_row) as cursor:
                rows = cursor.execute(query, parameters).fetchall()

    async def aenqueue_in(self, tx: Any, jobs: Sequence[Job]) -> list[Job]:
        connection = _connection_of(tx)
        if not isinstance(connection, psycopg.AsyncConnection):
            raise TypeError(
                "tx must be a psycopg async connection or cursor; pass a sync one to "
                "enqueue or send"
            )
        return await _driven(connection, self._joining(connection, jobs))

    def _joining(self, connection: Any, jobs: Sequence[Job]) -> Steps[list[Job]]:
        queries = self._queries
        if self._identity is None:
            raise RuntimeError("call setup() before enqueueing inside a transaction")
        if connection not in self._joined:
            rows = yield queries.located, {
                "schema": queries.schema,
                "table": f"{self._tables.prefix}_meta",
            }
            identity = None
            if rows[0]["present"]:
                rows = yield queries.identity, {}
                identity = _identity_of(rows[0]) if rows else None
            if identity != self._identity:
                raise ConfigurationError(
                    f"the connection passed as tx does not reach the tinyjobs store this "
                    f"app uses ({self._server()}, prefix {self._tables.prefix!r}); "
                    "enqueue through a connection to that database"
                )
            self._joined[connection] = True
        now = self._clock.now()
        due: set[str] = set()
        ordered, places, copies = _lock_order(jobs)
        stored: list[Job] = []
        for start in range(0, len(ordered), ENQUEUE_BATCH):
            batch = ordered[start : start + ENQUEUE_BATCH]
            stored.extend((yield from self._inserting(batch, now, due)))
        if self._notify and due:
            yield queries.notify, {
                "channel": self._channel,
                "queues": sorted(due),
            }
        return _in_order(places, copies, len(jobs), stored)

    def _inserting(
        self,
        jobs: Sequence[Job],
        now: float,
        due: set[str],
    ) -> Steps[list[Job]]:
        queries = self._queries
        resolved: dict[int, Job] = {}
        pending = list(enumerate(jobs))
        for _ in range(INSERT_ROUNDS):
            batch = [job for _, job in pending]
            parameters = dict(zip(JOB_COLUMNS, job_arrays(batch)))
            rows = yield queries.insert, self._at(now, **parameters)
            inserted = {row["id"] for row in rows}
            due.update(row["queue"] for row in rows if row["due"])
            missing = [(index, job) for index, job in pending if job.id not in inserted]
            for index, job in pending:
                if job.id in inserted:
                    resolved[index] = job
            if not missing:
                break
            pending = yield from self._holders(missing, resolved)
            if not pending:
                break
        else:
            raise RuntimeError(
                "enqueue kept meeting dedup holders that finished before they could "
                "be read; try again"
            )
        return [resolved[index] for index in range(len(jobs))]

    def _holders(
        self,
        missing: list[tuple[int, Job]],
        resolved: dict[int, Job],
    ) -> Steps[list[tuple[int, Job]]]:
        queries = self._queries
        rows = yield queries.by_ids, {"ids": [job.id for _, job in missing]}
        existing = {row["id"]: job_from_row(row) for row in rows}
        keys = [job.dedup_key for _, job in missing if job.id not in existing and job.dedup_key]
        holders: dict[str, Job] = {}
        if keys:
            rows = yield queries.active_by_keys, {"keys": keys}
            holders = {row["dedup_key"]: job_from_row(row) for row in rows}
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
            error_type=_storable(error.type) if error else None,
            traceback=_storable(error.traceback) if error else None,
            **extra,
        )

    def _held(self, job: Job, now: float, **extra: Any) -> dict[str, Any]:
        return self._at(
            now, id=job.id, worker=job.worker_id, claims=job.claims, **extra
        )

    def _at(self, now: float, **extra: Any) -> dict[str, Any]:
        return {"now": now, **extra}

    async def _changed(self, query: str, parameters: dict[str, Any]) -> bool:
        rows = await self._retrying(query, parameters)
        return rows[0]["changed"] == 1

    async def _retrying(
        self, query: str, parameters: dict[str, Any], *, rows: bool = True
    ) -> list[dict[str, Any]]:
        for attempt in range(RETRIES):
            try:
                return await self._rows(query, parameters, rows=rows)
            except RETRIED_ERRORS as error:
                if attempt == RETRIES - 1:
                    raise
                logger.warning("retrying a postgres statement after %s", error)
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
        async with self._connection() as connection, AsyncExitStack() as stack:
            transaction = psycopg.AsyncTransaction(connection)
            await _begin(transaction)
            stack.push_async_exit(transaction)
            yield connection

    async def _configure(self, connection: psycopg.AsyncConnection) -> None:
        timeout = self._statement_timeout
        statement = "0" if timeout is None else f"{int(timeout * 1000)}ms"
        await connection.execute(
            "SELECT set_config('default_transaction_isolation', 'read committed', false), "
            "set_config('statement_timeout', %(statement)s, false), "
            "set_config('idle_in_transaction_session_timeout', %(idle)s, false)",
            {"statement": statement, "idle": f"{IDLE_IN_TRANSACTION_SECONDS}s"},
        )

    async def _wake(self, queues: set[str]) -> None:
        if not self._notify or not queues or self._closed or self._channel is None:
            return
        moment = time.monotonic()
        quiet = [
            queue
            for queue in sorted(queues)
            if moment - self._woken.get(queue, float("-inf")) >= NOTIFY_SECONDS
        ]
        self._waiting.update(queues.difference(quiet))
        if quiet:
            for queue in quiet:
                self._woken[queue] = moment
            await self._send(quiet)
        if self._waiting and self._waking is None:
            self._waking = asyncio.create_task(self._wake_later(), name="tinyjobs-postgres-wake")

    async def _wake_later(self) -> None:
        try:
            while self._waiting:
                await asyncio.sleep(NOTIFY_SECONDS)
                queues, self._waiting = self._waiting, set()
                moment = time.monotonic()
                for queue in queues:
                    self._woken[queue] = moment
                await self._send(sorted(queues))
        finally:
            if self._waking is asyncio.current_task():
                self._waking = None

    async def _send(self, queues: Sequence[str]) -> None:
        try:
            await self._rows(
                self._queries.notify,
                {"channel": self._channel, "queues": list(queues)},
                rows=False,
            )
        except WAKE_ERRORS as error:
            logger.debug("could not wake workers on %s: %s", ", ".join(queues), error)

    async def _stored_version(self, connection: psycopg.AsyncConnection) -> int | None:
        present = await _fetch(
            connection, self._unplaced.table_exists, {"name": self._tables.meta}
        )
        if not present[0]["present"]:
            return None
        rows = await _fetch(connection, self._unplaced.schema_version, {})
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


def _connection_kwargs(conninfo: str, options: dict[str, Any]) -> dict[str, Any]:
    try:
        given = {**conninfo_to_dict(conninfo), **options}
    except psycopg.ProgrammingError:
        given = dict(options)
    defaults = {"application_name": "tinyjobs", **KEEPALIVES}
    kept = {key: value for key, value in defaults.items() if key not in given}
    return {**kept, **options, "autocommit": True, "row_factory": dict_row}


def _settled(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


async def _begin(transaction: psycopg.AsyncTransaction) -> None:
    try:
        await transaction.__aenter__()
    except BaseException as error:
        if transaction.status is psycopg.AsyncTransaction.Status.ACTIVE:
            await transaction.__aexit__(type(error), error, error.__traceback__)
        raise


def _lock_order(jobs: Sequence[Job]) -> tuple[list[Job], list[int], dict[int, int]]:
    firsts, copies = _firsts(jobs)
    firsts.sort(key=lambda pair: (pair[1].dedup_key is None, pair[1].dedup_key or "", pair[1].id))
    return [job for _, job in firsts], [index for index, _ in firsts], copies


def _firsts(jobs: Sequence[Job]) -> tuple[list[tuple[int, Job]], dict[int, int]]:
    seen: dict[tuple[str, str], int] = {}
    firsts: list[tuple[int, Job]] = []
    copies: dict[int, int] = {}
    for index, job in enumerate(jobs):
        marks = [("id", job.id)] + ([("key", job.dedup_key)] if job.dedup_key else [])
        earlier = next((seen[mark] for mark in marks if mark in seen), None)
        if earlier is None:
            seen.update((mark, index) for mark in marks)
            firsts.append((index, job))
        else:
            copies[index] = earlier
    return firsts, copies


def _in_order(
    places: list[int], copies: dict[int, int], size: int, stored: list[Job]
) -> list[Job]:
    placed: list[Job | None] = [None] * size
    for index, job in zip(places, stored):
        placed[index] = job
    for index, first in copies.items():
        placed[index] = placed[first]
    return placed


def _identity_of(row: dict[str, Any]) -> tuple[Any, ...]:
    return (row["instance"], row["database"], row["cluster"])


def _noticed(queue: str) -> str:
    return queue[:NOTICE_CHARACTERS]


def _storable(text: str) -> str:
    return text.replace("\x00", "\\x00")


def _connection_of(tx: Any) -> Any:
    if isinstance(tx, (psycopg.Cursor, psycopg.AsyncCursor)):
        return tx.connection
    return tx


async def _driven(connection: psycopg.AsyncConnection, steps: Steps[T]) -> T:
    rows: list[dict[str, Any]] | None = None
    while True:
        try:
            query, parameters = steps.send(rows)
        except StopIteration as finished:
            return finished.value
        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, parameters)
            rows = await cursor.fetchall()


async def _fetch(
    connection: psycopg.AsyncConnection, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    cursor = await connection.execute(query, parameters)
    return await cursor.fetchall()
