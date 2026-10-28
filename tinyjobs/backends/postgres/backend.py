from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import logging
import math
import re
import secrets
import select
import time
import weakref
from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
    contextmanager,
    nullcontext,
    suppress,
)
from dataclasses import dataclass
from functools import cache, partial
from typing import Any, AsyncIterator, Awaitable, Callable, Generator, Iterator, Sequence, TypeVar
from urllib.parse import unquote

import psycopg
from psycopg import errors, pq, waiting
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolClosed, PoolTimeout

from ...clock import Clock, FollowingClock, elapsed
from ...exceptions import ConfigurationError, UnsupportedCapabilityError
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
from .queries import NOTICE_CHARACTERS, REFUSAL, Queries, queue_is
from .records import (
    JOB_COLUMNS,
    dedup_digest,
    execution_from_row,
    job_arrays,
    job_from_row,
    schedule_from_row,
)
from .schema import COLUMNS, INDEXES, SCHEMA_VERSION, STATISTICS, Tables

DEFAULT_PREFIX = "tinyjobs"
DEFAULT_MIN_CONNECTIONS = 1
DEFAULT_MAX_CONNECTIONS = 10
DEFAULT_POOL_TIMEOUT = 20.0
DEFAULT_STATEMENT_TIMEOUT = 30.0
SHORTEST_STATEMENT_SECONDS = 0.001
LONGEST_STATEMENT_SECONDS = (2**31 - 1) / 1000
SAFE_SEARCH_PATH = "pg_catalog, pg_temp"
TABLE_KINDS = ("jobs", "executions", "schedules", "meta")
IDLE_IN_TRANSACTION_SECONDS = 60
SETUP_LOCK_SECONDS = 5
SETUP_LOCK_PAUSE = 0.05
REBUILD_LOCK_SECONDS = 30
SETUP_ATTEMPTS = 3
CREATION_RACES = (errors.UniqueViolation, errors.DuplicateTable, errors.DuplicateObject)
MAX_IDLE_SECONDS = 300.0
MAX_LIFETIME_SECONDS = 1800.0
NOTIFY_SECONDS = 0.05
LISTEN_RETRY_SECONDS = 1.0
KEEPALIVES = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
    "tcp_user_timeout": 60000,
}
SILENCE_SECONDS = 10.0
PROBE_SECONDS = 10.0
CANCEL_SECONDS = 5.0
ENDING_MILLISECONDS = 5000
WAIT_INTERVAL = 0.1
UNKNOWN_STATES = frozenset({None, "disabled"})
IN_TRANSIT_SECONDS = 300.0
RUNNING = (
    "SELECT state, wait_event, state_change FROM pg_catalog.pg_stat_activity "
    "WHERE pid = %(pid)s"
)
_LEFT: contextvars.ContextVar[tuple[int, Any] | None] = contextvars.ContextVar(
    "tinyjobs_postgres_left", default=None
)
FENCE = (
    "SELECT pg_catalog.pg_terminate_backend(pid, %(wait)s) AS ended "
    "FROM pg_catalog.pg_stat_activity WHERE pid = %(pid)s AND backend_start = %(born)s"
)
FENCE_MILLISECONDS = 1000
SETTLE_SECONDS = 5.0
ENDING = (
    "SELECT pg_catalog.pg_terminate_backend(pid, %(wait)s) FROM pg_catalog.pg_stat_activity "
    "WHERE pid = %(pid)s AND state_change = %(seen)s"
)
ENQUEUE_BATCH = 1000
ENQUEUE_BATCH_BYTES = 64 * 2**20
INSERT_ROUNDS = 50
RETRIED_ERRORS = (errors.DeadlockDetected, errors.SerializationFailure, errors.IdleSessionTimeout)
RETRIES = 3
READS_TO_SEE_A_CLOSE = 2
PSYCOPG_OPTIONS = frozenset({"prepare_threshold", "context"})
PIPELINED = psycopg.Pipeline.is_supported()
HELD = all(
    hasattr(kind, name)
    for kind, name in (
        (psycopg.Connection, "_pipeline_nolock"),
        (psycopg.AsyncConnection, "_pipeline_nolock"),
        (psycopg.Cursor, "_execute_gen"),
        (psycopg.AsyncCursor, "_execute_gen"),
        (psycopg.Pipeline, "_fetch_gen"),
        (psycopg.AsyncPipeline, "_fetch_gen"),
    )
)
URI_SCHEME = re.compile(r"^\s*postgres(?:ql)?://", re.IGNORECASE)
SECRET_SETTING = re.compile(r"password\s*=\s*(?:'((?:[^'\\]|\\.)*)'|([^\s&]+))")
LOST_STATES = frozenset({"57P01", "57P02", "57P05"})
WAKE_ERRORS = (psycopg.Error, PoolTimeout)

logger = logging.getLogger("tinyjobs.postgres")

T = TypeVar("T")
Steps = Generator[tuple[Any, ...], list[dict[str, Any]], T]


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
        conninfo = _lowered(conninfo)
        _parsed(conninfo)
        _refuse_unknown(connection_options)
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
        self._unclaimed: set[_Unclaimed] = set()
        self._waiters: dict[asyncio.Event, frozenset[str]] = {}
        self._listening: asyncio.Task | None = None
        self._connection_options = connection_options
        self._joined: weakref.WeakKeyDictionary[Any, bool] = weakref.WeakKeyDictionary()
        self._unplaced = self._queries
        self._schemas: list[str] | None = None
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
            connection_class=_Answering,
            configure=self._configure,
            name="tinyjobs",
        )

    @classmethod
    def check_options(cls, conninfo: str, /, **options: Any) -> None:
        named = {
            name
            for name, parameter in inspect.signature(cls.__init__).parameters.items()
            if parameter.kind is inspect.Parameter.KEYWORD_ONLY
        }
        _parsed(_lowered(conninfo))
        _refuse_unknown({key: value for key, value in options.items() if key not in named})

    @classmethod
    def from_url(
        cls, url: str, *, clock: Clock | None = None, **options: Any
    ) -> "PostgresBackend":
        return cls(url, clock=clock, **options)

    async def setup(self) -> None:
        await self._open()
        async with self._connection() as connection:
            placed = await self._placed(connection)
            stored = await self._stored_version(connection, placed) if placed.meta else None
            complete = (
                placed.tables == len(TABLE_KINDS)
                and stored == SCHEMA_VERSION
                and not await self._misplaced(connection, placed)
                and not await self._missing_columns(connection, placed)
            )
        if not complete:
            for attempt in range(SETUP_ATTEMPTS):
                try:
                    await self._create()
                    break
                except CREATION_RACES:
                    if attempt == SETUP_ATTEMPTS - 1:
                        raise
        skipped: set[str] = set()
        if not complete:
            async with self._connection() as connection:
                placed = await self._placed(connection)
                rebuilding = placed.tables == len(TABLE_KINDS) and bool(
                    await self._misplaced(connection, placed)
                )
            if rebuilding:
                skipped = await self._rebuild_indexes()
        async with self._connection() as connection:
            placed = await self._placed(connection)
            if placed.tables != len(TABLE_KINDS):
                raise ConfigurationError(
                    f"some tinyjobs tables for prefix {self._tables.prefix!r} are missing "
                    f"although the meta table records schema version {SCHEMA_VERSION}; "
                    "restore them or drop the rest so setup creates them again"
                )
            await self._check_indexes(connection, placed, skipped)
            queries = placed.queries
            rows = await _fetch(connection, queries.identity, {})
            if not rows:
                await connection.execute(placed.within.instance(), {})
                rows = await _fetch(connection, queries.identity, {})
            cluster = await _fetch(connection, queries.cluster_readable, {})
            if cluster[0]["readable"]:
                cluster = await _fetch(connection, queries.cluster, {})
        self._identity = _identity_of(rows[0], cluster[0].get("cluster"))
        self._channel = f"tinyjobs_{hashlib.sha256(rows[0]['instance'].encode()).hexdigest()[:24]}"
        self._queries = queries
        await self._now()

    async def _placed(self, connection: psycopg.AsyncConnection) -> "_Placement":
        tables = self._tables
        schemas = [tables.requested] if tables.requested else list(self._schemas or ())
        found = await _fetch(
            connection,
            self._unplaced.placement,
            {
                "names": [f"{tables.prefix}_{kind}" for kind in TABLE_KINDS],
                "schemas": schemas,
            },
        )
        if found:
            if len({row["schema"] for row in found}) > 1:
                raise ConfigurationError(
                    f"the tinyjobs tables for prefix {tables.prefix!r} resolve to more "
                    "than one schema on this connection's search_path; pass schema= "
                    "to name the one to use"
                )
            schema, count = found[0]["schema"], len(found)
            owned = all(row["owned"] for row in found)
            meta = any(row["name"] == f"{tables.prefix}_meta" for row in found)
            schema_missing = False
        else:
            meta = False
            schema = schemas[0] if schemas else None
            if schema is None:
                raise ConfigurationError(
                    "no schema on this connection's search_path exists to create the "
                    "tinyjobs tables in; pass schema= or fix the search_path"
                )
            kept = await _fetch(connection, self._unplaced.schema_trust, {"schema": schema})
            owned, count, schema_missing = all(row["owned"] for row in kept), 0, not kept
        if not owned:
            raise ConfigurationError(
                f"the tinyjobs tables for prefix {tables.prefix!r} or their schema "
                f"{schema!r} belong to a role that does not hold the connecting user's "
                "privileges, or carry a TRIGGER grant to such a role or code it owns (a trigger, "
                "constraint, default, index or policy function, operator or type); "
                "tinyjobs only uses tables and schemas owned by its own role or by a role "
                "that can act as it, so pass schema= with one it owns, or revoke the grant "
                "or drop that code"
            )
        within = tables.within(schema)
        return _Placement(within, Queries(within, self._following), count, meta, schema_missing)

    async def _misplaced(
        self, connection: psycopg.AsyncConnection, placed: "_Placement"
    ) -> list[dict[str, Any]]:
        prefix = self._tables.prefix
        schema = placed.within.schema
        indexes = await _fetch(
            connection,
            self._unplaced.misplaced_indexes,
            {
                "schema": schema,
                "indexes": [f"{prefix}_{name}" for name, _, _ in INDEXES],
                "tables": [f"{prefix}_{kind}" for _, kind, _ in INDEXES],
                "uniques": [unique for _, _, unique in INDEXES],
            },
        )
        statistics = await _fetch(
            connection,
            self._unplaced.missing_statistics,
            {"schema": schema, "statistics": [f"{prefix}_{name}" for name, _ in STATISTICS]},
        )
        return indexes + statistics

    async def _missing_columns(
        self, connection: psycopg.AsyncConnection, placed: "_Placement"
    ) -> list[dict[str, Any]]:
        if placed.tables != len(TABLE_KINDS):
            return []
        prefix = self._tables.prefix
        return await _fetch(
            connection,
            self._unplaced.missing_columns,
            {
                "schema": placed.within.schema,
                "columns": [name for name, _ in COLUMNS],
                "tables": [f"{prefix}_{kind}" for _, kind in COLUMNS],
            },
        )

    async def _rebuild_indexes(self) -> set[str]:
        prefix = self._tables.prefix
        unique = {f"{prefix}_{name}" for name, _, is_unique in INDEXES if is_unique}
        skipped: set[str] = set()
        async with await psycopg.AsyncConnection.connect(
            self._conninfo, **self._direct_options()
        ) as connection:
            for statement, parameters in self._settings():
                await connection.execute(statement, parameters)
            await connection.execute(
                "SELECT pg_catalog.set_config('statement_timeout', '0', false), "
                "pg_catalog.set_config('lock_timeout', %(wait)s, false)",
                {"wait": f"{REBUILD_LOCK_SECONDS}s"},
            )
            await self._lock_for_setup(connection, "pg_catalog.pg_try_advisory_lock")
            placed = await self._placed(connection)
            schema = placed.within.schema.replace('"', '""').replace("%", "%%")
            statements = {
                f"{prefix}_{name}": statement
                for name, statement in {
                    **placed.within.indexes(concurrently=True),
                    **placed.within.statistics(),
                }.items()
            }
            for row in await self._misplaced(connection, placed):
                name = row["name"]
                try:
                    if row["on_its_table"]:
                        await connection.execute(
                            f'DROP INDEX CONCURRENTLY IF EXISTS "{schema}"."{name}"', {}
                        )
                    await connection.execute(statements[name], {})
                except errors.UniqueViolation:
                    raise ConfigurationError(
                        await self._duplicates(connection, name, placed.within.jobs)
                    ) from None
                except (errors.LockNotAvailable, errors.QueryCanceled) as error:
                    if name in unique:
                        raise ConfigurationError(
                            f"setup could not rebuild the tinyjobs index {name}, which keeps "
                            f"jobs unique, within {REBUILD_LOCK_SECONDS}s ({error}); a "
                            "transaction holding the table that long must end first, then "
                            "run setup again"
                        ) from None
                    logger.warning(
                        "setup could not rebuild the tinyjobs index %s within %ss (%s); jobs "
                        "still run without it, more slowly, and the next setup tries again",
                        name,
                        REBUILD_LOCK_SECONDS,
                        error,
                    )
                    skipped.add(name)
        return skipped

    async def _duplicates(
        self, connection: psycopg.AsyncConnection, name: str, jobs: str
    ) -> str:
        if name != f"{self._tables.prefix}_jobs_dedup":
            return (
                f"setup could not rebuild the tinyjobs index {name} because rows it must keep "
                "unique are duplicated"
            )
        cursor = await connection.execute(
            f"SELECT pg_catalog.min(dedup_key) AS key FROM {jobs} "
            "WHERE dedup_digest IS NOT NULL AND status IN ('queued', 'running') "
            "GROUP BY dedup_digest HAVING pg_catalog.count(*) > 1 LIMIT 5",
            {},
        )
        keys = ", ".join(repr(row["key"]) for row in await cursor.fetchall())
        return (
            f"setup could not rebuild the tinyjobs index {name}: while it was missing, "
            f"jobs with the same dedup key became active more than once ({keys}); cancel "
            "all but one of each and run setup again"
        )

    async def _check_indexes(
        self, connection: psycopg.AsyncConnection, placed: "_Placement", skipped: set[str]
    ) -> None:
        rows = [row for row in await self._misplaced(connection, placed) if row["name"] not in skipped]
        if rows:
            names = ", ".join(row["name"] for row in rows)
            raise ConfigurationError(
                f"the tinyjobs indexes {names} in schema {placed.within.schema!r} are "
                "missing or are not the ones tinyjobs creates, which happens when another "
                "relation already holds their names; drop or rename it and run setup again"
            )

    async def _create(self) -> None:
        async with self._transaction() as connection:
            await connection.execute(
                f"SET LOCAL lock_timeout = '{SETUP_LOCK_SECONDS}s'"
            )
            await self._lock_for_setup(connection)
            placed = await self._placed(connection)
            stored = await self._stored_version(connection, placed) if placed.meta else None
            if stored is not None and stored > SCHEMA_VERSION:
                raise RuntimeError(
                    f"postgres schema version {stored} is newer than this tinyjobs "
                    f"build understands ({SCHEMA_VERSION}); upgrade the package"
                )
            if stored == SCHEMA_VERSION and not await self._missing_columns(connection, placed):
                return
            for statement in placed.within.ddl(schema_missing=placed.schema_missing):
                await connection.execute(statement, {})
            created = await self._placed(connection)
            if created.tables != len(TABLE_KINDS) or created.within.schema != placed.within.schema:
                raise ConfigurationError(
                    f"the tinyjobs tables for prefix {self._tables.prefix!r} changed while "
                    "setup created them; run setup again"
                )
            for statement in created.within.seeds():
                await connection.execute(statement, {})

    async def _lock_for_setup(
        self,
        connection: psycopg.AsyncConnection,
        taking: str = "pg_catalog.pg_try_advisory_xact_lock",
    ) -> None:
        deadline = time.monotonic() + SETUP_LOCK_SECONDS
        while True:
            cursor = await connection.execute(
                f"SELECT {taking}(%(key)s) AS taken",
                {"key": self._tables.lock},
            )
            if (await cursor.fetchone())["taken"]:
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "another session has held the tinyjobs setup lock for prefix %r for "
                    "%ss; creating the tables without it",
                    self._tables.prefix,
                    SETUP_LOCK_SECONDS,
                )
                return
            await asyncio.sleep(SETUP_LOCK_PAUSE)

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
        if self._unclaimed:
            with suppress(psycopg.Error, PoolTimeout, PoolClosed, TimeoutError):
                async with asyncio.timeout(SETTLE_SECONDS):
                    await self._unlease()
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
            key: value for key, value in self._direct_options().items() if key != "row_factory"
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
                if not _split(ordered):
                    async with self._connection() as connection:
                        stored = await self._insert(connection, ordered, now, due)
                else:
                    async with self._transaction() as connection:
                        stored = []
                        for batch in _batches(ordered):
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
        if self._unclaimed:
            await self._unlease()
        now = await self._now()
        query, parameters = self._claiming(now, wanted, worker_id, limit, lease_seconds)
        _LEFT.set(None)
        try:
            rows = await self._retrying(query, parameters)
        except BaseException as error:
            if _unanswered(error):
                self._unclaimed.add(_Unclaimed(worker_id, parameters["token"], _LEFT.get()))
            raise
        return [job_from_row(row) for row in rows]

    async def _unlease(self) -> None:
        for entry in list(self._unclaimed):
            async with self._connection() as connection:
                if entry.backend is not None:
                    pid, born = entry.backend
                    fenced = await _fetch(
                        connection, FENCE, {"pid": pid, "born": born, "wait": FENCE_MILLISECONDS}
                    )
                    if fenced and not fenced[0]["ended"]:
                        continue
                rows = await _fetch(
                    connection,
                    self._queries.unclaim,
                    {"worker": entry.worker_id, "token": entry.token},
                )
            self._unclaimed.discard(entry)
            if rows:
                logger.warning(
                    "handed back %d job(s) a postgres claim leased to %s although its answer "
                    "never arrived",
                    len(rows),
                    entry.worker_id,
                )
                await self._wake({row["queue"] for row in rows})

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
            token=secrets.token_hex(16),
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
        rows = await self._lookup(self._queries.get, {"id": job_id})
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
            conditions.append(queue_is("%(queue)s"))
            parameters["queue"] = queue
        if cursor is not None:
            conditions.append("id < %(cursor)s")
            parameters["cursor"] = cursor
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = await self._lookup(
            f"{self._queries.listed}{where} ORDER BY id DESC LIMIT %(limit)s", parameters
        )
        jobs = [job_from_row(row) for row in rows[:limit]]
        next_cursor = jobs[-1].id if len(rows) > limit and jobs else None
        return Page(items=jobs, cursor=next_cursor)

    async def executions(self, job_id: str) -> list[Execution]:
        rows = await self._lookup(self._queries.executions, {"id": job_id})
        return [execution_from_row(row) for row in rows]

    async def reap_expired(self) -> int:
        now = await self._now()
        rows = await self._retrying(
            self._queries.reap, self._at(now, message=LEASE_EXPIRED_MESSAGE)
        )
        await self._wake(set(rows[0]["queues"]))
        return rows[0]["reaped"]

    async def counts_by_status(self) -> dict[str, int]:
        rows = await self._lookup(self._queries.count_by_status, {})
        return {row["status"]: row["total"] for row in rows}

    async def stats(self, now: float) -> Stats:
        counted, oldest, expired, queued = await _again(partial(self._counted, now))
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

    async def _counted(self, now: float) -> list[list[dict[str, Any]]]:
        queries = self._queries
        async with self._connection() as connection:
            return [
                await _fetch(connection, queries.count_by_queue, {}),
                await _fetch(connection, queries.oldest_due_by_queue, {"now": now}),
                await _fetch(connection, queries.count_expired, {"now": now}),
                await _fetch(connection, queries.queued_by_task, {}),
            ]

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
        rows = await self._lookup(self._queries.schedules, {})
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
        _check_held(connection)
        with self._refusing():
            try:
                with _atomic(connection, jobs):
                    return _stepped(connection, self._joining(connection, jobs))
            except _Elsewhere as elsewhere:
                with psycopg.Connection.connect(self._conninfo, **self._direct_options()) as own:
                    for statement, parameters in self._settings():
                        own.execute(statement, parameters)
                    self._settle(_stepped(own, self._identifying()), elsewhere.identity)
                with _atomic(connection, jobs):
                    return _stepped(connection, self._joining(connection, jobs))

    async def aenqueue_in(self, tx: Any, jobs: Sequence[Job]) -> list[Job]:
        connection = _connection_of(tx)
        if not isinstance(connection, psycopg.AsyncConnection):
            raise TypeError(
                "tx must be a psycopg async connection or cursor; pass a sync one to "
                "enqueue or send"
            )
        _check_held(connection)
        with self._refusing():
            try:
                async with _atomic(connection, jobs):
                    return await _driven(connection, self._joining(connection, jobs))
            except _Elsewhere as elsewhere:
                async with await psycopg.AsyncConnection.connect(
                    self._conninfo, **self._direct_options()
                ) as own:
                    for statement, parameters in self._settings():
                        await own.execute(statement, parameters)
                    self._settle(await _driven(own, self._identifying()), elsewhere.identity)
                async with _atomic(connection, jobs):
                    return await _driven(connection, self._joining(connection, jobs))

    @contextmanager
    def _refusing(self) -> Iterator[None]:
        try:
            yield
        except errors.InvalidTextRepresentation as error:
            if REFUSAL not in (error.diag.message_primary or ""):
                raise
            raise ConfigurationError(
                f"the tinyjobs tables for prefix {self._tables.prefix!r} that the connection "
                "passed as tx reaches belong to a role that does not hold that connection's "
                "privileges, or carry a TRIGGER grant to such a role or code it owns (a trigger, "
                "constraint, default, index or policy function, operator or type); "
                "tinyjobs never writes through them"
            ) from None

    def _joining(self, connection: Any, jobs: Sequence[Job]) -> Steps[list[Job]]:
        queries = self._queries
        if self._identity is None:
            raise RuntimeError("call setup() before enqueueing inside a transaction")
        prefix = self._tables.prefix
        guard: tuple[Any, ...] = (
            queries.located,
            {
                "schema": queries.schema,
                "names": [f"{prefix}_{kind}" for kind in TABLE_KINDS],
                "meta": f"{prefix}_meta",
            },
        )
        if connection not in self._joined:
            [found] = yield guard
            identity = None
            if found["tables"] == len(TABLE_KINDS) and found["meta"]:
                identity = yield from self._identifying()
            if not _same_store(self._identity, identity):
                raise _Elsewhere(identity)
            self._joined[connection] = True
            guard = ()
        now = self._clock.now()
        due: set[str] = set()
        ordered, places, copies = _lock_order(jobs)
        stored: list[Job] = []
        for batch in _batches(ordered):
            stored.extend((yield from self._inserting(batch, now, due, guard)))
            guard = ()
        if self._notify and due:
            yield queries.notify, {
                "channel": self._channel,
                "queues": sorted(due),
            }
        return _in_order(places, copies, len(jobs), stored)

    def _identifying(self) -> Steps[tuple[Any, ...] | None]:
        queries = self._queries
        rows = yield queries.identity, {}
        if not rows:
            return None
        [readable] = yield queries.cluster_readable, {}
        cluster = None
        if readable["readable"]:
            [read] = yield queries.cluster, {}
            cluster = read["cluster"]
        return _identity_of(rows[0], cluster)

    def _settle(self, mine: tuple[Any, ...] | None, theirs: tuple[Any, ...] | None) -> None:
        known = self._identity
        if mine is None or known is None or mine[0] != known[0] or not _same_store(mine, theirs):
            raise ConfigurationError(
                f"the connection passed as tx does not reach the tinyjobs store this "
                f"app uses ({self._server()}, prefix {self._tables.prefix!r}); "
                "enqueue through a connection to that database"
            )
        self._identity = mine

    def _direct_options(self) -> dict[str, Any]:
        options = _connection_kwargs(self._conninfo, self._connection_options)
        try:
            given = conninfo_to_dict(self._conninfo)
        except psycopg.ProgrammingError:
            given = {}
        if "connect_timeout" not in given and "connect_timeout" not in options:
            options["connect_timeout"] = max(2, math.ceil(self._pool_timeout))
        return options

    def _settings(self) -> list[tuple[str, dict[str, Any]]]:
        timeout = self._statement_timeout
        statement = "0" if timeout is None else f"{int(timeout * 1000)}ms"
        return [
            (
                "SELECT pg_catalog.set_config('search_path', %(path)s, false)",
                {"path": SAFE_SEARCH_PATH},
            ),
            (
                "SELECT set_config('default_transaction_isolation', 'read committed', false), "
                "set_config('statement_timeout', %(statement)s, false), "
                "set_config('idle_in_transaction_session_timeout', %(idle)s, false), "
                "pg_catalog.pg_backend_pid() AS pid, (SELECT backend_start FROM "
                "pg_catalog.pg_stat_get_activity(pg_catalog.pg_backend_pid())) AS born",
                {"statement": statement, "idle": f"{IDLE_IN_TRANSACTION_SECONDS}s"},
            ),
        ]

    def _inserting(
        self,
        jobs: Sequence[Job],
        now: float,
        due: set[str],
        guard: tuple[Any, ...] = (),
    ) -> Steps[list[Job]]:
        queries = self._queries
        resolved: dict[int, Job] = {}
        pending = list(enumerate(jobs))
        for _ in range(INSERT_ROUNDS):
            batch = [job for _, job in pending]
            parameters = dict(zip(JOB_COLUMNS, job_arrays(batch)))
            rows = yield (queries.insert, self._at(now, **parameters), *guard)
            guard = ()
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
        keys = [
            job.dedup_key
            for _, job in missing
            if job.id not in existing and job.dedup_key is not None
        ]
        holders: dict[str, Job] = {}
        if keys:
            rows = yield queries.active_by_keys, {"digests": [dedup_digest(key) for key in keys]}
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

    async def _lookup(self, query: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        return await _again(partial(self._rows, query, parameters))

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
        _LEFT.set(None)
        try:
            connection = await self._live()
            try:
                yield connection
            except BaseException:
                _LEFT.set(_abandoned(connection))
                raise
            finally:
                await self._pool.putconn(connection)
        except (psycopg.OperationalError, PoolTimeout):
            self._forget()
            raise

    async def _live(self) -> psycopg.AsyncConnection:
        for _ in range(self._sizes[1]):
            connection = await self._pool.getconn()
            if _alive(connection):
                return connection
            await self._pool.putconn(connection)
        return await self._pool.getconn()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[psycopg.AsyncConnection]:
        async with self._connection() as connection, AsyncExitStack() as stack:
            transaction = psycopg.AsyncTransaction(connection)
            await _begin(transaction)
            stack.push_async_exit(transaction)
            yield connection

    async def _configure(self, connection: psycopg.AsyncConnection) -> None:
        if isinstance(connection, _Answering):
            connection.pid = connection.info.backend_pid
            connection.working = self._working
        if self._schemas is None:
            cursor = await connection.execute(
                "SELECT pg_catalog.current_schemas(false)::pg_catalog.text[] AS schemas"
            )
            self._schemas = (await cursor.fetchone())["schemas"]
        for statement, parameters in self._settings():
            cursor = await connection.execute(statement, parameters)
        if isinstance(connection, _Answering):
            row = await cursor.fetchone()
            connection.pid, connection.born = row["pid"], row["born"]

    async def _working(self, pid: int, quiet: float) -> bool:
        try:
            async with asyncio.timeout(PROBE_SECONDS):
                async with await psycopg.AsyncConnection.connect(
                    self._conninfo, **self._direct_options()
                ) as probe:
                    row = await (await probe.execute(RUNNING, {"pid": pid})).fetchone()
                    if row is not None and _busy(row, quiet):
                        return True
                    if row is None:
                        return False
                    with suppress(psycopg.Error):
                        cursor = await probe.execute(
                            ENDING,
                            {"pid": pid, "seen": row["state_change"], "wait": ENDING_MILLISECONDS},
                        )
                        return await cursor.fetchone() is None
                    return False
        except TimeoutError:
            return False
        except psycopg.Error as error:
            return error.sqlstate is not None

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
        if self._waiting and self._waking is None and not self._closed:
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

    async def _stored_version(
        self, connection: psycopg.AsyncConnection, placed: "_Placement"
    ) -> int | None:
        rows = await _fetch(connection, placed.queries.schema_version, {})
        return int(rows[0]["value"]) if rows else None

    async def _now(self) -> float:
        clock = self._clock
        if isinstance(clock, FollowingClock) and (not self._read or clock.due()):
            async with self._reading:
                if not self._read or clock.due():
                    clock.follow(*await self._forgetting(_again(self._timed)), self._server())
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


def _alive(connection: psycopg.AsyncConnection) -> bool:
    try:
        for _ in range(READS_TO_SEE_A_CLOSE):
            connection.pgconn.consume_input()
    except psycopg.OperationalError:
        return False
    return True


async def _again(call: Callable[[], Awaitable[T]]) -> T:
    try:
        return await call()
    except (PoolTimeout, PoolClosed):
        raise
    except psycopg.OperationalError as error:
        if error.sqlstate is not None and error.sqlstate not in LOST_STATES:
            raise
        logger.debug("reading again on a fresh connection after %s", error)
        return await call()


def _check_held(connection: Any) -> None:
    if connection.info.parameter_status("client_encoding") == "SQL_ASCII":
        raise ConfigurationError(
            "tx= needs a connection whose client_encoding is not SQL_ASCII, since psycopg "
            "reads every text value on it as bytes; connect it with client_encoding=UTF8"
        )
    if not HELD:
        raise UnsupportedCapabilityError(
            "tx= needs a psycopg that tinyjobs can hold a connection's lock with for a whole "
            f"call, which psycopg {psycopg.__version__} is not; use psycopg 3.2 or 3.3"
        )


def _parsed(conninfo: str) -> dict[str, Any]:
    try:
        parsed = conninfo_to_dict(conninfo)
    except psycopg.ProgrammingError as error:
        reason = _unrevealing(conninfo, str(error).strip())
        raise ConfigurationError(f"the postgres url is not valid: {reason}") from None
    ports = str(parsed.get("port") or "").split(",")
    if "@" in str(parsed.get("host") or "") or not all(
        port.strip().isdigit() for port in ports if port.strip()
    ):
        raise ConfigurationError(
            "the postgres url is not valid: its host or port is malformed; a password "
            "holding '@', '/', '?' or '#' must have them percent-encoded"
        )
    return parsed


def _unrevealing(conninfo: str, reason: str) -> str:
    secrets = _secrets(conninfo)
    if not secrets and "password" not in conninfo.lower():
        return reason
    head, quoted, _ = reason.partition('"')
    reason = head + ('"***"' if quoted else "")
    for secret in sorted(secrets, key=len, reverse=True):
        reason = reason.replace(secret, "***")
    return reason


def _secrets(conninfo: str) -> set[str]:
    found = {match.group(1) or match.group(2) for match in SECRET_SETTING.finditer(conninfo)}
    if "://" in conninfo:
        userinfo = conninfo.partition("://")[2].rpartition("@")[0]
        if ":" in userinfo:
            found.add(userinfo.partition(":")[2])
    found |= {unquote(secret) for secret in found}
    return {secret for secret in found if secret}


def _lowered(conninfo: str) -> str:
    return URI_SCHEME.sub(lambda match: match.group(0).lower(), conninfo, count=1)


def _refuse_unknown(options: dict[str, Any]) -> None:
    unknown = sorted(set(options).difference(_known_options()))
    if unknown:
        raise ConfigurationError(
            f"unknown postgres options {', '.join(unknown)}; besides the tinyjobs ones, "
            "PostgresBackend takes libpq connection parameters and the psycopg options "
            f"{', '.join(sorted(PSYCOPG_OPTIONS))}"
        )


@cache
def _known_options() -> frozenset[str]:
    return frozenset(
        option.keyword.decode() for option in pq.Conninfo.get_defaults()
    ) | PSYCOPG_OPTIONS


def _connection_kwargs(conninfo: str, options: dict[str, Any]) -> dict[str, Any]:
    try:
        given = {**conninfo_to_dict(conninfo), **options}
    except psycopg.ProgrammingError:
        given = dict(options)
    defaults = {"application_name": "tinyjobs", "client_encoding": "UTF8", **KEEPALIVES}
    kept = {key: value for key, value in defaults.items() if key not in given}
    return {**kept, **options, "autocommit": True, "row_factory": dict_row}


def _settled(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


async def _begin(transaction: psycopg.AsyncTransaction) -> None:
    try:
        await transaction.__aenter__()
    except BaseException as error:
        if _started(transaction):
            await transaction.__aexit__(type(error), error, error.__traceback__)
        raise


def _started(transaction: psycopg.AsyncTransaction) -> bool:
    status = getattr(transaction, "status", None)
    if status is None:
        return transaction._entered and not transaction._exited
    return status is type(transaction).Status.ACTIVE


def _split(jobs: Sequence[Job]) -> bool:
    return len(jobs) > ENQUEUE_BATCH or sum(map(_weight, jobs)) > ENQUEUE_BATCH_BYTES


def _batches(jobs: Sequence[Job]) -> Iterator[Sequence[Job]]:
    start, weight = 0, 0
    for index, job in enumerate(jobs):
        weight += _weight(job)
        if index > start and (index - start == ENQUEUE_BATCH or weight > ENQUEUE_BATCH_BYTES):
            yield jobs[start:index]
            start, weight = index, _weight(job)
    if start < len(jobs):
        yield jobs[start:]


def _weight(job: Job) -> int:
    return (
        len(job.payload)
        + len(job.result_payload or b"")
        + len(job.dedup_key or "")
        + len(job.last_error or "")
    )


def _lock_order(jobs: Sequence[Job]) -> tuple[list[Job], list[int], dict[int, int]]:
    firsts, copies = _firsts(jobs)
    firsts.sort(key=lambda pair: (pair[1].dedup_key is None, pair[1].dedup_key or "", pair[1].id))
    return [job for _, job in firsts], [index for index, _ in firsts], copies


def _firsts(jobs: Sequence[Job]) -> tuple[list[tuple[int, Job]], dict[int, int]]:
    seen: dict[tuple[str, str], int] = {}
    firsts: list[tuple[int, Job]] = []
    copies: dict[int, int] = {}
    for index, job in enumerate(jobs):
        marks = [("id", job.id)] + ([("key", job.dedup_key)] if job.dedup_key is not None else [])
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


@dataclass(frozen=True, slots=True)
class _Unclaimed:
    worker_id: str
    token: str
    backend: tuple[int, Any] | None


def _unanswered(error: BaseException) -> bool:
    if not isinstance(error, psycopg.Error) or error.sqlstate is None:
        return True
    return error.sqlstate in LOST_STATES or error.sqlstate.startswith("08")


def _abandoned(connection: psycopg.AsyncConnection) -> tuple[int, Any] | None:
    if not connection.closed or not isinstance(connection, _Answering) or connection.pid is None:
        return None
    return connection.pid, connection.born


@dataclass(frozen=True, slots=True)
class _Placement:
    within: Tables
    queries: Queries
    tables: int
    meta: bool
    schema_missing: bool


def _identity_of(row: dict[str, Any], cluster: str | None) -> tuple[Any, ...]:
    return (row["instance"], row["database"], cluster)


def _same_store(mine: tuple[Any, ...], theirs: tuple[Any, ...] | None) -> bool:
    if theirs is None or mine[:2] != theirs[:2]:
        return False
    return mine[2] is None or theirs[2] is None or mine[2] == theirs[2]


def _noticed(queue: str) -> str:
    return queue[:NOTICE_CHARACTERS]


def _storable(text: str) -> str:
    return text.replace("\x00", "\\x00")


def _connection_of(tx: Any) -> Any:
    if isinstance(tx, (psycopg.Cursor, psycopg.AsyncCursor)):
        return tx.connection
    return tx


class _Answering(psycopg.AsyncConnection):
    pid: int | None = None
    born: Any = None
    working: Callable[[int, float], Awaitable[bool]] | None = None
    heard: float = 0.0

    async def wait(self, gen: Any, *args: Any, **kwargs: Any) -> Any:
        if self.working is None or self.pid is None or args or kwargs:
            return await super().wait(gen, *args, **kwargs)
        try:
            return await self._heard(gen)
        except (asyncio.CancelledError, KeyboardInterrupt):
            if self.pgconn.transaction_status == pq.TransactionStatus.ACTIVE:
                await self._given_up(gen)
            raise

    async def _heard(self, gen: Any) -> Any:
        self.heard = time.monotonic()
        try:
            state = next(gen)
            while True:
                state = gen.send(await self._answer(state))
        except StopIteration as finished:
            return finished.value
        except OSError as error:
            raise psycopg.OperationalError("connection socket closed") from error

    async def _answer(self, state: int) -> int:
        ready = await _ready(self.pgconn.socket, state, SILENCE_SECONDS)
        while not ready:
            if not await self.working(self.pid, time.monotonic() - self.heard):
                ready = _ready_now(self.pgconn.socket, state)
                if ready:
                    return ready
                self.pgconn.finish()
                raise psycopg.OperationalError(
                    "the postgres server did not answer: it finished or stopped the statement "
                    "but its answer never arrived, or it cannot be reached; the connection was "
                    "closed"
                )
            ready = await _ready(self.pgconn.socket, state, SILENCE_SECONDS)
        self.heard = time.monotonic()
        return ready

    async def _given_up(self, gen: Any) -> None:
        await self._try_cancel(timeout=CANCEL_SECONDS)
        try:
            async with asyncio.timeout(CANCEL_SECONDS):
                await waiting.wait_async(gen, self.pgconn.socket, interval=WAIT_INTERVAL)
        except errors.QueryCanceled:
            pass
        except TimeoutError:
            self.pgconn.finish()


def _busy(row: dict[str, Any], quiet: float) -> bool:
    if row["state"] in UNKNOWN_STATES:
        return True
    if row["wait_event"] == "ClientWrite":
        return False
    return row["wait_event"] != "ClientRead" or quiet < IN_TRANSIT_SECONDS


def _ready_now(fileno: int, state: int) -> int:
    readable, writable, _ = select.select(
        [fileno] if state & waiting.WAIT_R else [],
        [fileno] if state & waiting.WAIT_W else [],
        [],
        0,
    )
    return (waiting.READY_R if readable else 0) | (waiting.READY_W if writable else 0)


async def _ready(fileno: int, state: int, seconds: float) -> int:
    loop = asyncio.get_running_loop()
    heard = loop.create_future()
    ready = 0

    def wake(flag: int) -> None:
        nonlocal ready
        ready |= flag
        if not heard.done():
            heard.set_result(None)

    if state & waiting.WAIT_R:
        loop.add_reader(fileno, wake, waiting.READY_R)
    if state & waiting.WAIT_W:
        loop.add_writer(fileno, wake, waiting.READY_W)
    timer = loop.call_later(seconds, wake, 0)
    try:
        await heard
    finally:
        timer.cancel()
        loop.remove_reader(fileno)
        loop.remove_writer(fileno)
    return ready


class _Elsewhere(Exception):
    def __init__(self, identity: tuple[Any, ...] | None) -> None:
        super().__init__(identity)
        self.identity = identity


def _atomic(connection: Any, jobs: Sequence[Job]) -> Any:
    if (
        _split(jobs)
        and connection.autocommit
        and connection._pipeline is None
        and connection.info.transaction_status == pq.TransactionStatus.IDLE
    ):
        return connection.transaction()
    return nullcontext()


def _stepped(connection: psycopg.Connection, steps: Steps[T]) -> T:
    rows: list[dict[str, Any]] | None = None
    with (
        connection.lock,
        psycopg.Cursor(connection, row_factory=dict_row) as checking,
        psycopg.Cursor(connection, row_factory=dict_row) as cursor,
    ):
        while True:
            try:
                query, parameters, *guard = steps.send(rows)
            except StopIteration as finished:
                return finished.value
            pipeline = connection._pipeline
            if pipeline is not None:
                if guard:
                    connection.wait(checking._execute_gen(*guard))
                    connection.wait(pipeline._fetch_gen(flush=True))
                connection.wait(cursor._execute_gen(query, parameters))
                connection.wait(pipeline._fetch_gen(flush=True))
            elif guard and PIPELINED:
                with connection._pipeline_nolock():
                    connection.wait(checking._execute_gen(*guard))
                    connection.wait(cursor._execute_gen(query, parameters, prepare=False))
            else:
                if guard:
                    connection.wait(checking._execute_gen(*guard))
                connection.wait(cursor._execute_gen(query, parameters))
            rows = cursor.fetchall()


async def _driven(connection: psycopg.AsyncConnection, steps: Steps[T]) -> T:
    rows: list[dict[str, Any]] | None = None
    async with (
        connection.lock,
        psycopg.AsyncCursor(connection, row_factory=dict_row) as checking,
        psycopg.AsyncCursor(connection, row_factory=dict_row) as cursor,
    ):
        while True:
            try:
                query, parameters, *guard = steps.send(rows)
            except StopIteration as finished:
                return finished.value
            pipeline = connection._pipeline
            if pipeline is not None:
                if guard:
                    await connection.wait(checking._execute_gen(*guard))
                    await connection.wait(pipeline._fetch_gen(flush=True))
                await connection.wait(cursor._execute_gen(query, parameters))
                await connection.wait(pipeline._fetch_gen(flush=True))
            elif guard and PIPELINED:
                async with connection._pipeline_nolock():
                    await connection.wait(checking._execute_gen(*guard))
                    await connection.wait(cursor._execute_gen(query, parameters, prepare=False))
            else:
                if guard:
                    await connection.wait(checking._execute_gen(*guard))
                await connection.wait(cursor._execute_gen(query, parameters))
            rows = await cursor.fetchall()


async def _fetch(
    connection: psycopg.AsyncConnection, query: str, parameters: dict[str, Any]
) -> list[dict[str, Any]]:
    cursor = await connection.execute(query, parameters)
    return await cursor.fetchall()
