from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid
from contextlib import AbstractContextManager, suppress
from functools import partial
from pathlib import Path
from typing import Any, Callable, Collection, Mapping, Sequence, TypeVar
from urllib.parse import urlsplit

from ...clock import Clock
from ...deadlines import Deadline, final
from ...models import (
    Execution,
    ExecutionError,
    Finding,
    Job,
    Page,
    QueueStats,
    ScheduleState,
    Stats,
)
from ...retention import RetentionPolicy
from ...exceptions import AppClosedError, ConfigurationError, UnsupportedCapabilityError
from ...limits import Limit
from ..loader import IN_MEMORY_JOIN_MESSAGE, in_memory
from ...runtime import CountingPool, pause_threads_at_fork, refuse_interrupted
from ...sheltered import landing
from ...status import ExecutionStatus, JobStatus
from ..base import LEASE_EXPIRED_MESSAGE, BaseBackend, Capabilities
from .audit import audit
from .claim import SUPPORTS_RETURNING, LimitedClaim, build_claim_strategy
from .connection import CALLING, FORK_GATE, ConnectionPool, StepAside
from .queries import Queries
from .records import (
    execution_from_row,
    job_from_row,
    job_to_row,
    schedule_from_row,
)
from .schema import (
    DEFAULT_PREFIX,
    LEGACY_KINDS,
    LEGACY_STUB,
    PREFIXED_SINCE,
    SCHEMA_VERSION,
    TABLES_OF,
    Tables,
    ddl,
    migrations,
    renames,
)

T = TypeVar("T")

MEMORY_TEMPLATE = "file:tinyjobs-{token}?mode=memory&cache=shared"

SQLITE_OPTIONS = frozenset({"busy_timeout", "prefix"})

RELEASE_BATCH = 1000

LEGACY_PREFIX_MESSAGE = (
    "this file holds a queue from a tinyjobs before 0.14, whose tables had no prefix, and "
    "it migrates into the default prefix only, where its jobs stay; open the file once with "
    "the default prefix, or give this app a file of its own"
)

LEGACY_JOB_COLUMNS = frozenset(
    {"task", "payload", "codec", "queue", "status", "run_at", "lease_expires_at", "dedup_key"}
)

TX_TYPE_MESSAGE = (
    "tx must be a sqlite3 connection or cursor open on the queue's file, not {kind}; under "
    'Django, point "database" in the backend\'s OPTIONS at the SQLite alias whose NAME is '
    "the file in url instead"
)

STALE_READ_MESSAGE = (
    "the transaction passed as tx read the file before the queue's tables existed, so it "
    "cannot see them; end it and enqueue in a new one"
)

READ_FIRST_MESSAGE = (
    "the transaction passed as tx read the file before this enqueue and another connection "
    "wrote since, so SQLite refuses its first write; begin it with BEGIN IMMEDIATE, under "
    'Django "transaction_mode": "IMMEDIATE", or enqueue before reading'
)

OPEN_CURSOR_MESSAGE = (
    "the queue's schema cannot be installed on this connection while a statement is still "
    "being read on it, and that read was ended with the attempt; read the rows first, as "
    "fetchall() or list(queryset) do, or let a worker or an ordinary call install the schema "
    "before enqueueing inside a transaction"
)

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
        "_audit_sync",
        "_schedules_sync",
        "_close_sync",
    }
)


class DatabaseBusyError(sqlite3.OperationalError):
    pass


BUSY_CODES = (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)


class SQLiteBackend(BaseBackend):
    transient_errors = (DatabaseBusyError,)
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=True,
        retention=True,
        schedules=True,
        max_payload_bytes=None,
        result_ttl=True,
        limits=True,
    )

    def __init__(
        self,
        database: str,
        *,
        uri: bool = False,
        busy_timeout: float = 5.0,
        clock: Clock | None = None,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        self._clock = clock or Clock()
        self._tables = Tables(prefix)
        self._queries = Queries(self._tables)
        self._ddl = ddl(self._tables)
        self._migrations = migrations(self._tables)
        self._connections = ConnectionPool(
            database, uri=uri, busy_timeout=busy_timeout
        )
        self._claim_strategy = build_claim_strategy(self._queries)
        self._limited_claim = LimitedClaim(self._queries)
        self._executor = _executor()
        self._busy_timeout = busy_timeout
        self._ready = False
        self._closed = False
        pause_threads_at_fork(self)

    async def _call(self, function, *args):
        refuse_interrupted(function.__name__.strip("_"))
        if function.__name__ not in LOCK_FREE_CALLS:
            self.refuse_held_transaction()
        end = final.get()
        while True:
            executor = self._executor
            try:
                running = executor.submit(_gated, executor, end, function, *args)
            except RuntimeError as error:
                if executor.retiring:
                    continue
                if self._closed or function.__name__ not in RETURNING_CALLS:
                    raise AppClosedError(EXECUTOR_GONE_MESSAGE) from error
                return _busy_as_transient(_gated, None, end, function, *args)
            try:
                result = await landing(running)
            except sqlite3.OperationalError as error:
                if not _busy(error):
                    raise
                raise _transient(error) from error
            except asyncio.CancelledError:
                if not running.cancel():
                    pending = asyncio.wrap_future(running)
                    pending.add_done_callback(_retrieved)
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
                self._executor.submit(_gated, None, None, self._close_sync)
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
        limits: Mapping[str, Limit] | None = None,
    ) -> list[Job]:
        return await self._call(
            self._claim_sync,
            worker_id,
            tuple(queues),
            limit,
            lease_seconds,
            dict(limits) if limits else None,
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

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        return await self._call(self._hand_back_sync, job, delay)

    async def release_held(
        self, tasks: Collection[str], queues: Sequence[str] | None = None
    ) -> int:
        if not tasks or queues is not None and not queues:
            return 0
        return await self._call(self._release_held_sync, tuple(tasks), queues)

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

    async def expire_results(
        self, task: str, ttl: float, now: float, batch: int = 500, since: float = 0.0
    ) -> int:
        return await self._call(self._expire_results_sync, task, since, now - ttl, batch)

    async def counts_by_status(self) -> dict[str, int]:
        return await self._call(self._counts_by_status_sync)

    async def stats(self, now: float) -> Stats:
        return await self._call(self._stats_sync, now)

    async def audit(self) -> list[Finding]:
        return await self._call(self._audit_sync)

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
    def check_options(cls, url: str, /, **options: Any) -> None:
        unknown = sorted(set(options) - SQLITE_OPTIONS)
        if unknown:
            raise ConfigurationError(
                f"the sqlite backend takes no option named {', '.join(unknown)}; "
                f"it takes busy_timeout and prefix only"
            )
        if "prefix" in options:
            Tables(options["prefix"])

    @staticmethod
    def joins_cold(url: str) -> bool:
        return not in_memory(url)

    @classmethod
    def from_url(cls, url: str, **options: Any) -> "SQLiteBackend":
        if in_memory(url):
            token = uuid.uuid4().hex
            return cls(MEMORY_TEMPLATE.format(token=token), uri=True, **options)
        path = Path(urlsplit(url).path[1:]).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(str(path), **options)

    def _setup_sync(self) -> None:
        if not self._current(self._connections.connection):
            with self._connections.write() as connection:
                self._install_schema(connection)
        self._assert_schema_supported()
        self._ready = True

    def _install_schema(self, connection: Any) -> None:
        stored, legacy = self._stored_version(connection)
        if stored is not None:
            _refuse_newer(stored)
            self._rename(connection, legacy)
            self._migrate(connection, stored)
        for statement in self._ddl:
            connection.execute(statement)
        connection.execute(self._queries.record_schema_version, (str(SCHEMA_VERSION),))
        connection.execute(self._queries.record_last_purge)
        if legacy:
            for statement in LEGACY_STUB:
                connection.execute(statement)

    def _rename(self, connection: Any, legacy: tuple[str, ...]) -> None:
        if not legacy:
            return
        kept = connection.execute("PRAGMA legacy_alter_table").fetchone()[0]
        connection.execute("PRAGMA legacy_alter_table = OFF")
        try:
            for statement in renames(self._tables, legacy):
                connection.execute(statement)
        finally:
            connection.execute(f"PRAGMA legacy_alter_table = {int(kept)}")

    def _install_through(self, cursor: sqlite3.Cursor) -> None:
        connection = cursor.connection
        if connection.in_transaction:
            cursor.execute("SAVEPOINT tinyjobs_install")
            try:
                self._take_write_lock(cursor)
                if not self._current(cursor):
                    self._install_schema(cursor)
            except BaseException as error:
                if connection.in_transaction:
                    cursor.execute("ROLLBACK TO tinyjobs_install")
                if _table_locked(error):
                    raise _reworded(OPEN_CURSOR_MESSAGE, error) from error
                raise
            finally:
                if connection.in_transaction:
                    cursor.execute("RELEASE tinyjobs_install")
            return
        with suppress(sqlite3.OperationalError):
            cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("BEGIN IMMEDIATE")
        try:
            if not self._current(cursor):
                self._install_schema(cursor)
            cursor.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                cursor.execute("ROLLBACK")
            raise

    def _take_write_lock(self, cursor: sqlite3.Cursor) -> None:
        lock = self._tables.lock
        try:
            cursor.execute(f"DELETE FROM {lock} WHERE 0")
            return
        except sqlite3.OperationalError as error:
            if str(error) != f"no such table: {lock}":
                raise
        try:
            cursor.execute(f"CREATE TABLE {lock} (taken INTEGER)")
        except sqlite3.OperationalError as error:
            if str(error) != f"table {lock} already exists":
                raise
            cursor.execute(f"DELETE FROM {lock} WHERE 0")

    def _stored_version(self, connection: Any) -> tuple[int | None, tuple[str, ...]]:
        present = {row[0] for row in connection.execute(TABLES_OF)}
        if self._tables.meta in present:
            row = connection.execute(self._queries.schema_version).fetchone()
            return (int(row[0]) if row else None), ()
        legacy = tuple(kind for kind in LEGACY_KINDS if kind in present)
        if not _legacy_queue(connection, present):
            return None, ()
        row = connection.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        version = str(row[0]) if row else ""
        if not version.isdigit() or not 1 <= int(version) < PREFIXED_SINCE:
            return None, ()
        if self._tables.prefix != DEFAULT_PREFIX:
            raise ConfigurationError(LEGACY_PREFIX_MESSAGE)
        return int(version), legacy

    def _current(self, connection: Any) -> bool:
        return self._stored_version(connection) == (SCHEMA_VERSION, ())

    def _migrate(self, connection: Any, current: int) -> None:
        if current >= SCHEMA_VERSION:
            return
        for version in range(current + 1, SCHEMA_VERSION + 1):
            for statement in self._migrations.get(version, ()):
                connection.execute(statement)
        connection.execute(self._queries.write_schema_version, (str(SCHEMA_VERSION),))

    def _close_sync(self) -> None:
        self._connections.close()

    def transaction(self) -> AbstractContextManager[sqlite3.Connection]:
        return self._connections.write()

    def refuse_held_transaction(self) -> None:
        if self._connections.held_here():
            raise RuntimeError(HELD_TRANSACTION_MESSAGE)

    def enqueue_in(self, tx: Any, jobs: Sequence[Job]) -> list[Job]:
        cursor = _connection_of(tx).cursor()
        cursor.row_factory = sqlite3.Row
        try:
            self._check_joined(cursor)
            try:
                return self._insert_through(cursor, jobs)
            except sqlite3.OperationalError as error:
                if getattr(error, "sqlite_errorcode", 0) == sqlite3.SQLITE_BUSY_SNAPSHOT:
                    raise _reworded(READ_FIRST_MESSAGE, error) from error
                raise
        finally:
            cursor.close()

    async def aenqueue_in(self, tx: Any, jobs: Sequence[Job]) -> list[Job]:
        return self.enqueue_in(tx, jobs)

    def _insert_through(self, cursor: sqlite3.Cursor, jobs: Sequence[Job]) -> list[Job]:
        if not self._ready:
            self._prepare_through(cursor)
        try:
            return self._insert_all(cursor, jobs)
        except sqlite3.OperationalError as error:
            if not self._jobs_missing(error):
                raise
        self._ready = False
        self._prepare_through(cursor)
        try:
            return self._insert_all(cursor, jobs)
        except sqlite3.OperationalError as error:
            if self._jobs_missing(error) and cursor.connection.in_transaction:
                raise _reworded(STALE_READ_MESSAGE, error) from error
            raise

    def _jobs_missing(self, error: sqlite3.OperationalError) -> bool:
        return str(error) == f"no such table: {self._tables.jobs}"

    def _prepare_through(self, cursor: sqlite3.Cursor) -> None:
        if self._current_outside():
            self._ready = True
            return
        self._install_through(cursor)
        self._ready = not cursor.connection.in_transaction

    def _current_outside(self) -> bool:
        connection = sqlite3.connect(str(self._connections.file), timeout=0)
        try:
            return self._current(connection)
        except sqlite3.OperationalError as error:
            if not _busy(error):
                raise
            return False
        finally:
            connection.close()

    def _check_joined(self, cursor: sqlite3.Cursor) -> None:
        if self._connections.memory:
            raise UnsupportedCapabilityError(IN_MEMORY_JOIN_MESSAGE)
        own = self._connections.file
        theirs = ""
        for row in cursor.execute("PRAGMA database_list"):
            if row["name"] == "main":
                theirs = row["file"]
        if not theirs:
            raise ConfigurationError(
                f"the connection passed as tx is open on an in-memory or temporary database, "
                f"not on {own}, the file this app uses; connect it to that file"
            )
        if os.path.realpath(theirs) != own:
            raise ConfigurationError(
                f"the connection passed as tx is open on {theirs}, not on {own}, the file "
                f"this app uses; a job can only join a transaction on the queue's own file, "
                f"so connect to it, or under Django make the alias's NAME, its TEST NAME in "
                f"tests, the file in url"
            )

    def _enqueue_sync(self, job: Job) -> Job:
        with self._connections.write() as connection:
            return self._insert(connection, job)

    def _enqueue_many_sync(self, jobs: Sequence[Job]) -> list[Job]:
        with self._connections.write() as connection:
            return self._insert_all(connection, jobs)

    def _insert_all(self, connection: Any, jobs: Sequence[Job]) -> list[Job]:
        if any(job.dedup_key is not None for job in jobs):
            return [self._insert(connection, job) for job in jobs]
        connection.executemany(self._queries.insert_job, [job_to_row(job) for job in jobs])
        return list(jobs)

    def _insert(self, connection: Any, job: Job) -> Job:
        try:
            connection.execute(self._queries.insert_job, job_to_row(job))
        except sqlite3.IntegrityError:
            if job.dedup_key is None:
                raise
            existing = self._active_duplicate(connection, job.dedup_key)
            if existing is None:
                raise
            return existing
        return job

    def _active_duplicate(self, connection: Any, dedup_key: str) -> Job | None:
        row = connection.execute(self._queries.select_active_by_dedup, (dedup_key,)).fetchone()
        return job_from_row(row) if row else None

    def _claim_sync(
        self,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
        limits: Mapping[str, Limit] | None,
    ) -> list[Job]:
        if limit <= 0 or not queues:
            return []
        now = self._clock.now()
        strategy = self._claim_strategy.claim
        if limits:
            strategy = partial(self._limited_claim.claim, limits=limits)
        with self._connections.write() as connection:
            rows = strategy(
                connection,
                worker_id=worker_id,
                queues=tuple(queues),
                limit=limit,
                lease_expires_at=now + lease_seconds,
                now=now,
            )
            if rows:
                connection.executemany(
                    self._queries.insert_execution,
                    [(row["id"], row["attempts"], row["claims"], worker_id, now) for row in rows],
                )
        return [job_from_row(row) for row in rows]

    def _claim_job_sync(
        self, job_id: str, worker_id: str, lease_seconds: float
    ) -> Job | None:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                self._queries.claim_due_one, (worker_id, now + lease_seconds, job_id, now)
            )
            if cursor.rowcount != 1:
                return None
            job = job_from_row(connection.execute(self._queries.select_job, (job_id,)).fetchone())
            connection.execute(
                self._queries.insert_execution, (job.id, job.attempts, job.claims, worker_id, now)
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
                self._queries.heartbeat.format(ids=placeholders),
                (expiry, worker_id, worker_id, *job_ids),
            )
            return [
                row[0]
                for row in connection.execute(
                    self._queries.renewed.format(ids=placeholders),
                    (expiry, worker_id, worker_id, *job_ids),
                )
            ]

    def _complete_sync(self, job: Job, result: bytes | None) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(
                self._queries.complete_job, (now, result, job.id, job.worker_id, job.claims)
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
                    self._queries.fail_job, (now, error.summary, job.id, job.worker_id, job.claims)
                )
            else:
                cursor = connection.execute(
                    self._queries.reschedule_job,
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
                self._queries.release_job, (now + delay, job.id, job.worker_id, job.claims)
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(
                self._queries.mark_executions_lost.format(ids="?"), (now, job.id)
            )
        return True

    def _release_many_sync(self, jobs: Sequence[Job]) -> int:
        now = self._clock.now()
        released = 0
        with self._connections.write() as connection:
            for job in jobs:
                cursor = connection.execute(
                    self._queries.release_job, (now, job.id, job.worker_id, job.claims)
                )
                if cursor.rowcount == 1:
                    connection.execute(self._queries.mark_executions_lost.format(ids="?"), (now, job.id))
                    released += 1
        return released

    def _hand_back_sync(self, job: Job, delay: float) -> bool:
        with self._connections.write() as connection:
            cursor = connection.execute(
                self._queries.hand_back_job,
                (self._clock.now() + delay, job.id, job.worker_id, job.claims),
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(self._queries.delete_execution, (job.id, job.claims))
        return True

    def _release_held_sync(self, tasks: Sequence[str], queues: Sequence[str] | None) -> int:
        marks = ", ".join("?" for _ in tasks)
        within = "" if queues is None else " AND queue IN (" + ", ".join("?" for _ in queues) + ")"
        statement = self._queries.release_held.format(tasks=marks, queues=within)
        released = 0
        while True:
            with self._connections.write() as connection:
                batch = connection.execute(
                    statement, (*tasks, *(queues or ()), RELEASE_BATCH)
                ).rowcount
            released += batch
            if batch < RELEASE_BATCH:
                return released

    def _cancel_sync(self, job_id: str) -> bool:
        now = self._clock.now()
        with self._connections.write() as connection:
            cursor = connection.execute(self._queries.cancel_job, (now, job_id))
            if cursor.rowcount != 1:
                return False
            connection.execute(self._queries.cancel_executions, (now, job_id))
        return True

    def _requeue_sync(self, job_id: str) -> bool:
        with self._connections.write() as connection:
            cursor = connection.execute(self._queries.requeue_job, (self._clock.now(), job_id))
        return cursor.rowcount == 1

    def _get_sync(self, job_id: str) -> Job | None:
        row = self._connections.connection.execute(self._queries.select_job, (job_id,)).fetchone()
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
        rows = self._connections.connection.execute(
            self._queries.listing(where), (*parameters, limit + 1)
        ).fetchall()
        jobs = [job_from_row(row) for row in rows[:limit]]
        next_cursor = jobs[-1].id if len(rows) > limit and jobs else None
        return Page(items=jobs, cursor=next_cursor)

    def _executions_sync(self, job_id: str) -> list[Execution]:
        rows = self._connections.connection.execute(
            self._queries.select_executions, (job_id,)
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
                self._queries.mark_executions_lost.format(ids=placeholders), (now, *expired)
            )
        return len(expired)

    def _purge_sync(self, policy: RetentionPolicy, now: float) -> int | None:
        if not policy.enabled:
            return None
        with self._connections.write() as connection:
            won = connection.execute(
                self._queries.claim_housekeeping, (str(now), now - policy.interval)
            ).fetchone()
        if won is None:
            return None
        removed = 0
        for status, cutoff in policy.horizons(now).items():
            while removed < policy.maximum_per_sweep:
                batch = min(policy.batch, policy.maximum_per_sweep - removed)
                with self._connections.write() as connection:
                    gone = connection.execute(
                        self._queries.purge_batch, (str(status), cutoff, batch)
                    ).fetchall()
                removed += len(gone)
                if len(gone) < batch:
                    break
        return removed

    def _expire_results_sync(
        self, task: str, since: float, cutoff: float, batch: int
    ) -> int:
        with self._connections.write() as connection:
            return connection.execute(self._queries.expire_results, (since, cutoff, task, batch)).rowcount

    def _counts_by_status_sync(self) -> dict[str, int]:
        rows = self._connections.connection.execute(self._queries.count_by_status).fetchall()
        return {row["status"]: row["total"] for row in rows}

    def _audit_sync(self) -> list[Finding]:
        connection = self._connections.connection
        database = connection.execute("PRAGMA database_list").fetchone()["file"]
        return audit(connection, database, self._connections.uri)

    def _stats_sync(self, now: float) -> Stats:
        connection = self._connections.connection
        counts: dict[str, dict[str, int]] = {}
        for row in connection.execute(self._queries.count_by_queue).fetchall():
            counts.setdefault(row["queue"], {})[row["status"]] = row["total"]
        oldest = {
            row["queue"]: row["oldest"]
            for row in connection.execute(self._queries.oldest_due_by_queue, (now,)).fetchall()
        }
        expired = connection.execute(self._queries.count_expired_leases, (now,)).fetchone()
        queued = connection.execute(self._queries.queued_by_task).fetchall()
        return Stats(
            queues=[
                QueueStats(queue, counts[queue], oldest.get(queue))
                for queue in sorted(counts)
            ],
            expired_leases=expired["total"],
            queued_tasks={row["task"]: row["total"] for row in queued},
        )

    def _schedules_sync(self) -> list[ScheduleState]:
        rows = self._connections.connection.execute(self._queries.select_schedules).fetchall()
        return [schedule_from_row(row) for row in rows]

    def _register_schedules_sync(self, states: Sequence[ScheduleState]) -> None:
        with self._connections.write() as connection:
            connection.executemany(
                self._queries.register_schedule,
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
            cursor = connection.execute(self._queries.advance_schedule, (next_run_at, name, expected))
            if cursor.rowcount != 1:
                return None
            stored = [self._insert(connection, job) for job in jobs]
            if stored:
                connection.execute(
                    self._queries.record_schedule_run, (self._clock.now(), stored[-1].id, name)
                )
        return stored

    def _select_expired(
        self, connection: sqlite3.Connection, now: float
    ) -> list[str]:
        if SUPPORTS_RETURNING:
            rows = connection.execute(self._queries.reap_expired, (now, now, now)).fetchall()
            return [row["id"] for row in rows]
        rows = connection.execute(self._queries.select_expired_ids, (now,)).fetchall()
        for row in rows:
            exhausted = row["attempts"] >= row["max_attempts"]
            connection.execute(
                self._queries.expire_one,
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
            self._queries.finish_execution,
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
        row = self._connections.connection.execute(self._queries.schema_version).fetchone()
        _refuse_newer(int(row[0]) if row else SCHEMA_VERSION)


def _refuse_newer(stored: int) -> None:
    if stored > SCHEMA_VERSION:
        raise RuntimeError(
            f"database schema version {stored} is newer than this tinyjobs "
            f"build understands ({SCHEMA_VERSION}); upgrade the package"
        )


def _legacy_queue(connection: Any, present: set[str]) -> bool:
    if not {"meta", "jobs", "executions"} <= present:
        return False
    return (
        _columns(connection, "meta") >= {"key", "value"}
        and _columns(connection, "jobs") >= LEGACY_JOB_COLUMNS
        and _columns(connection, "executions") >= {"job_id", "attempt"}
    )


def _columns(connection: Any, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _connection_of(tx: Any) -> sqlite3.Connection:
    if isinstance(tx, sqlite3.Cursor):
        return tx.connection
    if isinstance(tx, sqlite3.Connection):
        return tx
    kind = type(tx)
    name = kind.__qualname__ if kind.__module__ == "builtins" else f"{kind.__module__}.{kind.__qualname__}"
    raise TypeError(TX_TYPE_MESSAGE.format(kind=name))


def _executor() -> CountingPool:
    return CountingPool(max_workers=1, thread_name_prefix="tinyjobs-sqlite")


def _retrieved(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()


def _gated(
    pool: CountingPool | None, end: Deadline | None, function: Callable[..., T], *args: Any
) -> T:
    if end is not None:
        CALLING.end = end
    try:
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
    finally:
        if end is not None:
            CALLING.end = None


def _busy_as_transient(function: Callable[..., T], *args: Any) -> T:
    try:
        return function(*args)
    except sqlite3.OperationalError as error:
        if not _busy(error):
            raise
        raise _transient(error) from error


def _reworded(message: str, error: sqlite3.OperationalError) -> sqlite3.OperationalError:
    reworded = sqlite3.OperationalError(message)
    reworded.sqlite_errorcode = error.sqlite_errorcode
    reworded.sqlite_errorname = error.sqlite_errorname
    return reworded


def _table_locked(error: BaseException) -> bool:
    return isinstance(error, sqlite3.OperationalError) and str(error).startswith(
        "database table is locked"
    )


def _busy(error: sqlite3.OperationalError) -> bool:
    return getattr(error, "sqlite_errorcode", 0) & 0xFF in BUSY_CODES


def _transient(error: sqlite3.OperationalError) -> DatabaseBusyError:
    busy = DatabaseBusyError(*error.args)
    busy.sqlite_errorcode = error.sqlite_errorcode
    busy.sqlite_errorname = error.sqlite_errorname
    return busy
