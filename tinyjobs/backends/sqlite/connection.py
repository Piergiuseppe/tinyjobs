from __future__ import annotations

import _thread
import asyncio
import bisect
import contextlib
import dis
import gc
import inspect
import logging
import os
import sqlite3
import sys
import threading
import time
import weakref
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from functools import partial
from types import CodeType, FrameType
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit

from ...runtime import (
    _fork_marks,
    at_exit,
    before_each_call,
    fork_state,
    forking_elsewhere,
    forking_threads,
    hold_at_fork,
    refuse_inside,
    when_collected,
)
from ...exceptions import InterruptedCallError

logger = logging.getLogger("tinyjobs.sqlite")

FILE_PRAGMAS = ("PRAGMA synchronous = NORMAL",)

WAL_RETRY_SECONDS = 0.05

SHARED_PRAGMAS = ("PRAGMA foreign_keys = ON",)

FORK_WAIT_SECONDS = 10.0

FORK_POLL_SECONDS = 0.01

SEAL_POLL_SECONDS = 0.001

REGISTRY_PRUNE = 64

SHUT_SECONDS = 1.0

CLOSED_MESSAGE = "Cannot operate on a closed database."

EVERYONE = (0, 0)

FORKED_TRANSACTION_MESSAGE = (
    "this process was forked inside a tinyjobs transaction; the transaction belongs "
    "to the parent, so the child neither commits nor rolls it back"
)


class _Token:
    __slots__ = ("__weakref__",)


class StepAside(Exception):
    pass


ABANDONED_WRITE_MESSAGE = (
    "this tinyjobs transaction was interrupted and a later write on this thread "
    "rolled it back, so it cannot commit"
)

POISONED_WRITE_MESSAGE = (
    "a nested tinyjobs write inside this transaction was interrupted before it "
    "finished, so whether its work should stand is unknown; the whole transaction "
    "is rolled back instead of committing it"
)

HELD_ELSEWHERE_MESSAGE = (
    "another asyncio task on this thread holds an open tinyjobs transaction; SQLite "
    "gives each thread one connection, so this task cannot write until that "
    "transaction ends"
)

INSIDE_WRITE_MESSAGE = (
    "a tinyjobs call was made while this thread was opening or closing a tinyjobs "
    "transaction, which is where a signal handler lands; it is refused so the "
    "transaction stays whole. Set a flag in the handler and act on it afterwards"
)

CACHED_CODES = 1024

LENT_KEPT = 4

UNCONDITIONAL_JUMPS = frozenset(
    {
        "JUMP",
        "JUMP_FORWARD",
        "JUMP_BACKWARD",
        "JUMP_NO_INTERRUPT",
        "JUMP_BACKWARD_NO_INTERRUPT",
    }
)

SILENT_OPS = UNCONDITIONAL_JUMPS | {
    "NOP",
    "NOT_TAKEN",
    "EXTENDED_ARG",
    "POP_JUMP_IF_TRUE",
    "POP_JUMP_IF_FALSE",
    "POP_JUMP_IF_NONE",
    "POP_JUMP_IF_NOT_NONE",
}

ENDING_OPS = frozenset(
    {"RETURN_VALUE", "RETURN_CONST", "RAISE_VARARGS", "RERAISE", "RETURN_GENERATOR"}
)

JUMP_OPCODES = frozenset(
    getattr(dis, "hasjump", ()) or (*dis.hasjrel, *dis.hasjabs)
)

HANDLER_PREFIX = 8

BEFORE_WITH = dis.opmap.get("BEFORE_WITH")

CALL = dis.opmap["CALL"]

SEND = dis.opmap["SEND"]

LOAD_SPECIAL = dis.opmap.get("LOAD_SPECIAL")

ENTER_SPECIAL = (getattr(dis, "_special_method_names", None) or ["__enter__"]).index("__enter__")

CONTEXTLIB_FILE = contextlib.__file__

GENERATOR_FLAGS = inspect.CO_GENERATOR | inspect.CO_COROUTINE | inspect.CO_ASYNC_GENERATOR

FREE_FLAGS = inspect.CO_GENERATOR | inspect.CO_ASYNC_GENERATOR

ENTER_CONTEXT_CODE = contextlib._BaseExitStack.enter_context.__code__

STACK_CODES = frozenset(
    {
        ENTER_CONTEXT_CODE,
        contextlib.ExitStack.__exit__.__code__,
        contextlib.AsyncExitStack.__aexit__.__code__,
    }
)

Spans = tuple[tuple[int, ...], tuple[int, ...]]

NO_SPANS: Spans = ((), ())

NO_BODY: tuple[Spans | None, Spans] = (None, NO_SPANS)

_analyses: dict[int, tuple[CodeType, dict[int, Any]]] = {}

_writing: ContextVar[tuple[weakref.ref, ...]] = ContextVar("tinyjobs_writing", default=())

FOOTPRINT_QUERY = "SELECT * FROM pragma_schema_version, pragma_user_version, pragma_application_id"

JOINED_ELSEWHERE_MESSAGE = (
    "a write that joined this tinyjobs transaction is still running on another thread, "
    "so the transaction cannot commit under it and is rolled back instead"
)

OPEN_INNER_MESSAGE = (
    "a nested tinyjobs write entered by hand is still open inside this transaction, so "
    "it cannot commit and is rolled back instead"
)

DETACHED_WRITE_MESSAGE = (
    "this nested tinyjobs write outlived the write it joined, so it is no longer part "
    "of any transaction and can no longer write"
)

ENDED_OUTER_MESSAGE = (
    "the transaction this tinyjobs write would join was already ended by SQLite, for "
    "example by INSERT OR ROLLBACK, so the write is refused and the outer one rolls back"
)

ENDING_INSIDE_MESSAGE = (
    "this connection is inside an open tinyjobs write, which commits or rolls back "
    "when its block exits, so commit(), rollback(), executescript(), `with connection:` "
    "and setting isolation_level or autocommit are refused: they would end its "
    "transaction early"
)

ENDED_ROOT_MESSAGE = (
    "the transaction of this tinyjobs write was ended inside its block, for example "
    "by INSERT OR ROLLBACK or an explicit COMMIT, so it did not commit or roll back "
    "as one unit and every statement after that point committed on its own"
)

ABANDONED_ROLLBACK_MESSAGE = (
    "a tinyjobs transaction was dropped without exiting its block; its work, and any "
    "write that joined it, was rolled back"
)


OPENING, OPEN, CLOSING = "opening", "open", "closing"


class _WriteScope:
    __slots__ = (
        "__weakref__",
        "_pool",
        "state",
        "outer",
        "inner",
        "connection",
        "lent",
        "token",
        "entry",
        "generation",
        "savepoint",
        "frame",
        "callbacks",
        "holder",
        "generator",
        "body",
        "exits",
        "task",
        "poisoned",
        "orphan",
        "footprint",
        "entry_offset",
        "handed",
        "ending",
        "detached",
        "parted",
        "anchors",
        "awaited",
    )

    def __init__(self, pool: "ConnectionPool | None") -> None:
        self._pool = pool
        self.state = OPENING
        self.outer: _WriteScope | None = None
        self.inner: Callable[[], _WriteScope | None] = _nothing
        self.lent = False
        self.token: _Token | None = None
        self.entry: weakref.ref | None = None
        self.savepoint: str | None = None
        self.frame: FrameType | None = None
        self.callbacks: weakref.ref | None = None
        self.holder: Any = None
        self.generator = False
        self.body: Spans | None = None
        self.exits: Spans = NO_SPANS
        self.task: Any = None
        self.poisoned = False
        self.orphan: tuple[int, bool, tuple[Any, ...]] | None = None
        self.footprint: tuple[Any, ...] = ()
        self.entry_offset = -1
        self.handed = [False]
        self.ending: tuple[type[BaseException] | None] | None = None
        self.detached = False
        self.parted = False
        self.anchors: tuple[FrameType, ...] = ()
        self.awaited = False

    @property
    def strict(self) -> bool:
        return not self.generator

    def __enter__(self) -> sqlite3.Connection:
        connection = self._pool._begin(self, sys._getframe(1))
        self.handed[0] = True
        return connection

    def __exit__(self, kind, error, traceback) -> bool:
        try:
            self._pool._end(self, kind)
        except BaseException:
            self._pool._salvage(self)
            raise
        return False


EXIT_CODE = _WriteScope.__exit__.__code__


class ForkGate:
    def __init__(self) -> None:
        self._holders: dict[weakref.ref, tuple[int, int]] = {}
        self._alive: dict[weakref.ref, Callable[[], bool]] = {}
        self._connections: dict[weakref.ref, None] = {}
        self._prune_at = REGISTRY_PRUNE

    @contextmanager
    def using(
        self, give_up: Callable[[], bool] | None = None
    ) -> Iterator[weakref.ref]:
        token = _Token()
        ref = self.enter(token, give_up)
        try:
            yield ref
        finally:
            self._holders.pop(ref, None)

    def enter(
        self,
        token: _Token,
        give_up: Callable[[], bool] | None = None,
        alive: Callable[[], bool] | None = None,
    ) -> weakref.ref:
        ref = weakref.ref(token)
        try:
            if alive is not None:
                self._alive[ref] = alive
            self._enter(ref, give_up)
        except BaseException:
            self.leave(ref)
            raise
        return ref

    def _step_aside(self, key: tuple[int, int], flights: dict[tuple[int, int], int]) -> None:
        if self._in_call(key, flights):
            return
        while forking_elsewhere(*key) and not self._holds_any(key):
            flights.pop(key, None)
            time.sleep(FORK_POLL_SECONDS)
            flights[key] = 1

    def _in_call(self, key: tuple[int, int], flights: dict[tuple[int, int], int]) -> bool:
        return any(
            connection.flights is not flights and key in connection.flights
            for connection in self._live_connections()
        )

    def _live_connections(self) -> list["GatedConnection"]:
        return [connection for ref in list(self._connections) if (connection := ref()) is not None]

    def _register(self, connection: "GatedConnection") -> None:
        connections = self._connections
        connections[weakref.ref(connection)] = None
        if len(connections) > self._prune_at:
            for ref in [ref for ref in list(connections) if ref() is None]:
                connections.pop(ref, None)
            self._prune_at = 2 * len(connections) + REGISTRY_PRUNE

    def _holds_any(self, key: tuple[int, int]) -> bool:
        return any(
            held() is not None and holder == key for held, holder in list(self._holders.items())
        )

    def leave(self, ref: weakref.ref | None) -> None:
        self._holders.pop(ref, None)
        self._alive.pop(ref, None)

    def wait_quiet(self, ident: int, pid: int) -> None:
        if self._holds_any((ident, pid)):
            return
        deadline = time.monotonic() + FORK_WAIT_SECONDS
        while not self._quiet(pid) and time.monotonic() < deadline:
            time.sleep(FORK_POLL_SECONDS)

    def _enter(self, ref: weakref.ref, give_up: Callable[[], bool] | None) -> None:
        ident = _thread.get_ident()
        pid = os.getpid()
        while True:
            self._holders[ref] = (ident, pid)
            if self._holds_another(ref, ident, pid) or not forking_elsewhere(ident, pid):
                return
            self._holders.pop(ref, None)
            if give_up is not None and give_up():
                raise StepAside
            time.sleep(FORK_POLL_SECONDS)

    def _holds_another(self, ref: weakref.ref, ident: int, pid: int) -> bool:
        another = False
        for held, holder in list(self._holders.items()):
            if held() is None:
                self.leave(held)
            elif held is not ref and holder == (ident, pid):
                another = True
        return another

    def _quiet(self, pid: int) -> bool:
        excused = forking_threads(pid)
        for connection in self._live_connections():
            for ident, owner in list(connection.flights):
                if owner == pid and ident not in excused:
                    return False
        for held, (ident, owner) in list(self._holders.items()):
            if owner != pid or held() is None:
                self.leave(held)
            elif ident not in excused and self._still_held(held):
                return False
        return True

    def _still_held(self, held: weakref.ref) -> bool:
        alive = self._alive.get(held)
        return alive is None or alive()


FORK_GATE = ForkGate()
hold_at_fork(FORK_GATE)


def _gated(method: Callable[..., Any], on_cursor: bool) -> Callable[..., Any]:
    get_ident, getpid = _thread.get_ident, os.getpid

    def call(*args: Any, **options: Any) -> Any:
        connection = args[0].connection if on_cursor else args[0]
        key = (get_ident(), getpid())
        flights = connection.flights
        flying = flights.get(key, 0)
        flights[key] = flying + 1
        opened = False
        try:
            if _fork_marks and not flying:
                FORK_GATE._step_aside(key, flights)
            connection.visitors.add(key)
            retired = connection.retired
            if retired is not None and retired[0] != key:
                raise sqlite3.ProgrammingError(retired[1])
            opened = connection.in_transaction
            return method(*args, **options)
        except sqlite3.Error as error:
            if opened:
                _note_ending(connection, error)
            raise
        finally:
            if flying:
                flights[key] = flying
            else:
                flights.pop(key, None)

    return call


def _note_ending(connection: sqlite3.Connection, error: sqlite3.Error) -> None:
    with suppress(sqlite3.ProgrammingError):
        if not connection.in_transaction:
            connection.ending = (weakref.ref(error), connection.total_changes)


def _sparing_writes(method: Callable[..., Any], on_cursor: bool) -> Callable[..., Any]:
    def call(*args: Any, **options: Any) -> Any:
        connection = args[0].connection if on_cursor else args[0]
        if any(pool._writer(connection) is not None for pool in _live_pools()):
            raise sqlite3.ProgrammingError(ENDING_INSIDE_MESSAGE)
        return method(*args, **options)

    return call


def _seal(
    connection: sqlite3.Connection,
    holder: tuple[int, int],
    message: str,
    patience: float | None,
    stopping: bool = False,
) -> bool:
    connection.retired = (holder, message)
    deadline = None if patience is None else time.monotonic() + patience
    while _flying_besides(connection, holder):
        if stopping:
            with suppress(sqlite3.Error):
                connection.interrupt()
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(SEAL_POLL_SECONDS)
    return True


def _visited_besides(connection: sqlite3.Connection, holder: tuple[int, int]) -> bool:
    pid = os.getpid()
    return _flying_besides(connection, holder) or any(
        key != holder and key[1] == pid for key in list(connection.visitors)
    )


def _roll_back(connection: sqlite3.Connection) -> None:
    if not _in_transaction(connection):
        return
    try:
        connection.execute("ROLLBACK")
    except sqlite3.OperationalError:
        try:
            _clear_statements(connection)
        except BaseException:
            with suppress(sqlite3.Error):
                _clear_statements(connection)
            raise


def _roll_back_retired(connection: sqlite3.Connection) -> None:
    with suppress(sqlite3.OperationalError):
        _roll_back(connection)


def _clear_statements(connection: sqlite3.Connection) -> None:
    if _close_until_rolled_back(connection):
        return
    if gc.get_freeze_count():
        try:
            gc.unfreeze()
            if _close_until_rolled_back(connection):
                return
        finally:
            try:
                gc.collect()
            finally:
                gc.freeze()
    connection.execute("ROLLBACK")


def _close_until_rolled_back(connection: sqlite3.Connection) -> bool:
    holders = [
        holder
        for holder in gc.get_referrers(connection)
        if isinstance(holder, (sqlite3.Cursor, sqlite3.Blob))
    ]
    running = [holder for holder in holders if _running_statement(holder)]
    for holder in (*running, *holders):
        with suppress(sqlite3.Error):
            if isinstance(holder, sqlite3.Cursor):
                sqlite3.Cursor.close(holder)
            else:
                holder.close()
        if _rolled_back(connection):
            return True
    return False


def _running_statement(holder: sqlite3.Cursor | sqlite3.Blob) -> bool:
    if isinstance(holder, sqlite3.Blob):
        return True
    return any(type(held).__name__ == "Statement" for held in gc.get_referents(holder))


def _rolled_back(connection: sqlite3.Connection) -> bool:
    if not _in_transaction(connection):
        return True
    try:
        connection.execute("ROLLBACK")
    except sqlite3.OperationalError:
        return False
    return True


def _flying_besides(connection: sqlite3.Connection, holder: tuple[int, int]) -> bool:
    pid = os.getpid()
    return any(key != holder and key[1] == pid for key in list(connection.flights))


def _shut(
    connection: sqlite3.Connection,
    message: str = CLOSED_MESSAGE,
    patience: float = SHUT_SECONDS,
) -> None:
    if _seal(connection, EVERYONE, message, patience, True) and not _flying_besides(connection, EVERYONE):
        with FORK_GATE.using():
            connection.close()


class GatedCursor(sqlite3.Cursor):
    execute = _gated(sqlite3.Cursor.execute, True)
    executemany = _gated(sqlite3.Cursor.executemany, True)
    executescript = _sparing_writes(_gated(sqlite3.Cursor.executescript, True), True)
    fetchone = _gated(sqlite3.Cursor.fetchone, True)
    fetchmany = _gated(sqlite3.Cursor.fetchmany, True)
    fetchall = _gated(sqlite3.Cursor.fetchall, True)
    __next__ = _gated(sqlite3.Cursor.__next__, True)


class GatedConnection(sqlite3.Connection):
    def __init__(self, *args: Any, **options: Any) -> None:
        super().__init__(*args, **options)
        self.flights: dict[tuple[int, int], int] = {}
        self.retired: tuple[tuple[int, int], str] | None = None
        self.visitors: set[tuple[int, int]] = set()
        self.ending: tuple[weakref.ref, int] | None = None
        FORK_GATE._register(self)

    def cursor(self, factory: Any = GatedCursor) -> sqlite3.Cursor:
        retired = self.retired
        if retired is not None and retired[0] != (_thread.get_ident(), os.getpid()):
            raise sqlite3.ProgrammingError(retired[1])
        return super().cursor(factory)

    def execute(self, *args: Any) -> sqlite3.Cursor:
        return self.cursor().execute(*args)

    def executemany(self, *args: Any) -> sqlite3.Cursor:
        return self.cursor().executemany(*args)

    def executescript(self, *args: Any) -> sqlite3.Cursor:
        return self.cursor().executescript(*args)

    commit = _sparing_writes(_gated(sqlite3.Connection.commit, False), False)
    rollback = _sparing_writes(_gated(sqlite3.Connection.rollback, False), False)
    __enter__ = _sparing_writes(sqlite3.Connection.__enter__, False)
    __exit__ = _sparing_writes(sqlite3.Connection.__exit__, False)
    isolation_level = property(
        sqlite3.Connection.isolation_level.__get__,
        _sparing_writes(sqlite3.Connection.isolation_level.__set__, False),
    )
    autocommit = property(
        sqlite3.Connection.autocommit.__get__,
        _sparing_writes(sqlite3.Connection.autocommit.__set__, False),
    )


_pools: dict[weakref.ref, None] = {}

_pools_prune = [REGISTRY_PRUNE]


def _register_pool(pool: "ConnectionPool") -> None:
    _pools[weakref.ref(pool)] = None
    if len(_pools) > _pools_prune[0]:
        for ref in [ref for ref in list(_pools) if ref() is None]:
            _pools.pop(ref, None)
        _pools_prune[0] = 2 * len(_pools) + REGISTRY_PRUNE


def _live_pools() -> list["ConnectionPool"]:
    return [pool for ref in list(_pools) if (pool := ref()) is not None]


class ConnectionPool:
    def __init__(
        self, database: str, *, uri: bool = False, busy_timeout: float = 5.0
    ) -> None:
        self._database = database
        self._uri = uri
        self._file = _file_of(database, uri, self)
        self._busy_timeout = busy_timeout
        self._local = threading.local()
        self._all: set[sqlite3.Connection] = set()
        self._spare: list[sqlite3.Connection] = []
        self._owners: dict[sqlite3.Connection, threading.Thread] = {}
        self._writers: dict[sqlite3.Connection, weakref.ref] = {}
        self._lent: dict[sqlite3.Connection, None] = {}
        self._reclaims: dict[sqlite3.Connection, Callable[[], None]] = {}
        self._orphans: set[sqlite3.Connection] = set()
        self._detached: dict[int, weakref.ref] = {}
        self._pid = os.getpid()
        when_collected(self, _close_all, self._all, self._pid)
        _register_pool(self)
        self._keepalive = self._connect() if uri else None

    @property
    def database(self) -> str:
        return self._database

    @property
    def connection(self) -> sqlite3.Connection:
        if os.getpid() != self._pid:
            self._forget_inherited()
        if self._detached and self._from_detached(sys._getframe(1)):
            raise sqlite3.ProgrammingError(DETACHED_WRITE_MESSAGE)
        existing = getattr(self._local, "connection", None)
        if existing is not None and existing not in self._all:
            self._forget_local(existing)
            existing = None
        if existing is None:
            existing = self._adopt() or self._connect()
            owner = getattr(self._local, "owner", None) or _Token()
            forget = when_collected(
                owner, _retire, weakref.ref(self), existing, self._all, self._owners, self._pid
            )
            self._local.connection = existing
            self._local.owner = owner
            self._local.forget = forget
            self._owners[existing] = threading.current_thread()
        return existing

    def _forget_local(self, existing: sqlite3.Connection) -> None:
        self._owners.pop(existing, None)
        self._local.forget()
        del self._local.connection
        if self._writer(existing) is None and _in_transaction(existing):
            _shut(existing)

    def hand_over(self) -> None:
        existing = getattr(self._local, "connection", None)
        if existing is None:
            return
        self._local.forget()
        self._local.__dict__.clear()
        self._owners.pop(existing, None)
        self._spare.append(existing)

    def _adopt(self) -> sqlite3.Connection | None:
        try:
            spare = self._spare.pop()
        except IndexError:
            return None
        return spare if spare in self._all else None

    def write(self) -> _WriteScope:
        return _WriteScope(self)

    def held_here(self) -> bool:
        current = _current_task()
        for ref in _writing.get():
            scope = ref()
            if scope is None or scope.state != OPEN or scope._pool._file != self._file:
                continue
            if current is None or scope.task is None or scope.task is current:
                return True
        return False

    def _begin(self, scope: _WriteScope, frame: FrameType) -> sqlite3.Connection:
        if _inside_write(frame):
            raise InterruptedCallError(INSIDE_WRITE_MESSAGE)
        if self._detached and self._from_detached(frame):
            raise sqlite3.ProgrammingError(DETACHED_WRITE_MESSAGE)
        _place(scope, frame)
        scope.task = _current_task()
        connection = self.connection
        outer = self._open_write(connection)
        if connection.retired is not None:
            connection = self.connection
            outer = self._open_write(connection)
        if outer is not None and _live(outer) and not _shared_with(scope, outer, frame):
            raise RuntimeError(HELD_ELSEWHERE_MESSAGE)
        for top in (outer, *self._lent_here()):
            if top is None:
                continue
            if not _live(top):
                if _busy(top, frame):
                    raise InterruptedCallError(INSIDE_WRITE_MESSAGE)
            elif _joins(scope, top, frame):
                return self._nest(scope, top)
        if outer is None:
            return self._start(scope, connection)
        scope.lent = True
        return self._start(scope, self._lend())

    def _lent_here(self) -> list[_WriteScope]:
        here = threading.current_thread()
        tops = []
        for connection, owner in list(self._owners.items()):
            if owner is not here or connection is getattr(self._local, "connection", None):
                continue
            self._heal_lent(connection)
            writer = self._writer(connection)
            if writer is not None:
                tops.append(writer if not _live(writer) else self._prune(writer))
        return tops

    def _start(self, scope: _WriteScope, connection: sqlite3.Connection) -> sqlite3.Connection:
        scope.connection = connection
        scope.generation = fork_state.generation
        scope.token = _Token()
        try:
            scope.entry = FORK_GATE.enter(
                scope.token, alive=partial(_held, weakref.ref(scope))
            )
            settle = partial(
                _abandoned,
                weakref.ref(self),
                connection,
                scope.generation,
                scope.lent,
                scope.handed,
            )
            self._writers[connection] = _WriterRef(scope, settle)
            connection.execute("BEGIN IMMEDIATE")
            connection.ending = None
            connection.visitors = {(_thread.get_ident(), os.getpid()), *list(connection.flights)}
            scope.state = OPEN
            _writing.set((*(ref for ref in _writing.get() if _open_root(ref)), weakref.ref(scope)))
        except BaseException:
            self._finish(scope)
            raise
        return connection

    def _nest(self, scope: _WriteScope, outer: _WriteScope) -> sqlite3.Connection:
        scope.outer = outer
        scope.connection = outer.connection
        try:
            if not scope.connection.in_transaction:
                _poison(outer)
                raise RuntimeError(ENDED_OUTER_MESSAGE)
            scope.footprint = _footprint(scope.connection)
            outer.inner = weakref.ref(
                scope,
                partial(
                    _lost,
                    weakref.ref(outer),
                    scope.connection,
                    scope.generator,
                    id(scope),
                    scope.footprint,
                ),
            )
            scope.savepoint = f"tinyjobs_{id(scope)}"
            scope.connection.execute(f"SAVEPOINT {scope.savepoint}")
            scope.state = OPEN
        except BaseException:
            self._drop(scope)
            raise
        return scope.connection

    def _end(self, scope: _WriteScope, kind: type[BaseException] | None) -> None:
        self._prune(scope)
        if scope.outer is None and scope.ending is None and _running_here(scope.inner()):
            _await_running(scope.inner(), sys._getframe(0))
            scope.ending = (kind,)
            return
        if scope.outer is None and scope.inner() is not None:
            scope.parted = True
        owns = self._owns(scope)
        scope.state = CLOSING
        if scope.outer is not None:
            self._end_nested(scope, kind, owns)
            return
        self._close_root(scope, kind is None, owns)

    def _close_root(self, scope: _WriteScope, succeeded: bool, owns: bool) -> None:
        connection = scope.connection
        child = scope.inner()
        here = (_thread.get_ident(), os.getpid())
        retiring = scope.parted or child is not None or _visited_besides(connection, here)
        stopping = not (succeeded and owns and not scope.poisoned)
        ended = owns and _ended(connection)
        cause = _ending_cause(connection) if ended else None
        committed = False
        try:
            elsewhere = not owns and _running_below(child)
            if retiring:
                self._all.discard(connection)
                _seal(connection, here, DETACHED_WRITE_MESSAGE, None, stopping)
            if child is not None:
                self._remember_cut(child)
                _cut(scope, child)
            if succeeded:
                if elsewhere:
                    raise RuntimeError(JOINED_ELSEWHERE_MESSAGE)
                if not owns and child is not None:
                    raise RuntimeError(OPEN_INNER_MESSAGE)
                if not owns:
                    raise RuntimeError(ABANDONED_WRITE_MESSAGE)
                if _orphan_changed(scope):
                    scope.poisoned = True
                if scope.poisoned:
                    raise RuntimeError(POISONED_WRITE_MESSAGE)
                if fork_state.generation != scope.generation:
                    raise RuntimeError(FORKED_TRANSACTION_MESSAGE)
                if ended:
                    raise RuntimeError(ENDED_ROOT_MESSAGE)
                scope.connection.execute("COMMIT")
                committed = True
        finally:
            try:
                if retiring:
                    _seal(connection, here, DETACHED_WRITE_MESSAGE, None, stopping or not committed)
                    try:
                        if fork_state.generation == scope.generation and self._writer(connection) is scope:
                            _roll_back_retired(connection)
                    finally:
                        self._finish(scope)
                else:
                    self._finish(scope)
            finally:
                if retiring:
                    _shut(connection, DETACHED_WRITE_MESSAGE)
        if ended and not succeeded and _unreported_ending(sys.exception(), cause):
            raise RuntimeError(ENDED_ROOT_MESSAGE)

    def _remember_cut(self, child: _WriteScope | None) -> None:
        detached = self._detached
        while child is not None:
            detached[id(child)] = weakref.ref(child)
            child = child.inner()

    def _from_detached(self, frame: FrameType) -> bool:
        detached = self._detached
        found = False
        for key, ref in list(detached.items()):
            scope = ref()
            if scope is None:
                if detached.get(key) is ref:
                    detached.pop(key, None)
            elif any(_on_stack(anchor, frame) for anchor in scope.anchors):
                found = True
        return found

    def _end_nested(
        self, scope: _WriteScope, kind: type[BaseException] | None, owns: bool
    ) -> None:
        if scope.detached:
            self._detached.pop(id(scope), None)
        try:
            self._settle_nested(scope, kind, owns)
        except BaseException:
            _unlink(scope)
            self._end_waiting(scope, kind, False)
            raise
        _unlink(scope)
        self._end_waiting(scope, kind, kind is None)

    def _settle_nested(
        self, scope: _WriteScope, kind: type[BaseException] | None, owns: bool
    ) -> None:
        connection = scope.connection
        kept = kind is None or (not scope.strict and issubclass(kind, GeneratorExit))
        if not owns or not _in_transaction(connection) or self._writer(connection) is not _root(scope):
            current = self._writer(connection) is _root(scope) and _in_transaction(connection)
            if scope.detached:
                if not kept and current:
                    _poison(scope)
                return
            if kind is None:
                raise RuntimeError(ABANDONED_WRITE_MESSAGE)
            if not kept and current and _linked(scope):
                _poison(scope)
            return
        if not self._here(connection):
            if not kept and _changed(scope):
                _poison(scope)
            return
        try:
            self._undo_savepoint(scope, kept)
        except BaseException:
            _poison(scope)
            raise

    def _end_waiting(self, scope: _WriteScope, kind: type[BaseException] | None, loud: bool) -> None:
        root = _root(scope)
        if root.ending is None or root.state != OPEN or _awaits(root.inner()):
            return
        poisoned = root.ending[0] is None and root.poisoned
        succeeded = root.ending[0] is None and not root.poisoned
        owns = self._owns(root)
        root.state = CLOSING
        self._close_root(root, succeeded, owns)
        current = sys.exception()
        if poisoned and (current is None or isinstance(current, Exception)):
            raise RuntimeError(POISONED_WRITE_MESSAGE)
        if not succeeded and loud:
            raise RuntimeError(ABANDONED_WRITE_MESSAGE)

    def _undo_savepoint(self, scope: _WriteScope, kept: bool) -> None:
        connection = scope.connection
        if not kept and (scope.parted or _visited_besides(connection, (_thread.get_ident(), os.getpid()))):
            _poison(scope)
        if scope.savepoint is None:
            if not kept and _changed(scope):
                _poison(scope)
            return
        if not kept:
            if scope.strict:
                connection.execute(f"ROLLBACK TO {scope.savepoint}")
            elif _changed(scope):
                _poison(scope)
        connection.execute(f"RELEASE {scope.savepoint}")

    def _here(self, connection: sqlite3.Connection) -> bool:
        return self._owners.get(connection) is threading.current_thread()

    def _finish(self, scope: _WriteScope) -> None:
        connection = scope.connection
        try:
            if connection is not None and _in_transaction(connection):
                if fork_state.generation != scope.generation:
                    raise RuntimeError(FORKED_TRANSACTION_MESSAGE)
                if self._writer(connection) is scope:
                    _roll_back(connection)
        finally:
            try:
                if self._stuck(scope):
                    _roll_back_retired(connection)
            finally:
                try:
                    if self._stuck(scope):
                        self._all.discard(connection)
                        _shut(connection, DETACHED_WRITE_MESSAGE)
                finally:
                    self._release(scope)

    def _salvage(self, scope: _WriteScope) -> None:
        connection = scope.connection
        if scope.outer is not None or connection is None or self._writer(connection) is not scope:
            return
        scope.state = CLOSING
        with suppress(Exception):
            self._close_root(scope, False, True)

    def _stuck(self, scope: _WriteScope) -> bool:
        connection = scope.connection
        return (
            connection is not None
            and fork_state.generation == scope.generation
            and self._writer(connection) is scope
            and _in_transaction(connection)
        )

    def _release(self, scope: _WriteScope) -> None:
        connection = scope.connection
        if connection is not None and self._writer(connection) is scope:
            self._writers.pop(connection, None)
            if scope.lent or _claim_orphan(self, connection):
                self._give_back(connection)
        FORK_GATE.leave(scope.entry)
        scope.frame = scope.holder = None
        scope.anchors = ()
        scope.inner = _nothing

    def _drop(self, scope: _WriteScope) -> None:
        if scope.outer is None:
            self._release(scope)
            return
        if scope.state == OPEN:
            _poison(scope.outer)
        try:
            if scope.savepoint is not None and scope.connection.in_transaction:
                with suppress(sqlite3.OperationalError):
                    scope.connection.execute(f"RELEASE {scope.savepoint}")
        finally:
            _unlink(scope)

    def _prune(self, scope: _WriteScope) -> _WriteScope:
        current = scope
        while (child := current.inner()) is not None:
            if not _live(child):
                self._drop(child)
                break
            current = child
        return current

    def _open_write(self, connection: sqlite3.Connection) -> _WriteScope | None:
        root = self._writer(connection)
        if root is not None and not _live(root):
            if _busy(root):
                return root
            if root.parted or root.inner() is not None:
                self._retire_dead(root, connection)
                return None
            self._release(root)
            root = None
        if root is None:
            _roll_back(connection)
            return None
        return self._prune(root)

    def _retire_dead(self, root: _WriteScope, connection: sqlite3.Connection) -> None:
        here = (_thread.get_ident(), os.getpid())
        self._all.discard(connection)
        try:
            _seal(connection, here, DETACHED_WRITE_MESSAGE, None, True)
            self._remember_cut(root.inner())
            _cut(root, root.inner())
            _roll_back_retired(connection)
        finally:
            self._release(root)
            _shut(connection, DETACHED_WRITE_MESSAGE)

    def _owns(self, scope: _WriteScope) -> bool:
        child = scope.inner()
        if child is not None and _running_below(child) and (scope.outer is None or not _running_here(child)):
            return False
        if scope.outer is not None:
            linked = _linked(scope)
            if linked and child is not None:
                _hand_up(scope, child)
            return linked
        if child is not None:
            if _frameless(child):
                return False
            scope.parted = True
            self._remember_cut(child)
            _cut(scope, child)
        return self._writer(scope.connection) is scope

    def _leaving_here(self, frame: FrameType) -> bool:
        if os.getpid() != self._pid:
            return False
        here = threading.current_thread()
        for connection, owner in list(self._owners.items()):
            scope = self._writer(connection) if owner is here else None
            if scope is not None and not _live(scope) and _busy(scope, frame):
                return True
        return False

    def _adopt_orphan(self, connection: sqlite3.Connection) -> bool:
        writer = self._writer(connection)
        if writer is None or not (_live(writer) or _busy(writer)):
            return False
        self._owners.pop(connection, None)
        self._reclaims.pop(connection, None)
        self._orphans.add(connection)
        if self._writer(connection) is not writer and _claim_orphan(self, connection):
            self._give_back(connection)
        return True

    def _writer(self, connection: sqlite3.Connection) -> _WriteScope | None:
        ref = self._writers.get(connection)
        if ref is None:
            return None
        scope = ref()
        if scope is None and not _claimed_by_anyone(ref):
            ref.settle(ref)
            ref = self._writers.get(connection)
            if ref is None:
                return None
            scope = ref()
        return _SETTLING if scope is None else scope

    def _lend(self) -> sqlite3.Connection:
        while True:
            try:
                connection = self._lent.popitem()[0]
            except KeyError:
                connection = self._connect()
                break
            if connection in self._all:
                break
        here = threading.current_thread()
        self._owners[connection] = here
        self._reclaims[connection] = when_collected(
            self._local.owner, _reclaim, weakref.ref(self), connection, here
        )
        return connection

    def _give_back(self, connection: sqlite3.Connection) -> None:
        self._owners.pop(connection, None)
        forget = self._reclaims.pop(connection, None)
        if forget is not None:
            forget()
        if connection not in self._all:
            return
        if len(self._lent) < LENT_KEPT:
            self._lent[connection] = None
            return
        self._all.discard(connection)
        _shut(connection)

    def heal_here(self) -> None:
        if os.getpid() != self._pid:
            return
        here = threading.current_thread()
        mine = [
            connection
            for connection, owner in list(self._owners.items())
            if owner is here
            and connection is not getattr(self._local, "connection", None)
            and connection in self._all
        ]
        for connection in mine:
            self._heal_lent(connection)
        connection = getattr(self._local, "connection", None)
        if connection is not None and connection not in self._all:
            self._forget_local(connection)
            return
        if connection is None:
            return
        if self._writer(connection) is not None or connection.in_transaction:
            self._open_write(connection)

    def _heal_lent(self, connection: sqlite3.Connection) -> None:
        writer = self._writer(connection)
        if writer is not None and (_live(writer) or _busy(writer)):
            return
        if writer is not None and (writer.parted or writer.inner() is not None):
            self._retire_dead(writer, connection)
            return
        if writer is not None:
            self._writers.pop(connection, None)
            FORK_GATE.leave(writer.entry)
        _roll_back(connection)
        if self._writers.get(connection) is None:
            self._give_back(connection)

    def _forget_inherited(self) -> None:
        pid = os.getpid()
        self._all = set()
        self._spare = []
        self._owners = {}
        self._writers = {}
        self._lent = {}
        self._reclaims = {}
        self._orphans = set()
        self._detached = {}
        self._local = threading.local()
        when_collected(self, _close_all, self._all, pid)
        self._pid = pid

    def close(self) -> None:
        _close_all(self._all, self._pid)
        self._owners = {}
        self._writers = {}
        self._lent = {}
        self._reclaims = {}
        self._orphans = set()
        self._detached = {}
        self._spare = []
        self._keepalive = None
        self._local = threading.local()

    def close_idle(self) -> None:
        if os.getpid() != self._pid:
            return
        here = threading.current_thread()
        running = sys._current_frames()
        busy = {
            connection
            for connection, owner in list(self._owners.items())
            if owner is not here and (owner.is_alive() or owner.ident in running)
        }
        busy.update(self._orphans)
        busy.update(connection for connection in list(self._writers) if self._writer(connection) is not None)
        for connection in list(self._all):
            if connection not in busy:
                self._all.discard(connection)
                _shut(connection)

    def _connect(self) -> sqlite3.Connection:
        with FORK_GATE.using():
            return self._open()

    def _open(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database,
            uri=self._uri,
            isolation_level=None,
            timeout=self._busy_timeout,
            check_same_thread=False,
            factory=GatedConnection,
        )
        try:
            self._configure(connection)
        except BaseException:
            connection.close()
            raise
        self._all.add(connection)
        return connection

    def _configure(self, connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout * 1000)}")
        if not self._uri:
            self._enable_wal(connection)
            for pragma in FILE_PRAGMAS:
                connection.execute(pragma)
        for pragma in SHARED_PRAGMAS:
            connection.execute(pragma)

    def _enable_wal(self, connection: sqlite3.Connection) -> None:
        deadline = time.monotonic() + self._busy_timeout
        while True:
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(WAL_RETRY_SECONDS)


def _unreported_ending(current: BaseException | None, cause: sqlite3.Error | None) -> bool:
    if current is None:
        return True
    if not isinstance(current, Exception) or current is cause:
        return False
    return not (isinstance(current, RuntimeError) and current.args == (ENDED_OUTER_MESSAGE,))


def _ending_cause(connection: sqlite3.Connection) -> sqlite3.Error | None:
    ending = connection.ending
    if ending is None or ending[1] != connection.total_changes:
        return None
    return ending[0]()


def _ended(connection: sqlite3.Connection) -> bool:
    try:
        return not connection.in_transaction
    except sqlite3.ProgrammingError:
        return False


def _in_transaction(connection: sqlite3.Connection) -> bool:
    try:
        return connection.in_transaction
    except sqlite3.ProgrammingError:
        return False


def _live(scope: _WriteScope) -> bool:
    if scope.state != OPEN:
        return False
    if scope.ending is not None:
        return _live_below(scope.inner())
    if scope.callbacks is not None:
        return _registered(scope)
    frame = scope.frame
    if scope.body is None or frame is None:
        return True
    offset = frame.f_lasti
    return _contains(scope.body, offset) or (offset == scope.entry_offset and _running(frame))


def _registered(scope: _WriteScope) -> bool:
    callbacks = scope.callbacks()
    if callbacks is None:
        return False
    holder = scope.holder() if isinstance(scope.holder, weakref.ref) else scope.holder
    for _, callback in list(callbacks):
        bound = getattr(callback, "__self__", None)
        if bound is scope or (holder is not None and bound is holder):
            return True
    return False


def _held(ref: weakref.ref) -> bool:
    scope = ref()
    if scope is None:
        return False
    if scope.ending is not None and scope.state == OPEN:
        return True
    return scope.state == OPENING or _live(scope) or _busy(scope)


def _busy(scope: _WriteScope, here: FrameType | None = None) -> bool:
    if scope is _SETTLING:
        return here is None
    guards = _guards(scope)
    if here is not None:
        return any(_on_stack(guard, here) for guard in guards)
    return any(_running(guard) for guard in guards)


def _guards(scope: _WriteScope) -> list[FrameType]:
    if scope.state == OPENING:
        return []
    guards = _exiting(scope)
    if scope.callbacks is not None:
        callbacks = scope.callbacks()
        if callbacks is not None:
            guards.extend(_stack_exits(callbacks))
        return guards
    frame = scope.frame
    if frame is not None and scope.body is not None and _contains(scope.exits, frame.f_lasti):
        guards.append(frame)
    return guards


def _exiting(scope: _WriteScope) -> list[FrameType]:
    found = []
    for top in list(sys._current_frames().values()):
        while top is not None:
            if id(top.f_code) in EXIT_CODE_IDS:
                locals_ = top.f_locals
                if locals_.get("self") is scope or locals_.get("scope") is scope:
                    found.append(top)
            top = top.f_back
    return found


def _stack_exits(callbacks: Any) -> list[FrameType]:
    found = []
    for top in list(sys._current_frames().values()):
        while top is not None:
            if top.f_code in STACK_CODES:
                owner = top.f_locals.get("self")
                if getattr(owner, "_exit_callbacks", None) is callbacks:
                    found.append(top)
            top = top.f_back
    return found


def _running(frame: FrameType) -> bool:
    for top in list(sys._current_frames().values()):
        while top is not None:
            if top is frame:
                return True
            top = top.f_back
    return False


def _nothing() -> None:
    return None


def _unlink(scope: _WriteScope) -> None:
    outer = scope.outer
    if outer is not None:
        if outer.inner() is scope:
            outer.inner = _nothing
        if outer.orphan is not None and outer.orphan[0] == id(scope):
            outer.orphan = None
    scope.frame = scope.holder = None
    scope.anchors = ()


def _root(scope: _WriteScope) -> _WriteScope:
    while scope.outer is not None:
        scope = scope.outer
    return scope


def _live_below(scope: _WriteScope | None) -> bool:
    while scope is not None:
        if _live(scope):
            return True
        scope = scope.inner()
    return False


def _hand_up(scope: _WriteScope, child: _WriteScope) -> None:
    outer = scope.outer
    link = weakref.ref(
        child,
        partial(
            _lost,
            weakref.ref(outer),
            child.connection,
            child.generator,
            id(child),
            child.footprint,
        ),
    )
    scope.parted = True
    scope.inner = _nothing
    child.outer = outer
    outer.inner = link
    item: _WriteScope | None = child
    while item is not None:
        item.savepoint = None
        item = item.inner()


def _frameless(child: _WriteScope | None) -> bool:
    while child is not None:
        if child.frame is None:
            return True
        child = child.inner()
    return False


def _cut(scope: _WriteScope, child: _WriteScope | None) -> None:
    scope.parted = True
    scope.inner = _nothing
    while child is not None:
        child.detached = True
        child = child.inner()


def _await_running(scope: _WriteScope | None, here: FrameType) -> None:
    while scope is not None:
        if scope.frame is not None and _on_stack(scope.frame, here):
            scope.awaited = True
        scope = scope.inner()


def _awaits(scope: _WriteScope | None) -> bool:
    while scope is not None:
        if scope.awaited:
            return True
        scope = scope.inner()
    return False


def _running_here(scope: _WriteScope | None) -> bool:
    here = sys._getframe(1)
    while scope is not None:
        if scope.frame is not None and _on_stack(scope.frame, here):
            return True
        scope = scope.inner()
    return False


def _running_below(scope: _WriteScope | None) -> bool:
    while scope is not None:
        if scope.frame is not None and _running(scope.frame):
            return True
        scope = scope.inner()
    return False


def _footprint(connection: sqlite3.Connection) -> tuple[Any, ...]:
    return (connection.total_changes, *connection.execute(FOOTPRINT_QUERY).fetchone())


def _changed(scope: _WriteScope) -> bool:
    if scope.connection.total_changes != scope.footprint[0]:
        return True
    return len(scope.footprint) == 1 or _footprint(scope.connection) != scope.footprint


def _orphan_changed(scope: _WriteScope) -> bool:
    orphan = scope.orphan
    if orphan is None:
        return False
    if orphan[1]:
        return True
    return bool(orphan[2]) and _footprint(scope.connection)[1:] != orphan[2]


def _open_root(ref: weakref.ref) -> bool:
    scope = ref()
    return scope is not None and scope.state == OPEN


def _linked(scope: _WriteScope) -> bool:
    outer = scope.outer
    return outer.inner() is scope or (
        outer.orphan is not None and outer.orphan[0] == id(scope)
    )


def _lost(
    outer: weakref.ref,
    connection: sqlite3.Connection,
    generator: bool,
    key: int,
    footprint: tuple[Any, ...],
    ref: weakref.ref,
) -> None:
    scope = outer()
    if scope is None or scope.inner is not ref:
        return
    try:
        changed = connection.total_changes != footprint[0]
    except sqlite3.ProgrammingError:
        return
    if changed and not generator:
        _poison(scope)
    earlier = scope.orphan
    if earlier is not None:
        scope.orphan = (key, changed or earlier[1], earlier[2])
    else:
        scope.orphan = (key, changed, footprint[1:])


def _reclaim(
    pool_ref: weakref.ref, connection: sqlite3.Connection, thread: threading.Thread
) -> None:
    pool = pool_ref()
    if pool is None or os.getpid() != pool._pid or pool._owners.get(connection) is not thread:
        return
    if pool._adopt_orphan(connection):
        return
    writer = pool._writer(connection)
    if writer is not None and (writer.parted or writer.inner() is not None):
        pool._retire_dead(writer, connection)
        return
    if writer is not None:
        pool._writers.pop(connection, None)
        FORK_GATE.leave(writer.entry)
    with suppress(sqlite3.Error):
        if pool._owners.get(connection) is thread:
            _roll_back(connection)
    if pool._owners.get(connection) is thread:
        pool._give_back(connection)


def _abandoned(
    pool_ref: weakref.ref,
    connection: sqlite3.Connection,
    generation: int,
    lent: bool,
    handed: list[bool],
    ref: weakref.ref,
) -> None:
    pool = pool_ref()
    if pool is None:
        return
    claim = _Token()
    mine = weakref.ref(claim)
    ref.claims[mine] = None
    try:
        if _claimed_before(ref, mine):
            return
        if pool._writers.get(connection) is not ref:
            return
        if os.getpid() != pool._pid or fork_state.generation != generation:
            return
        try:
            _roll_back_abandoned(connection, handed[0])
        finally:
            try:
                _roll_back_abandoned(connection, handed[0])
            finally:
                if pool._writers.get(connection) is ref:
                    pool._writers.pop(connection, None)
                if _claim_orphan(pool, connection) or lent:
                    pool._give_back(connection)
    finally:
        ref.claims.pop(mine, None)


def _claimed_before(ref: weakref.ref, mine: weakref.ref) -> bool:
    for other in list(ref.claims):
        if other is mine:
            return False
        if other() is not None:
            return True
    return False


def _claimed_by_anyone(ref: weakref.ref) -> bool:
    return any(claim() is not None for claim in list(ref.claims))


class _WriterRef(weakref.ref):
    __slots__ = ("settle", "claims")

    def __init__(self, scope: _WriteScope, settle: Callable[[weakref.ref], None]) -> None:
        super().__init__(scope, settle)
        self.settle = settle
        self.claims: dict[weakref.ref, None] = {}


def _roll_back_abandoned(connection: sqlite3.Connection, handed: bool) -> None:
    with suppress(sqlite3.Error):
        if connection.in_transaction:
            _roll_back(connection)
            if handed:
                logger.warning(ABANDONED_ROLLBACK_MESSAGE)


def _claim_orphan(pool: "ConnectionPool", connection: sqlite3.Connection) -> bool:
    try:
        pool._orphans.remove(connection)
    except KeyError:
        return False
    return True


def _claimed(scope: _WriteScope) -> bool:
    root = _root(scope)
    return any(ref() is root for ref in _writing.get())


def _poison(scope: _WriteScope) -> None:
    while scope.outer is not None:
        scope = scope.outer
    scope.poisoned = True


def _place(scope: _WriteScope, frame: FrameType) -> None:
    holder: FrameType | None = frame
    while holder is not None:
        code = holder.f_code
        if code is ENTER_CONTEXT_CODE:
            values = holder.f_locals
            callbacks = getattr(values.get("self"), "_exit_callbacks", None)
            if callbacks is not None:
                scope.callbacks = weakref.ref(callbacks)
                scope.holder = _weak(values.get("cm"))
            scope.generator = scope.generator or _free(_outside(holder.f_back))
            return
        if code.co_filename != CONTEXTLIB_FILE:
            scope.generator = scope.generator or _free(holder)
            body, exits = _with_body(code, holder.f_lasti)
            if body is not None:
                scope.frame, scope.body, scope.exits = holder, body, exits
                scope.entry_offset = holder.f_lasti
                scope.anchors = _anchors(holder)
                return
            if code.co_flags & GENERATOR_FLAGS:
                return
        holder = holder.f_back


def _anchors(frame: FrameType) -> tuple[FrameType, ...]:
    anchors = [frame]
    while frame.f_code.co_flags & GENERATOR_FLAGS:
        driver = frame.f_back
        if driver is None or driver.f_code.co_filename != CONTEXTLIB_FILE:
            break
        frame = _outside(driver)
        if frame is None:
            break
        anchors.append(frame)
    return tuple(anchors)


def _free(frame: FrameType | None) -> bool:
    if frame is None or not frame.f_code.co_flags & FREE_FLAGS:
        return False
    driver = frame.f_back
    while driver is not None and driver.f_code.co_flags & FREE_FLAGS and _delegating(driver):
        driver = driver.f_back
    return driver is None or driver.f_code.co_filename != CONTEXTLIB_FILE


def _delegating(frame: FrameType) -> bool:
    raw, offset = frame.f_code.co_code, frame.f_lasti
    return any(0 <= at < len(raw) and raw[at] == SEND for at in (offset, offset - 2))


def _weak(target: Any) -> Any:
    try:
        return weakref.ref(target)
    except TypeError:
        return target


def _outside(frame: FrameType | None) -> FrameType | None:
    while frame is not None and frame.f_code.co_filename == CONTEXTLIB_FILE:
        frame = frame.f_back
    return frame


def _joins(scope: _WriteScope, top: _WriteScope, frame: FrameType) -> bool:
    if scope.task is not None and top.task is not None:
        return top.task is scope.task
    return _on_stack(top.frame, frame) or _claimed(top)


def _shared_with(scope: _WriteScope, outer: _WriteScope, frame: FrameType) -> bool:
    if outer.task is scope.task:
        return True
    if outer.task is not None or not (_on_stack(outer.frame, frame) or _claimed(outer)):
        return False
    child = outer.inner()
    while child is not None:
        if child.task is not None and child.task is not scope.task and _live(child):
            return False
        child = child.inner()
    return True


def _file_of(database: str, uri: bool, pool: "ConnectionPool") -> object:
    if uri:
        parts = urlsplit(database)
        if "mode=memory" in parts.query:
            return database
        path = parts.path
    else:
        path = database
    if path in ("", ":memory:"):
        return pool
    return os.path.realpath(path)


def _current_task() -> Any:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def _inside_write(frame: FrameType | None) -> bool:
    while frame is not None:
        if id(frame.f_code) in WRITE_CODE_IDS:
            return True
        frame = frame.f_back
    return False


def _inside_write_here() -> bool:
    frame = sys._getframe(1)
    return _inside_write(frame) or any(pool._leaving_here(frame) for pool in _live_pools())


def _on_stack(target: FrameType | None, frame: FrameType | None) -> bool:
    while target is not None and frame is not None:
        if frame is target:
            return True
        frame = frame.f_back
    return False


def _with_body(code: CodeType, offset: int) -> tuple[Spans | None, Spans]:
    if not _enters_at(code.co_code, offset):
        return NO_BODY
    record = _analyses.get(id(code))
    if record is None or record[0] is not code:
        if len(_analyses) >= CACHED_CODES:
            _analyses.clear()
        record = _analyses[id(code)] = (code, {})
    found = record[1].get(offset)
    if found is None:
        found = record[1][offset] = _find_with(code, offset)
    return found


def _enters_at(raw: bytes, offset: int) -> bool:
    if not 0 <= offset < len(raw) - 2:
        return False
    if raw[offset] == BEFORE_WITH:
        return True
    return (
        raw[offset] == CALL
        and offset >= 2
        and raw[offset - 2] == LOAD_SPECIAL
        and raw[offset - 1] == ENTER_SPECIAL
    )


def _find_with(code: CodeType, offset: int) -> tuple[Spans | None, Spans]:
    instructions = list(dis.get_instructions(code))
    positions = {instruction.offset: i for i, instruction in enumerate(instructions)}
    index = positions[offset]
    table = sorted(
        (entry.start, entry.end, entry.target) for entry in dis._parse_exception_table(code)
    )
    starts = [start for start, _, _ in table]
    handler = _target(table, starts, instructions[index + 1].offset)
    if handler is None:
        return NO_BODY
    chains: dict[int, frozenset[int]] = {}
    covered = [
        i
        for i, instruction in enumerate(instructions)
        if handler in _chain(table, starts, _target(table, starts, instruction.offset), chains)
    ]
    body, exits = _find_body(instructions, index, covered, positions)
    exits.update(_handler_offsets(instructions, positions.get(handler)))
    ends = {
        instruction.offset: end
        for instruction, end in zip(
            instructions,
            [instruction.offset for instruction in instructions[1:]] + [len(code.co_code)],
        )
    }
    return _spans(body, ends), _spans(exits - body, ends)


def _spans(offsets: set[int], ends: dict[int, int]) -> Spans:
    starts: list[int] = []
    stops: list[int] = []
    for offset in sorted(offsets):
        if stops and stops[-1] == offset:
            stops[-1] = ends[offset]
        else:
            starts.append(offset)
            stops.append(ends[offset])
    return tuple(starts), tuple(stops)


def _contains(spans: Spans, offset: int) -> bool:
    position = bisect.bisect_right(spans[0], offset) - 1
    return position >= 0 and offset < spans[1][position]


def _find_body(
    instructions: list[dis.Instruction],
    index: int,
    covered: list[int],
    positions: dict[int, int],
) -> tuple[set[int], set[int]]:
    body = {instructions[i].offset for i in covered}
    exits: set[int] = set()
    pending, seen = [index + 1, *covered], set()
    while pending:
        current = pending.pop()
        if current in seen or current >= len(instructions):
            continue
        seen.add(current)
        instruction = instructions[current]
        if instruction.offset not in body and instruction.opname not in SILENT_OPS:
            exits.update(_exit_offsets(instructions, current))
            continue
        body.add(instruction.offset)
        pending.extend(_successors(instruction, current, positions))
    return body, exits


def _exit_offsets(instructions: list[dis.Instruction], index: int) -> list[int]:
    window = instructions[index : index + 5]
    if window and window[0].opname == "SWAP":
        window = window[1:]
    names = [instruction.opname for instruction in window[:4]]
    if names != ["LOAD_CONST", "LOAD_CONST", "LOAD_CONST", "CALL"]:
        return []
    if any(instruction.argval is not None for instruction in window[:3]):
        return []
    return [instruction.offset for instruction in instructions[index : index + 5]]


def _handler_offsets(instructions: list[dis.Instruction], index: int | None) -> list[int]:
    offsets: list[int] = []
    if index is None:
        return offsets
    for instruction in instructions[index:]:
        offsets.append(instruction.offset)
        if instruction.opname == "WITH_EXCEPT_START" or len(offsets) > HANDLER_PREFIX:
            break
    return offsets


def _successors(
    instruction: dis.Instruction, index: int, positions: dict[int, int]
) -> list[int]:
    name = instruction.opname
    if name in ENDING_OPS:
        return []
    jump = positions.get(instruction.argval) if instruction.opcode in JUMP_OPCODES else None
    if jump is None:
        return [index + 1]
    if name in UNCONDITIONAL_JUMPS:
        return [jump]
    return [index + 1, jump]


def _chain(
    table: list[tuple[int, int, int]],
    starts: list[int],
    target: int | None,
    chains: dict[int, frozenset[int]],
) -> frozenset[int]:
    if target is None:
        return frozenset()
    found = chains.get(target)
    if found is None:
        chains[target] = frozenset()
        found = chains[target] = frozenset({target}) | _chain(
            table, starts, _target(table, starts, target), chains
        )
    return found


def _target(table: list[tuple[int, int, int]], starts: list[int], offset: int) -> int | None:
    position = bisect.bisect_right(starts, offset) - 1
    if position >= 0 and offset < table[position][1]:
        return table[position][2]
    return None


def _retire(
    pool_ref: weakref.ref,
    connection: sqlite3.Connection,
    connections: set[sqlite3.Connection],
    owners: dict[sqlite3.Connection, threading.Thread],
    pid: int,
) -> None:
    pool = pool_ref()
    if pool is not None and os.getpid() == pid and pool._adopt_orphan(connection):
        return
    _close_one(connection, connections, owners, pid)


def _close_one(
    connection: sqlite3.Connection,
    connections: set[sqlite3.Connection],
    owners: dict[sqlite3.Connection, threading.Thread],
    pid: int,
) -> None:
    if os.getpid() != pid:
        return
    owners.pop(connection, None)
    connections.discard(connection)
    _shut(connection)


def _close_all(connections: set[sqlite3.Connection], pid: int) -> None:
    if os.getpid() != pid:
        return
    while connections:
        try:
            connection = connections.pop()
        except KeyError:
            return
        _shut(connection)


def _close_pools() -> None:
    for pool in _live_pools():
        pool.close_idle()


def _heal_pools() -> None:
    for pool in _live_pools():
        pool.heal_here()


_SETTLING = _WriteScope(None)
_SETTLING.state = CLOSING

WRITE_CODE_IDS = frozenset(
    id(function.__code__)
    for function in (
        _WriteScope.__enter__,
        _WriteScope.__exit__,
        ConnectionPool._begin,
        ConnectionPool._nest,
        ConnectionPool._end,
        ConnectionPool._end_nested,
        ConnectionPool._settle_nested,
        ConnectionPool._close_root,
        ConnectionPool._end_waiting,
        ConnectionPool._start,
        ConnectionPool._undo_savepoint,
        ConnectionPool._finish,
        ConnectionPool._release,
        ConnectionPool._drop,
        ConnectionPool._prune,
        ConnectionPool._open_write,
        ConnectionPool.heal_here,
        ConnectionPool._heal_lent,
        ConnectionPool._lent_here,
    )
)

EXIT_CODE_IDS = frozenset(
    id(function.__code__)
    for function in (
        _WriteScope.__exit__,
        ConnectionPool._end,
        ConnectionPool._end_nested,
        ConnectionPool._settle_nested,
        ConnectionPool._close_root,
        ConnectionPool._end_waiting,
        ConnectionPool._undo_savepoint,
        ConnectionPool._finish,
        ConnectionPool._release,
    )
)

at_exit(_close_pools)
before_each_call(_heal_pools)
refuse_inside(_inside_write_here)
