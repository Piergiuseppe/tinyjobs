from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

FILE_PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
)

SHARED_PRAGMAS = ("PRAGMA foreign_keys = ON",)


class ConnectionPool:
    def __init__(
        self, database: str, *, uri: bool = False, busy_timeout: float = 5.0
    ) -> None:
        self._database = database
        self._uri = uri
        self._busy_timeout = busy_timeout
        self._local = threading.local()
        self._all: list[sqlite3.Connection] = []
        self._guard = threading.Lock()
        self._keepalive = self._connect() if uri else None

    @property
    def database(self) -> str:
        return self._database

    @property
    def connection(self) -> sqlite3.Connection:
        existing = getattr(self._local, "connection", None)
        if existing is None:
            existing = self._connect()
            self._local.connection = existing
            self._local.depth = 0
        return existing

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        connection = self.connection
        depth = getattr(self._local, "depth", 0)
        if depth:
            self._local.depth = depth + 1
            try:
                yield connection
            finally:
                self._local.depth = depth
            return
        connection.execute("BEGIN IMMEDIATE")
        self._local.depth = 1
        try:
            yield connection
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")
        finally:
            self._local.depth = 0

    def close(self) -> None:
        with self._guard:
            for connection in self._all:
                connection.close()
            self._all.clear()
        self._keepalive = None
        self._local = threading.local()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database,
            uri=self._uri,
            isolation_level=None,
            timeout=self._busy_timeout,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout * 1000)}")
        if not self._uri:
            for pragma in FILE_PRAGMAS:
                connection.execute(pragma)
        for pragma in SHARED_PRAGMAS:
            connection.execute(pragma)
        with self._guard:
            self._all.append(connection)
        return connection
