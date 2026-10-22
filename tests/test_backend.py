from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from tinyjobs.backends import SQLiteBackend, load_backend
from tinyjobs.backends.sqlite.connection import ConnectionPool
from tinyjobs.testing import BackendContract


class SQLiteContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def make_backend(self, clock):
        return SQLiteBackend(str(Path(self.directory.name) / "jobs.db"), clock=clock)


class FreshDatabaseTests(unittest.TestCase):
    def test_opening_waits_for_another_process_holding_a_fresh_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
            holder.execute("CREATE TABLE claimed (id INTEGER)")
            holder.execute("BEGIN IMMEDIATE")
            threading.Timer(0.3, holder.execute, ("COMMIT",)).start()
            pool = ConnectionPool(path, busy_timeout=5.0)
            try:
                mode = pool.connection.execute("PRAGMA journal_mode").fetchone()[0]
            finally:
                pool.close()
                holder.close()
            self.assertEqual(mode, "wal")


class MemoryContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    independent_clients = False

    async def make_backend(self, clock):
        return load_backend("memory://", clock=clock)


if __name__ == "__main__":
    unittest.main()
