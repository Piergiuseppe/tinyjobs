from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tinyjobs.backends import SQLiteBackend, load_backend
from tinyjobs.testing import BackendContract


class SQLiteContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def make_backend(self, clock):
        return SQLiteBackend(str(Path(self.directory.name) / "jobs.db"), clock=clock)


class MemoryContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    independent_clients = False

    async def make_backend(self, clock):
        return load_backend("memory://", clock=clock)


if __name__ == "__main__":
    unittest.main()
