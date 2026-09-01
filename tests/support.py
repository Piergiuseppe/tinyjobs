from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tinyjobs import TinyJobs


class BackendTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.db"
        self.app = TinyJobs(f"sqlite:///{self.database}").start()
        self.backend = self.app.backend

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def drive(self, coroutine):
        return self.app.run_sync(coroutine, name="test")


class AsyncBackendTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.db"
        self.app = TinyJobs(f"sqlite:///{self.database}")
        await self.app.ready()
        self.backend = self.app.backend

    async def asyncTearDown(self) -> None:
        await self.app.aclose()
        self.directory.cleanup()
