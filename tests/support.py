from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tinyjobs import TinyJobs


class BackendTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.db"
        self.app = TinyJobs(f"sqlite:///{self.database}")
        self.backend = self.app.backend

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()
