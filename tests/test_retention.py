from __future__ import annotations

import unittest

from tinyjobs.retention import KEEP_EVERYTHING, RetentionPolicy
from tinyjobs.status import JobStatus


class PolicyTests(unittest.TestCase):
    def test_defaults_keep_failures_longest(self):
        policy = RetentionPolicy()
        self.assertGreater(policy.failed, policy.success)
        self.assertEqual(policy.success, policy.cancelled)

    def test_zero_means_forever(self):
        self.assertFalse(KEEP_EVERYTHING.enabled)
        self.assertEqual(KEEP_EVERYTHING.horizons(1000.0), {})
        self.assertIn("nothing is pruned", KEEP_EVERYTHING.describe())

    def test_a_single_zero_only_exempts_that_status(self):
        policy = RetentionPolicy(failed=0)
        horizons = policy.horizons(1_000_000.0)
        self.assertNotIn(JobStatus.FAILED, horizons)
        self.assertIn(JobStatus.SUCCESS, horizons)
        self.assertTrue(policy.enabled)

    def test_horizons_are_cutoff_timestamps(self):
        policy = RetentionPolicy(success=100.0, cancelled=0, failed=0)
        self.assertEqual(policy.horizons(1000.0), {JobStatus.SUCCESS: 900.0})

    def test_the_check_interval_is_a_fraction_of_the_sweep(self):
        self.assertEqual(RetentionPolicy(interval=400.0).check_interval, 100.0)
        self.assertEqual(RetentionPolicy(interval=86400.0).check_interval, 300.0)
        self.assertEqual(RetentionPolicy(interval=1.0).check_interval, 1.0)


class MigrationTests(unittest.TestCase):
    def build_v1(self, path) -> None:
        import sqlite3

        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute("PRAGMA journal_mode = WAL")
        handle.execute(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        handle.execute("INSERT INTO meta VALUES ('schema_version', '1')")
        handle.execute(
            "CREATE TABLE jobs (id TEXT PRIMARY KEY, task TEXT NOT NULL, "
            "payload BLOB NOT NULL, codec TEXT NOT NULL, "
            "queue TEXT NOT NULL DEFAULT 'default', status TEXT NOT NULL, "
            "priority INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, "
            "max_attempts INTEGER NOT NULL DEFAULT 1, run_at REAL NOT NULL, "
            "created_at REAL NOT NULL, finished_at REAL, lease_expires_at REAL, "
            "worker_id TEXT, dedup_key TEXT, result BLOB, last_error TEXT)"
        )
        handle.execute(
            "CREATE INDEX idx_jobs_listing ON jobs (status, created_at DESC)"
        )
        handle.execute(
            "CREATE TABLE executions (id INTEGER PRIMARY KEY, job_id TEXT NOT NULL "
            "REFERENCES jobs(id) ON DELETE CASCADE, attempt INTEGER NOT NULL, "
            "status TEXT NOT NULL, worker_id TEXT NOT NULL, started_at REAL NOT NULL, "
            "finished_at REAL, error_type TEXT, traceback TEXT)"
        )
        handle.execute(
            "INSERT INTO jobs VALUES ('keep-me', 't', x'7b7d', 'json', 'default', "
            "'success', 0, 1, 1, 0, 0, 1, NULL, NULL, NULL, NULL, NULL)"
        )
        handle.close()

    def indexes(self, path) -> list[str]:
        import sqlite3

        handle = sqlite3.connect(path)
        names = sorted(
            row[0]
            for row in handle.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' "
                "AND name LIKE 'idx%'"
            )
        )
        handle.close()
        return names

    def test_a_v1_database_is_upgraded_in_place(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.backends.sqlite.schema import SCHEMA_VERSION

        path = Path(tempfile.mkdtemp()) / "old.db"
        self.build_v1(path)
        self.assertIn("idx_jobs_listing", self.indexes(path))

        async def upgrade() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            survivor = await app.aget("keep-me")
            self.assertIsNotNone(survivor)
            await app.aclose()

        asyncio.run(upgrade())

        handle = sqlite3.connect(path)
        version = handle.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        purge_marker = handle.execute(
            "SELECT count(*) FROM meta WHERE key = 'last_purge_at'"
        ).fetchone()[0]
        handle.close()

        self.assertEqual(int(version), SCHEMA_VERSION)
        self.assertEqual(purge_marker, 1)
        names = self.indexes(path)
        self.assertNotIn("idx_jobs_listing", names)
        self.assertIn("idx_jobs_by_status", names)
        self.assertIn("idx_jobs_purge", names)

    def test_upgrading_twice_is_harmless(self):
        import asyncio
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "old.db"
        self.build_v1(path)

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            await app.aclose()

        asyncio.run(open_it())
        asyncio.run(open_it())
        self.assertIn("idx_jobs_purge", self.indexes(path))
