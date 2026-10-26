from __future__ import annotations

import unittest

from tinyjobs.retention import KEEP_EVERYTHING, RetentionPolicy
from tinyjobs.status import JobStatus

OLDER_RELEASE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_attempt "
    "ON executions (job_id, attempt)"
)


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

    def build_v2(self, path) -> None:
        import sqlite3

        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute("PRAGMA journal_mode = WAL")
        handle.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        handle.execute("INSERT INTO meta VALUES ('schema_version', '2')")
        handle.execute("INSERT INTO meta VALUES ('last_purge_at', '0')")
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
            "CREATE TABLE executions (id INTEGER PRIMARY KEY, job_id TEXT NOT NULL "
            "REFERENCES jobs(id) ON DELETE CASCADE, attempt INTEGER NOT NULL, "
            "status TEXT NOT NULL, worker_id TEXT NOT NULL, started_at REAL NOT NULL, "
            "finished_at REAL, error_type TEXT, traceback TEXT)"
        )
        handle.execute(
            "CREATE UNIQUE INDEX idx_executions_attempt ON executions (job_id, attempt)"
        )
        handle.execute(
            "CREATE TABLE schedules (name TEXT PRIMARY KEY, cron TEXT NOT NULL, "
            "timezone TEXT NOT NULL, next_run_at REAL NOT NULL, last_run_at REAL, "
            "last_job_id TEXT)"
        )
        handle.execute(
            "INSERT INTO jobs VALUES ('retried', 't', x'7b7d', 'json', 'default', "
            "'queued', 0, 0, 2, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        handle.execute(
            "INSERT INTO jobs VALUES ('running', 't', x'7b7d', 'json', 'default', "
            "'running', 0, 1, 2, 0, 0, NULL, 99, 'old-worker', NULL, NULL, NULL)"
        )
        for row in (
            (1, "retried", 1, "failed", 30.0),
            (2, "retried", 2, "failed", 20.0),
            (3, "running", 2, "failed", 40.0),
            (4, "running", 1, "running", 50.0),
        ):
            handle.execute(
                "INSERT INTO executions (id, job_id, attempt, status, worker_id, "
                "started_at) VALUES (?, ?, ?, ?, 'old-worker', ?)",
                row,
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

    def unique_indexes(self, path) -> list[str]:
        import sqlite3

        handle = sqlite3.connect(path)
        names = sorted(
            row[0]
            for row in handle.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' "
                "AND sql LIKE 'CREATE UNIQUE%'"
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
            self.assertEqual(survivor.hand_backs, 0)
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

    def test_history_written_before_claims_keeps_its_order_and_its_rows(self):
        import asyncio
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.status import ExecutionStatus

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.build_v2(path)

        async def upgrade() -> tuple:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                claims = [(await app.aget(name)).claims for name in ("retried", "running")]
                claimed = await app.backend.claim(
                    worker_id="new", queues=["default"], limit=1, lease_seconds=60
                )
                await app.backend.complete(claimed[0], None)
                histories = [
                    [(e.attempt, e.status, e.started_at) for e in await app.aexecutions(name)]
                    for name in ("retried", "running")
                ]
                return claims, claimed[0].claims, histories
            finally:
                await app.aclose()

        claims, claimed, (retried, running) = asyncio.run(upgrade())
        self.assertEqual(claims, [2, 2])
        self.assertEqual(claimed, 3)
        self.assertEqual(
            [(attempt, status) for attempt, status, _ in retried],
            [(2, ExecutionStatus.FAILED), (1, ExecutionStatus.FAILED), (1, ExecutionStatus.SUCCESS)],
        )
        self.assertEqual([started for _, _, started in retried][:2], [20.0, 30.0])
        self.assertEqual(
            [(attempt, status) for attempt, status, _ in running],
            [(2, ExecutionStatus.FAILED), (1, ExecutionStatus.RUNNING)],
        )
        self.assertIn("idx_executions_claim", self.indexes(path))
        self.assertNotIn("idx_executions_attempt", self.unique_indexes(path))

    def test_executions_that_started_together_keep_the_order_they_were_written(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.build_v2(path)
        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute(
            "INSERT INTO jobs VALUES ('tied', 't', x'7b7d', 'json', 'default', "
            "'failed', 0, 3, 3, 0, 0, 1, NULL, NULL, NULL, NULL, NULL)"
        )
        for row in ((10, 3), (11, 1), (12, 2)):
            handle.execute(
                "INSERT INTO executions (id, job_id, attempt, status, worker_id, "
                "started_at) VALUES (?, 'tied', ?, 'failed', 'old-worker', 5.0)",
                row,
            )
        handle.close()

        async def read() -> list[int]:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                return [e.attempt for e in await app.aexecutions("tied")]
            finally:
                await app.aclose()

        self.assertEqual(asyncio.run(read()), [3, 1, 2])

    def test_an_older_release_cannot_put_the_index_back_under_a_running_app(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.models import ExecutionError

        path = Path(tempfile.mkdtemp()) / "live.db"

        async def scenario() -> list[int]:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                job = await app.asend("t")
                backend = app.backend
                first = await backend.claim(
                    worker_id="w", queues=["default"], limit=1, lease_seconds=60
                )
                await backend.fail(first[0], ExecutionError("E", "boom", "tb"), None)
                handle = sqlite3.connect(path, isolation_level=None)
                handle.execute(OLDER_RELEASE_INDEX)
                handle.close()
                self.assertTrue(await backend.requeue(job.id))
                again = await backend.claim(
                    worker_id="w", queues=["default"], limit=1, lease_seconds=60
                )
                return [claimed.claims for claimed in first + again]
            finally:
                await app.aclose()

        self.assertEqual(asyncio.run(scenario()), [1, 2])
        self.assertNotIn("idx_executions_attempt", self.unique_indexes(path))

    def test_reopening_a_current_database_leaves_its_schema_alone(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "current.db"

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            await app.aclose()

        def schema_version() -> int:
            handle = sqlite3.connect(path)
            version = handle.execute("PRAGMA schema_version").fetchone()[0]
            handle.close()
            return version

        asyncio.run(open_it())
        before = schema_version()
        asyncio.run(open_it())
        self.assertEqual(schema_version(), before)

    def test_an_older_release_cannot_put_the_index_back(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.build_v2(path)

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            await app.aclose()

        asyncio.run(open_it())
        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute(OLDER_RELEASE_INDEX)
        handle.close()

        async def claim_twice() -> list[int]:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                claims = []
                for _ in range(2):
                    claimed = await app.backend.claim(
                        worker_id="new", queues=["default"], limit=1, lease_seconds=60
                    )
                    claims.append(claimed[0].claims)
                    await app.backend.release(claimed[0])
                return claims
            finally:
                await app.aclose()

        self.assertEqual(asyncio.run(claim_twice()), [3, 4])
        self.assertNotIn("idx_executions_attempt", self.unique_indexes(path))

    def test_a_newer_database_is_refused_before_anything_is_written(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.backends.sqlite.schema import SCHEMA_VERSION

        path = Path(tempfile.mkdtemp()) / "newer.db"
        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        handle.execute(
            "INSERT INTO meta VALUES ('schema_version', ?)", (str(SCHEMA_VERSION + 1),)
        )
        handle.close()

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            try:
                await app.ready()
            finally:
                await app.aclose()

        with self.assertRaisesRegex(RuntimeError, "newer than this tinyjobs"):
            asyncio.run(open_it())
        handle = sqlite3.connect(path)
        tables = [row[0] for row in handle.execute("SELECT name FROM sqlite_master")]
        handle.close()
        self.assertEqual(tables, ["meta", "sqlite_autoindex_meta_1"])

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
