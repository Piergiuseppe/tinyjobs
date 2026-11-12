from __future__ import annotations

import time
import shutil
import unittest

from tinyjobs import TinyJobs
from tinyjobs.exceptions import ConfigurationError, NewerStoreError
from tinyjobs.retention import KEEP_EVERYTHING, RetentionPolicy
from tinyjobs.status import JobStatus

class PolicyTests(unittest.TestCase):
    def test_defaults_keep_failures_longest(self):
        policy = RetentionPolicy()
        self.assertGreater(policy.failed, policy.success)
        self.assertEqual(policy.success, policy.cancelled)

    def test_the_sweep_sizes_must_be_at_least_one(self):
        for field, value in (("batch", 0), ("batch", -1), ("batch", 1.5), ("batch", True),
                             ("maximum_per_sweep", 0)):
            with self.subTest(field=field, value=value), self.assertRaises(ConfigurationError) as caught:
                RetentionPolicy(**{field: value})
            self.assertIn(f"retention {field} must be a whole number of at least 1", str(caught.exception))

    def test_the_durations_must_be_finite_seconds(self):
        for field, value in (("interval", float("nan")), ("interval", float("inf")),
                             ("success", -1), ("success", 10**400), ("failed", "7d"),
                             ("cancelled", True), ("failed", 10**5000),
                             ("failed", [10**5000])):
            with self.subTest(field=field, value=value), self.assertRaises(ConfigurationError) as caught:
                RetentionPolicy(**{field: value})
            self.assertIn(f"retention {field} must be a finite number of seconds", str(caught.exception))
        self.assertEqual(RetentionPolicy(interval=0).check_interval, 1.0)

    def test_an_app_and_a_worker_refuse_anything_but_a_policy(self):
        for value in ({"success": 3600}, 3600, "1h"):
            with self.subTest(value=value), self.assertRaises(ConfigurationError) as caught:
                TinyJobs("memory://", retention=value)
            self.assertIn("retention must be a RetentionPolicy", str(caught.exception))
        app = TinyJobs("memory://")
        self.addCleanup(app.close)
        with self.assertRaises(ConfigurationError) as caught:
            app.worker(retention={"success": 3600})
        self.assertIn("retention must be a RetentionPolicy, got dict", str(caught.exception))

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
                "AND (name LIKE 'idx%' OR name LIKE 'tinyjobs%')"
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
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
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
            "SELECT value FROM tinyjobs_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        purge_marker = handle.execute(
            "SELECT count(*) FROM tinyjobs_meta WHERE key = 'last_purge_at'"
        ).fetchone()[0]
        stub = handle.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in handle.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        handle.close()

        self.assertEqual(int(version), SCHEMA_VERSION)
        self.assertEqual(purge_marker, 1)
        self.assertEqual(int(stub), SCHEMA_VERSION)
        self.assertEqual(
            tables,
            {
                "meta",
                "tinyjobs_install_lock",
                "tinyjobs_meta",
                "tinyjobs_jobs",
                "tinyjobs_executions",
                "tinyjobs_schedules",
                "tinyjobs_limits",
            },
        )
        names = self.indexes(path)
        self.assertNotIn("idx_jobs_listing", names)
        self.assertNotIn("idx_jobs_claim", names)
        self.assertIn("tinyjobs_jobs_by_status", names)
        self.assertIn("tinyjobs_jobs_purge", names)

    def test_history_written_before_claims_keeps_its_order_and_its_rows(self):
        import asyncio
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.status import ExecutionStatus

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
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
        self.assertIn("tinyjobs_executions_claim", self.indexes(path))
        self.assertNotIn("idx_executions_attempt", self.unique_indexes(path))

    def test_executions_that_started_together_keep_the_order_they_were_written(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
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

    def test_a_migrated_file_still_refuses_an_older_release(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        self.build_v2(path)

        async def upgrade() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            await app.aclose()

        asyncio.run(upgrade())
        handle = sqlite3.connect(path)
        stub = handle.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        handle.close()
        self.assertEqual(int(stub), 6)
        self.assertGreater(int(stub), 3)

    def test_a_legacy_file_is_only_migrated_into_the_default_prefix(self):
        import asyncio
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.exceptions import ConfigurationError

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        self.build_v2(path)

        async def open_with(prefix: str) -> None:
            app = TinyJobs(f"sqlite:///{path}", prefix=prefix)
            try:
                await app.ready()
            finally:
                await app.aclose()

        with self.assertRaisesRegex(ConfigurationError, "before 0.14"):
            asyncio.run(open_with("other"))
        asyncio.run(open_with("tinyjobs"))
        asyncio.run(open_with("other"))

    def test_the_renamed_tables_keep_their_foreign_key(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "v2.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        self.build_v2(path)

        async def upgrade_and_purge() -> int:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                job = await app.aget("retried")
                await app.backend.cancel(job.id)
                await app.backend.purge(RetentionPolicy(cancelled=1), time.time() + 60)
            finally:
                await app.aclose()
            handle = sqlite3.connect(path)
            left = handle.execute(
                "SELECT count(*) FROM tinyjobs_executions WHERE job_id = 'retried'"
            ).fetchone()[0]
            handle.close()
            return left

        self.assertEqual(asyncio.run(upgrade_and_purge()), 0)

    def build_v4(self, path) -> None:
        import sqlite3

        from tinyjobs.backends.sqlite.schema import Tables, ddl

        tables = Tables()
        added = (tables.limits, tables.index("jobs_held"), tables.index("jobs_running_by_task"))
        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute("PRAGMA journal_mode = WAL")
        for statement in ddl(tables):
            if any(name in statement for name in added):
                continue
            handle.execute(statement.replace("'queued', 'held', 'running'", "'queued', 'running'"))
        handle.execute("INSERT INTO tinyjobs_meta VALUES ('schema_version', '4')")
        handle.execute("INSERT INTO tinyjobs_meta VALUES ('last_purge_at', '0')")
        handle.execute(
            "INSERT INTO tinyjobs_jobs (id, task, payload, codec, status, run_at, created_at, "
            "dedup_key) VALUES ('waiting', 't', x'7b7d', 'json', 'queued', 0, 0, 'key')"
        )
        handle.close()

    def test_a_v4_database_gains_the_limits_in_place(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.limits import Limit
        from tinyjobs.status import JobStatus
        from tinyjobs.testing.contract import make_job

        path = Path(tempfile.mkdtemp()) / "v4.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        self.build_v4(path)

        async def upgrade() -> tuple:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            try:
                backend = app.backend._inner
                first = await backend.enqueue(make_job(task="t", run_at=-1.0))
                claimed = await backend.claim(
                    worker_id="w", queues=["default"], limit=10, lease_seconds=60,
                    limits={"t": Limit(concurrency=1)},
                )
                twin = await backend.enqueue(make_job(task="t", dedup_key="key"))
                held = await backend.get("waiting")
                return [job.id for job in claimed] == [first.id], held.status, twin.id
            finally:
                await app.aclose()

        self.assertEqual(asyncio.run(upgrade()), (True, JobStatus.HELD, "waiting"))
        handle = sqlite3.connect(path)
        version = handle.execute(
            "SELECT value FROM tinyjobs_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        schema = dict(handle.execute("SELECT name, sql FROM sqlite_master WHERE name LIKE 'tinyjobs%'"))
        handle.close()
        self.assertEqual(int(version), 6)
        self.assertIn("tinyjobs_limits", schema)
        self.assertIn("WHERE status = 'held'", schema["tinyjobs_jobs_held"])
        self.assertIn("WHERE status = 'running'", schema["tinyjobs_jobs_running_by_task"])
        self.assertIn("status IN ('queued', 'held', 'waiting', 'running')", schema["tinyjobs_jobs_dedup"])

    def test_reopening_a_current_database_leaves_its_schema_alone(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "current.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)

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

    def test_a_newer_database_is_refused_before_anything_is_written(self):
        import asyncio
        import sqlite3
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs
        from tinyjobs.backends.sqlite.schema import SCHEMA_VERSION

        path = Path(tempfile.mkdtemp()) / "newer.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        handle = sqlite3.connect(path, isolation_level=None)
        handle.execute(
            "CREATE TABLE tinyjobs_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        handle.execute(
            "INSERT INTO tinyjobs_meta VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION + 1),),
        )
        handle.close()

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            try:
                await app.ready()
            finally:
                await app.aclose()

        with self.assertRaisesRegex(NewerStoreError, "newer than this tinyjobs"):
            asyncio.run(open_it())
        handle = sqlite3.connect(path)
        tables = [row[0] for row in handle.execute("SELECT name FROM sqlite_master")]
        handle.close()
        self.assertEqual(tables, ["tinyjobs_meta", "sqlite_autoindex_tinyjobs_meta_1"])

    def test_upgrading_twice_is_harmless(self):
        import asyncio
        import tempfile
        from pathlib import Path

        from tinyjobs import TinyJobs

        path = Path(tempfile.mkdtemp()) / "old.db"
        self.addCleanup(shutil.rmtree, path.parent, ignore_errors=True)
        self.build_v1(path)

        async def open_it() -> None:
            app = TinyJobs(f"sqlite:///{path}")
            await app.ready()
            await app.aclose()

        asyncio.run(open_it())
        asyncio.run(open_it())
        self.assertIn("tinyjobs_jobs_purge", self.indexes(path))
