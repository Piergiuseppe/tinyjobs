from __future__ import annotations

import time
import unittest

from tinyjobs.ids import new_job_id
from tinyjobs.models import Job
from tinyjobs.retention import KEEP_EVERYTHING, RetentionPolicy
from tinyjobs.status import JobStatus
from tests.support import AsyncBackendTestCase


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


class PurgeTests(AsyncBackendTestCase):
    async def seed(self, status: JobStatus, finished_at: float | None,
                   count: int = 1) -> list[str]:
        made = []
        for _ in range(count):
            job = Job(
                id=new_job_id(), task="t", payload=b'{"args":[],"kwargs":{}}',
                codec="json", queue="default", status=status,
                run_at=0.0, created_at=0.0, finished_at=finished_at,
            )
            await self.backend.enqueue(job)
            made.append(job.id)
        return made

    async def count(self) -> int:
        return len((await self.backend.list(limit=10_000)).items)

    async def test_it_prunes_past_the_horizon(self):
        old = await self.seed(JobStatus.SUCCESS, 100.0)
        removed = await self.backend.purge(
            RetentionPolicy(success=50.0, cancelled=0, failed=0, interval=0.0), 1000.0
        )
        self.assertEqual(removed, 1)
        self.assertIsNone(await self.backend.get(old[0]))

    async def test_it_leaves_anything_inside_the_horizon(self):
        recent = await self.seed(JobStatus.SUCCESS, 990.0)
        removed = await self.backend.purge(
            RetentionPolicy(success=50.0, cancelled=0, failed=0, interval=0.0), 1000.0
        )
        self.assertEqual(removed, 0)
        self.assertIsNotNone(await self.backend.get(recent[0]))

    async def test_it_never_touches_queued_or_running(self):
        await self.seed(JobStatus.QUEUED, None, 3)
        claimed = await self.backend.claim(
            worker_id="w", queues=("default",), limit=1, lease_seconds=60
        )
        self.assertEqual(len(claimed), 1)
        removed = await self.backend.purge(
            RetentionPolicy(success=0.0001, cancelled=0.0001, failed=0.0001,
                            interval=0.0),
            time.time() + 10.0,
        )
        self.assertEqual(removed, 0)
        self.assertEqual(await self.count(), 3)

    async def test_each_status_has_its_own_horizon(self):
        await self.seed(JobStatus.SUCCESS, 100.0)
        failed = await self.seed(JobStatus.FAILED, 100.0)
        removed = await self.backend.purge(
            RetentionPolicy(success=50.0, cancelled=50.0, failed=5000.0,
                            interval=0.0),
            1000.0,
        )
        self.assertEqual(removed, 1)
        self.assertIsNotNone(await self.backend.get(failed[0]))

    async def test_executions_go_with_their_job(self):
        await self.seed(JobStatus.QUEUED, None)
        claimed = await self.backend.claim(
            worker_id="w", queues=("default",), limit=1, lease_seconds=60
        )
        await self.backend.complete(claimed[0], None)
        self.assertEqual(len(await self.backend.executions(claimed[0].id)), 1)

        await self.backend.purge(
            RetentionPolicy(success=0.0001, cancelled=0, failed=0, interval=0.0),
            time.time() + 10.0,
        )
        self.assertIsNone(await self.backend.get(claimed[0].id))
        self.assertEqual(await self.backend.executions(claimed[0].id), [])

    async def test_the_first_sweep_is_always_due(self):
        await self.seed(JobStatus.SUCCESS, 100.0, 2)
        policy = RetentionPolicy(
            success=50.0, cancelled=0, failed=0, interval=86400.0
        )
        self.assertEqual(await self.backend.purge(policy, 1000.0), 2)

    async def test_a_disabled_policy_does_nothing(self):
        await self.seed(JobStatus.SUCCESS, 100.0)
        self.assertIsNone(await self.backend.purge(KEEP_EVERYTHING, 1_000_000.0))
        self.assertEqual(await self.count(), 1)

    async def test_only_one_sweep_happens_per_interval(self):
        await self.seed(JobStatus.SUCCESS, 100.0, 4)
        policy = RetentionPolicy(success=50.0, cancelled=0, failed=0, interval=3600.0)
        first = await self.backend.purge(policy, 1000.0)
        second = await self.backend.purge(policy, 1000.0)
        third = await self.backend.purge(policy, 1000.0 + 3601.0)
        self.assertEqual(first, 4)
        self.assertIsNone(second)
        self.assertEqual(third, 0)

    async def test_a_sweep_is_bounded_and_resumes_next_time(self):
        await self.seed(JobStatus.SUCCESS, 100.0, 25)
        policy = RetentionPolicy(
            success=50.0, cancelled=0, failed=0, interval=0.0,
            batch=4, maximum_per_sweep=10,
        )
        self.assertEqual(await self.backend.purge(policy, 1000.0), 10)
        self.assertEqual(await self.count(), 15)
        self.assertEqual(await self.backend.purge(policy, 1000.0), 10)
        self.assertEqual(await self.backend.purge(policy, 1000.0), 5)
        self.assertEqual(await self.count(), 0)

    async def test_the_oldest_go_first(self):
        newest = await self.seed(JobStatus.SUCCESS, 300.0)
        await self.seed(JobStatus.SUCCESS, 100.0)
        await self.seed(JobStatus.SUCCESS, 200.0)
        policy = RetentionPolicy(
            success=50.0, cancelled=0, failed=0, interval=0.0,
            batch=2, maximum_per_sweep=2,
        )
        await self.backend.purge(policy, 1000.0)
        survivors = [job.id for job in (await self.backend.list(limit=10)).items]
        self.assertEqual(survivors, newest)

    async def test_the_backend_declares_it_can_prune(self):
        self.assertTrue(self.backend.capabilities.retention)


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
