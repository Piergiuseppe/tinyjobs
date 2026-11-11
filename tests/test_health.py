from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest
from collections import namedtuple
from unittest import mock

from tinyjobs import TinyJobs
from tinyjobs.backends.sqlite import audit as sqlite_audit
from tinyjobs.exceptions import ConfigurationError

try:
    from tinyjobs.backends.postgres import audit as postgres_audit
except ConfigurationError:
    postgres_audit = None
try:
    from tinyjobs.backends.redis import audit as redis_audit
except ConfigurationError:
    redis_audit = None
from tinyjobs.clock import FrozenClock
from tinyjobs.duration import format_age
from tinyjobs.backends.base import Capabilities
from tinyjobs.backends.managed import ManagedBackend
from tinyjobs.models import Finding, Stats
from tinyjobs.services.health import HealthService

START = 1_791_633_600.0


class HealthTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FrozenClock(START)
        self.app = TinyJobs("sqlite://", clock=self.clock)

        @self.app.task(name="health.echo")
        def echo(value=None):
            return value

        self.echo = echo
        self.app.start()

    def tearDown(self) -> None:
        self.app.close()

    def claim(self, lease_seconds: float = 30.0):
        return self.app.run_sync(
            lambda: self.app.backend.claim(
                worker_id="w", queues=("default",), limit=1, lease_seconds=lease_seconds
            ),
            name="claim",
        )

    def failures(self, **options) -> list[str]:
        return [f.message for f in self.app.check(**options) if not f.ok]


class StatsTests(HealthTestCase):
    def test_counts_are_split_by_queue_and_status(self):
        self.echo.enqueue(1)
        self.echo.enqueue(2)
        self.echo.with_options(queue="mail").enqueue(3)
        self.claim()
        stats = self.app.stats()
        by_queue = {queue.queue: queue.counts for queue in stats.queues}
        self.assertEqual(
            by_queue, {"default": {"queued": 1, "running": 1}, "mail": {"queued": 1}}
        )
        self.assertEqual(stats.queued_tasks, {"health.echo": 2})

    def test_the_oldest_due_job_ignores_jobs_scheduled_for_later(self):
        self.echo.with_options(delay=600).enqueue()
        self.assertIsNone(self.app.stats().queues[0].oldest_due_at)
        self.clock.advance(5)
        self.echo.enqueue()
        self.clock.advance(40)
        self.assertEqual(self.app.stats().queues[0].oldest_due_at, START + 5)

    def test_expired_leases_are_counted(self):
        self.echo.enqueue()
        self.claim(lease_seconds=30)
        self.assertEqual(self.app.stats().expired_leases, 0)
        self.clock.advance(31)
        self.assertEqual(self.app.stats().expired_leases, 1)


class CheckTests(HealthTestCase):
    def test_a_healthy_app_passes(self):
        self.echo.enqueue()
        self.assertEqual(self.failures(max_wait=60), [])

    def test_an_app_without_tasks_fails(self):
        empty = TinyJobs("sqlite://")
        try:
            self.assertIn("no tasks registered", " ".join(
                f.message for f in empty.check() if not f.ok
            ))
        finally:
            empty.close()

    def test_queued_jobs_for_an_unregistered_task_fail(self):
        self.app.send("elsewhere.task")
        self.app.send("elsewhere.task")
        self.assertEqual(len(self.failures()), 1)
        self.assertIn("2 queued job(s) for elsewhere.task", self.failures()[0])

    def test_store_warnings_report_the_audit_without_failing_on_it(self):
        audited = [Finding(False, "the file is on tmpfs"), Finding(True, "journal mode WAL")]
        with mock.patch.object(self.app.backend, "audit", mock.AsyncMock(return_value=audited)):
            self.assertEqual(self.failures(), ["the file is on tmpfs"])
            self.assertEqual(self.failures(store_warnings=True), [])
            findings = self.app.check(store_warnings=True)
        warned = [f for f in findings if f.warning]
        self.assertEqual(warned, [Finding(True, "the file is on tmpfs", warning=True)])
        self.assertIn(Finding(True, "journal mode WAL"), findings)
        self.assertFalse(any(f.warning for f in self.app.check()))

    def test_a_job_past_its_lease_fails(self):
        self.echo.enqueue()
        self.claim(lease_seconds=30)
        self.clock.advance(31)
        self.assertIn("past their lease", self.failures()[0])

    def test_a_queue_that_waited_too_long_fails_only_with_a_limit(self):
        self.echo.enqueue()
        self.clock.advance(300)
        self.assertEqual(self.failures(), [])
        self.assertIn("over the 1m00s allowed", self.failures(max_wait=60)[0])

    def test_a_schedule_nothing_fires_fails(self):
        @self.app.task(name="health.tick", cron="* * * * *")
        def tick():
            return None

        self.app.run_sync(lambda: self.app.scheduling.register(), name="register")
        self.assertEqual(self.failures(), [])
        self.clock.advance(180)
        self.assertIn("schedule health.tick is", self.failures()[0])

    def test_a_paused_schedule_is_reported_and_never_overdue(self):
        @self.app.task(name="health.tick", cron="* * * * *")
        def tick():
            return None

        self.app.pause_schedule("health.tick")
        self.clock.advance(180)
        self.assertEqual(self.failures(), [])
        messages = [f.message for f in self.app.check()]
        self.assertIn("schedule health.tick is paused", messages)


class StoreAuditTests(unittest.TestCase):
    def test_an_in_memory_database_audits_clean(self):
        app = TinyJobs("sqlite://")
        self.addCleanup(app.close)
        [finding] = [f for f in app.check() if "audit" in f.message]
        self.assertTrue(finding.ok)

    def test_a_file_database_reports_its_journal_and_room(self):
        folder = tempfile.mkdtemp()
        app = TinyJobs(f"sqlite:///{folder}/jobs.db")
        self.addCleanup(app.close)
        [finding] = [f for f in app.check() if "WAL" in f.message]
        self.assertTrue(finding.ok)
        self.assertIn("synchronous NORMAL, which holds across process crashes, not power loss",
                      finding.message)
        self.assertIn("free", finding.message)

    def connection(self, folder: str, *pragmas: str) -> sqlite3.Connection:
        connection = sqlite3.connect(os.path.join(folder, "jobs.db"))
        self.addCleanup(connection.close)
        for pragma in pragmas:
            connection.execute(pragma)
        return connection

    def audit(self, connection: sqlite3.Connection, folder: str, mounts=(), device=None,
              **options):
        with mock.patch.object(sqlite_audit, "_mounts", return_value=list(mounts)), \
                mock.patch.object(sqlite_audit, "_device", return_value=device):
            return sqlite_audit.audit(connection, os.path.join(folder, "jobs.db"), **options)

    def test_a_rollback_journal_fails_the_audit(self):
        folder = tempfile.mkdtemp()
        connection = self.connection(folder, "PRAGMA journal_mode = DELETE")
        findings = self.audit(connection, folder)
        self.assertEqual([f.ok for f in findings], [False, True])
        self.assertIn("journal mode is delete, not WAL", findings[0].message)
        self.assertIn("keep the file on a local disk", findings[0].message)
        self.assertIn("filesystem not checked", findings[1].message)
        findings = self.audit(connection, folder, uri=True)
        self.assertIn("opened as a URI", findings[0].message)

    def test_synchronous_off_fails_the_audit(self):
        folder = tempfile.mkdtemp()
        connection = self.connection(
            folder, "PRAGMA journal_mode = WAL", "PRAGMA synchronous = OFF"
        )
        findings = self.audit(connection, folder)
        self.assertEqual([f.ok for f in findings], [False, True])
        self.assertIn("synchronous is OFF", findings[0].message)
        self.assertIn("something else reset it", findings[0].message)
        self.assertIn("opened as a URI", self.audit(connection, folder, uri=True)[0].message)

    def mounts(self, *lines: str) -> list:
        return sqlite_audit.parse_mounts(["junk", *lines])

    def test_a_network_filesystem_fails_the_audit(self):
        folder = os.path.realpath(tempfile.mkdtemp())
        connection = self.connection(folder, "PRAGMA journal_mode = WAL")
        mounts = self.mounts(
            "17 23 0:4 / /sys rw - sysfs sysfs rw",
            "23 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw",
            f"40 23 0:50 / {folder} rw - nfs4 nas:/export rw",
            f"41 40 0:51 / {folder}/other\\040dir rw - tmpfs tmpfs rw",
        )
        self.assertEqual(mounts[-1], (f"{folder}/other dir", "tmpfs", "0:51"))
        findings = self.audit(connection, folder, mounts, "0:50")
        self.assertEqual([f.ok for f in findings], [False, True])
        self.assertIn("the file is on nfs4", findings[0].message)
        [finding] = self.audit(connection, folder, mounts, "8:1")
        self.assertIn("on ext4", finding.message)
        local = [("/", "ext4", "8:1"), (folder, "apfs", "1:5")]
        path = sqlite_audit.Path(folder) / "jobs.db"
        self.assertEqual(sqlite_audit.mount_of(path, local), (folder, "apfs", "1:5"))
        self.assertEqual(sqlite_audit.mount_of(path, local, "8:1"), ("/", "ext4", "8:1"))
        self.assertEqual(sqlite_audit.mount_of(path, local, "9:9"), (folder, "apfs", "1:5"))
        self.assertEqual(sqlite_audit.mount_of(sqlite_audit.Path("/x/jobs.db"), local),
                         ("/", "ext4", "8:1"))
        self.assertIsNone(sqlite_audit.mount_of(path, [("/elsewhere", "ext4", "8:1")]))

    def test_a_file_that_is_itself_a_mount_point_fails_the_audit(self):
        folder = os.path.realpath(tempfile.mkdtemp())
        connection = self.connection(folder, "PRAGMA journal_mode = WAL")
        mounts = [("/", "ext4", "8:1"), (f"{folder}/jobs.db", "ext4", "8:1")]
        for device in ("8:1", "0:60", None):
            findings = self.audit(connection, folder, mounts, device)
            self.assertEqual([f.ok for f in findings], [False, True], device)
            self.assertIn("mount the directory, not the file", findings[0].message)
        [finding] = self.audit(connection, folder, [("/", "ext4", "8:1")], "8:1")
        self.assertIn("on ext4", finding.message)
        path = sqlite_audit.Path(folder) / "jobs.db"
        self.assertEqual(sqlite_audit._device(path), sqlite_audit._device(path.parent))
        self.assertIsNone(sqlite_audit._device(sqlite_audit.Path("/no/such/dir/x.db")))

    def test_a_filesystem_a_reboot_empties_fails_the_audit(self):
        folder = os.path.realpath(tempfile.mkdtemp())
        connection = self.connection(folder, "PRAGMA journal_mode = WAL")
        for kind, device, phrase in (
            ("tmpfs", "0:40", "memory that a reboot empties"),
            ("overlay", "0:60", "the container's writable layer"),
        ):
            mounts = [("/", kind, device)]
            findings = self.audit(connection, folder, mounts, device)
            self.assertEqual([f.ok for f in findings], [False, True], kind)
            self.assertIn(f"the file is on {kind}, {phrase}", findings[0].message)
        mounts = [("/", "overlay", "0:60"), (folder, "ext4", "8:1")]
        [finding] = self.audit(connection, folder, mounts, "8:1")
        self.assertIn("on ext4", finding.message)

    def test_a_full_disk_fails_the_audit(self):
        folder = tempfile.mkdtemp()
        connection = self.connection(folder, "PRAGMA journal_mode = WAL")
        usage = namedtuple("usage", "total used free")(1000, 990, 10 * 1024 * 1024)
        with mock.patch.object(sqlite_audit.shutil, "disk_usage", return_value=usage):
            findings = self.audit(connection, folder)
        self.assertEqual([f.ok for f in findings], [False, True])
        self.assertIn("has 10 MB free", findings[0].message)
        with mock.patch.object(sqlite_audit.shutil, "disk_usage", side_effect=OSError):
            [finding] = self.audit(connection, folder)
        self.assertTrue(finding.ok)
        self.assertIn("room not checked", finding.message)

    def test_a_file_database_opened_as_a_uri_is_audited_as_a_file(self):
        from tinyjobs.backends.sqlite.backend import SQLiteBackend

        def opened(database: str) -> list[Finding]:
            backend = SQLiteBackend(database, uri=True)
            self.addCleanup(lambda: asyncio.run(backend.close()))
            asyncio.run(backend.setup())
            return asyncio.run(backend.audit())

        folder = tempfile.mkdtemp()
        findings = opened(f"file:{folder}/jobs.db?mode=rwc")
        self.assertFalse(findings[0].ok)
        self.assertIn("journal mode is delete", findings[0].message)
        self.assertIn("opened as a URI", findings[0].message)
        for memory in (
            "file:audit-test?mode=memory&cache=shared",
            f"file:{folder}/twice.db?mode=rwc&mode=memory",
            f"file:{folder}/vfs.db?vfs=memdb",
        ):
            [finding] = opened(memory)
            self.assertIn("in-memory", finding.message, memory)
        self.assertFalse(os.path.exists(f"{folder}/twice.db"))
        odd = os.path.join(folder, "mode=memory", "my db")
        os.makedirs(odd)
        findings = opened(f"file:{odd.replace(' ', '%20')}/jobs.db?mode=rwc")
        self.assertNotIn("in-memory", findings[0].message)
        self.assertTrue(os.path.exists(f"{odd}/jobs.db"))
        literal = f"{folder}/literal.db?mode=memory"
        findings = opened(literal)
        self.assertNotIn("in-memory", findings[0].message)
        self.assertTrue(os.path.exists(literal))

    def test_the_deepest_mount_on_the_files_device_wins_and_escapes_are_decoded(self):
        folder = os.path.realpath(tempfile.mkdtemp())
        mounts = self.mounts(
            "23 1 8:1 / / rw - ext4 /dev/sda1 rw",
            f"40 23 0:50 / {folder} rw - autofs systemd-1 rw",
            f"41 40 0:51 / {folder} rw - nfs4 nas:/home rw",
            "42 23 0:52 / /mnt/a\\134b\\011c rw - nfs nas:/x rw",
            "43 23 0:53 /sub /mnt/bind rw shared:2 master:1 - nfs nas:/y rw",
        )
        self.assertEqual(mounts[-2], ("/mnt/a\\b\tc", "nfs", "0:52"))
        self.assertEqual(mounts[-1], ("/mnt/bind", "nfs", "0:53"))
        path = sqlite_audit.Path(folder) / "jobs.db"
        self.assertEqual(sqlite_audit.mount_of(path, mounts), (folder, "nfs4", "0:51"))
        self.assertEqual(sqlite_audit.mount_of(path, mounts, "0:50"), (folder, "autofs", "0:50"))
        with mock.patch.object(sqlite_audit, "_device", return_value="0:51"):
            connection = self.connection(folder, "PRAGMA journal_mode = WAL")
            findings = self.audit(connection, folder, mounts)
        self.assertIn("the file is on nfs4", findings[0].message)

    def test_a_mount_hidden_under_a_later_one_is_told_by_the_device(self):
        folder = os.path.realpath(tempfile.mkdtemp())
        mounts = self.mounts(
            "23 1 8:1 / / rw - ext4 /dev/sda1 rw",
            f"40 23 0:50 / {folder}/db rw - nfs4 nas:/export rw",
            f"41 23 0:51 / {folder} rw - tmpfs tmpfs rw",
        )
        path = sqlite_audit.Path(folder) / "db" / "jobs.db"
        self.assertEqual(sqlite_audit.mount_of(path, mounts, "0:51"), (folder, "tmpfs", "0:51"))
        self.assertEqual(sqlite_audit.mount_of(path, mounts, "0:50"),
                         (f"{folder}/db", "nfs4", "0:50"))
        self.assertEqual(sqlite_audit.mount_of(path, mounts), (f"{folder}/db", "nfs4", "0:50"))

    @unittest.skipIf(redis_audit is None, "needs the redis extra")
    def test_redis_settings_are_judged_from_info(self):
        ok = {"role": "master", "aof_enabled": 1, "maxmemory": 0, "used_memory": 5 * 1024 * 1024,
              "aof_last_write_status": "ok", "rdb_last_bgsave_status": "ok"}
        [finding] = redis_audit.audit(ok)
        self.assertTrue(finding.ok)
        self.assertEqual(finding.message, "appendonly on; no maxmemory limit; 5 MB used")
        capped = {**ok, "maxmemory": 100 * 1024**2, "maxmemory_policy": "noeviction"}
        self.assertEqual(redis_audit.audit(capped)[0].message,
                         "appendonly on; noeviction; 5 MB of 100 MB used, 5%")
        volatile = {**capped, "maxmemory_policy": "volatile-lru"}
        self.assertEqual(redis_audit.audit(volatile)[0].message,
                         "appendonly on; volatile-lru; 5 MB of 100 MB used, 5%")
        findings = redis_audit.audit(
            {"aof_enabled": 0, "maxmemory": 100 * 1024**2, "maxmemory_policy": "allkeys-lru",
             "used_memory": 95 * 1024**2})
        self.assertEqual([f.ok for f in findings], [False, False, False, True])
        messages = [f.message for f in findings]
        self.assertIn("appendonly is off", messages[0])
        self.assertIn("maxmemory-policy is allkeys-lru", messages[1])
        self.assertIn("95 MB of 100 MB used, 95%: Redis is evicting", messages[2])
        self.assertEqual(messages[3], "role not checked: INFO holds no role; the last snapshot "
                                      "not checked: INFO holds no rdb_last_bgsave_status")
        [off, snapshot, _] = redis_audit.audit({**ok, "aof_enabled": 0,
                                                "rdb_last_bgsave_status": "err"})
        self.assertIn("appendonly is off", off.message)
        self.assertIn("the last snapshot failed (rdb_last_bgsave_status is err)", snapshot.message)
        [full, _] = redis_audit.audit({**capped, "used_memory": 95 * 1024**2})
        self.assertIn("enqueues fail with OOM", full.message)
        [full, _] = redis_audit.audit({**volatile, "used_memory": 95 * 1024**2})
        self.assertIn("enqueues fail with OOM", full.message)
        [replica, rest] = redis_audit.audit({**ok, "role": "slave"})
        self.assertFalse(replica.ok)
        self.assertIn("connected to a replica (role slave)", replica.message)
        self.assertEqual(rest.message, "appendonly on; no maxmemory limit; 5 MB used")
        [broken, _] = redis_audit.audit({**ok, "aof_last_write_status": "err"})
        self.assertFalse(broken.ok)
        self.assertIn("aof_last_write_status is err", broken.message)
        [snapshot, _] = redis_audit.audit({**ok, "rdb_last_bgsave_status": "err"})
        self.assertFalse(snapshot.ok)
        self.assertIn("the last snapshot failed (rdb_last_bgsave_status is err)", snapshot.message)
        [active] = redis_audit.audit({**ok, "role": "active-replica"})
        self.assertTrue(active.ok)
        [unanswered] = redis_audit.audit(None)
        self.assertTrue(unanswered.ok)
        self.assertIn("not checked", unanswered.message)
        [partial] = redis_audit.audit({"maxmemory": 1})
        self.assertTrue(partial.ok)
        self.assertIn("persistence not checked", partial.message)
        self.assertIn("eviction not checked", partial.message)
        self.assertIn("memory not checked", partial.message)
        [odd] = redis_audit.audit({"aof_enabled": "yes", "maxmemory": b"x", "used_memory": None,
                                   "role": b"master", "maxmemory_policy": 3})
        self.assertTrue(odd.ok)
        self.assertEqual(odd.message, "persistence not checked: INFO holds no aof_enabled; "
                                      "the last snapshot not checked: INFO holds no "
                                      "rdb_last_bgsave_status; "
                                      "eviction not checked: INFO holds no maxmemory; "
                                      "memory not checked: INFO holds no used_memory")
        [empty] = redis_audit.audit({})
        self.assertTrue(empty.ok)
        self.assertTrue(empty.message.startswith("role not checked: INFO holds no role; "))
        [statuses] = redis_audit.audit({**ok, "aof_last_write_status": None})
        self.assertIn("the last append-only write not checked: INFO holds no "
                      "aof_last_write_status", statuses.message)
        [capped_policy] = redis_audit.audit({**ok, "maxmemory": "x", "maxmemory_policy": "allkeys-lru"})
        self.assertTrue(capped_policy.ok)
        self.assertIn("eviction not checked", capped_policy.message)

    @unittest.skipIf(postgres_audit is None, "needs the postgres extra")
    def test_postgres_settings_are_judged_from_the_server(self):
        [finding] = postgres_audit.audit("on", "on", False)
        self.assertTrue(finding.ok)
        self.assertEqual(finding.message, "synchronous_commit on; fsync on; on the primary")
        self.assertTrue(postgres_audit.audit("remote_apply", "on", False)[0].ok)
        [unknown] = postgres_audit.audit(None, None, False)
        self.assertTrue(unknown.ok)
        self.assertEqual(unknown.message, "synchronous_commit not checked: the server has no "
                                          "such setting; fsync not checked: the server has no "
                                          "such setting; on the primary")
        messages = [f.message for f in postgres_audit.audit("off", "off", True)]
        self.assertEqual(len(messages), 3)
        self.assertIn("synchronous_commit is off", messages[0])
        self.assertIn("fsync is off", messages[1])
        self.assertIn("standby", messages[2])
        [failing, rest] = postgres_audit.audit("on", None, True)
        self.assertIn("standby", failing.message)
        self.assertEqual(rest.message, "synchronous_commit on; fsync not checked: the server "
                                       "has no such setting")
        [neon] = postgres_audit.audit("on", "off", False, True)
        self.assertEqual(neon.message, "synchronous_commit on; fsync off, with durability from "
                                       "Neon's storage; on the primary")
        self.assertFalse(postgres_audit.audit("on", "off", False, False)[0].ok)

    @unittest.skipIf(postgres_audit is None, "needs the postgres extra")
    def test_a_server_default_of_commit_off_is_told_beside_the_connection_setting(self):
        [forced] = postgres_audit.audit("on", "on", False, False, "off")
        self.assertTrue(forced.ok)
        self.assertEqual(forced.message, "synchronous_commit on, set by tinyjobs where the "
                                         "server's default is off: a job enqueued through a "
                                         "connection of your own commits under that default; "
                                         "fsync on; on the primary")
        [plain] = postgres_audit.audit("on", "on", False, False, "on")
        self.assertEqual(plain.message, "synchronous_commit on; fsync on; on the primary")
        [unknown] = postgres_audit.audit(None, "on", False, False, "off")
        self.assertTrue(unknown.message.startswith("synchronous_commit not checked"))
        [reset, _] = postgres_audit.audit("off", "on", False, False, "off")
        self.assertFalse(reset.ok)
        self.assertIn("so something else reset it", reset.message)

    def test_a_backend_without_an_audit_adds_nothing(self):
        class Plain:
            capabilities = Capabilities()

            async def stats(self, now):
                return Stats(queues=[])

        class Blocking(Plain):
            def audit(self):
                return [Finding(True, "audited without a coroutine")]

        app = TinyJobs("sqlite://").start()
        self.addCleanup(app.close)
        for raw, expected in ((Plain(), [False, True]), (Blocking(), [False, True, True])):
            managed = ManagedBackend(raw, app.backend._runner)
            service = HealthService(managed, app.registry, app.clock)
            findings = app.run_sync(service.check, name="check")
            self.assertEqual([f.ok for f in findings], expected)
            self.assertIn("no tasks registered", findings[0].message)
        self.assertEqual(findings[1].message, "audited without a coroutine")


class AgeFormattingTests(unittest.TestCase):
    def test_ages_read_naturally(self):
        cases = {0: "0s", 59.9: "59s", 61: "1m01s", 3600: "1h00m", 3 * 86400 + 7200: "3d2h"}
        for seconds, text in cases.items():
            self.assertEqual(format_age(seconds), text)


if __name__ == "__main__":
    unittest.main()
