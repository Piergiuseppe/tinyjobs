from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.exceptions import (
    AppClosedError,
    ConfigurationError,
    UnsupportedCapabilityError,
)
from tinyjobs.status import JobStatus


class SQLiteTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "shop.db"
        self.app = TinyJobs(f"sqlite:///{self.path}")
        await self.app.ready()
        self.ran: list = []

        @self.app.task(name="noted", queue="notes")
        def noted(value=None) -> None:
            self.ran.append(value)

        self.noted = noted

    async def asyncTearDown(self) -> None:
        await self.app.aclose()
        self.directory.cleanup()

    def connect(self, path: Path | None = None) -> sqlite3.Connection:
        connection = sqlite3.connect(path or self.path, isolation_level=None)
        self.addCleanup(connection.close)
        return connection

    async def test_a_job_sent_inside_a_transaction_appears_when_it_commits(self):
        connection = self.connect()
        connection.execute("BEGIN IMMEDIATE")
        job = await self.noted.with_options(tx=connection, priority=4).aenqueue(7)
        self.assertIsNone(await self.app.aget(job.id))
        connection.execute("COMMIT")
        stored = await self.app.aget(job.id)
        self.assertEqual(
            (stored.status, stored.queue, stored.priority, self.app.decode(stored).args),
            (JobStatus.QUEUED, "notes", 4, (7,)),
        )

    async def test_a_job_sent_inside_a_rolled_back_transaction_is_never_stored(self):
        connection = self.connect()
        connection.execute("BEGIN")
        job = await self.noted.with_options(tx=connection).aenqueue()
        connection.execute("ROLLBACK")
        self.assertIsNone(await self.app.aget(job.id))
        self.assertEqual((await self.app.ajobs()).items, [])

    async def test_send_and_a_cursor_join_the_transaction_too(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("BEGIN")
        job = await self.app.asend("x.task", args=[1], tx=cursor, queue="q")
        self.assertEqual(cursor.execute("SELECT 1").fetchall(), [(1,)])
        self.assertIsNone(await self.app.aget(job.id))
        cursor.execute("COMMIT")
        self.assertEqual((await self.app.aget(job.id)).queue, "q")

    async def test_a_bound_task_keeps_its_transaction_through_more_options(self):
        connection = self.connect()
        connection.execute("BEGIN")
        bound = self.noted.with_options(tx=connection).with_options(queue="later")
        job = await bound.aenqueue()
        self.assertIsNone(await self.app.aget(job.id))
        connection.execute("COMMIT")
        self.assertEqual((await self.app.aget(job.id)).queue, "later")

    async def test_a_dedup_key_inside_a_transaction_finds_its_holder(self):
        held = await self.noted.with_options(key="k1").aenqueue()
        connection = self.connect()
        connection.execute("BEGIN")
        keyed = self.noted.with_options(tx=connection, key="k1")
        self.assertEqual((await keyed.aenqueue()).id, held.id)
        first = await self.noted.with_options(tx=connection, key="k2").aenqueue()
        again = await self.noted.with_options(tx=connection, key="k2").aenqueue()
        self.assertEqual(again.id, first.id)
        connection.execute("COMMIT")
        self.assertEqual(len((await self.app.ajobs()).items), 2)

    async def test_the_callers_rows_and_the_job_commit_together(self):
        connection = self.connect()
        connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)")
        connection.execute("BEGIN")
        connection.execute("INSERT INTO orders (total) VALUES (9.5)")
        job = await self.app.asend("x.task", tx=connection)
        other = self.connect()
        self.assertEqual(other.execute("SELECT count(*) FROM orders").fetchone(), (0,))
        self.assertIsNone(await self.app.aget(job.id))
        connection.execute("COMMIT")
        self.assertEqual(other.execute("SELECT count(*) FROM orders").fetchone(), (1,))
        self.assertIsNotNone(await self.app.aget(job.id))

    async def test_a_failure_later_in_the_transaction_takes_the_job_with_it(self):
        connection = self.connect()
        connection.execute("CREATE TABLE codes (code TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO codes VALUES ('taken')")
        connection.execute("BEGIN")
        job = await self.app.asend("x.task", tx=connection)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO codes VALUES ('taken')")
        connection.execute("ROLLBACK")
        self.assertIsNone(await self.app.aget(job.id))

    async def test_workers_run_a_job_only_once_its_transaction_commits(self):
        worker = self.app.worker(
            concurrency=1, queues=["notes"], scheduler=False, poll_maximum=0.05
        )
        runner = asyncio.create_task(worker.start())
        try:
            connection = self.connect()
            connection.execute("BEGIN")
            job = await self.noted.with_options(tx=connection).aenqueue("later")
            await asyncio.sleep(0.3)
            self.assertEqual(self.ran, [])
            connection.execute("COMMIT")
            deadline = time.monotonic() + 10
            while (await job.arefresh()).status is not JobStatus.SUCCESS:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.05)
            self.assertEqual(self.ran, ["later"])
        finally:
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)

    async def test_a_connection_to_another_file_is_refused(self):
        other = Path(self.directory.name) / "other.db"
        connection = self.connect(other)
        connection.execute("BEGIN")
        with self.assertRaises(ConfigurationError) as caught:
            await self.app.asend("x.task", tx=connection)
        self.assertIn(os.path.realpath(other), str(caught.exception))
        self.assertIn(os.path.realpath(self.path), str(caught.exception))
        self.assertIn("the queue's own file", str(caught.exception))
        self.assertEqual((await self.app.ajobs()).items, [])

    async def test_a_connection_through_a_symlink_to_the_file_is_accepted(self):
        link = Path(self.directory.name) / "link.db"
        link.symlink_to(self.path)
        connection = self.connect(link)
        connection.execute("BEGIN")
        job = await self.app.asend("x.task", tx=connection)
        connection.execute("COMMIT")
        self.assertIsNotNone(await self.app.aget(job.id))

    async def test_an_in_memory_connection_of_the_caller_is_refused(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(ConfigurationError, "in-memory or temporary database"):
            await self.app.asend("x.task", tx=connection)

    async def test_an_in_memory_app_refuses_a_transaction(self):
        for url in ("memory://", "sqlite:///:memory:"):
            with self.subTest(url=url):
                app = TinyJobs(url)
                await app.ready()
                try:
                    with self.assertRaisesRegex(UnsupportedCapabilityError, "in-memory"):
                        await app.asend("x.task", tx=self.connect())
                finally:
                    await app.aclose()

    async def test_anything_but_a_sqlite3_connection_or_cursor_is_refused(self):
        for tx in (object(), "connection", 1):
            with self.subTest(tx=tx):
                with self.assertRaisesRegex(TypeError, "sqlite3 connection or cursor"):
                    await self.app.asend("x.task", tx=tx)

    async def test_eager_mode_refuses_a_transaction(self):
        eager = TinyJobs(f"sqlite:///{self.path}", eager=True)
        await eager.ready()
        self.addAsyncCleanup(eager.aclose)
        with self.assertRaisesRegex(UnsupportedCapabilityError, "eager"):
            await eager.asend("x.task", tx=self.connect())


class ColdSQLiteTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "shop.db"

    def connect(self, path: Path | None = None) -> sqlite3.Connection:
        connection = sqlite3.connect(path or self.path, isolation_level=None)
        self.addCleanup(connection.close)
        return connection

    def tables(self, connection: sqlite3.Connection) -> set[str]:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {row[0] for row in rows}

    def test_the_first_call_inside_a_transaction_installs_the_schema(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        connection.execute("BEGIN")
        connection.execute("INSERT INTO orders DEFAULT VALUES")
        job = app.send("x.task", tx=connection)
        self.assertIn("tinyjobs_jobs", self.tables(connection))
        self.assertNotIn("tinyjobs_jobs", self.tables(self.connect()))
        connection.execute("COMMIT")
        self.assertIsNotNone(app.get(job.id))
        self.assertIsNone(app._cold)

    def test_a_rolled_back_first_call_leaves_no_schema_behind(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("BEGIN")
        app.send("x.task", tx=connection)
        connection.execute("ROLLBACK")
        self.assertEqual(self.tables(connection), set())
        self.assertEqual(app.jobs().items, [])

    def test_a_current_schema_is_left_alone_by_a_cold_call(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        before = connection.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()
        connection.execute("BEGIN")
        job = app.send("x.task", tx=connection)
        after = connection.execute("SELECT sql FROM sqlite_master ORDER BY name").fetchall()
        connection.execute("COMMIT")
        self.assertEqual(after, before)
        self.assertIsNotNone(app.get(job.id))

    def test_a_cold_call_runs_in_the_calling_thread(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        outcome: list = []

        def elsewhere() -> None:
            connection = sqlite3.connect(self.path, isolation_level=None)
            try:
                connection.execute("BEGIN")
                job = app.send("x.task", tx=connection, key="once")
                again = app.send("x.task", tx=connection, key="once")
                connection.execute("COMMIT")
                outcome.append((job.id, again.id))
            except BaseException as error:
                outcome.append(error)
            finally:
                connection.close()

        thread = threading.Thread(target=elsewhere)
        thread.start()
        thread.join(10)
        self.assertEqual(len(outcome), 1, outcome)
        self.assertNotIsInstance(outcome[0], BaseException, outcome)
        job_id, again_id = outcome[0]
        self.assertEqual(again_id, job_id)
        self.assertEqual(len(app.jobs().items), 1)

    def test_the_async_call_joins_a_transaction_cold_too(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        other = self.connect()

        async def scenario() -> str:
            connection = sqlite3.connect(self.path, isolation_level=None)
            try:
                connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
                connection.execute("BEGIN")
                connection.execute("INSERT INTO orders DEFAULT VALUES")
                job = await app.asend("x.task", tx=connection)
                self.assertIsNone(app._backend)
                self.assertIsNotNone(app._cold)
                self.assertNotIn("tinyjobs_jobs", self.tables(other))
                connection.execute("COMMIT")
                return job.id
            finally:
                connection.close()

        job_id = asyncio.run(scenario())
        self.assertIsNotNone(app.get(job_id))

    def test_a_closed_app_refuses_a_cold_call(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        app.close()
        with self.assertRaises(AppClosedError):
            app.send("x.task", tx=self.connect())

    def test_closing_a_cold_app_drops_its_cold_backend(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        connection = self.connect()
        app.send("x.task", tx=connection)
        self.assertIsNotNone(app._cold)
        app.close()
        self.assertIsNone(app._cold)

    def test_a_shared_cache_memory_uri_refuses_a_transaction(self):
        from tinyjobs.backends.sqlite import SQLiteBackend

        backend = SQLiteBackend("file::memory:?cache=shared", uri=True)
        self.addCleanup(backend._connections.close)
        with self.assertRaisesRegex(UnsupportedCapabilityError, "in-memory"):
            backend.enqueue_in(self.connect(), [])

    def test_an_in_memory_url_is_refused_before_anything_connects(self):
        for url in ("sqlite:///:memory:", "memory://"):
            with self.subTest(url=url):
                app = TinyJobs(url)
                self.addCleanup(app.close)
                with self.assertRaisesRegex(UnsupportedCapabilityError, "in-memory"):
                    app.send("x.task", tx=self.connect())
                self.assertIsNone(app._cold)
                self.assertIsNone(app._backend)

    def test_eager_mode_is_refused_before_anything_connects(self):
        eager = TinyJobs(f"sqlite:///{self.path}", eager=True, busy_timeout=0.2)
        self.addCleanup(eager.close)
        connection = self.connect()
        connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO orders DEFAULT VALUES")
        with self.assertRaisesRegex(UnsupportedCapabilityError, "eager"):
            eager.send("x.task", tx=connection)
        self.assertIsNone(eager._backend)
        self.assertIsNone(eager._cold)
        connection.execute("ROLLBACK")

    def test_a_sync_call_with_tx_works_inside_a_running_loop_once_connected(self):
        app = TinyJobs(f"sqlite:///{self.path}").start()
        self.addCleanup(app.close)

        async def scenario() -> str:
            connection = sqlite3.connect(self.path, isolation_level=None)
            try:
                connection.execute("BEGIN")
                job = app.send("x.task", tx=connection)
                connection.execute("COMMIT")
                return job.id
            finally:
                connection.close()

        self.assertIsNotNone(app.get(asyncio.run(scenario())))

    def test_a_tx_call_on_the_apps_own_held_transaction_is_not_refused(self):
        app = TinyJobs(f"sqlite:///{self.path}").start()
        self.addCleanup(app.close)
        with app.backend._inner.transaction() as connection:
            job = app.send("x.task", tx=connection)
            self.assertIsNone(app.get(job.id))
        self.assertIsNotNone(app.get(job.id))

    def test_a_legacy_file_is_migrated_inside_the_callers_transaction(self):
        from tests.test_retention import MigrationTests

        MigrationTests.build_v2(self, self.path)
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        other = self.connect()
        connection.execute("BEGIN")
        app.send("x.task", tx=connection)
        self.assertNotIn("jobs", self.tables(connection))
        self.assertIn("tinyjobs_jobs", self.tables(connection))
        self.assertIn("jobs", self.tables(other))
        self.assertNotIn("tinyjobs_jobs", self.tables(other))
        connection.execute("ROLLBACK")
        self.assertIn("jobs", self.tables(connection))
        self.assertNotIn("tinyjobs_jobs", self.tables(connection))
        self.assertEqual(
            connection.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone(),
            ("2",),
        )
        connection.execute("BEGIN")
        job = app.send("x.task", tx=connection)
        connection.execute("COMMIT")
        ids = {stored.id for stored in app.jobs().items}
        self.assertEqual(ids, {"retried", "running", job.id})

    def test_a_first_call_without_a_transaction_installs_atomically(self):
        from tests.test_retention import MigrationTests

        modes = {
            "legacy": lambda: sqlite3.connect(self.path),
            "autocommit": lambda: sqlite3.connect(self.path, isolation_level=None),
            "managed": lambda: sqlite3.connect(self.path, autocommit=False),
        }
        for mode, connect in modes.items():
            with self.subTest(mode=mode):
                self.path.unlink(missing_ok=True)
                MigrationTests.build_v2(self, self.path)
                app = TinyJobs(f"sqlite:///{self.path}")
                connection = connect()
                job = app.send("x.task", tx=connection)
                connection.rollback()
                if mode == "managed":
                    self.assertIn("jobs", self.tables(self.connect()))
                    self.assertNotIn("tinyjobs_jobs", self.tables(self.connect()))
                    job = app.send("x.task", tx=connection)
                    connection.commit()
                self.assertIn("tinyjobs_jobs", self.tables(self.connect()))
                self.assertNotIn("jobs", self.tables(self.connect()))
                self.assertEqual(
                    self.connect()
                    .execute("SELECT value FROM tinyjobs_meta WHERE key = 'schema_version'")
                    .fetchone(),
                    ("4",),
                )
                if mode == "legacy":
                    self.assertIsNone(app.get(job.id))
                else:
                    self.assertIsNotNone(app.get(job.id))
                connection.close()
                app.close()

    def test_an_install_that_fails_halfway_leaves_the_legacy_queue_intact(self):
        from tests.test_retention import MigrationTests

        MigrationTests.build_v2(self, self.path)
        blocker = self.connect()
        blocker.execute("CREATE TABLE tinyjobs_jobs_claim (x)")
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        for mode in ("autocommit", "transaction"):
            with self.subTest(mode=mode):
                connection = self.connect()
                if mode == "transaction":
                    connection.execute("BEGIN")
                with self.assertRaises(sqlite3.OperationalError):
                    app.send("x.task", tx=connection)
                if mode == "transaction":
                    self.assertTrue(connection.in_transaction)
                    seen = self.tables(connection)
                    self.assertIn("jobs", seen)
                    self.assertIn("meta", seen)
                    self.assertNotIn("tinyjobs_jobs", seen)
                    connection.execute("COMMIT")
                self.assertFalse(connection.in_transaction)
                tables = self.tables(self.connect())
                self.assertIn("jobs", tables)
                self.assertIn("meta", tables)
                self.assertNotIn("tinyjobs_jobs", tables)
                self.assertEqual(
                    blocker.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone(),
                    ("2",),
                )
        blocker.execute("DROP TABLE tinyjobs_jobs_claim")
        job = app.send("x.task", tx=self.connect())
        self.assertIsNotNone(app.get(job.id))

    def test_a_deferred_transaction_writes_before_it_reads_while_others_commit(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        stop = threading.Event()
        committed = [0]

        def commit_often() -> None:
            writer = sqlite3.connect(self.path, isolation_level=None, timeout=5)
            writer.execute("CREATE TABLE ticks (n INTEGER)")
            while not stop.is_set():
                writer.execute("INSERT INTO ticks VALUES (1)")
                committed[0] += 1
                time.sleep(0.001)
            writer.close()

        thread = threading.Thread(target=commit_often)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(stop.set)
        connection = self.connect()
        sent = 0
        while sent < 300 or committed[0] < 100:
            connection.execute("BEGIN")
            if sent % 2:
                app.send("x.task", tx=connection, key=f"k{sent}")
            else:
                app.send("x.task", tx=connection)
            connection.execute("ROLLBACK")
            sent += 1
        stop.set()
        thread.join()
        self.assertIn("ticks", self.tables(connection))

    def test_first_calls_started_together_on_a_new_file_all_install_and_commit(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        gate = threading.Barrier(4)
        failures: list = []

        def first_request() -> None:
            connection = sqlite3.connect(self.path, isolation_level=None, timeout=5)
            try:
                gate.wait(5)
                connection.execute("BEGIN")
                app.send("x.task", tx=connection)
                time.sleep(0.01)
                connection.execute("COMMIT")
            except Exception as error:
                failures.append(error)
            finally:
                connection.close()

        threads = [threading.Thread(target=first_request) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(15)
        self.assertEqual(failures, [])
        self.assertEqual(len(app.jobs().items), 4)

    def test_a_caller_holding_the_exclusive_lock_is_not_waited_on(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        first = self.connect()
        first.execute("BEGIN")
        app.send("x.task", tx=first)
        first.execute("COMMIT")
        self.assertEqual(first.execute("PRAGMA journal_mode").fetchone(), ("delete",))
        importer = self.connect()
        importer.execute("CREATE TABLE imports (blob BLOB)")
        importer.execute("BEGIN")
        importer.executemany(
            "INSERT INTO imports VALUES (?)", [(bytes(65536),) for _ in range(64)]
        )
        started = time.monotonic()
        job = app.send("x.task", tx=importer)
        self.assertLess(time.monotonic() - started, 1.0)
        importer.execute("COMMIT")
        self.assertIsNotNone(app.get(job.id))

    def test_a_current_schema_is_checked_outside_a_transaction_once(self):
        from unittest import mock

        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        app.send("x.task", tx=self.connect())
        backend = app._cold[0]
        with mock.patch.object(backend, "_current_outside", side_effect=AssertionError("again")):
            app.send("x.task", tx=self.connect())
        self.assertEqual(len(app.jobs().items), 2)

    def test_a_transaction_that_read_before_the_tables_existed_is_told_so(self):
        reader = self.connect()
        reader.execute("PRAGMA journal_mode = WAL")
        reader.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM orders").fetchone()
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        with self.assertRaisesRegex(sqlite3.OperationalError, "before the queue's tables existed"):
            app.send("x.task", tx=reader)
        reader.execute("ROLLBACK")
        self.assertIsNotNone(app.get(app.send("x.task", tx=reader).id))

    def test_the_first_call_installs_while_the_caller_still_reads_a_cursor(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        connection.executemany("INSERT INTO orders VALUES (?)", [(number,) for number in range(3)])
        connection.execute("BEGIN")
        for (order,) in connection.execute("SELECT id FROM orders"):
            app.send("x.task", tx=connection, key=f"order:{order}")
        connection.execute("COMMIT")
        self.assertEqual(len(app.jobs().items), 3)

    def test_a_ready_backend_installs_again_when_the_tables_are_gone(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        app.send("x.task", tx=connection)
        for table in ("tinyjobs_executions", "tinyjobs_jobs", "tinyjobs_schedules", "tinyjobs_meta"):
            connection.execute(f"DROP TABLE {table}")
        job = app.send("x.task", tx=connection)
        self.assertIsNotNone(app.get(job.id))

    def test_an_install_on_a_file_with_the_lock_table_writes_to_it_first(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        self.connect().execute(
            "UPDATE tinyjobs_meta SET value = '3' WHERE key = 'schema_version'"
        )
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        other = self.connect()
        other.execute("CREATE TABLE ticks (n INTEGER)")
        traced: list[str] = []

        def commit_elsewhere(statement: str) -> None:
            if traced or statement.upper().startswith(("BEGIN", "SAVEPOINT", "PRAGMA")):
                return
            traced.append(statement)
            other.execute("INSERT INTO ticks VALUES (1)")

        connection = self.connect()
        connection.set_trace_callback(commit_elsewhere)
        connection.execute("BEGIN")
        job = app.send("x.task", tx=connection)
        connection.execute("COMMIT")
        self.assertEqual(len(traced), 1)
        self.assertTrue(traced[0].startswith("DELETE FROM tinyjobs_install_lock"), traced)
        self.assertEqual(
            self.connect().execute("SELECT value FROM tinyjobs_meta WHERE key = 'schema_version'").fetchone(),
            ("4",),
        )
        self.assertIsNotNone(app.get(job.id))

    def test_reworded_errors_keep_the_sqlite_error_code(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        reader = self.connect()
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM tinyjobs_jobs").fetchone()
        self.connect().execute("CREATE TABLE ticks (n INTEGER)")
        with self.assertRaises(sqlite3.OperationalError) as caught:
            app.send("x.task", tx=reader)
        self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_BUSY_SNAPSHOT)
        self.assertEqual(caught.exception.sqlite_errorname, "SQLITE_BUSY_SNAPSHOT")

    def test_a_transaction_that_read_before_a_write_elsewhere_is_told_to_begin_immediately(self):
        TinyJobs(f"sqlite:///{self.path}").start().close()
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        reader = self.connect()
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM tinyjobs_jobs").fetchone()
        self.connect().execute("CREATE TABLE ticks (n INTEGER)")
        with self.assertRaisesRegex(sqlite3.OperationalError, "BEGIN IMMEDIATE"):
            app.send("x.task", tx=reader)
        reader.execute("ROLLBACK")
        reader.execute("BEGIN IMMEDIATE")
        reader.execute("SELECT count(*) FROM tinyjobs_jobs").fetchone()
        job = app.send("x.task", tx=reader)
        reader.execute("COMMIT")
        self.assertIsNotNone(app.get(job.id))

    def test_a_migration_cannot_run_while_the_caller_still_reads_a_cursor(self):
        from tests.test_retention import MigrationTests

        MigrationTests.build_v2(self, self.path)
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("BEGIN")
        pending = connection.execute("SELECT key FROM meta")
        pending.fetchone()
        with self.assertRaisesRegex(sqlite3.OperationalError, "read the rows first"):
            app.send("x.task", tx=connection)
        pending.close()
        connection.execute("ROLLBACK")
        self.assertIn("jobs", self.tables(connection))
        job = app.send("x.task", tx=connection)
        self.assertIsNotNone(app.get(job.id))

    def test_the_join_check_resolves_the_path_the_connection_reports(self):
        from tinyjobs.backends.sqlite import SQLiteBackend

        class Listing:
            def __init__(self, file: Path) -> None:
                self.rows = [{"name": "main", "file": str(file)}]

            def execute(self, statement: str) -> list:
                return self.rows

        self.path.touch()
        link = Path(self.directory.name) / "link.db"
        link.symlink_to(self.path)
        backend = SQLiteBackend(str(self.path))
        self.addCleanup(backend._connections.close)
        backend._check_joined(Listing(link))
        with self.assertRaisesRegex(ConfigurationError, "connect to it"):
            backend._check_joined(Listing(link.with_name("other.db")))

    def test_a_cold_install_outside_a_transaction_switches_the_file_to_wal(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone(), ("delete",))
        app.send("x.task", tx=connection)
        self.assertEqual(self.connect().execute("PRAGMA journal_mode").fetchone(), ("wal",))

    def test_an_applications_own_jobs_and_meta_tables_are_left_alone(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, title TEXT)")
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE executions (job_id INTEGER, attempt INTEGER)")
        connection.execute("INSERT INTO meta VALUES ('schema_version', '3')")
        connection.execute("INSERT INTO jobs (title) VALUES ('plumbing')")
        connection.execute("INSERT INTO executions VALUES (1, 1)")
        connection.execute("BEGIN")
        job = app.send("x.task", tx=connection)
        connection.execute("COMMIT")
        tables = self.tables(connection)
        self.assertEqual(
            {"jobs", "meta", "executions", "tinyjobs_jobs", "tinyjobs_meta"} - tables, set()
        )
        self.assertEqual(connection.execute("SELECT title FROM jobs").fetchall(), [("plumbing",)])
        self.assertEqual(connection.execute("SELECT * FROM executions").fetchall(), [(1, 1)])
        self.assertIsNotNone(app.get(job.id))
        app.close()
        again = TinyJobs(f"sqlite:///{self.path}").start()
        self.addCleanup(again.close)
        self.assertIsNotNone(again.get(job.id))

    def test_the_foreign_key_follows_the_rename_whatever_the_caller_set(self):
        from tests.test_retention import MigrationTests

        MigrationTests.build_v2(self, self.path)
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        connection = self.connect()
        connection.execute("PRAGMA legacy_alter_table = ON")
        connection.execute("BEGIN")
        app.send("x.task", tx=connection)
        connection.execute("COMMIT")
        sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'tinyjobs_executions'"
        ).fetchone()[0]
        self.assertIn("tinyjobs_jobs", sql)
        self.assertEqual(connection.execute("PRAGMA legacy_alter_table").fetchone(), (1,))
        self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        app.start()
        claimed = app.run_sync(
            lambda: app.backend.claim(
                worker_id="w", queues=["default"], limit=1, lease_seconds=60
            ),
            name="test",
        )
        self.assertEqual(len(claimed), 1)

    def test_a_cold_call_on_another_file_is_refused_without_touching_it(self):
        app = TinyJobs(f"sqlite:///{self.path}")
        self.addCleanup(app.close)
        other = Path(self.directory.name) / "other.db"
        connection = self.connect(other)
        connection.execute("BEGIN")
        with self.assertRaisesRegex(ConfigurationError, "the queue's own file"):
            app.send("x.task", tx=connection)
        connection.execute("ROLLBACK")
        self.assertEqual(self.tables(connection), set())


class PrefixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "shop.db"

    def test_an_invalid_prefix_is_refused_before_anything_is_opened(self):
        for prefix in ("Bad", "a-b", "1abc", "", "x" * 41, 3):
            with self.subTest(prefix=prefix):
                with self.assertRaisesRegex(ConfigurationError, "prefix must be"):
                    TinyJobs(f"sqlite:///{self.path}", prefix=prefix)
        self.assertFalse(self.path.exists())

    def test_two_prefixes_on_one_file_keep_their_queues_apart(self):
        first = TinyJobs(f"sqlite:///{self.path}", prefix="shop").start()
        self.addCleanup(first.close)
        second = TinyJobs(f"sqlite:///{self.path}", prefix="mail").start()
        self.addCleanup(second.close)
        one = first.send("x.task")
        two = second.send("y.task")
        self.assertEqual([job.id for job in first.jobs().items], [one.id])
        self.assertEqual([job.id for job in second.jobs().items], [two.id])
        self.assertIsNone(first.get(two.id))
        connection = sqlite3.connect(self.path)
        self.addCleanup(connection.close)
        names = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        self.assertEqual(
            names,
            {f"{prefix}_{kind}" for prefix in ("shop", "mail")
             for kind in ("jobs", "executions", "schedules", "meta", "install_lock")},
        )

    def test_a_transaction_lands_in_the_prefix_of_its_app(self):
        first = TinyJobs(f"sqlite:///{self.path}", prefix="shop").start()
        self.addCleanup(first.close)
        second = TinyJobs(f"sqlite:///{self.path}", prefix="mail").start()
        self.addCleanup(second.close)
        connection = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(connection.close)
        connection.execute("BEGIN")
        job = second.send("y.task", tx=connection)
        connection.execute("COMMIT")
        self.assertIsNone(first.get(job.id))
        self.assertIsNotNone(second.get(job.id))

    def test_the_default_prefix_is_tinyjobs(self):
        app = TinyJobs(f"sqlite:///{self.path}").start()
        self.addCleanup(app.close)
        app.send("x.task")
        connection = sqlite3.connect(self.path)
        self.addCleanup(connection.close)
        self.assertEqual(
            connection.execute("SELECT count(*) FROM tinyjobs_jobs").fetchone(), (1,)
        )


if __name__ == "__main__":
    unittest.main()
