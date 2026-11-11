from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from tinyjobs import FrozenClock, JobStatus, TinyJobs
from tinyjobs.backends.base import Capabilities
from tinyjobs.backends.sqlite.schema import COMPATIBLE_SINCE, SCHEMA_VERSION
from tinyjobs.cli import main
from tinyjobs.dashboard.core import Dashboard, Request
from tinyjobs.exceptions import JobNotFoundError, UnsupportedCapabilityError, WaitTimeout
from tinyjobs.metrics import _store_lines
from tinyjobs.following import AFTER_MESSAGE, PARENT_ENDED_MESSAGE, parent_id
from tinyjobs.testing.contract import make_job
from tests.support import BackendTestCase


class AppCase(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.ran: list[tuple[str, int]] = []

        @self.app.task(name="first", store_result=True)
        def first(number: int) -> int:
            self.ran.append(("first", number))
            return number * 2

        @self.app.task(name="then")
        def then(number: int) -> None:
            self.ran.append(("then", number))

        @self.app.task(name="broken")
        def broken() -> None:
            raise ValueError("no")

        self.first, self.then, self.broken = first, then, broken


class SendingTests(AppCase):
    def test_after_takes_a_job_or_its_id(self):
        parent = self.first.enqueue(1)
        by_job = self.then.with_options(after=parent).enqueue(2)
        by_id = self.then.with_options(after=parent.id).enqueue(3)
        sent = self.app.send("then", (4,), after=parent)
        for child in (by_job, by_id, sent):
            self.assertEqual((child.status, child.after), (JobStatus.WAITING, parent.id))
        self.assertEqual(self.app.get(by_job.id).after, parent.id)
        self.assertIsNone(parent.after)

    def test_after_refuses_what_is_not_a_job(self):
        for value in (7, "", b"id", object()):
            with self.subTest(value=value), self.assertRaises(TypeError) as raised:
                self.then.with_options(after=value).enqueue(1)
            self.assertEqual(str(raised.exception), AFTER_MESSAGE.format(value=value))
        self.assertIsNone(parent_id(None))
        self.assertEqual(self.app.counts(), {})

    def test_a_job_sent_after_one_that_is_gone_is_refused(self):
        with self.assertRaises(JobNotFoundError) as raised:
            self.then.with_options(after="01-no-such-job").enqueue(1)
        self.assertIn("01-no-such-job", str(raised.exception))
        self.assertEqual(self.app.counts(), {})

    def test_a_store_that_cannot_keep_a_job_waiting_refuses_after(self):
        parent = self.first.enqueue(1)
        self.backend.capabilities = Capabilities(after=False)
        with self.assertRaisesRegex(UnsupportedCapabilityError, "until another has succeeded"):
            self.then.with_options(after=parent).enqueue(2)
        self.assertEqual(self.app.counts(), {"queued": 1})

    def test_the_async_calls_take_after_too(self):
        async def send():
            parent = await self.first.aenqueue(1)
            child = await self.then.with_options(after=parent).aenqueue(2)
            sent = await self.app.asend("then", (3,), after=parent.id)
            return parent, child, sent

        parent, child, sent = self.app.run_sync(send, name="test")
        self.assertEqual((child.after, sent.after), (parent.id, parent.id))
        self.assertEqual(self.app.drain(), 3)
        self.assertEqual(self.ran, [("first", 1), ("then", 2), ("then", 3)])


class RunningTests(AppCase):
    def test_a_line_of_jobs_runs_in_order(self):
        first = self.first.enqueue(1)
        second = self.then.with_options(after=first).enqueue(2)
        third = self.then.with_options(after=second, priority=9).enqueue(3)
        self.assertEqual(self.app.drain(), 3)
        self.assertEqual(self.ran, [("first", 1), ("then", 2), ("then", 3)])
        self.assertEqual(
            [job.refresh().status for job in (first, second, third)], [JobStatus.SUCCESS] * 3
        )

    def test_the_child_reads_the_result_of_the_job_before_it(self):
        seen = []

        @self.app.task(name="reader")
        def reader(parent: str) -> None:
            seen.append(self.app.get(parent).result())

        parent = self.first.enqueue(21)
        reader.with_options(after=parent).enqueue(parent.id)
        self.app.drain()
        self.assertEqual(seen, [42])

    def test_a_failure_cancels_the_jobs_after_it_and_no_task_of_theirs_runs(self):
        parent = self.broken.enqueue()
        child = self.then.with_options(after=parent).enqueue(1)
        grandchild = self.then.with_options(after=child).enqueue(2)
        self.app.drain()
        self.assertEqual(self.ran, [])
        self.assertEqual(parent.refresh().status, JobStatus.FAILED)
        for job in (child, grandchild):
            job.refresh()
            self.assertEqual(
                (job.status, job.last_error, job.attempts),
                (JobStatus.CANCELLED, PARENT_ENDED_MESSAGE, 0),
            )

    def test_the_jobs_after_a_retried_job_wait_for_its_last_attempt(self):
        attempts = []

        @self.app.task(name="flaky", max_attempts=3, retry_base=0, retry_jitter=False)
        def flaky() -> None:
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("again")

        parent = flaky.enqueue()
        child = self.then.with_options(after=parent).enqueue(1)
        self.app.drain()
        self.assertEqual((len(attempts), self.ran), (3, [("then", 1)]))
        self.assertEqual(child.refresh().status, JobStatus.SUCCESS)

    def test_wait_follows_a_waiting_job_to_its_end(self):
        parent = self.broken.enqueue()
        child = self.then.with_options(after=parent).enqueue(1)
        with self.assertRaisesRegex(WaitTimeout, "still waiting"):
            child.wait(timeout=0.05)
        draining = threading.Timer(0.2, self.app.drain)
        draining.start()
        self.addCleanup(draining.join)
        self.assertEqual(child.wait(timeout=10).status, JobStatus.CANCELLED)

    def test_a_job_cancelled_with_its_parent_reaches_no_hook(self):
        seen = []
        self.app.on_job_start(lambda job: seen.append(("start", job.task)))
        self.app.on_job_failure(lambda job, error, will_retry: seen.append(("failure", job.task)))
        self.app.on_job_success(lambda job, result, duration: seen.append(("success", job.task)))
        parent = self.broken.enqueue()
        self.then.with_options(after=parent).enqueue(1)
        self.app.drain()
        self.assertEqual(seen, [("start", "broken"), ("failure", "broken")])

    def test_a_cancelled_line_is_sent_again_job_by_job(self):
        parent = self.broken.enqueue()
        child = self.then.with_options(after=parent).enqueue(1)
        self.app.drain()
        self.assertFalse(self.app.retry(child.id))
        self.assertTrue(self.app.retry(parent.id))
        self.assertEqual(child.refresh().status, JobStatus.CANCELLED)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main(["jobs", "retry", child.id], app=self.app), 0)
        self.assertEqual(output.getvalue(), f"job {child.id} waits for job {parent.id}\n")
        self.assertEqual(child.refresh().status, JobStatus.WAITING)
        self.assertTrue(self.app.cancel(child.id))
        self.assertEqual(child.refresh().status, JobStatus.CANCELLED)

    def test_a_job_joins_the_transaction_that_sends_it_after_another(self):
        parent = self.first.enqueue(1)
        connection = sqlite3.connect(self.database, isolation_level=None)
        self.addCleanup(connection.close)
        connection.execute("BEGIN IMMEDIATE")
        child = self.then.with_options(tx=connection, after=parent).enqueue(2)
        self.assertEqual(child.status, JobStatus.WAITING)
        connection.execute("ROLLBACK")
        self.assertIsNone(self.app.get(child.id))
        child = self.then.with_options(tx=connection, after=parent).enqueue(3)
        self.assertEqual(self.app.drain(), 2)
        self.assertEqual(self.ran, [("first", 1), ("then", 3)])
        late = self.then.with_options(tx=connection, after=parent).enqueue(4)
        self.assertEqual(late.status, JobStatus.QUEUED)
        with self.assertRaises(JobNotFoundError):
            self.then.with_options(tx=connection, after="gone").enqueue(5)


    def test_a_child_is_the_first_write_of_a_transaction_other_writers_ran_beside(self):
        parent = self.first.enqueue(1)
        connection = sqlite3.connect(self.database, isolation_level=None)
        self.addCleanup(connection.close)
        connection.execute("BEGIN")
        other = self.first.enqueue(2)
        child = self.then.with_options(tx=connection, after=parent).enqueue(3)
        with self.assertRaises(JobNotFoundError):
            self.app.backend.enqueue_in(
                connection, [make_job(id="same-call"), make_job(after="same-call")]
            )
        connection.execute("COMMIT")
        self.assertEqual(child.status, JobStatus.WAITING)
        self.assertEqual(self.app.counts(), {"queued": 2, "waiting": 1})
        self.assertEqual(self.app.get(other.id).status, JobStatus.QUEUED)


class EagerTests(unittest.TestCase):
    def test_eager_runs_a_job_sent_after_one_that_succeeded_and_cancels_after_a_failure(self):
        app = TinyJobs("memory://", eager=True, clock=FrozenClock(1_791_633_600.0))
        self.addCleanup(app.close)
        ran = []

        @app.task(name="step")
        def step(number: int) -> None:
            ran.append(number)
            if number < 0:
                raise ValueError("no")

        first = step.enqueue(1)
        second = step.with_options(after=first).enqueue(2)
        self.assertEqual((ran, second.status), ([1, 2], JobStatus.SUCCESS))
        failed = step.enqueue(-1)
        cancelled = step.with_options(after=failed).enqueue(3)
        self.assertEqual((ran, cancelled.status), ([1, 2, -1], JobStatus.CANCELLED))
        delayed = step.with_options(delay=3600).enqueue(4)
        waiting = step.with_options(after=delayed).enqueue(5)
        self.assertEqual((ran, waiting.status), ([1, 2, -1], JobStatus.WAITING))
        self.assertEqual(app.drain(), 0)
        app.clock.advance(3601)
        self.assertEqual((app.drain(), ran), (2, [1, 2, -1, 4, 5]))


class LookingTests(AppCase):
    def test_jobs_show_names_the_job_it_runs_after(self):
        parent = self.first.enqueue(1)
        child = self.then.with_options(after=parent).enqueue(2)
        shown = {}
        for job in (parent, child):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(["jobs", "show", job.id], app=self.app), 0)
            shown[job.id] = output.getvalue()
        self.assertIn(f"after      {parent.id}\n", shown[child.id])
        self.assertIn("status     waiting\n", shown[child.id])
        self.assertNotIn("after ", shown[parent.id])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            main(["jobs", "list", "--status", "waiting"], app=self.app)
            main(["stats"], app=self.app)
        self.assertIn(child.id, output.getvalue())
        self.assertNotIn(parent.id, output.getvalue())
        self.assertIn("WAITING", output.getvalue())

    def test_send_takes_the_job_to_run_after(self):
        parent = self.first.enqueue(1)
        failed = self.broken.enqueue()
        self.app.drain()
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            self.assertEqual(main(["send", "then", "--args", "[2]", "--after", parent.id], app=self.app), 0)
            self.assertEqual(main(["send", "then", "--args", "[3]", "--after", failed.id], app=self.app), 0)
        queued, cancelled = [self.app.get(line) for line in output.getvalue().split()]
        self.assertEqual((queued.after, queued.status), (parent.id, JobStatus.QUEUED))
        self.assertEqual((cancelled.after, cancelled.status), (failed.id, JobStatus.CANCELLED))
        self.assertEqual(errors.getvalue(), f"stored as cancelled: {PARENT_ENDED_MESSAGE}\n")
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            with self.assertRaises(SystemExit) as refused:
                main(["send", "then", "--after", ""], app=self.app)
            self.assertNotEqual(main(["send", "then", "--after", "no-such-job"], app=self.app), 0)
        self.assertEqual(refused.exception.code, 2)
        self.assertIn("not an empty value", errors.getvalue())
        self.assertIn("no-such-job", errors.getvalue() + output.getvalue())

    def test_jobs_retry_says_the_parent_may_be_why(self):
        parent = self.broken.enqueue()
        child = self.then.with_options(after=parent).enqueue(1)
        self.app.drain()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main(["jobs", "retry", child.id], app=self.app), 1)
        self.assertIn("the job it runs after did not succeed", output.getvalue())

    def test_jobs_cancel_takes_a_waiting_job(self):
        parent = self.first.enqueue(1)
        child = self.then.with_options(after=parent).enqueue(2)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main(["jobs", "cancel", child.id], app=self.app), 0)
            main(["jobs", "cancel", child.id], app=self.app)
        self.assertEqual(child.refresh().status, JobStatus.CANCELLED)
        self.assertIn("has finished already", output.getvalue())

    def test_the_dashboard_links_a_job_to_the_one_it_runs_after(self):
        dashboard = Dashboard(self.app, page_size=10)
        parent = self.first.enqueue(1)
        child = self.then.with_options(after=parent).enqueue(2)

        def page(path: str, **query: str) -> str:
            response = dashboard.respond(Request("GET", path, query, ""))
            self.assertEqual(response.status, 200, response.body)
            return response.body.decode()

        self.assertEqual(json.loads(page(f"/api/jobs/{child.id}"))["after"], parent.id)
        self.assertIn(
            'tinyjobs_store_jobs{queue="default",status="waiting"} 1',
            list(_store_lines(self.app.stats(), time.time())),
        )
        detail = page(f"/jobs/{child.id}")
        self.assertIn(f'<dt>Runs after</dt><dd><a href="/jobs/{parent.id}">{parent.id}</a>', detail)
        self.assertIn("status-waiting", detail)
        self.assertIn("Cancel", detail)
        self.assertIn("<dt>Runs after</dt><dd>-</dd>", page(f"/jobs/{parent.id}"))
        listed = page("/jobs", status="waiting")
        self.assertIn(child.id, listed)
        self.assertNotIn(f"/jobs/{parent.id}", listed)
        cancelled = dashboard.respond(Request("POST", f"/jobs/{child.id}/cancel", {}, ""))
        self.assertLess(cancelled.status, 400)
        self.assertEqual(child.refresh().status, JobStatus.CANCELLED)


OLD_DEDUP = (
    "CREATE UNIQUE INDEX tinyjobs_jobs_dedup ON tinyjobs_jobs (dedup_key) "
    "WHERE dedup_key IS NOT NULL AND status IN ('queued', 'held', 'running')"
)


class StoreVersionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "jobs.db"

    def opened(self) -> TinyJobs:
        app = TinyJobs(f"sqlite:///{self.database}").start()
        self.addCleanup(app.close)
        return app

    def sql(self, statement: str, *parameters: object) -> list[tuple]:
        connection = sqlite3.connect(self.database, isolation_level=None)
        try:
            return connection.execute(statement, parameters).fetchall()
        finally:
            connection.close()

    def meta(self) -> dict[str, str]:
        return dict(
            self.sql(
                "SELECT key, value FROM tinyjobs_meta "
                "WHERE key IN ('schema_version', 'compatible_since')"
            )
        )

    def test_a_file_written_by_0_16_learns_to_keep_waiting_jobs(self):
        app = self.opened()
        parent = app.run_sync(
            lambda: app.backend.enqueue(make_job(dedup_key="invoice:1")), name="test"
        )
        app.close()
        self.sql("DROP INDEX tinyjobs_jobs_waiting")
        self.sql("DROP INDEX tinyjobs_jobs_dedup")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN waiting_on")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN runs_after")
        self.sql(OLD_DEDUP)
        self.sql("DELETE FROM tinyjobs_meta WHERE key = 'compatible_since'")
        self.sql("UPDATE tinyjobs_meta SET value = '5' WHERE key = 'schema_version'")
        upgraded = self.opened()
        self.assertEqual(
            self.meta(),
            {"schema_version": str(SCHEMA_VERSION), "compatible_since": str(COMPATIBLE_SINCE)},
        )
        indexes = dict(
            self.sql(
                "SELECT name, sql FROM sqlite_master WHERE name IN "
                "('tinyjobs_jobs_waiting', 'tinyjobs_jobs_dedup')"
            )
        )
        self.assertIn("WHERE waiting_on IS NOT NULL", indexes["tinyjobs_jobs_waiting"])
        self.assertIn("'waiting'", indexes["tinyjobs_jobs_dedup"])

        async def send():
            child = await upgraded.backend.enqueue(make_job(after=parent.id, dedup_key="report:1"))
            again = await upgraded.backend.enqueue(make_job(dedup_key="report:1"))
            return child, again, await upgraded.backend.get(parent.id)

        child, again, kept = upgraded.run_sync(send, name="test")
        self.assertEqual((child.status, again.id), (JobStatus.WAITING, child.id))
        self.assertEqual((kept.dedup_key, kept.after), ("invoice:1", None))

    def test_a_file_that_lost_its_meta_table_gets_the_column_back_with_it(self):
        self.opened().close()
        self.sql("DROP INDEX tinyjobs_jobs_waiting")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN waiting_on")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN runs_after")
        self.sql("DROP TABLE tinyjobs_meta")
        app = self.opened()
        parent = app.run_sync(lambda: app.backend.enqueue(make_job()), name="test")
        child = app.run_sync(
            lambda: app.backend.enqueue(make_job(after=parent.id)), name="test"
        )
        self.assertEqual(child.status, JobStatus.WAITING)
        self.assertEqual(self.meta()["schema_version"], str(SCHEMA_VERSION))

    def test_a_file_that_lost_its_version_row_is_brought_up_like_an_old_one(self):
        self.opened().close()
        self.sql("DROP INDEX tinyjobs_jobs_waiting")
        self.sql("DROP INDEX tinyjobs_jobs_dedup")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN waiting_on")
        self.sql("ALTER TABLE tinyjobs_jobs DROP COLUMN runs_after")
        self.sql(OLD_DEDUP)
        self.sql("DELETE FROM tinyjobs_meta")
        app = self.opened()

        async def send():
            parent = await app.backend.enqueue(make_job())
            child = await app.backend.enqueue(make_job(after=parent.id, dedup_key="k"))
            return child, await app.backend.enqueue(make_job(dedup_key="k"))

        child, again = app.run_sync(send, name="test")
        self.assertEqual((child.status, again.id), (JobStatus.WAITING, child.id))
        self.assertEqual(self.meta()["schema_version"], str(SCHEMA_VERSION))

    def test_a_newer_file_that_says_this_release_works_on_it_is_used_as_it_is(self):
        self.opened().close()
        newer = str(SCHEMA_VERSION + 1)
        self.sql("UPDATE tinyjobs_meta SET value = ? WHERE key = 'schema_version'", newer)
        app = self.opened()

        @app.task(name="noop")
        def noop() -> None:
            return None

        noop.enqueue()
        self.assertEqual(app.drain(), 1)
        self.assertEqual(
            self.meta(), {"schema_version": newer, "compatible_since": str(COMPATIBLE_SINCE)}
        )

    def test_a_newer_file_this_release_cannot_work_on_is_refused(self):
        self.opened().close()
        newer = str(SCHEMA_VERSION + 1)
        self.sql(
            "UPDATE tinyjobs_meta SET value = ? "
            "WHERE key IN ('schema_version', 'compatible_since')",
            newer,
        )
        for prepare in (lambda: None, lambda: self.sql(
            "DELETE FROM tinyjobs_meta WHERE key = 'compatible_since'"
        )):
            prepare()
            app = TinyJobs(f"sqlite:///{self.database}")
            self.addCleanup(app.close)
            with self.assertRaisesRegex(RuntimeError, "newer than this tinyjobs"):
                app.start()
        self.assertEqual(self.meta(), {"schema_version": newer})


if __name__ == "__main__":
    unittest.main()
