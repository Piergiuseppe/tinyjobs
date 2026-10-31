from __future__ import annotations

import contextlib
import io
import os
import uuid
import tempfile
import threading
import unittest
from datetime import timedelta
from pathlib import Path

from tinyjobs.exceptions import ConfigurationError, UnknownTaskError

POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")

try:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
except ImportError:
    psycopg = None

try:
    import django
    from django.test.utils import override_settings
except ImportError:
    django = None


def setUpModule() -> None:
    if django is None:
        return
    from django.conf import settings

    if settings.configured:
        return
    databases = {
        "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
    }
    if POSTGRES_URL and psycopg is not None:
        settings_of = conninfo_to_dict(POSTGRES_URL)
        databases["pg"] = {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": settings_of.get("dbname", ""),
            "USER": settings_of.get("user", ""),
            "PASSWORD": settings_of.get("password", ""),
            "HOST": settings_of.get("host", ""),
            "PORT": settings_of.get("port", ""),
        }
    settings.configure(
        INSTALLED_APPS=["tinyjobs.contrib.django", "tests.djangoapp"],
        DATABASES=databases,
        USE_TZ=True,
        TASKS={},
    )
    django.setup()


@unittest.skipIf(django is None, "pip install django to run the django tests")
class DjangoTestCase(unittest.TestCase):
    options: dict = {}
    queues = ["default", "emails"]

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.directory.name) / 'jobs.db'}"
        overrides = override_settings(
            TASKS={
                "default": {
                    "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
                    "QUEUES": self.queues,
                    "OPTIONS": {"url": url, **self.options},
                }
            }
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        from django.tasks import default_task_backend

        from tests.djangoapp import tasks

        self.backend = default_task_backend
        self.tasks = tasks
        self.addCleanup(self.backend.app.close)
        self.addCleanup(self.directory.cleanup)

    def drain(self) -> int:
        return self.backend.app.drain()


class EnqueueTests(DjangoTestCase):
    def test_enqueue_returns_a_ready_result_with_the_task_settings(self):
        result = self.tasks.greet.enqueue("ada")
        self.assertEqual(result.status, "READY")
        self.assertEqual(result.task.queue_name, "emails")
        self.assertEqual(result.task.priority, 10)
        self.assertEqual(result.args, ["ada"])
        self.assertEqual(result.backend, "default")
        self.assertEqual(result.worker_ids, [])
        self.assertIsNotNone(result.enqueued_at.tzinfo)

    def test_threads_sharing_an_app_for_the_first_time_get_the_same_one(self):
        from tinyjobs.contrib.django import connections

        options = {"url": "sqlite:///:memory:"}
        self.addCleanup(connections._apps.pop, ("race", repr(sorted(options.items()))), None)
        both_preparing = threading.Barrier(2)
        apps: list = []

        def share() -> None:
            apps.append(
                connections.shared_app("race", options, lambda app: both_preparing.wait(5))
            )

        sharers = [threading.Thread(target=share) for _ in range(2)]
        for sharer in sharers:
            sharer.start()
        for sharer in sharers:
            sharer.join(10)
        self.assertEqual(len(apps), 2)
        self.assertEqual(len({id(app) for app in apps}), 1)

    def test_the_job_carries_queue_priority_and_task_path(self):
        result = self.tasks.greet.using(priority=-5).enqueue("ada")
        job = self.backend.app.get(result.id)
        self.assertEqual(job.task, "tests.djangoapp.tasks.greet")
        self.assertEqual(job.queue, "emails")
        self.assertEqual(job.priority, -5)

    def test_arguments_are_normalised_to_json_like_django_does(self):
        result = self.tasks.echo.enqueue((1, 2), key=("a", "b"))
        self.assertEqual(result.args, [[1, 2]])
        self.assertEqual(result.kwargs, {"key": ["a", "b"]})

    def test_an_argument_json_cannot_carry_is_refused_at_enqueue(self):
        with self.assertRaises(TypeError):
            self.tasks.echo.enqueue(object())

    def test_run_after_delays_the_job(self):
        from django.utils import timezone

        result = self.tasks.double.using(
            run_after=timezone.now() + timedelta(hours=1)
        ).enqueue(2)
        self.assertEqual(self.drain(), 0)
        self.assertEqual(self.backend.get_result(result.id).status, "READY")

    def test_run_after_is_reported_on_the_clock_the_job_keeps(self):
        import time
        from unittest import mock

        from django.utils import timezone

        clock = self.backend.app.clock
        with (
            mock.patch.object(clock, "now", lambda: time.time() + 900),
            mock.patch.object(clock, "from_wall", lambda instant: instant + 900),
        ):
            result = self.tasks.double.using(
                run_after=timezone.now() + timedelta(hours=1)
            ).enqueue(2)
        waited = result.task.run_after - result.enqueued_at
        self.assertAlmostEqual(waited.total_seconds(), 3600, delta=1)

    def test_a_delayed_result_read_back_still_says_when_it_runs(self):
        from django.utils import timezone

        result = self.tasks.double.using(
            run_after=timezone.now() + timedelta(hours=1)
        ).enqueue(2)
        found = self.backend.get_result(result.id)
        self.assertEqual(found.task.run_after, result.task.run_after)

    def test_a_result_handed_back_has_no_run_after_it_was_not_given(self):
        result = self.tasks.double.enqueue(2)
        app = self.backend.app

        async def hand_back() -> None:
            [job] = await app.backend.claim(
                worker_id="w", queues=["default"], limit=1, lease_seconds=30.0
            )
            await app.backend.hand_back(job, 0.0)

        app.run_sync(hand_back, name="hand back")
        self.assertIsNone(self.backend.get_result(result.id).task.run_after)

    def test_a_run_after_in_the_past_is_still_reported(self):
        from django.utils import timezone

        past = timezone.now() - timedelta(minutes=5)
        result = self.tasks.double.using(run_after=past).enqueue(2)
        found = self.backend.get_result(result.id)
        self.assertAlmostEqual(
            found.task.run_after.timestamp(), past.timestamp(), delta=0.001
        )

    def test_a_result_due_at_once_has_no_run_after(self):
        result = self.tasks.double.enqueue(2)
        self.assertIsNone(self.backend.get_result(result.id).task.run_after)

    def test_a_result_retried_before_any_claim_has_no_run_after(self):
        from django.utils import timezone

        result = self.tasks.double.using(
            run_after=timezone.now() + timedelta(hours=1)
        ).enqueue(2)
        self.backend.app.cancel(result.id)
        self.assertTrue(self.backend.app.retry(result.id))
        self.assertIsNone(self.backend.get_result(result.id).task.run_after)

    def test_aenqueue_works_from_async_code(self):
        import asyncio

        result = asyncio.run(self.tasks.double.aenqueue(4))
        job = self.backend.app.get(result.id)
        self.assertEqual(job.task, "tests.djangoapp.tasks.double")


class ExecutionTests(DjangoTestCase):
    def test_a_sync_task_runs_and_its_result_reads_back(self):
        result = self.tasks.double.enqueue(21)
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual(result.status, "SUCCESSFUL")
        self.assertEqual(result.return_value, 42)
        self.assertEqual(result.attempts, 1)
        self.assertEqual(result.worker_ids, ["drain"])
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)

    def test_an_async_task_runs(self):
        result = self.tasks.greet.enqueue("ada")
        self.drain()
        greeting = self.tasks.greet.get_result(result.id).return_value
        self.assertEqual(greeting, "hello ada")

    def test_a_task_that_takes_context_sees_its_own_result(self):
        result = self.tasks.introspect.enqueue()
        self.drain()
        self.assertEqual(
            self.backend.get_result(result.id).return_value,
            {"attempt": 1, "id": result.id},
        )

    def test_a_failure_is_reported_with_the_exception_path_and_traceback(self):
        result = self.tasks.refuse.enqueue()
        self.drain()
        result.refresh()
        self.assertEqual(result.status, "FAILED")
        [error] = result.errors
        self.assertEqual(error.exception_class_path, "tests.djangoapp.tasks.Refused")
        self.assertIs(error.exception_class, self.tasks.Refused)
        self.assertIn("raise Refused", error.traceback)

    def test_a_return_value_json_cannot_carry_fails_the_task(self):
        result = self.tasks.unserialisable.enqueue()
        self.drain()
        result.refresh()
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.errors[0].exception_class_path, "builtins.TypeError")

    def test_a_cancelled_job_reads_as_failed_with_a_cancellation_error(self):
        result = self.tasks.double.enqueue(1)
        self.backend.app.cancel(result.id)
        cancelled = self.backend.get_result(result.id)
        self.assertEqual(cancelled.status, "FAILED")
        self.assertEqual(
            cancelled.errors[-1].exception_class_path,
            "tinyjobs.contrib.django.exceptions.JobCancelled",
        )

    def test_a_job_failed_without_a_traceback_reports_a_lost_worker(self):
        from dataclasses import replace

        from tinyjobs.contrib.django.exceptions import WorkerLost
        from tinyjobs.contrib.django.results import errors_of
        from tinyjobs.status import JobStatus

        job = self.backend.app.get(self.tasks.double.enqueue(1).id)
        errors = errors_of(replace(job, status=JobStatus.FAILED), [])
        self.assertIs(errors[-1].exception_class, WorkerLost)

    def test_a_cancelled_async_task_does_not_report_a_finished_failure(self):
        import asyncio

        from django.tasks.signals import task_finished

        from tests import djangooutside
        from tinyjobs.services.execution import current_job

        finished = []

        def receiver(sender, task_result, **kwargs):
            finished.append(task_result.status)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        job = self.backend.app.get(self.tasks.double.enqueue(1).id)
        token = current_job.set(job)
        self.addCleanup(current_job.reset, token)
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(self.backend.arun(djangooutside.halted, (), {}))
        self.assertEqual(finished, [])

    def test_a_rebound_task_keeps_the_queue_it_was_sent_to(self):
        from django.tasks import task_backends

        url = f"sqlite:///{Path(self.directory.name) / 'other.db'}"
        backend = "tinyjobs.contrib.django.TinyJobsBackend"
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": backend,
                    "QUEUES": self.queues,
                    "OPTIONS": {"url": url},
                },
                "other": {"BACKEND": backend, "OPTIONS": {"url": url}},
            }
        ):
            other = task_backends["other"]
            self.addCleanup(other.app.close)
            result = self.tasks.greet.using(backend="other", queue_name="default")
            sent = result.enqueue("ada")
            self.assertEqual(other.app.drain(), 1)
            self.assertEqual(other.get_result(sent.id).return_value, "hello ada")

    def test_a_priority_outside_the_django_range_still_reads_back(self):
        job = self.backend.app.send(
            "tests.djangoapp.tasks.double", [2], {}, priority=500
        )
        self.drain()
        result = self.backend.get_result(job.id)
        self.assertEqual(result.status, "SUCCESSFUL")
        self.assertEqual(result.task.priority, 500)

    def test_a_task_sent_to_another_alias_runs_there(self):
        from django.tasks import task_backends

        url = f"sqlite:///{Path(self.directory.name) / 'other.db'}"
        backend = "tinyjobs.contrib.django.TinyJobsBackend"
        with override_settings(
            TASKS={
                "default": {"BACKEND": backend, "OPTIONS": {"url": url}},
                "other": {"BACKEND": backend, "OPTIONS": {"url": url}},
            }
        ):
            other = task_backends["other"]
            self.addCleanup(other.app.close)
            result = self.tasks.double.using(backend="other").enqueue(3)
            self.assertEqual(other.app.drain(), 1)
            self.assertEqual(other.get_result(result.id).return_value, 6)

    def test_signals_follow_the_task_through_its_life(self):
        from django.tasks.signals import task_enqueued, task_finished, task_started

        seen = []

        def record(signal):
            def receiver(sender, task_result, **kwargs):
                seen.append((signal, task_result.status))

            return receiver

        receivers = {
            "enqueued": record("enqueued"),
            "started": record("started"),
            "finished": record("finished"),
        }
        signals = {
            "enqueued": task_enqueued,
            "started": task_started,
            "finished": task_finished,
        }
        for name, signal in signals.items():
            signal.connect(receivers[name])
            self.addCleanup(signal.disconnect, receivers[name])
        self.tasks.double.enqueue(1)
        self.drain()
        self.assertEqual(
            seen,
            [("enqueued", "READY"), ("started", "RUNNING"), ("finished", "SUCCESSFUL")],
        )

    def test_a_finished_task_is_stamped_by_the_app_clock_not_by_django(self):
        from datetime import timedelta
        from unittest import mock

        from django.tasks.signals import task_finished
        from django.utils import timezone

        finished = []

        def receiver(sender, task_result, **kwargs):
            finished.append(task_result)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        self.tasks.double.enqueue(1)
        later = timezone.now() + timedelta(hours=1)
        with mock.patch("tinyjobs.contrib.django.results.timezone.now", return_value=later):
            self.drain()
        [result] = finished
        self.assertLess(result.finished_at - result.started_at, timedelta(minutes=1))


class ResolutionTests(DjangoTestCase):
    def test_an_unknown_result_id_does_not_exist(self):
        from django.tasks.exceptions import TaskResultDoesNotExist

        with self.assertRaises(TaskResultDoesNotExist):
            self.backend.get_result("01a00000-0000-7000-8000-000000000000")

    def test_a_task_outside_the_installed_apps_is_refused_at_enqueue(self):
        from django.tasks.exceptions import InvalidTask

        from tests import djangooutside

        with self.assertRaisesRegex(InvalidTask, "INSTALLED_APPS"):
            djangooutside.stray.enqueue()
        self.assertEqual(self.backend.app.jobs().items, [])

    def test_a_job_row_naming_a_task_outside_the_installed_apps_never_runs(self):
        self.backend.app.send("tests.djangooutside.stray")
        with self.assertRaises(UnknownTaskError):
            self.drain()
        self.assertNotIn("tests.djangooutside.stray", self.backend.app.registry)

    def test_a_module_that_fails_to_import_does_not_resolve(self):
        with self.assertLogs("tinyjobs.django", "ERROR"):
            self.assertNotIn("tests.djangoapp.broken.task", self.backend.app.registry)

    def test_a_job_name_cannot_forge_a_log_line(self):
        name = "tests.djangoapp.\nCRITICAL forged.x"
        with self.assertLogs("tinyjobs.django", "ERROR") as logs:
            self.assertNotIn(name, self.backend.app.registry)
        self.assertNotIn("\n", logs.records[0].getMessage())

    def test_a_job_sent_by_name_for_a_non_task_does_not_resolve(self):
        self.backend.app.send("tests.djangoapp.tasks.Refused")
        with self.assertRaises(UnknownTaskError):
            self.drain()


class EagerTests(DjangoTestCase):
    options = {"eager": True}

    def test_eager_mode_runs_the_task_inside_enqueue(self):
        result = self.tasks.double.enqueue(5)
        self.assertEqual(result.status, "SUCCESSFUL")
        self.assertEqual(result.return_value, 10)


class EagerDatabaseTests(DjangoTestCase):
    options = {"eager": True, "database": "default"}

    def test_eager_mode_ignores_the_database_and_still_runs_the_task(self):
        result = self.tasks.double.enqueue(6)
        self.assertEqual((result.status, result.return_value), ("SUCCESSFUL", 12))


class CommandTests(DjangoTestCase):
    def call(self, *arguments: str) -> str:
        from django.core.management import call_command

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            call_command("tinyjobs", *arguments)
        return buffer.getvalue()

    def test_check_finds_the_tasks_of_the_installed_apps(self):
        self.assertIn("6 task(s) registered", self.call("check"))

    def test_a_task_exposed_twice_is_discovered_once(self):
        names = self.backend.discover()
        self.assertEqual(len(names), len(set(names)))
        self.assertIn("tests.djangoapp.tasks.double", names)

    def test_check_on_another_alias_counts_the_tasks_it_would_run(self):
        url = f"sqlite:///{Path(self.directory.name) / 'other.db'}"
        backend = "tinyjobs.contrib.django.TinyJobsBackend"
        with override_settings(
            TASKS={
                "default": {"BACKEND": backend, "OPTIONS": {"url": url}},
                "other": {"BACKEND": backend, "OPTIONS": {"url": url}},
            }
        ):
            from django.tasks import task_backends

            self.addCleanup(task_backends["other"].app.close)
            output = self.call("--backend", "other", "check")
            self.assertIn("6 task(s) registered", output)

    def test_stats_counts_jobs_enqueued_through_django(self):
        self.tasks.double.enqueue(1)
        self.assertIn("default", self.call("stats"))

    def test_a_backend_that_is_not_tinyjobs_is_refused(self):
        from django.core.management.base import CommandError

        with override_settings(
            TASKS={"default": {"BACKEND": "django.tasks.backends.dummy.DummyBackend"}}
        ):
            with self.assertRaises(CommandError):
                self.call("stats")


@unittest.skipIf(django is None, "pip install django to run the django tests")
@unittest.skipUnless(
    POSTGRES_URL and psycopg is not None,
    "set TINYJOBS_POSTGRES_URL and install psycopg to run the django postgres tests",
)
class TransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        with psycopg.connect(POSTGRES_URL) as connection:
            version = connection.info.server_version
        if version < 150000:
            self.skipTest("django 6 needs postgres 15 or newer")
        self.prefix = f"tinyjobs_test_{uuid.uuid4().hex[:16]}"
        self.backend = self.configured({"database": "pg"})
        from django.db import connections

        from tests.djangoapp import tasks

        self.tasks = tasks
        self.addCleanup(connections["pg"].close)
        self.addCleanup(self.dropped)

    def configured(self, extra: dict):
        overrides = override_settings(
            TASKS={
                "default": {
                    "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
                    "QUEUES": ["default", "emails"],
                    "OPTIONS": {"url": POSTGRES_URL, "prefix": self.prefix, **extra},
                }
            }
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        from django.tasks import default_task_backend

        self.addCleanup(default_task_backend.app.close)
        return default_task_backend

    def dropped(self) -> None:
        with psycopg.connect(POSTGRES_URL, autocommit=True) as connection:
            for table in ("executions", "jobs", "schedules", "meta"):
                connection.execute(f'DROP TABLE IF EXISTS "{self.prefix}_{table}" CASCADE')

    def stored(self, result):
        return self.backend.app.get(result.id)

    def test_a_task_enqueued_inside_atomic_appears_when_it_commits(self):
        from django.db import transaction

        with transaction.atomic(using="pg"):
            result = self.tasks.double.enqueue(2)
            self.assertIsNone(self.stored(result))
        self.assertEqual(self.stored(result).task, "tests.djangoapp.tasks.double")
        self.assertEqual(self.backend.app.drain(), 1)
        self.assertEqual(self.backend.get_result(result.id).return_value, 4)

    def test_a_task_enqueued_inside_a_rolled_back_atomic_is_never_stored(self):
        from django.db import transaction

        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic(using="pg"):
                result = self.tasks.double.enqueue(2)
                1 / 0
        self.assertIsNone(self.stored(result))

    def test_a_rolled_back_savepoint_takes_only_its_own_tasks(self):
        from django.db import transaction

        with transaction.atomic(using="pg"):
            kept = self.tasks.double.enqueue(1)
            with contextlib.suppress(ZeroDivisionError):
                with transaction.atomic(using="pg"):
                    dropped = self.tasks.double.enqueue(2)
                    1 / 0
        self.assertIsNotNone(self.stored(kept))
        self.assertIsNone(self.stored(dropped))

    def test_a_task_enqueued_outside_atomic_is_stored_at_once(self):
        result = self.tasks.greet.enqueue("ada")
        self.assertEqual(self.stored(result).queue, "emails")

    def test_aenqueue_joins_the_transaction_of_the_calling_thread(self):
        from asgiref.sync import async_to_sync
        from django.db import transaction

        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic(using="pg"):
                result = async_to_sync(self.tasks.double.aenqueue)(5)
                self.assertIsNone(self.stored(result))
                1 / 0
        self.assertIsNone(self.stored(result))

    def dropped_by_the_server(self):
        from django.db import connections

        self.tasks.double.enqueue(1)
        wrapper = connections["pg"]
        wrapper.close_at = None
        with psycopg.connect(POSTGRES_URL, autocommit=True) as admin:
            admin.execute(
                "SELECT pg_terminate_backend(%s)", [wrapper.connection.info.backend_pid]
            )
        return wrapper

    def test_a_connection_the_server_dropped_is_replaced_after_the_request(self):
        from django.db import OperationalError, close_old_connections

        wrapper = self.dropped_by_the_server()
        with self.assertRaises(OperationalError) as caught:
            self.tasks.double.enqueue(2)
        self.assertIsInstance(caught.exception.__cause__, psycopg.OperationalError)
        self.assertTrue(wrapper.errors_occurred)
        close_old_connections()
        result = self.tasks.double.enqueue(3)
        self.assertIsNotNone(self.stored(result))

    def test_a_health_check_replaces_a_dropped_connection_before_enqueueing(self):
        wrapper = self.dropped_by_the_server()
        wrapper.health_check_enabled = True
        wrapper.health_check_done = False
        result = self.tasks.double.enqueue(2)
        self.assertIsNotNone(self.stored(result))

    def test_a_connection_from_another_thread_is_refused_as_django_refuses_it(self):
        from django.db import DatabaseError, connections

        self.tasks.double.enqueue(1)
        wrapper = connections["pg"]
        raised = []

        def enqueue_elsewhere() -> None:
            connections["pg"] = wrapper
            try:
                self.tasks.double.enqueue(2)
            except DatabaseError as error:
                raised.append(error)

        other = threading.Thread(target=enqueue_elsewhere)
        other.start()
        other.join(10)
        self.assertEqual(len(raised), 1)
        self.assertIn("same thread", str(raised[0]))
        self.assertEqual(len(self.backend.app.jobs().items), 1)

    def test_a_database_that_is_not_postgres_is_refused(self):
        backend = self.configured({"database": "default"})
        with self.assertRaisesRegex(ConfigurationError, "not PostgreSQL"):
            self.tasks.double.using(backend=backend.alias).enqueue(1)


class ExceptionPathTests(unittest.TestCase):
    def setUp(self) -> None:
        if django is None:
            self.skipTest("pip install django to run the django tests")
        from tinyjobs.contrib.django.results import exception_path

        self.exception_path = exception_path

    def test_a_builtin_exception_gets_the_builtins_module(self):
        self.assertEqual(
            self.exception_path("ValueError", "Traceback...\nValueError: bad\n"),
            "builtins.ValueError",
        )

    def test_a_module_exception_keeps_its_module(self):
        self.assertEqual(
            self.exception_path("Refused", "Traceback...\nshop.tasks.Refused: no\n"),
            "shop.tasks.Refused",
        )

    def test_notes_after_the_exception_line_are_skipped(self):
        self.assertEqual(
            self.exception_path(
                "Refused", "Traceback...\nshop.tasks.Refused: no\nsee the docs\n"
            ),
            "shop.tasks.Refused",
        )

    def test_an_exception_group_subclass_loses_the_traceback_border(self):
        self.assertEqual(
            self.exception_path(
                "MyGroup",
                "  + Exception Group Traceback...\n  | shop.tasks.MyGroup: two (2)\n",
            ),
            "shop.tasks.MyGroup",
        )


if __name__ == "__main__":
    unittest.main()
