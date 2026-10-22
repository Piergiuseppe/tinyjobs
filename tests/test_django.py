from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from tinyjobs.exceptions import UnknownTaskError

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
    settings.configure(
        INSTALLED_APPS=["tinyjobs.contrib.django", "tests.djangoapp"],
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
        },
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
