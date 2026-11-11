from __future__ import annotations

import contextlib
import io
import os
import sqlite3
import tempfile
import time
import uuid
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


LITE_DIRECTORY = tempfile.TemporaryDirectory()
LITE_PATH = Path(LITE_DIRECTORY.name) / "lite.db"

def setUpModule() -> None:
    if django is None:
        return
    from django.conf import settings

    if settings.configured:
        return
    databases = {
        "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        "lite": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": str(LITE_PATH),
            "OPTIONS": {"transaction_mode": "IMMEDIATE"},
        },
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


def tearDownModule() -> None:
    LITE_DIRECTORY.cleanup()


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
    def test_a_held_job_reads_as_ready(self):
        from django.tasks import TaskResultStatus

        from tinyjobs.contrib.django.results import STATUSES
        from tinyjobs.status import JobStatus

        self.assertIs(STATUSES[JobStatus.HELD], TaskResultStatus.READY)
        self.assertEqual(set(STATUSES), set(JobStatus))

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


class OptionTests(DjangoTestCase):
    def tinyjobs_task(self, task):
        return self.backend.app.registry.get(task.module_path)

    def heard(self) -> dict[str, list[str]]:
        from django.tasks.signals import task_finished, task_started

        heard: dict[str, list[str]] = {"started": [], "finished": []}

        def started(sender, task_result, **_):
            heard["started"].append(task_result.status)

        def finished(sender, task_result, **_):
            heard["finished"].append(task_result.status)

        task_started.connect(started)
        task_finished.connect(finished)
        self.addCleanup(task_started.disconnect, started)
        self.addCleanup(task_finished.disconnect, finished)
        return heard

    def test_the_decorator_takes_the_tinyjobs_options(self):
        from tinyjobs.contrib.django import Task
        from tinyjobs.limits import Limit

        capped = self.tasks.capped
        self.assertIsInstance(capped, Task)
        self.assertEqual((capped.concurrency, capped.timeout, capped.result_ttl), (2, 5, 60))
        adapted = self.tinyjobs_task(capped)
        self.assertEqual(adapted.limit, Limit(concurrency=2, interval=None))
        self.assertEqual((adapted.timeout, adapted.result_ttl), (5, 60))
        self.assertTrue(adapted.store_result)
        self.assertEqual(self.tinyjobs_task(self.tasks.hourly).limit.interval, 3600.0)
        flaky = self.tinyjobs_task(self.tasks.flaky)
        self.assertEqual(flaky.retry_policy.max_attempts, 3)
        self.assertEqual(flaky.options.max_attempts, 3)
        self.assertIsNone(self.tinyjobs_task(self.tasks.plain).limit)

    def test_djangos_own_decorator_passes_the_options_from_6_1(self):
        from django.tasks import task

        if django.VERSION < (6, 1):
            with self.assertRaises(TypeError):
                task(timeout=5)(self.tasks.double.func)
            return
        declared = task(timeout=5, concurrency=2)(self.tasks.double.func)
        self.assertEqual((declared.timeout, declared.concurrency), (5, 2))

    def test_the_decorator_of_the_backend_is_djangos_with_the_options(self):
        from tinyjobs.contrib.django import task

        bare = task(self.tasks.double.func)
        self.assertEqual((bare.priority, bare.queue_name, bare.backend), (0, "default", "default"))
        called = task(priority=7, queue_name="emails", takes_context=True, rate="1/s")(
            self.tasks.introspect.func
        )
        self.assertEqual(
            (called.priority, called.queue_name, called.takes_context, called.rate),
            (7, "emails", True, "1/s"),
        )

    def test_after_sends_a_task_to_run_once_another_has_succeeded(self):
        first = self.tasks.double.enqueue(4)
        child = self.tasks.double.after(first).using(priority=5).enqueue(8)
        by_id = self.tasks.greet.using(queue_name="default").after(first.id).enqueue("ada")
        job = self.backend.app.get(child.id)
        self.assertEqual((job.after, job.status.value, job.priority), (first.id, "waiting", 5))
        self.assertEqual((child.status, child.task.follows), ("READY", first.id))
        self.assertIsNone(self.tasks.double.follows)
        self.assertEqual(self.drain(), 3)
        self.assertEqual(
            [self.backend.get_result(result.id).return_value for result in (child, by_id)],
            [16, "hello ada"],
        )

    def test_a_task_after_one_that_failed_reads_as_failed_and_never_runs(self):
        failed = self.tasks.refuse.enqueue()
        child = self.tasks.double.after(failed).enqueue(1)
        self.assertEqual(self.drain(), 1)
        cancelled = self.backend.get_result(child.id)
        self.assertEqual(cancelled.status, "FAILED")
        self.assertEqual(
            cancelled.errors[-1].exception_class_path,
            "tinyjobs.contrib.django.exceptions.JobCancelled",
        )
        late = self.tasks.double.after(failed).enqueue(2)
        self.assertEqual(late.status, "FAILED")

    def test_after_is_not_an_option_of_the_settings_or_the_decorator(self):
        from tinyjobs.contrib.django import task
        from tinyjobs.contrib.django.options import OPTIONS

        self.assertNotIn("follows", OPTIONS)
        self.assertIn("timeout", OPTIONS)
        with self.assertRaisesRegex(TypeError, r"use \.after\(result\)"):
            task(follows="some-job")(self.tasks.double.func)

    def test_a_task_sent_after_another_still_copies_and_pickles(self):
        import copy
        import pickle

        first = self.tasks.double.enqueue(1)
        bound = self.tasks.double.after(first)
        self.assertEqual(copy.deepcopy(bound).follows, first.id)
        self.assertEqual(copy.copy(self.tasks.double), self.tasks.double)
        self.assertEqual(bound.using(priority=3).follows, first.id)
        self.assertEqual(self.tasks.double.using(priority=3).after(first).follows, first.id)
        if django.VERSION >= (6, 1):
            self.assertEqual(pickle.loads(pickle.dumps(bound)), bound)
            result = bound.enqueue(2)
            self.assertEqual(pickle.loads(pickle.dumps(result)).task.follows, first.id)

    def test_the_async_calls_send_a_task_after_another(self):
        import asyncio

        async def send():
            first = await self.tasks.double.aenqueue(1)
            child = await self.tasks.double.after(first).aenqueue(2)
            read = await self.backend.aget_result(child.id)
            await child.arefresh()
            return first, child, read

        first, child, read = asyncio.run(send())
        self.assertEqual((child.task.follows, read.task.follows), (first.id, first.id))
        self.assertEqual(self.backend.app.get(child.id).status.value, "waiting")

    def test_a_result_read_back_names_the_task_it_ran_after_and_why_it_was_cancelled(self):
        failed = self.tasks.refuse.enqueue()
        child = self.tasks.double.after(failed).enqueue(1)
        self.assertEqual(self.backend.get_result(child.id).task.follows, failed.id)
        self.assertIsNone(self.backend.get_result(failed.id).task.follows)
        self.drain()
        cancelled = self.backend.get_result(child.id)
        self.assertIn("was to run after did not succeed", cancelled.errors[-1].traceback)
        self.backend.app.retry(failed.id)
        self.backend.app.retry(child.id)
        for job_id in (child.id, self.tasks.double.enqueue(1).id):
            self.backend.app.cancel(job_id)
            self.assertIn(
                "the job was cancelled", self.backend.get_result(job_id).errors[-1].traceback
            )

    def test_a_task_cancelled_with_the_one_before_it_finishes_without_the_signal(self):
        from django.tasks.signals import task_finished

        finished = []

        def note(sender, task_result, **_):
            finished.append(task_result.id)

        task_finished.connect(note)
        self.addCleanup(task_finished.disconnect, note)
        failed = self.tasks.refuse.enqueue()
        child = self.tasks.double.after(failed).enqueue(1)
        self.drain()
        last = self.tasks.double.enqueue(2)
        self.drain()
        deadline = time.monotonic() + 5
        while len(finished) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(sorted(finished), sorted([failed.id, last.id]))
        self.assertEqual(self.backend.get_result(child.id).status, "FAILED")

    def test_a_task_released_by_the_one_before_it_reports_no_run_after(self):
        from datetime import timedelta

        from django.utils import timezone

        first = self.tasks.double.enqueue(1)
        child = self.tasks.double.using(queue_name="emails").after(first).enqueue(2)
        later = timezone.now() + timedelta(hours=1)
        delayed = self.tasks.double.using(run_after=later).after(first).enqueue(3)
        self.assertIsNone(child.task.run_after)
        self.assertEqual(self.backend.get_result(delayed.id).task.run_after, later)
        self.assertEqual(self.backend.app.drain(["default"]), 1)
        self.assertIsNone(self.backend.get_result(child.id).task.run_after)
        self.assertEqual(self.backend.app.get(child.id).status.value, "queued")

    def test_using_keeps_the_options(self):
        moved = self.tasks.capped.using(queue_name="emails", priority=5)
        self.assertEqual((moved.concurrency, moved.timeout, moved.queue_name), (2, 5, "emails"))

    def test_an_option_that_cannot_be_is_an_invalid_task_where_it_is_declared(self):
        from tinyjobs.contrib.django import task
        from django.tasks.exceptions import InvalidTask

        refused = {
            "rate": "ten a minute",
            "concurrency": 0,
            "timeout": -1,
            "max_attempts": 0,
            "retry_backoff": "quadratic",
            "retry_on": 5,
            "retry_base": -1,
            "retry_jitter": "yes",
            "result_ttl": 0,
        }
        for name, value in refused.items():
            with self.subTest(name), self.assertRaisesRegex(InvalidTask, name):
                task(**{name: value})(self.tasks.double.func)
        with self.assertRaisesRegex(InvalidTask, "async def"):
            task(isolated=True)(self.tasks.greet.func)
        with self.assertRaises(TypeError):
            task(cron="* * * * *")(self.tasks.double.func)

    def test_a_failed_attempt_that_will_run_again_reads_as_ready_and_finishes_nothing(self):
        heard = self.heard()
        result = self.tasks.later.enqueue()
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual(result.status, "READY")
        self.assertEqual(result.attempts, 1)
        self.assertIsNone(result.finished_at)
        [error] = result.errors
        self.assertIn("attempt 1", error.traceback)
        self.assertEqual(heard, {"started": ["RUNNING"], "finished": []})

    def test_a_task_that_succeeds_on_a_later_attempt_finishes_once(self):
        heard = self.heard()
        self.tasks.CALLS["flaky"] = 0
        result = self.tasks.flaky.enqueue(3)
        self.assertEqual(self.drain(), 3)
        result.refresh()
        self.assertEqual(result.status, "SUCCESSFUL")
        self.assertEqual((result.return_value, result.attempts, len(result.errors)), (3, 3, 2))
        self.assertEqual(heard, {"started": ["RUNNING"] * 3, "finished": ["SUCCESSFUL"]})

    def test_the_attempt_that_uses_the_last_one_fails_the_task_and_finishes_it(self):
        heard = self.heard()
        self.tasks.CALLS["flaky"] = 0
        result = self.tasks.flaky.enqueue(9)
        self.assertEqual(self.drain(), 3)
        result.refresh()
        self.assertEqual((result.status, result.attempts, len(result.errors)), ("FAILED", 3, 3))
        self.assertEqual(heard["finished"], ["FAILED"])

    def test_an_error_outside_retry_on_fails_at_once(self):
        heard = self.heard()
        result = self.tasks.picky.enqueue()
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual((result.status, result.attempts), ("FAILED", 1))
        self.assertEqual(heard["finished"], ["FAILED"])

    def test_a_task_past_its_timeout_finishes_failed_once_whatever_it_does_later(self):
        heard = self.heard()
        result = self.tasks.slow.enqueue(0.6)
        self.assertEqual(self.drain(), 1)
        self.assertEqual(heard, {"started": ["RUNNING"], "finished": ["FAILED"]})
        time.sleep(0.7)
        result.refresh()
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.errors[-1].exception_class_path, "builtins.TimeoutError")
        self.assertEqual(heard, {"started": ["RUNNING"], "finished": ["FAILED"]})

    def test_a_task_timing_out_on_every_attempt_finishes_once(self):
        heard = self.heard()
        result = self.tasks.slow_twice.enqueue(0.6)
        self.assertEqual(self.drain(), 2)
        time.sleep(0.7)
        result.refresh()
        self.assertEqual((result.status, result.attempts), ("FAILED", 2))
        self.assertEqual(heard, {"started": ["RUNNING"] * 2, "finished": ["FAILED"]})

    def test_an_async_task_past_its_timeout_finishes_failed(self):
        heard = self.heard()
        result = self.tasks.slow_async.enqueue(5)
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(heard, {"started": ["RUNNING"], "finished": ["FAILED"]})

    def test_a_task_within_its_timeout_finishes_as_usual(self):
        heard = self.heard()
        self.tasks.capped.enqueue()
        self.assertEqual(self.drain(), 1)
        self.assertEqual(heard["finished"], ["SUCCESSFUL"])

    def test_task_finished_carries_what_the_store_recorded(self):
        from django.db import connection
        from django.tasks.signals import task_finished

        seen = []

        def receiver(sender, task_result, **_):
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            seen.append((str(task_result.status), task_result._return_value, task_result.attempts,
                         task_result.finished_at is not None, len(task_result.errors)))

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        self.tasks.CALLS["flaky"] = 0
        self.tasks.flaky.enqueue(2)
        self.tasks.slow.enqueue(0.6)
        self.tasks.slow_async.enqueue(5)
        self.assertEqual(self.drain(), 4)
        self.assertEqual(
            sorted(seen, key=str),
            [("FAILED", None, 1, True, 1), ("FAILED", None, 1, True, 1),
             ("SUCCESSFUL", 2, 2, True, 1)],
        )

    def test_a_result_the_store_refuses_finishes_failed_not_successful(self):
        heard = self.heard()
        result = self.tasks.double.enqueue("x" * 64)
        self.backend.app._lifecycle._max_result_bytes = 16
        self.addCleanup(setattr, self.backend.app._lifecycle, "_max_result_bytes", None)
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(heard["finished"], ["FAILED"])

    def test_task_finished_survives_a_store_that_cannot_be_read_back(self):
        from unittest import mock

        heard = self.heard()
        self.tasks.double.enqueue(4)
        app = self.backend.app

        async def unreadable(job_id):
            raise OSError("the store went away")

        with mock.patch.object(app, "aget", unreadable), self.assertLogs("tinyjobs.django"):
            self.assertEqual(self.drain(), 1)
        self.assertEqual(heard["finished"], ["SUCCESSFUL"])

    def test_a_failure_still_carries_its_error_when_the_store_cannot_be_read_back(self):
        from unittest import mock

        from django.tasks.signals import task_finished

        seen = []

        def receiver(sender, task_result, **_):
            seen.append((str(task_result.status), task_result.errors[0].exception_class_path,
                         task_result.finished_at is not None))

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        self.tasks.refuse.enqueue()
        app = self.backend.app

        async def unreadable(job_id):
            raise OSError("the store went away")

        with mock.patch.object(app, "aget", unreadable), self.assertLogs("tinyjobs.django"):
            self.assertEqual(self.drain(), 1)
        self.assertEqual(seen, [("FAILED", "tests.djangoapp.tasks.Refused", True)])

    def test_receivers_of_tasks_finishing_together_do_not_wait_for_each_other(self):
        from django.tasks.signals import task_finished

        threads = set()

        def receiver(sender, task_result, **_):
            threads.add(threading.get_ident())
            time.sleep(0.4)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        for _ in range(4):
            self.tasks.plain.enqueue()
        app = self.backend.app
        worker = app.worker(
            concurrency=4, scheduler=False, handle_signals=False, poll_maximum=0.05
        )

        def stop_when_done() -> None:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and app.counts() != {"success": 4}:
                time.sleep(0.02)
            worker.stop()

        stopper = threading.Thread(target=stop_when_done, daemon=True)
        began = time.monotonic()
        stopper.start()
        worker.run()
        stopper.join(5)
        self.assertEqual(app.counts(), {"success": 4})
        self.assertGreater(len(threads), 1)
        self.assertLess(time.monotonic() - began, 1.4)

    def test_a_failure_reaches_the_receivers_with_its_error_in_flight(self):
        import sys

        from django.tasks.signals import task_finished

        seen = []

        def receiver(sender, task_result, **_):
            seen.append((threading.current_thread().name, sys.exc_info()[0]))

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        self.tasks.refuse.enqueue()
        self.tasks.plain.enqueue()
        with self.assertLogs("django.tasks", "ERROR") as logs:
            self.assertEqual(self.drain(), 2)
        self.assertEqual(
            set(seen),
            {("tinyjobs-django-signal", None), ("tinyjobs-django-signal", self.tasks.Refused)},
        )
        self.assertIsNotNone(logs.records[0].exc_info)

    def test_a_receiver_that_exits_does_not_take_the_worker_with_it(self):
        from django.tasks.signals import task_finished

        def receiver(sender, task_result, **_):
            raise SystemExit(3)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        result = self.tasks.plain.enqueue()
        with self.assertLogs("tinyjobs.hooks", "ERROR"):
            self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual(result.status, "SUCCESSFUL")

    def test_the_signal_thread_closes_the_connections_its_receivers_opened(self):
        from django.db import connections
        from django.tasks.signals import task_finished

        opened = []

        def receiver(sender, task_result, **_):
            connection = connections["lite"]
            connection.settings_dict["CONN_MAX_AGE"] = None
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            opened.append(connection)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        self.addCleanup(connections["lite"].settings_dict.__setitem__, "CONN_MAX_AGE", 0)
        self.tasks.plain.enqueue()
        self.assertEqual(self.drain(), 1)
        [connection] = opened
        self.assertIsNone(connection.connection)

    def test_run_after_on_the_decorator_is_refused_as_django_refuses_it(self):
        from datetime import datetime

        from tinyjobs.contrib.django import task

        with self.assertRaisesRegex(TypeError, "using"):
            task(run_after=datetime.now())(self.tasks.double.func)

    def test_a_dotted_retry_on_is_checked_for_its_shape_where_it_is_declared(self):
        from tinyjobs.contrib.django import task
        from django.tasks.exceptions import InvalidTask

        for refused in ("KeyError", ["builtins.KeyError", 5], ["tests.djangoapp.tasks.", KeyError]):
            with self.subTest(refused), self.assertRaisesRegex(InvalidTask, "retry_on"):
                task(retry_on=refused)(self.tasks.double.func)
        accepted = task(retry_on=["builtins.KeyError", ValueError], max_attempts=2)(
            self.tasks.double.func
        )
        self.assertEqual(accepted.retry_on, ["builtins.KeyError", ValueError])

    def test_a_rate_holds_the_second_job_and_it_reads_as_ready(self):
        first = self.tasks.hourly.enqueue()
        second = self.tasks.hourly.enqueue()
        self.assertEqual(self.drain(), 1)
        self.assertEqual(self.backend.app.counts(), {"success": 1, "held": 1})
        first.refresh()
        second.refresh()
        self.assertEqual((first.status, second.status), ("SUCCESSFUL", "READY"))

    def test_a_limited_task_first_seen_with_its_job_is_decided_again_under_the_limit(self):
        from tests.djangoapp import jobs

        jobs.spaced.enqueue()
        jobs.spaced.enqueue()
        registry = self.backend.app.registry
        del registry._tasks[jobs.spaced.module_path]
        self.assertNotIn(jobs.spaced.module_path, registry.limits())
        self.assertEqual(self.drain(), 1)
        self.assertEqual(self.backend.app.counts(), {"success": 1, "held": 1})

    def test_a_worker_decides_a_task_first_seen_with_its_job_again_under_the_limit(self):
        from tests.djangoapp import jobs

        jobs.spaced.enqueue()
        jobs.spaced.enqueue()
        app = self.backend.app
        del app.registry._tasks[jobs.spaced.module_path]
        worker = app.worker(
            concurrency=4, scheduler=False, handle_signals=False, poll_maximum=0.05
        )

        def stop_when_settled() -> None:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and app.counts() != {"success": 1, "held": 1}:
                time.sleep(0.02)
            time.sleep(0.3)
            worker.stop()

        stopper = threading.Thread(target=stop_when_settled, daemon=True)
        stopper.start()
        with self.assertLogs("tinyjobs.worker", "INFO") as logs:
            worker.run()
        stopper.join(5)
        self.assertEqual(app.counts(), {"success": 1, "held": 1})
        self.assertTrue(any("the claim did not know" in line for line in logs.output))


class SettingsOptionTests(DjangoTestCase):
    options = {
        "tasks": {
            "tests.djangoapp.tasks.plain": {"rate": "1/h", "max_attempts": 4},
            "tests.djangoapp.tasks.capped": {"concurrency": 7},
            "tests.djangoapp.tasks.refuse": {
                "max_attempts": 2,
                "retry_on": ["tests.djangoapp.tasks.Refused"],
                "retry_base": 3600,
            },
        }
    }

    def test_the_settings_give_options_to_a_task_and_win_over_its_decorator(self):
        registry = self.backend.app.registry
        plain = registry.get("tests.djangoapp.tasks.plain")
        self.assertEqual(plain.limit.interval, 3600.0)
        self.assertEqual(plain.options.max_attempts, 4)
        capped = registry.get("tests.djangoapp.tasks.capped")
        self.assertEqual((capped.limit.concurrency, capped.timeout), (7, 5))
        self.assertEqual(self.backend.discover().count("tests.djangoapp.tasks.plain"), 1)

    def test_a_limit_from_the_settings_holds_jobs(self):
        self.tasks.plain.enqueue()
        self.tasks.plain.enqueue()
        self.assertEqual(self.drain(), 1)
        self.assertEqual(self.backend.app.counts(), {"success": 1, "held": 1})

    def test_retry_on_takes_dotted_paths(self):
        result = self.tasks.refuse.enqueue()
        self.assertEqual(self.drain(), 1)
        result.refresh()
        self.assertEqual((result.status, result.attempts), ("READY", 1))


class SettingsOptionRefusalTests(DjangoTestCase):
    def refusing(self, tasks):
        from tinyjobs.contrib.django import TinyJobsBackend

        return TinyJobsBackend(
            "default", {"OPTIONS": {"url": f"memory://{uuid.uuid4().hex}", "tasks": tasks}}
        )

    def test_a_tasks_option_of_the_wrong_shape_is_refused(self):
        for tasks in (["tests.djangoapp.tasks.plain"], {"tests.djangoapp.tasks.plain": 3}):
            with self.subTest(tasks), self.assertRaisesRegex(ConfigurationError, "import path"):
                self.refusing(tasks)

    def test_an_option_that_does_not_exist_is_refused_by_name(self):
        with self.assertRaisesRegex(ConfigurationError, "'retries'.*max_attempts"):
            self.refusing({"tests.djangoapp.tasks.plain": {"retries": 3}})

    def test_a_retry_on_path_is_imported_when_the_task_is_resolved_not_before(self):
        from django.tasks.exceptions import InvalidTask

        from tests.djangoapp import tasks

        for path in ("nowhere.Missing", "tests.djangoapp.tasks.Absent"):
            backend = self.refusing({"tests.djangoapp.tasks.plain": {"retry_on": [path]}})
            self.addCleanup(backend.app.close)
            backend.validate_task(tasks.plain)
            with self.subTest(path):
                with self.assertRaisesRegex(ConfigurationError, "not an exception class"):
                    backend.discover()
                with self.assertRaisesRegex(InvalidTask, "not an exception class"):
                    backend.enqueue(tasks.plain, (), {})

    def test_a_job_of_a_task_that_cannot_be_resolved_goes_back_to_the_queue(self):
        backend = self.refusing(
            {"tests.djangoapp.tasks.plain": {"retry_on": ["nowhere.Missing"]}}
        )
        self.addCleanup(backend.app.close)
        from unittest import mock

        app = backend.app.start()
        with mock.patch.object(
            app.dispatch, "_with_task_defaults", lambda name, options: options
        ):
            app.send("tests.djangoapp.tasks.plain")
        with self.assertRaisesRegex(ConfigurationError, "not an exception class"):
            app.drain()
        self.assertEqual(backend.app.counts(), {"queued": 1})
        worker = app.worker(scheduler=False, handle_signals=False, poll_maximum=0.05)
        stopper = threading.Timer(0.6, worker.stop)
        stopper.start()
        with self.assertLogs("tinyjobs.worker", "ERROR") as logs:
            worker.run()
        stopper.join(5)
        self.assertTrue(any("cannot be used" in line for line in logs.output), logs.output)
        self.assertEqual(app.counts(), {"queued": 1})

    def test_a_path_that_names_no_task_stops_the_command(self):
        backend = self.refusing({"tests.djangoapp.tasks.mispelled": {"concurrency": 1}})
        self.addCleanup(backend.app.close)
        with self.assertRaisesRegex(ConfigurationError, "mispelled.*INSTALLED_APPS"):
            backend.discover()

    def test_a_value_that_cannot_be_is_refused_when_the_task_is_resolved(self):
        backend = self.refusing({"tests.djangoapp.tasks.plain": {"rate": "often"}})
        self.addCleanup(backend.app.close)
        with self.assertRaisesRegex(ConfigurationError, "rate"):
            backend.discover()


@unittest.skipIf(django is None, "pip install django to run the django tests")
class IsolatedProjectTests(unittest.TestCase):
    def setUp(self) -> None:
        import tinyjobs

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = root = Path(directory.name)
        self.url = f"sqlite:///{root / 'jobs.db'}"
        self.log = root / "signals.log"
        (root / "shop").mkdir()
        (root / "shop" / "__init__.py").write_text("")
        (root / "shop" / "tasks.py").write_text(
            "import os\n"
            "from tinyjobs.contrib.django import task\n"
            "from django.tasks.signals import task_finished, task_started\n"
            "\n"
            "def note(kind):\n"
            "    def receiver(sender, task_result, **_):\n"
            "        with open(os.environ['SHOP_LOG'], 'a') as log:\n"
            "            log.write(f'{kind} {os.getpid()} {task_result.status}\\n')\n"
            "    return receiver\n"
            "\n"
            "started, finished = note('started'), note('finished')\n"
            "task_started.connect(started)\n"
            "task_finished.connect(finished)\n"
            "\n"
            "@task(isolated=True, takes_context=True, timeout=30)\n"
            "def where(context):\n"
            "    return {'pid': os.getpid(), 'attempt': context.attempt}\n"
            "\n"
            "@task(isolated=True, takes_context=True, max_attempts=2, retry_base=0)\n"
            "def twice(context):\n"
            "    if context.attempt == 1:\n"
            "        raise ValueError('first')\n"
            "    return context.attempt\n"
        )
        (root / "shop_settings.py").write_text(
            "SECRET_KEY = 'tests'\n"
            "INSTALLED_APPS = ['tinyjobs.contrib.django', 'shop']\n"
            "DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}}\n"
            "TASKS = {'default': {'BACKEND': 'tinyjobs.contrib.django.TinyJobsBackend',\n"
            f"    'OPTIONS': {{'url': {self.url!r}}}}}}}\n"
        )
        (root / "manage.py").write_text(
            "import sys\n"
            "from django.core.management import execute_from_command_line\n"
            "if __name__ == '__main__':\n"
            "    execute_from_command_line(sys.argv)\n"
        )
        self.env = dict(
            os.environ,
            DJANGO_SETTINGS_MODULE="shop_settings",
            SHOP_LOG=str(self.log),
            PYTHONDONTWRITEBYTECODE="1",
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )

    def enqueued(self, name: str) -> str:
        import subprocess
        import sys

        sent = subprocess.run(
            [sys.executable, "-c",
             "import django; django.setup()\n"
             f"from shop.tasks import {name}\n"
             f"print({name}.enqueue().id)"],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(sent.returncode, 0, sent.stderr)
        return sent.stdout.strip()

    def worked(self, *ids: str) -> tuple[int, list]:
        import signal
        import subprocess
        import sys

        from tinyjobs import TinyJobs
        from tinyjobs.status import JobStatus

        reader = TinyJobs(self.url)
        self.addCleanup(reader.close)
        worker = subprocess.Popen(
            [sys.executable, "manage.py", "tinyjobs", "worker", "--no-scheduler",
             "--poll-max", "0.1", "--processes", "1"],
            cwd=self.root, env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                jobs = [reader.get(job_id) for job_id in ids]
                if all(job.status in (JobStatus.SUCCESS, JobStatus.FAILED) for job in jobs):
                    break
                self.assertIsNone(worker.poll(), "the worker stopped early")
                time.sleep(0.1)
            worker.send_signal(signal.SIGTERM)
            _, errors = worker.communicate(timeout=30)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=10)
        self.assertEqual(worker.returncode, 0, errors)
        self.logged = errors
        return worker.pid, [reader.get(job_id) for job_id in ids]

    def result_of(self, job) -> object:
        import json

        return json.loads(job.result_payload)

    def test_an_isolated_task_runs_in_a_job_process_with_its_context_and_its_signals(self):
        from tinyjobs.status import JobStatus

        worker_pid, [job] = self.worked(self.enqueued("where"))
        self.assertEqual(job.status, JobStatus.SUCCESS, self.logged)
        ran = self.result_of(job)
        self.assertEqual(ran["attempt"], 1)
        self.assertNotEqual(ran["pid"], worker_pid)
        self.assertEqual(
            self.log.read_text().splitlines(),
            [f"started {ran['pid']} RUNNING", f"finished {worker_pid} SUCCESSFUL"],
        )

    def test_an_isolated_task_is_retried_and_finishes_once(self):
        from tinyjobs.status import JobStatus

        _, [job] = self.worked(self.enqueued("twice"))
        self.assertEqual((job.status, job.attempts), (JobStatus.SUCCESS, 2), self.logged)
        self.assertEqual(self.result_of(job), 2)
        kinds = [line.split()[0] + " " + line.split()[2] for line in self.log.read_text().splitlines()]
        self.assertEqual(kinds, ["started RUNNING", "started RUNNING", "finished SUCCESSFUL"])


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

    def test_a_long_chain_of_enqueues_from_task_finished_receivers_ends(self):
        from django.tasks.signals import task_finished

        depths = []

        def receiver(sender, task_result, **_):
            depth = task_result.return_value
            depths.append(depth)
            if depth < 40:
                self.tasks.double.enqueue(depth / 2 + 1)

        task_finished.connect(receiver)
        self.addCleanup(task_finished.disconnect, receiver)
        ended = threading.Event()

        def chain() -> None:
            self.tasks.double.enqueue(0.5)
            ended.set()

        threading.Thread(target=chain, daemon=True).start()
        self.assertTrue(ended.wait(20), "the chain never ended")
        self.assertEqual(len(depths), 21)

    def test_an_eager_enqueue_from_a_thread_of_an_event_loop_returns(self):
        import asyncio

        from asgiref.sync import sync_to_async

        async def main():
            return await asyncio.wait_for(sync_to_async(self.tasks.double.enqueue)(2), 10)

        self.assertEqual(asyncio.run(main()).return_value, 4)

    def test_eager_mode_runs_the_task_inside_enqueue(self):
        result = self.tasks.double.enqueue(5)
        self.assertEqual(result.status, "SUCCESSFUL")
        self.assertEqual(result.return_value, 10)


class EagerDatabaseTests(DjangoTestCase):
    options = {"eager": True, "database": "default"}

    def test_eager_mode_ignores_the_database_and_still_runs_the_task(self):
        result = self.tasks.double.enqueue(6)
        self.assertEqual((result.status, result.return_value), ("SUCCESSFUL", 12))

    def test_an_eager_async_task_sent_from_a_plain_event_loop_runs(self):
        import asyncio

        async def send():
            return await asyncio.wait_for(self.tasks.greet.aenqueue("ada"), 8)

        result = asyncio.run(send())
        self.assertEqual(result.status, "SUCCESSFUL")


class CommandTests(DjangoTestCase):
    def call(self, *arguments: str) -> str:
        from django.core.management import call_command

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            call_command("tinyjobs", *arguments)
        return buffer.getvalue()

    def test_only_the_command_line_owns_the_process(self):
        from unittest import mock

        from django.core.management import ManagementUtility

        command_line = ["manage.py", "tinyjobs", "check"]
        with mock.patch(
            "tinyjobs.contrib.django.management.commands.tinyjobs.main", return_value=0
        ) as entry, mock.patch("sys.argv", list(command_line)):
            self.call("check")
            ManagementUtility(list(command_line)).execute()
            ManagementUtility(["host.py", "tinyjobs", "check"]).execute()
        self.assertEqual(
            [call.kwargs["owns_process"] for call in entry.call_args_list], [False, True, False]
        )

    def test_a_stop_signal_ends_manage_py_held_by_an_exit_hook_of_its_settings(self):
        import signal
        import subprocess
        import sys

        import tinyjobs

        root = Path(self.directory.name)
        (root / "hooked_settings.py").write_text(
            "import atexit, time\n"
            "atexit.register(time.sleep, 30)\n"
            "SECRET_KEY = 'tests'\n"
            "INSTALLED_APPS = ['tinyjobs.contrib.django']\n"
            "DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}}\n"
            "TASKS = {'default': {'BACKEND': 'tinyjobs.contrib.django.TinyJobsBackend',\n"
            f"    'OPTIONS': {{'url': {f'sqlite:///{root / "hooked.db"}'!r}}}}}}}\n"
        )
        (root / "manage.py").write_text(
            "import sys\n"
            "from django.core.management import execute_from_command_line\n"
            "execute_from_command_line(sys.argv)\n"
        )
        env = dict(
            os.environ,
            DJANGO_SETTINGS_MODULE="hooked_settings",
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )
        worker = subprocess.Popen(
            [sys.executable, "manage.py", "tinyjobs", "worker", "--shutdown-timeout", "1",
             "--no-scheduler"],
            cwd=root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            time.sleep(3.0)
            worker.send_signal(signal.SIGTERM)
            time.sleep(2.5)
            self.assertIsNone(worker.poll())
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(5), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
            worker.communicate(timeout=10)

    def test_code_after_manage_py_keeps_the_clean_exit_and_its_threads(self):
        import signal
        import subprocess
        import sys

        import tinyjobs

        root = Path(self.directory.name)
        late = root / "late"
        (root / "after_settings.py").write_text(
            "SECRET_KEY = 'tests'\n"
            "INSTALLED_APPS = ['tinyjobs.contrib.django']\n"
            "DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}}\n"
            "TASKS = {'default': {'BACKEND': 'tinyjobs.contrib.django.TinyJobsBackend',\n"
            f"    'OPTIONS': {{'url': {f'sqlite:///{root / "after.db"}'!r}}}}}}}\n"
        )
        (root / "manage.py").write_text(
            "import pathlib, signal, sys, threading, time\n"
            "from django.core.management import execute_from_command_line\n"
            "execute_from_command_line(sys.argv)\n"
            "print(getattr(signal.getsignal(signal.SIGTERM), '__name__', 'ignored'), flush=True)\n"
            "threading.Thread(target=lambda: (time.sleep(0.5),\n"
            f"    pathlib.Path({str(late)!r}).write_text('done'))).start()\n"
        )
        env = dict(
            os.environ,
            DJANGO_SETTINGS_MODULE="after_settings",
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )
        worker = subprocess.Popen(
            [sys.executable, "manage.py", "tinyjobs", "worker", "--shutdown-timeout", "1",
             "--no-scheduler"],
            cwd=root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            time.sleep(3.0)
            worker.send_signal(signal.SIGTERM)
            printed, errors = worker.communicate(timeout=20)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=10)
        self.assertEqual(worker.returncode, 0, errors[-2000:])
        self.assertEqual(printed.split()[-1], "exit_cleanly")
        self.assertEqual(late.read_text(), "done")

    def test_a_second_command_line_run_in_the_same_process_handles_its_own_signals(self):
        import subprocess
        import sys

        import tinyjobs

        root = Path(self.directory.name)
        (root / "again_settings.py").write_text(
            "SECRET_KEY = 'tests'\n"
            "INSTALLED_APPS = ['tinyjobs.contrib.django']\n"
            "DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}}\n"
            "TASKS = {'default': {'BACKEND': 'tinyjobs.contrib.django.TinyJobsBackend',\n"
            f"    'OPTIONS': {{'url': {f'sqlite:///{root / "again.db"}'!r}}}}}}}\n"
        )
        (root / "launcher.py").write_text(
            "import logging, os, signal, sys, threading, time\n"
            "from django.core.management import execute_from_command_line\n"
            "class Late(logging.Handler):\n"
            "    def emit(self, record):\n"
            "        if record.getMessage().endswith(' stopped'):\n"
            "            os.kill(os.getpid(), signal.SIGINT)\n"
            "            time.sleep(0.3)\n"
            "logging.getLogger('tinyjobs.worker').addHandler(Late())\n"
            "sys.argv = ['launcher.py', 'tinyjobs', 'worker', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "for _ in range(2):\n"
            "    threading.Timer(1.5, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "    began = time.monotonic()\n"
            "    execute_from_command_line(sys.argv)\n"
            "    print(round(time.monotonic() - began, 1), flush=True)\n"
            "print('after', flush=True)\n"
        )
        env = dict(
            os.environ,
            DJANGO_SETTINGS_MODULE="again_settings",
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )
        done = subprocess.run(
            [sys.executable, "launcher.py"],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(done.returncode, 0, done.stderr[-2000:])
        *runs, last = done.stdout.split()
        self.assertEqual((len(runs), last), (2, "after"), done.stderr[-2000:])
        self.assertTrue(all(float(took) >= 1.4 for took in runs), done.stdout)

    def test_check_finds_the_tasks_of_the_installed_apps(self):
        self.assertIn("15 task(s) registered", self.call("check"))

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
            self.assertIn("15 task(s) registered", output)

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

    def test_a_database_of_another_engine_is_refused(self):
        backend = self.configured({"database": "default"})
        with self.assertRaisesRegex(ConfigurationError, "runs on sqlite and the app's url is on postgres"):
            self.tasks.double.using(backend=backend.alias).enqueue(1)


@unittest.skipIf(django is None, "pip install django to run the django tests")
class SQLiteTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prefix = f"t{uuid.uuid4().hex[:12]}"
        overrides = override_settings(
            TASKS={
                "default": {
                    "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
                    "QUEUES": ["default", "emails"],
                    "OPTIONS": {
                        "url": f"sqlite:///{LITE_PATH}",
                        "prefix": self.prefix,
                        "database": "lite",
                    },
                }
            }
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        from django.db import connections
        from django.tasks import default_task_backend

        from tests.djangoapp import tasks

        self.backend = default_task_backend
        self.tasks = tasks
        self.addCleanup(default_task_backend.app.close)
        self.addCleanup(connections["lite"].close)

    def stored(self, result):
        return self.backend.app.get(result.id)

    def test_a_task_enqueued_inside_atomic_appears_when_it_commits(self):
        from django.db import transaction

        self.backend.app.start()
        with transaction.atomic(using="lite"):
            result = self.tasks.double.enqueue(2)
            self.assertIsNone(self.stored(result))
        self.assertEqual(self.stored(result).task, "tests.djangoapp.tasks.double")
        self.assertEqual(self.backend.app.drain(), 1)
        self.assertEqual(self.backend.get_result(result.id).return_value, 4)

    def test_a_task_enqueued_inside_a_rolled_back_atomic_is_never_stored(self):
        from django.db import transaction

        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic(using="lite"):
                result = self.tasks.double.enqueue(2)
                1 / 0
        self.assertIsNone(self.stored(result))

    def test_a_rolled_back_savepoint_takes_only_its_own_tasks(self):
        from django.db import transaction

        with transaction.atomic(using="lite"):
            kept = self.tasks.double.enqueue(1)
            with contextlib.suppress(ZeroDivisionError):
                with transaction.atomic(using="lite"):
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

        self.backend.app.start()
        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic(using="lite"):
                result = async_to_sync(self.tasks.double.aenqueue)(5)
                self.assertIsNone(self.stored(result))
                1 / 0
        self.assertIsNone(self.stored(result))

    def test_the_first_enqueue_installs_the_queue_inside_atomic(self):

        from django.db import connections, transaction

        outside = sqlite3.connect(LITE_PATH)
        self.addCleanup(outside.close)
        with transaction.atomic(using="lite"):
            connections["lite"].cursor().execute(
                "CREATE TABLE IF NOT EXISTS receipts (id INTEGER PRIMARY KEY)"
            )
            connections["lite"].cursor().execute("INSERT INTO receipts DEFAULT VALUES")
            result = self.tasks.double.enqueue(3)
            names = {
                row[0]
                for row in connections["lite"].cursor().execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertIn(f"{self.prefix}_jobs", names)
            self.assertIsNone(self.backend.app._backend)
            seen = {
                row[0]
                for row in outside.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
            self.assertNotIn(f"{self.prefix}_jobs", seen)
        self.assertIsNotNone(self.stored(result))

    def test_a_deferred_atomic_block_writes_before_it_reads(self):
        from unittest import mock

        from django.db import connections, transaction

        connections["lite"].ensure_connection()
        other = sqlite3.connect(LITE_PATH, isolation_level=None, timeout=5)
        self.addCleanup(other.close)
        other.execute("CREATE TABLE IF NOT EXISTS ticks (n INTEGER)")
        traced: list[str] = []

        def commit_elsewhere(statement: str) -> None:
            if traced or statement.upper().startswith(("BEGIN", "SAVEPOINT", "PRAGMA")):
                return
            traced.append(statement)
            other.execute("INSERT INTO ticks VALUES (1)")

        raw = connections["lite"].connection
        raw.set_trace_callback(commit_elsewhere)
        self.addCleanup(raw.set_trace_callback, None)
        with mock.patch.object(connections["lite"], "transaction_mode", None):
            with transaction.atomic(using="lite"):
                result = self.tasks.double.enqueue(1)
        self.assertEqual(len(traced), 1)
        self.assertTrue(traced[0].upper().startswith("CREATE TABLE"), traced)
        self.assertEqual(self.stored(result).task, "tests.djangoapp.tasks.double")

    def test_an_in_memory_test_database_is_refused_with_the_reason(self):
        from unittest import mock

        from django.db import connections, transaction

        with mock.patch.object(connections["lite"], "is_in_memory_db", return_value=True):
            with transaction.atomic(using="lite"):
                with self.assertRaisesRegex(ConfigurationError, "test runner"):
                    self.tasks.double.enqueue(1)

    def test_aenqueue_joins_atomic_before_the_app_ever_connected(self):
        from asgiref.sync import async_to_sync
        from django.db import transaction

        with self.assertRaises(ZeroDivisionError):
            with transaction.atomic(using="lite"):
                dropped = async_to_sync(self.tasks.double.aenqueue)(5)
                self.assertIsNone(self.backend.app._backend)
                1 / 0
        with transaction.atomic(using="lite"):
            kept = async_to_sync(self.tasks.double.aenqueue)(6)
        self.assertIsNone(self.stored(dropped))
        self.assertEqual(self.stored(kept).task, "tests.djangoapp.tasks.double")

    def test_a_database_of_another_engine_is_refused(self):
        from unittest import mock

        from django.db import connections, transaction

        with mock.patch.object(connections["lite"], "vendor", "oracle"):
            with transaction.atomic(using="lite"):
                with self.assertRaisesRegex(ConfigurationError, "runs on oracle and the app's url is on sqlite"):
                    self.tasks.double.enqueue(1)

    def test_a_store_without_transactions_refuses_the_database_option(self):
        from django.db import transaction

        overrides = override_settings(
            TASKS={
                "default": {
                    "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
                    "QUEUES": ["default"],
                    "OPTIONS": {"url": "memory://", "database": "lite"},
                }
            }
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        from django.tasks import default_task_backend

        self.addCleanup(default_task_backend.app.close)
        with transaction.atomic(using="lite"):
            with self.assertRaisesRegex(ConfigurationError, "memory:// cannot join"):
                self.tasks.double.enqueue(1)

    def test_a_database_on_another_file_is_refused(self):
        from django.db import transaction

        overrides = override_settings(
            TASKS={
                "default": {
                    "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
                    "QUEUES": ["default"],
                    "OPTIONS": {
                        "url": f"sqlite:///{LITE_PATH.with_name('elsewhere.db')}",
                        "database": "lite",
                    },
                }
            }
        )
        overrides.enable()
        self.addCleanup(overrides.disable)
        from django.tasks import default_task_backend

        self.addCleanup(default_task_backend.app.close)
        with transaction.atomic(using="lite"):
            with self.assertRaisesRegex(ConfigurationError, "the queue's own file"):
                self.tasks.double.enqueue(1)


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
