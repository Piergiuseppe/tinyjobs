from __future__ import annotations

import asyncio
import unittest

from tinyjobs import TinyJobs
from tinyjobs.exceptions import (
    AppClosedError,
    ConfigurationError,
    TaskAlreadyRegisteredError,
    UnknownTaskError,
)
from tinyjobs.status import JobStatus
from tests.support import BackendTestCase


class TaskDeclarationTests(BackendTestCase):
    def test_bare_decorator_registers_the_task(self):
        @self.app.task
        def handler(value):
            return value

        self.assertIn(handler.name, self.app.registry)
        self.assertIs(self.app.registry.get(handler.name), handler)

    def test_decorator_with_options_registers_the_task(self):
        @self.app.task(queue="urgent", max_attempts=4, timeout=12.0)
        def handler():
            return None

        self.assertEqual(handler.options.queue, "urgent")
        self.assertEqual(handler.retry_policy.max_attempts, 4)
        self.assertEqual(handler.timeout, 12.0)

    def test_explicit_name_is_honoured(self):
        @self.app.task(name="pdf.generate")
        def handler():
            return None

        self.assertEqual(handler.name, "pdf.generate")

    def test_two_tasks_cannot_share_a_name(self):
        @self.app.task(name="clash")
        def first():
            return None

        with self.assertRaises(TaskAlreadyRegisteredError):

            @self.app.task(name="clash")
            def second():
                return None

    def test_the_task_is_still_directly_callable(self):
        @self.app.task
        def double(value):
            return value * 2

        self.assertEqual(double(21), 42)

    def test_unknown_task_lookup_explains_itself(self):
        with self.assertRaises(UnknownTaskError) as caught:
            self.app.registry.get("nope")
        self.assertIn("--import", str(caught.exception))


class CronDeclarationTests(BackendTestCase):
    def test_a_cron_task_carries_its_schedule(self):
        @self.app.task(cron="*/5 * * * *", timezone="Europe/Rome")
        def refresh():
            return None

        self.assertEqual(refresh.schedule.cron.source, "*/5 * * * *")
        self.assertEqual(refresh.schedule.timezone, "Europe/Rome")
        self.assertEqual(str(refresh.schedule.misfire), "skip")
        self.assertFalse(refresh.schedule.overlap)

    def test_a_plain_task_has_no_schedule(self):
        @self.app.task
        def handler():
            return None

        self.assertIsNone(handler.schedule)

    def test_a_cron_task_is_still_enqueueable_by_hand(self):
        @self.app.task(cron="@daily")
        def nightly():
            return None

        self.assertIs(self.app.get(nightly.enqueue().id).status, JobStatus.QUEUED)

    def test_mistakes_fail_at_declaration_not_at_midnight(self):
        cases = {
            "bad cron": dict(cron="61 * * * *"),
            "bad timezone": dict(cron="@daily", timezone="Europe/Atlantis"),
            "bad policy": dict(cron="@daily", misfire="sometimes"),
            "run_all without overlap": dict(cron="@daily", misfire="run_all"),
        }
        for label, options in cases.items():
            with self.subTest(label):
                with self.assertRaises(ConfigurationError):

                    @self.app.task(name=f"broken.{label}", **options)
                    def broken():
                        return None

                self.assertNotIn(f"broken.{label}", self.app.registry)

    def test_a_cron_task_must_be_callable_without_arguments(self):
        with self.assertRaises(ConfigurationError) as caught:

            @self.app.task(name="needs.args", cron="@hourly")
            def needs(document_id):
                return None

        self.assertIn("no arguments", str(caught.exception))

    def test_optional_arguments_are_fine(self):
        @self.app.task(cron="@hourly")
        def optional(limit=100, *, dry_run=False):
            return None

        self.assertIsNotNone(optional.schedule)


class EnqueueTests(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()

        @self.app.task(queue="default", max_attempts=3)
        def handler(document_id, *, reason="none"):
            return document_id

        self.handler = handler

    def test_the_returned_job_is_a_snapshot_not_a_live_handle(self):
        job = self.handler.enqueue(7)
        self.assertTrue(self.app.cancel(job.id))
        self.assertIs(job.status, JobStatus.QUEUED)
        self.assertIs(self.app.get(job.id).status, JobStatus.CANCELLED)

    def test_enqueue_persists_a_queued_job(self):
        job = self.handler.enqueue(7, reason="test")
        stored = self.app.get(job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual(stored.task, self.handler.name)
        self.assertEqual(stored.max_attempts, 3)

    def test_arguments_round_trip_through_the_payload(self):
        job = self.handler.enqueue(7, reason="test")
        payload = self.app.decode(self.app.get(job.id))
        self.assertEqual(payload.args, (7,))
        self.assertEqual(payload.kwargs, {"reason": "test"})

    def test_wrong_arguments_fail_at_enqueue_time(self):
        with self.assertRaises(TypeError):
            self.handler.enqueue()
        with self.assertRaises(TypeError):
            self.handler.enqueue(1, 2, 3)
        with self.assertRaises(TypeError):
            self.handler.enqueue(1, nonexistent=True)

    def test_a_rejected_enqueue_writes_nothing(self):
        with self.assertRaises(TypeError):
            self.handler.enqueue()
        self.assertEqual(self.app.jobs(limit=10).items, [])

    def test_with_options_overrides_without_touching_the_task(self):
        job = self.handler.with_options(queue="urgent", priority=5).enqueue(1)
        stored = self.app.get(job.id)
        self.assertEqual(stored.queue, "urgent")
        self.assertEqual(stored.priority, 5)
        self.assertEqual(self.handler.options.queue, "default")

    def test_delay_pushes_run_at_into_the_future(self):
        job = self.handler.with_options(delay=120).enqueue(1)
        self.assertGreater(job.run_at, job.created_at + 119)

    def test_deduplication_key_collapses_duplicates(self):
        first = self.handler.with_options(key="doc:1").enqueue(1)
        second = self.handler.with_options(key="doc:1").enqueue(1)
        self.assertEqual(first.id, second.id)

    def test_aenqueue_matches_enqueue(self):
        job = asyncio.run(self.handler.aenqueue(3))
        self.assertIs(self.app.get(job.id).status, JobStatus.QUEUED)


class ConcurrentInitialisationTests(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.fresh = TinyJobs(f"sqlite:///{self.database}")

        @self.fresh.task
        def handler(value):
            return value

        self.handler = handler

    def tearDown(self) -> None:
        self.fresh.close()
        super().tearDown()

    def test_many_threads_can_enqueue_against_an_uninitialised_app(self):
        import threading

        errors: list[BaseException] = []
        jobs: list[str] = []
        guard = threading.Lock()
        start = threading.Barrier(12)

        def enqueue(index: int) -> None:
            start.wait()
            try:
                job = self.handler.enqueue(index)
            except BaseException as error:
                with guard:
                    errors.append(error)
                return
            with guard:
                jobs.append(job.id)

        threads = [threading.Thread(target=enqueue, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertEqual(len(set(jobs)), 12)


class SendByNameTests(BackendTestCase):
    def test_send_does_not_require_the_task_to_be_registered(self):
        job = self.app.send("ml.train", args=[1], kwargs={"epochs": 5})
        stored = self.app.get(job.id)
        self.assertEqual(stored.task, "ml.train")
        payload = self.app.decode(stored)
        self.assertEqual(payload.args, (1,))
        self.assertEqual(payload.kwargs, {"epochs": 5})

    def test_send_inherits_the_defaults_of_a_locally_registered_task(self):
        @self.app.task(name="local.task", queue="slow", max_attempts=4)
        def handler():
            return None

        stored = self.app.get(self.app.send("local.task").id)
        self.assertEqual(stored.max_attempts, 4)
        self.assertEqual(stored.queue, "slow")

    def test_explicit_send_options_beat_the_task_declaration(self):
        @self.app.task(name="local.task", queue="slow", max_attempts=4)
        def handler():
            return None

        stored = self.app.get(self.app.send("local.task", queue="fast").id)
        self.assertEqual(stored.queue, "fast")
        self.assertEqual(stored.max_attempts, 4)

    def test_send_to_an_unknown_task_uses_documented_defaults(self):
        stored = self.app.get(self.app.send("not.registered").id)
        self.assertEqual(stored.max_attempts, 1)
        self.assertEqual(stored.queue, "default")

    def test_send_accepts_enqueue_options(self):
        job = self.app.send("ml.train", queue="gpu", priority=9, max_attempts=4)
        stored = self.app.get(job.id)
        self.assertEqual(stored.queue, "gpu")
        self.assertEqual(stored.priority, 9)
        self.assertEqual(stored.max_attempts, 4)


class ControlTests(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()

        @self.app.task
        def handler():
            return None

        self.handler = handler

    def test_cancel_moves_a_queued_job_to_cancelled(self):
        job = self.handler.enqueue()
        self.assertTrue(self.app.cancel(job.id))
        self.assertIs(self.app.get(job.id).status, JobStatus.CANCELLED)

    def test_listing_returns_newest_first(self):
        ids = [self.handler.enqueue().id for _ in range(3)]
        listed = [job.id for job in self.app.jobs(limit=10).items]
        self.assertEqual(listed, list(reversed(ids)))


if __name__ == "__main__":
    unittest.main()


class RefreshTests(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()

        @self.app.task(store_result=True)
        def handler(value):
            return {"doubled": value * 2}

        self.handler = handler

    def test_refresh_picks_up_a_status_written_elsewhere(self):
        job = self.handler.enqueue(3)
        self.assertIs(job.status, JobStatus.QUEUED)
        self.app.cancel(job.id)

        self.assertIs(job.refresh().status, JobStatus.CANCELLED)

    def test_refresh_mutates_in_place_and_returns_self(self):
        job = self.handler.enqueue(3)
        self.app.cancel(job.id)
        returned = job.refresh()
        self.assertIs(returned, job)
        self.assertIs(job.status, JobStatus.CANCELLED)

    def test_refresh_leaves_the_identity_alone(self):
        job = self.handler.enqueue(3)
        original_id, original_task = job.id, job.task
        self.app.cancel(job.id)
        job.refresh()
        self.assertEqual(job.id, original_id)
        self.assertEqual(job.task, original_task)

    def test_refresh_raises_once_the_job_is_gone(self):
        from tinyjobs.exceptions import JobNotFoundError

        job = self.handler.enqueue(3)
        self.drive(
            self.app.backend._call(
                lambda: self.app.backend._connections.connection.execute(
                    "DELETE FROM jobs WHERE id = ?", (job.id,)
                )
            )
        )
        with self.assertRaises(JobNotFoundError):
            job.refresh()

    def test_jobs_from_listings_are_refreshable_too(self):
        self.handler.enqueue(3)
        listed = self.app.jobs(limit=1).items[0]
        self.app.cancel(listed.id)
        self.assertIs(listed.refresh().status, JobStatus.CANCELLED)

    def test_a_hand_built_job_says_it_is_unbound(self):
        from tinyjobs.exceptions import TinyJobsError
        from tinyjobs.models import Job

        orphan = Job(
            id="x",
            task="t",
            payload=b"{}",
            codec="json",
            queue="default",
            status=JobStatus.QUEUED,
        )
        with self.assertRaises(TinyJobsError) as caught:
            orphan.refresh()
        self.assertIn("not handed out by a TinyJobs app", str(caught.exception))

    def test_result_is_none_until_the_task_has_run(self):
        self.assertIsNone(self.handler.enqueue(3).result())

    def test_result_decodes_the_stored_return_value(self):
        job = self.handler.enqueue(21)
        claimed = self.drive(
            self.app.backend.claim(
                worker_id="w", queues=("default",), limit=1, lease_seconds=60
            )
        )[0]
        self.drive(
            self.app.lifecycle.succeed(
                claimed, self.handler, {"doubled": 42}
            )
        )
        self.assertEqual(job.refresh().result(), {"doubled": 42})


class ClosedAppTests(BackendTestCase):
    def setUp(self) -> None:
        super().setUp()

        @self.app.task
        def noop():
            return None

        self.noop = noop

    def test_a_closed_app_does_not_reconnect_on_its_own(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.noop.enqueue()

    def test_draining_a_closed_app_does_not_reconnect_it(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.app.drain()

    def test_a_scheduler_tick_does_not_reconnect_a_closed_app(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.app.run_sync(self.app.scheduler().tick(), name="tick")

    def test_start_reopens_a_closed_app(self):
        self.app.close()
        self.app.start()
        self.assertIs(self.noop.enqueue().status, JobStatus.QUEUED)
