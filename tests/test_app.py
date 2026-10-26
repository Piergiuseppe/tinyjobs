from __future__ import annotations

import asyncio
import gc
import importlib.util
import os
import signal
import threading
import time
import traceback
import unittest
import warnings
import weakref
from contextlib import ExitStack, contextmanager, suppress
from unittest import mock

from tinyjobs import RetryPolicy, Task, TinyJobs, runtime
from tinyjobs.backends import managed
from tinyjobs.backends.sqlite import SQLiteBackend
from tinyjobs.exceptions import (
    AppClosedError,
    BackendNotReadyError,
    ConfigurationError,
    JobNotFoundError,
    SynchronousCallInsideEventLoopError,
    TaskAlreadyRegisteredError,
    TinyJobsError,
    UnknownTaskError,
)
from tinyjobs.models import Job
from tinyjobs.options import EnqueueOptions
from tinyjobs.services import SchedulingService
from tinyjobs.status import ExecutionStatus, JobStatus
from tests.support import (
    BackendTestCase,
    assert_fork_hooks_settled,
    disarm_alarm,
    flush_control,
    fork_without_warning,
    interrupt_repeatedly,
    race_open_and_close,
    sweep_interrupts,
)


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

    def test_a_write_inside_your_own_transaction_is_refused_at_once(self):
        started = time.monotonic()
        with self.app.backend.transaction() as connection:
            connection.execute("UPDATE meta SET value = value")
            with self.assertRaisesRegex(RuntimeError, "after the transaction ends"):
                self.handler.enqueue(1)
            with self.assertRaisesRegex(RuntimeError, "after the transaction ends"):
                self.app.cancel("missing")
            self.assertEqual(self.app.jobs(limit=10).items, [])
        self.assertLess(time.monotonic() - started, 2)
        self.assertIs(self.app.get(self.handler.enqueue(2).id).status, JobStatus.QUEUED)

    def test_an_aenqueue_inside_your_own_transaction_is_refused_at_once(self):
        async def enqueue_inside():
            with self.app.backend.transaction() as connection:
                connection.execute("UPDATE meta SET value = value")
                with self.assertRaisesRegex(RuntimeError, "after the transaction ends"):
                    await self.handler.aenqueue(1)
            return await self.handler.aenqueue(2)

        started = time.monotonic()
        job = asyncio.run(enqueue_inside())
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual([item.id for item in self.app.jobs(limit=10).items], [job.id])

    def test_a_transaction_whose_cleanup_was_interrupted_does_not_lock_later_enqueues(self):
        from tinyjobs.backends.sqlite import connection as connection_module

        class Interrupted(BaseException):
            pass

        names = ("_roll_back", "_roll_back_retired", "_shut")
        originals = {name: getattr(connection_module, name) for name in names}
        armed, left = [], dict.fromkeys(names, 1)

        def interrupting(name):
            def call(*args, **options):
                if armed and left[name]:
                    left[name] -= 1
                    raise Interrupted
                return originals[name](*args, **options)

            return call

        self.handler.enqueue(0)
        with ExitStack() as patches:
            for name in names:
                patches.enter_context(mock.patch.object(connection_module, name, interrupting(name)))
            with self.assertRaises(Interrupted):
                with self.app.backend.transaction() as connection:
                    connection.execute("UPDATE meta SET value = value")
                    armed.append(True)
                    raise ValueError
        gc.collect()
        started = time.monotonic()
        job = self.handler.enqueue(1)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(left, dict.fromkeys(names, 0))
        self.assertIs(self.app.get(job.id).status, JobStatus.QUEUED)

    def test_a_task_started_inside_your_own_transaction_enqueues_once_it_commits(self):
        async def fire_and_forget():
            with self.app.backend.transaction() as connection:
                connection.execute("UPDATE meta SET value = value")
                later = asyncio.create_task(self.handler.aenqueue(1))
                await asyncio.sleep(0.1)
                self.assertFalse(later.done())
            return await later

        job = asyncio.run(fire_and_forget())
        self.assertIs(self.app.get(job.id).status, JobStatus.QUEUED)

    def test_a_write_through_another_app_on_the_same_file_is_refused_at_once(self):
        other = TinyJobs(f"sqlite:///{self.database}").start()
        try:

            @other.task
            def ping():
                return None

            started = time.monotonic()
            with self.app.backend.transaction() as connection:
                connection.execute("UPDATE meta SET value = value")
                with self.assertRaisesRegex(RuntimeError, "after the transaction ends"):
                    ping.enqueue()
            self.assertLess(time.monotonic() - started, 2)
            self.assertEqual(other.jobs(limit=10).items, [])
        finally:
            other.close()


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


    def test_event_loops_and_threads_connecting_at_once_share_one_connection(self):
        errors: list[BaseException] = []
        start = threading.Barrier(8)

        def from_a_loop() -> None:
            async def enqueue() -> None:
                start.wait()
                await self.handler.aenqueue(1)

            try:
                asyncio.run(enqueue())
            except BaseException as error:
                errors.append(error)

        def from_a_thread() -> None:
            start.wait()
            try:
                self.handler.enqueue(2)
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=from_a_loop) for _ in range(4)]
        threads += [threading.Thread(target=from_a_thread) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(self.fresh.counts()["queued"], 8)


class FailedConnectionTests(BackendTestCase):
    def test_a_backend_whose_setup_fails_is_closed(self):
        closed: list[object] = []
        close = SQLiteBackend.close

        async def record(backend):
            closed.append(backend)
            await close(backend)

        refused = RuntimeError("schema is newer than this tinyjobs")
        fresh = TinyJobs(f"sqlite:///{self.database}")
        with mock.patch.object(
            SQLiteBackend, "setup", side_effect=refused
        ), mock.patch.object(SQLiteBackend, "close", record):
            with self.assertRaises(RuntimeError):
                fresh.start()
        fresh.close()
        self.assertEqual(len(closed), 1)

    def test_callers_waiting_on_a_failed_connection_share_its_error(self):
        attempts: list[int] = []

        async def refuse(backend):
            attempts.append(1)
            await asyncio.sleep(0.2)
            raise RuntimeError("disk is full")

        fresh = TinyJobs(f"sqlite:///{self.database}")

        async def connect_at_once():
            return await asyncio.gather(
                *(fresh.ensure_connected() for _ in range(5)), return_exceptions=True
            )

        with mock.patch.object(SQLiteBackend, "setup", refuse):
            outcomes = asyncio.run(connect_at_once())
        fresh.close()
        self.assertEqual(len(attempts), 1)
        self.assertEqual([str(outcome) for outcome in outcomes], ["disk is full"] * 5)

    def test_a_connection_finished_after_a_close_is_closed_too(self):
        opened: list[object] = []
        closed: list[object] = []
        setup, close = SQLiteBackend.setup, SQLiteBackend.close
        release = threading.Event()

        async def slow(backend):
            opened.append(backend)
            await asyncio.to_thread(release.wait, 5)
            await setup(backend)

        async def record(backend):
            closed.append(backend)
            await close(backend)

        fresh = TinyJobs(f"sqlite:///{self.database}")
        outcome: list[BaseException] = []

        def connect() -> None:
            try:
                asyncio.run(fresh.ready())
            except BaseException as error:
                outcome.append(error)

        with mock.patch.object(SQLiteBackend, "setup", slow), mock.patch.object(
            SQLiteBackend, "close", record
        ):
            connecting = threading.Thread(target=connect)
            connecting.start()
            while not opened:
                time.sleep(0.01)
            asyncio.run(fresh.aclose())
            release.set()
            connecting.join(10)
        fresh.close()
        self.assertIsInstance(outcome[0], AppClosedError)
        self.assertEqual(closed, opened)

    def test_a_waiter_takes_over_when_the_first_connecting_loop_stops(self):
        setup = SQLiteBackend.setup

        async def slow(backend):
            await asyncio.sleep(1.0)
            await setup(backend)

        fresh = TinyJobs(f"sqlite:///{self.database}")
        owner = asyncio.new_event_loop()
        serving = threading.Thread(target=owner.run_forever)
        with mock.patch.object(SQLiteBackend, "setup", slow):
            serving.start()
            asyncio.run_coroutine_threadsafe(fresh.ready(), owner)
            time.sleep(0.3)
            owner.call_soon_threadsafe(owner.stop)
            serving.join()
            waiter = threading.Thread(target=asyncio.run, args=(fresh.ready(),), daemon=True)
            started = time.monotonic()
            waiter.start()
            waiter.join(4)
            took = time.monotonic() - started
        fresh.close()
        abandoned = asyncio.all_tasks(owner)
        for task in abandoned:
            task.cancel()
        owner.run_until_complete(asyncio.gather(*abandoned, return_exceptions=True))
        owner.close()
        self.assertLess(took, 2.0)

    def test_a_sync_call_on_a_closed_app_starts_no_thread(self):
        def loop_threads() -> int:
            return sum(t.name == "tinyjobs-loop" for t in threading.enumerate())

        fresh = TinyJobs(f"sqlite:///{self.database}").start()
        fresh.close()
        before = loop_threads()
        with self.assertRaises(AppClosedError):
            fresh.counts()
        self.assertEqual(loop_threads(), before)

    def test_calls_racing_open_and_close_cycles_see_only_closed_errors(self):
        fresh = TinyJobs(f"sqlite:///{self.database}")
        handler = fresh.task(name="handler")(lambda: None)
        unexpected: list[BaseException] = []
        stop = threading.Event()

        def hammer() -> None:
            while not stop.is_set():
                try:
                    handler.enqueue()
                except AppClosedError:
                    time.sleep(0.001)
                except BaseException as error:
                    unexpected.append(error)

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for thread in threads:
            thread.start()

        async def cycle() -> None:
            for _ in range(15):
                await fresh.ready()
                await asyncio.sleep(0.02)
                await fresh.aclose()

        try:
            for _ in range(2):
                asyncio.run(cycle())
        finally:
            stop.set()
            for thread in threads:
                thread.join(10)
            fresh.close()
        self.assertEqual(unexpected, [])

    def test_sync_and_async_callers_racing_start_and_close_leave_nothing_behind(self):
        race_open_and_close(self, TinyJobs(f"sqlite:///{self.database}"))

    def test_a_backend_whose_close_fails_still_stops_the_loop_thread(self):
        def loop_threads() -> int:
            return sum(t.name == "tinyjobs-loop" for t in threading.enumerate())

        before = loop_threads()
        fresh = TinyJobs(f"sqlite:///{self.database}").start()
        refused = RuntimeError("close failed")
        with mock.patch.object(SQLiteBackend, "close", side_effect=refused):
            with self.assertRaises(RuntimeError):
                fresh.close()
        self.assertEqual(loop_threads(), before)


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


class ResolverTests(BackendTestCase):
    def make_resolver(self, known: str):
        asked = []

        def resolve(name):
            asked.append(name)
            if name != known:
                return None
            return Task(
                self.app,
                lambda: "resolved",
                name=name,
                options=EnqueueOptions(queue="found"),
                retry_policy=RetryPolicy(),
            )

        self.app.registry.add_resolver(resolve)
        return asked

    def test_a_resolver_supplies_a_task_nobody_registered(self):
        self.make_resolver("lazy.task")
        self.assertEqual(self.app.registry.get("lazy.task")(), "resolved")

    def test_a_resolved_task_is_registered_once_and_kept(self):
        asked = self.make_resolver("lazy.task")
        first = self.app.registry.get("lazy.task")
        self.assertIs(self.app.registry.get("lazy.task"), first)
        self.assertEqual(asked, ["lazy.task"])

    def test_membership_and_send_defaults_go_through_the_resolver(self):
        self.make_resolver("lazy.task")
        self.assertIn("lazy.task", self.app.registry)
        self.assertNotIn("other.task", self.app.registry)
        self.assertEqual(self.app.get(self.app.send("lazy.task").id).queue, "found")

    def test_a_name_no_resolver_knows_is_still_unknown(self):
        self.make_resolver("lazy.task")
        with self.assertRaises(UnknownTaskError):
            self.app.registry.get("other.task")


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


class JobFieldTests(unittest.TestCase):
    def test_fields_built_by_position_keep_their_place(self):
        job = Job("id", "task", b"", "json", "q", JobStatus.QUEUED, 0, 0, 1, 123.0, 5.0)
        self.assertEqual((job.run_at, job.created_at, job.hand_backs), (123.0, 5.0, 0))


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

    def test_closing_an_app_that_never_connected_keeps_it_closed(self):
        app = TinyJobs(f"sqlite:///{self.database}")

        @app.task
        def noop():
            return None

        app.close()
        with self.assertRaises(AppClosedError):
            noop.enqueue()

    def test_a_claim_on_a_closed_app_starts_no_loop(self):
        self.app.start()
        backend = self.app.backend
        self.app.close()

        async def claim() -> None:
            await backend.claim(
                worker_id="w", queues=("default",), limit=1, lease_seconds=60.0
            )

        with self.assertRaises(AppClosedError):
            asyncio.run(claim())
        self.assertIsNone(self.app._runner._loop)

    def test_an_app_dropped_without_closing_stops_its_loop(self):
        app = TinyJobs(f"sqlite:///{self.database}")
        app.run_sync(lambda: asyncio.sleep(0), name="tick")
        loop_thread = app._runner._thread
        del app
        gc.collect()
        loop_thread.join(5)
        self.assertFalse(loop_thread.is_alive())

    def test_an_app_dropped_after_an_enqueue_stops_its_loop(self):
        app = TinyJobs(f"sqlite:///{self.database}")
        handler = app.task(name="handler")(lambda value: value)
        handler.enqueue(1)
        loop_thread = app._runner._thread
        collected = weakref.ref(app)
        del app, handler
        gc.collect()
        loop_thread.join(5)
        self.assertIsNone(collected())
        self.assertFalse(loop_thread.is_alive())

    def test_a_child_connects_although_a_parent_thread_held_the_app_lock(self):
        app = TinyJobs(f"sqlite:///{self.database}")
        self.addCleanup(app.close)
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with app._lock_guard:
                held.set()
                release.wait(10)

        holder = threading.Thread(target=hold)
        holder.start()
        self.addCleanup(holder.join, 10)
        self.addCleanup(release.set)
        held.wait(5)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.alarm(10)
            try:
                asyncio.run(app.ensure_connected())
                connected = app._backend is not None
            except BaseException:
                connected = False
            os._exit(0 if connected else 1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_draining_a_closed_app_does_not_reconnect_it(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.app.drain()

    def test_a_scheduler_tick_does_not_reconnect_a_closed_app(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.app.run_sync(self.app.scheduler().tick, name="tick")

    def test_start_reopens_a_closed_app(self):
        self.app.close()
        self.app.start()
        self.assertIs(self.noop.enqueue().status, JobStatus.QUEUED)

    def test_closing_from_two_threads_at_once_never_raises(self):
        errors: list[BaseException] = []

        def close():
            try:
                self.app.close()
            except BaseException as error:
                errors.append(error)

        for _ in range(100):
            self.app.start()
            closers = [threading.Thread(target=close) for _ in range(2)]
            for closer in closers:
                closer.start()
            for closer in closers:
                closer.join()
        self.assertEqual(errors, [])

    def test_two_closes_at_once_both_wait_for_the_backend_to_close(self):
        inner = self.app.backend._inner
        closing = inner._close_sync
        entered = threading.Event()

        def _close_sync():
            entered.set()
            time.sleep(0.3)
            closing()

        with mock.patch.object(inner, "_close_sync", _close_sync):
            first = threading.Thread(target=self.app.close)
            first.start()
            self.assertTrue(entered.wait(5))
            self.app.close()
            closed_when_second_returned = inner._closed
            first.join(5)
        self.assertTrue(closed_when_second_returned)
        self.assertTrue(inner._closed)
        self.assertFalse(first.is_alive())

    def test_a_cancelled_close_never_stalls_the_loop_on_a_busy_backend(self):
        @self.app.task
        def noop():
            return None

        inner = self.app.backend._inner
        enqueueing = inner._enqueue_sync
        entered = threading.Event()

        closing = inner._close_sync
        closed = threading.Event()

        def _enqueue_sync(job):
            entered.set()
            time.sleep(1.5)
            return enqueueing(job)

        def _close_sync():
            closing()
            closed.set()

        async def scenario():
            gaps = []

            async def tick():
                last = time.monotonic()
                while True:
                    await asyncio.sleep(0.02)
                    now = time.monotonic()
                    gaps.append(now - last)
                    last = now

            ticker = asyncio.create_task(tick())
            with suppress(TimeoutError):
                await asyncio.wait_for(self.app.aclose(), 0.4)
            await asyncio.sleep(0.1)
            ticker.cancel()
            return max(gaps)

        def enqueue():
            with suppress(Exception):
                noop.enqueue()

        with (
            mock.patch.object(inner, "_enqueue_sync", _enqueue_sync),
            mock.patch.object(inner, "_close_sync", _close_sync),
            mock.patch.object(managed, "CLOSE_GRACE_SECONDS", 0.05),
        ):
            writer = threading.Thread(target=enqueue)
            writer.start()
            self.assertTrue(entered.wait(5))
            stall = asyncio.run(scenario())
            writer.join(5)
            self.assertTrue(closed.wait(5))
        self.assertLess(stall, 0.5)

    def test_a_worker_stops_once_its_app_is_closed(self):
        async def scenario():
            worker = self.app.worker(poll_maximum=0.02, handle_signals=False)
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.2)
            await self.app.aclose()
            await asyncio.wait_for(runner, timeout=5)

        asyncio.run(scenario())

    def test_a_scheduler_stops_once_its_app_is_closed(self):
        @self.app.task(name="report.hourly", cron="0 * * * *")
        def hourly():
            return None

        async def scenario():
            scheduler = self.app.scheduler(maximum_sleep=0.02)
            runner = asyncio.create_task(scheduler.start())
            await asyncio.sleep(0.2)
            await self.app.aclose()
            await asyncio.wait_for(runner, timeout=5)

        asyncio.run(scenario())

    def test_a_scheduler_closed_while_registering_stops(self):
        @self.app.task(name="report.hourly", cron="0 * * * *")
        def hourly():
            return None

        refused = AppClosedError("closed")

        async def scenario():
            scheduler = self.app.scheduler(error_backoff=0.01)
            with mock.patch.object(SchedulingService, "register", side_effect=refused):
                await asyncio.wait_for(scheduler.start(), timeout=2)

        asyncio.run(scenario())

    def test_a_backend_kept_from_before_a_close_refuses_calls(self):
        kept = self.app.backend
        self.app.close()
        with self.assertRaises(AppClosedError):
            asyncio.run(kept.get("anything"))

    def test_closing_inside_a_running_loop_refuses_and_leaves_the_app_open(self):
        async def close_inside():
            self.app.close()

        with self.assertRaises(SynchronousCallInsideEventLoopError):
            asyncio.run(close_inside())
        self.assertIs(self.noop.enqueue().status, JobStatus.QUEUED)

    def test_a_closed_app_says_so_instead_of_not_connected(self):
        self.app.close()
        with self.assertRaises(AppClosedError):
            self.app.backend

    def test_a_reopened_app_without_a_backend_is_not_connected_rather_than_closed(self):
        self.app.close()
        self.app.start()
        self.app.forget_inherited_connection()
        with self.assertRaises(BackendNotReadyError) as caught:
            self.app.backend
        self.assertNotIsInstance(caught.exception, AppClosedError)

    def test_starting_inside_a_running_loop_points_to_ready(self):
        async def start_inside():
            self.app.start()

        with self.assertRaisesRegex(SynchronousCallInsideEventLoopError, r"await ready\(\)"):
            asyncio.run(start_inside())

    def test_closing_waits_for_the_jobs_a_worker_is_running(self):
        finished: list[int] = []

        @self.app.task(name="slow")
        def slow():
            time.sleep(0.2)
            finished.append(1)

        for _ in range(12):
            slow.enqueue()
        worker = self.app.worker(
            handle_signals=False, scheduler=False, concurrency=4, poll_minimum=0.0
        )
        running = threading.Thread(target=worker.run)
        running.start()
        time.sleep(0.3)
        started = time.monotonic()
        self.app.close()
        self.assertLess(time.monotonic() - started, 3.0)
        running.join(10)
        self.assertFalse(running.is_alive())
        self.app.start()
        counts = self.app.counts()
        self.assertEqual(counts.get("running", 0), 0)
        self.assertEqual(counts.get("success", 0), len(finished))

    def test_closing_waits_past_the_grace_for_a_worker_given_longer_to_finish(self):
        started = threading.Event()

        @self.app.task(name="slow")
        def slow():
            started.set()
            time.sleep(0.6)

        job = slow.enqueue()
        worker = self.app.worker(
            handle_signals=False, scheduler=False, shutdown_timeout=5.0
        )
        running = threading.Thread(target=worker.run)
        running.start()
        started.wait(5)
        with mock.patch("tinyjobs.app.WORKER_GRACE_SECONDS", 0.0):
            self.app.close()
        running.join(10)
        self.app.start()
        self.assertEqual(job.refresh().status, JobStatus.SUCCESS)

    def test_a_worker_that_starts_as_the_app_closes_still_stops(self):
        async def scenario():
            worker = self.app.worker(poll_maximum=0.02, handle_signals=False)
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.2)
            with mock.patch.object(self.app, "_stop_workers", mock.AsyncMock()):
                await self.app.aclose()
            await asyncio.wait_for(runner, timeout=5)

        asyncio.run(scenario())

    def test_a_worker_stopped_from_another_thread_stops_at_once(self):
        worker = self.app.worker(
            handle_signals=False, scheduler=False, poll_minimum=30.0, poll_maximum=30.0
        )
        running = threading.Thread(target=worker.run)
        running.start()
        time.sleep(0.3)
        started = time.monotonic()
        worker.stop()
        running.join(10)
        self.assertLess(time.monotonic() - started, 2.0)

    def test_a_job_that_closes_its_own_app_does_not_wait_for_itself(self):
        @self.app.task(name="closer")
        def closer():
            self.app.close()
            return "recorded"

        job = closer.enqueue()
        closer.enqueue()
        worker = self.app.worker(
            handle_signals=False, scheduler=False, shutdown_timeout=30.0, concurrency=1
        )
        started = time.monotonic()
        running = threading.Thread(target=worker.run)
        with self.assertNoLogs("tinyjobs.worker", "ERROR"):
            running.start()
            running.join(20)
        self.assertLess(time.monotonic() - started, 10.0)
        self.assertFalse(running.is_alive())
        with self.assertRaises(AppClosedError):
            self.app.counts()
        self.app.start()
        self.assertEqual(job.refresh().status, JobStatus.SUCCESS)
        self.assertEqual(self.app.counts(), {"success": 1, "queued": 1})
        self.assertEqual(
            [e.status for e in self.app.executions(job.id)], [ExecutionStatus.SUCCESS]
        )

    def test_jobs_in_two_workers_that_each_close_the_app_keep_their_outcomes(self):
        both = threading.Barrier(2)
        closed_in: list[float] = []

        @self.app.task(name="closer")
        def closer():
            both.wait(10)
            started = time.monotonic()
            self.app.close()
            closed_in.append(time.monotonic() - started)

        jobs = [closer.enqueue(), closer.enqueue()]
        workers = [
            self.app.worker(
                handle_signals=False, scheduler=False, concurrency=1, shutdown_timeout=30.0
            )
            for _ in range(2)
        ]
        running = [threading.Thread(target=worker.run) for worker in workers]
        for thread in running:
            thread.start()
        for thread in running:
            thread.join(40)
        self.assertEqual(len(closed_in), 2)
        self.assertLess(max(closed_in), 1.0)
        self.app.start()
        for job in jobs:
            self.assertEqual(job.refresh().status, JobStatus.SUCCESS)
            self.assertEqual(
                [e.status for e in self.app.executions(job.id)],
                [ExecutionStatus.SUCCESS],
            )

    def test_a_drain_stopped_before_it_runs_takes_no_job(self):
        from tinyjobs.worker.drain import Drain

        @self.app.task(name="noop")
        def noop():
            return None

        job = noop.enqueue()
        drain = Drain(self.app)
        drain.stop()
        self.assertEqual(self.app.run_sync(drain.run, name="drain"), 0)
        self.assertEqual(job.refresh().status, JobStatus.QUEUED)

    def test_a_drain_runs_again_after_it_finished(self):
        from tinyjobs.worker.drain import Drain

        seen: list[bool] = []
        drain = Drain(self.app)

        @self.app.task(name="noted")
        def noted():
            seen.append(drain.finished)

        for _ in range(2):
            noted.enqueue()
            self.assertEqual(self.app.run_sync(drain.run, name="drain"), 1)
        self.assertEqual(seen, [False, False])
        self.assertTrue(drain.finished)

    def test_a_stop_delivered_after_its_run_ended_spares_the_next_run(self):
        from tinyjobs.worker.drain import Drain

        runners = (
            self.app.worker(handle_signals=False, scheduler=False),
            Drain(self.app),
            self.app.scheduler(),
        )
        for runner in runners:
            with self.subTest(type(runner).__name__):
                delivered: list = []
                runner._loop = mock.Mock()
                runner._loop.call_soon_threadsafe.side_effect = (
                    lambda callback, *args: delivered.append((callback, args))
                )
                runner.stop()
                runner._prepare_run()
                for callback, args in delivered:
                    callback(*args)
                self.assertEqual(len(delivered), 1)
                self.assertFalse(runner._stopping.is_set())

    def test_a_stop_racing_the_end_of_its_run_stops_only_that_run(self):
        from tinyjobs import scheduler as scheduler_module
        from tinyjobs.worker import drain as drain_module
        from tinyjobs.worker import worker as worker_module
        from tinyjobs.worker.drain import Drain

        runners = (
            (self.app.worker(handle_signals=False, scheduler=False), worker_module),
            (Drain(self.app), drain_module),
            (self.app.scheduler(), scheduler_module),
        )
        for runner, module in runners:
            for on_its_loop in (True, False):
                with self.subTest(type(runner).__name__, on_its_loop=on_its_loop):

                    def run_ends() -> bool:
                        runner._prepare_run()
                        return on_its_loop

                    async def stop_as_the_run_ends() -> asyncio.Event:
                        ending = runner._stopping
                        loop = asyncio.get_running_loop() if on_its_loop else mock.Mock()
                        runner._loop = loop
                        with mock.patch.object(module, "inside_running_loop", run_ends):
                            runner.stop()
                        if not on_its_loop:
                            for call in loop.call_soon_threadsafe.call_args_list:
                                call.args[0](*call.args[1:])
                        return ending

                    ending = asyncio.run(stop_as_the_run_ends())
                    self.assertTrue(ending.is_set())
                    self.assertFalse(runner._stopping.is_set())

    def test_a_stop_from_another_loop_goes_through_the_runs_loop(self):
        from tinyjobs.worker.drain import Drain

        runners = (
            self.app.worker(handle_signals=False, scheduler=False),
            Drain(self.app),
            self.app.scheduler(),
        )

        async def stop_from_here(runner) -> None:
            runner.stop()

        for runner in runners:
            with self.subTest(type(runner).__name__):
                runner._loop = mock.Mock()
                asyncio.run(stop_from_here(runner))
                runner._loop.call_soon_threadsafe.assert_called_once()
                self.assertFalse(runner._stopping.is_set())

    def test_closing_waits_for_a_worker_on_its_second_run(self):
        import tempfile
        from pathlib import Path

        path = Path(tempfile.mkdtemp()) / "rerun.db"
        app = TinyJobs(f"sqlite:///{path}")

        @app.task(name="slow")
        def slow():
            time.sleep(0.5)
            return "done"

        app.start()
        worker = app.worker(handle_signals=False, scheduler=False, poll_maximum=0.05)
        first = threading.Thread(target=worker.run)
        first.start()
        time.sleep(0.3)
        worker.stop()
        first.join(10)
        second = threading.Thread(target=worker.run)
        second.start()
        job = slow.enqueue()
        deadline = time.monotonic() + 10
        while job.refresh().status is not JobStatus.RUNNING and time.monotonic() < deadline:
            time.sleep(0.01)
        app.close()
        second.join(10)
        check = TinyJobs(f"sqlite:///{path}").start()
        self.addCleanup(check.close)
        self.assertIs(check.get(job.id).status, JobStatus.SUCCESS)

    def test_a_worker_forced_to_stop_waits_again_on_its_next_run(self):
        release = threading.Event()

        @self.app.task(name="slow")
        def slow():
            release.wait(2)

        worker = self.app.worker(
            handle_signals=False, scheduler=False, poll_maximum=0.05, shutdown_timeout=10.0
        )
        worker._request_force()
        worker.stop()
        worker.run()
        slow.enqueue()
        running = threading.Thread(target=worker.run)
        running.start()
        time.sleep(0.5)
        worker.stop()
        time.sleep(0.2)
        release.set()
        running.join(20)
        self.assertEqual(self.app.counts(), {"success": 1})

    def test_a_worker_that_closed_its_app_once_does_not_close_it_again(self):
        closing = threading.Event()

        @self.app.task(name="maybe_close")
        def maybe_close():
            if closing.is_set():
                closing.clear()
                self.app.close()

        worker = self.app.worker(handle_signals=False, scheduler=False, poll_maximum=0.05)
        closing.set()
        maybe_close.enqueue()
        first = threading.Thread(target=worker.run)
        first.start()
        first.join(20)
        self.app.start()
        maybe_close.enqueue()
        second = threading.Thread(target=worker.run)
        second.start()
        time.sleep(0.5)
        worker.stop()
        second.join(20)
        self.assertEqual(self.app.counts(), {"success": 2})

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_forked_child_forgets_the_closes_its_parent_deferred(self):
        self.app.counts()
        worker = self.app.worker(handle_signals=False)
        self.app.attach(worker)
        self.assertTrue(self.app._defer_close(worker))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(10)
            try:
                self.app.counts()
                os._exit(0 if not self.app.detach(worker) else 1)
            finally:
                os._exit(2)
        _, status = os.waitpid(child, 0)
        self.assertTrue(self.app.detach(worker))
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_an_async_job_that_closes_its_own_app_keeps_its_outcome(self):
        @self.app.task(name="closer")
        async def closer():
            await self.app.aclose()

        job = closer.enqueue()
        worker = self.app.worker(handle_signals=False, scheduler=False)
        running = threading.Thread(target=worker.run)
        running.start()
        running.join(20)
        self.assertFalse(running.is_alive())
        self.app.start()
        self.assertEqual(job.refresh().status, JobStatus.SUCCESS)

    def test_closing_waits_for_the_job_a_drain_is_running(self):
        started = threading.Event()

        @self.app.task(name="slow")
        def slow():
            started.set()
            time.sleep(0.3)

        for _ in range(3):
            slow.enqueue()
        drained: list[int] = []
        running = threading.Thread(target=lambda: drained.append(self.app.drain()))
        running.start()
        started.wait(5)
        self.app.close()
        running.join(10)
        self.app.start()
        counts = self.app.counts()
        self.assertEqual(counts.get("running", 0), 0)
        self.assertEqual(drained, [1])
        self.assertEqual((counts.get("success"), counts.get("queued")), (1, 2))

    def test_a_job_that_closes_its_own_app_during_a_drain_keeps_its_outcome(self):
        @self.app.task(name="closer")
        def closer():
            self.app.close()

        job = closer.enqueue()
        closer.enqueue()
        started = time.monotonic()
        self.assertEqual(self.app.drain(), 1)
        self.assertLess(time.monotonic() - started, 4.0)
        with self.assertRaises(AppClosedError):
            self.app.counts()
        self.assertEqual(
            [
                thread.name
                for thread in threading.enumerate()
                if thread.name.startswith("tinyjobs")
                and thread.name != runtime.CONTROL_THREAD_NAME
            ],
            [],
        )
        self.app.start()
        self.assertEqual(job.refresh().status, JobStatus.SUCCESS)
        self.assertEqual(self.app.counts(), {"success": 1, "queued": 1})

class ForkGateTests(unittest.TestCase):
    def setUp(self) -> None:
        from tinyjobs.backends.sqlite.connection import ForkGate

        self.gate = ForkGate()

    def enter_from_another_thread(self) -> threading.Event:
        entered = threading.Event()

        def use() -> None:
            with self.gate.using():
                entered.set()

        threading.Thread(target=use, daemon=True).start()
        return entered

    @contextmanager
    def forking(self, ident: int | None = None):
        ident = threading.get_ident() if ident is None else ident
        runtime._fork_marks[ident] = os.getpid()
        try:
            yield
        finally:
            runtime._fork_marks.pop(ident, None)

    def live_holders(self) -> list:
        return [ref for ref in self.gate._holders if ref() is not None]

    def test_a_holder_from_another_process_does_not_hold_up_a_fork(self):
        from tinyjobs.backends.sqlite.connection import _Token

        token = _Token()
        self.gate._holders[weakref.ref(token)] = (threading.get_ident(), os.getpid() + 1)
        started = time.monotonic()
        self.gate.wait_quiet(threading.get_ident(), os.getpid())
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(self.gate._holders, {})

    def test_a_dead_entry_of_the_forking_thread_does_not_skip_the_wait(self):
        from tinyjobs.backends.sqlite.connection import _Token

        inside, release = threading.Event(), threading.Event()

        def hold() -> None:
            with self.gate.using():
                inside.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        self.assertTrue(inside.wait(5))
        token = _Token()
        self.gate._holders[weakref.ref(token)] = (threading.get_ident(), os.getpid())
        del token
        timer = threading.Timer(0.3, release.set)
        timer.start()
        started = time.monotonic()
        self.gate.wait_quiet(threading.get_ident(), os.getpid())
        waited = time.monotonic() - started
        holder.join(5)
        timer.join(5)
        self.assertGreater(waited, 0.2)

    def test_a_holder_that_is_gone_does_not_hold_up_a_fork(self):
        from tinyjobs.backends.sqlite.connection import _Token

        token = _Token()
        self.gate._holders[weakref.ref(token)] = (threading.get_ident() + 1, os.getpid())
        del token
        started = time.monotonic()
        self.gate.wait_quiet(threading.get_ident(), os.getpid())
        self.assertLess(time.monotonic() - started, 0.5)

    def test_a_fork_waits_until_the_last_holder_leaves(self):
        entered, leave = threading.Event(), threading.Event()

        def use() -> None:
            with self.gate.using():
                entered.set()
                leave.wait(5)

        user = threading.Thread(target=use)
        user.start()
        entered.wait(5)
        threading.Timer(0.2, leave.set).start()
        started = time.monotonic()
        with self.forking():
            self.gate.wait_quiet(threading.get_ident(), os.getpid())
        took = time.monotonic() - started
        user.join(5)
        self.assertGreaterEqual(took, 0.15)
        self.assertLess(took, 1.0)

    def test_an_interrupted_entry_leaves_no_live_holder(self):
        from tests.support import sweep_interrupts

        def use() -> None:
            with self.gate.using():
                pass

        def check(point: int) -> None:
            gc.collect()
            self.assertEqual(self.live_holders(), [], point)

        sweep_interrupts(self, use, check)

    def test_an_entry_that_fails_leaves_no_holder_while_its_error_is_kept(self):
        from tinyjobs.backends.sqlite import connection

        kept, me = None, threading.get_ident()

        def interrupted(ident: int, pid: int) -> bool:
            if ident == me:
                raise KeyboardInterrupt
            return False

        with mock.patch.object(connection, "forking_elsewhere", interrupted):
            try:
                with self.gate.using():
                    pass
            except KeyboardInterrupt as error:
                kept = error
        self.assertIsNotNone(kept)
        self.assertEqual(self.live_holders(), [])

    def test_a_holder_that_is_gone_is_not_taken_for_a_nested_use(self):
        from tinyjobs.backends.sqlite import connection
        from tinyjobs.backends.sqlite.connection import _Token

        token = _Token()
        self.gate._holders[weakref.ref(token)] = (threading.get_ident(), os.getpid())
        del token
        me, asked = threading.get_ident(), []

        def elsewhere(ident: int, pid: int) -> bool:
            if ident != me:
                return False
            asked.append(ident)
            return len(asked) == 1

        with mock.patch.object(
            connection, "forking_elsewhere", elsewhere
        ), mock.patch.object(connection, "FORK_POLL_SECONDS", 0.001):
            with self.gate.using():
                pass
        self.assertEqual(len(asked), 2)
        self.assertEqual(self.gate._holders, {})

    def test_the_gate_stays_shut_while_another_thread_forks(self):
        marked, done = threading.Event(), threading.Event()

        def fork_slowly() -> None:
            with self.forking():
                marked.set()
                done.wait(5)

        forker = threading.Thread(target=fork_slowly)
        forker.start()
        marked.wait(5)
        entered = self.enter_from_another_thread()
        self.assertFalse(entered.wait(0.2))
        done.set()
        forker.join(5)
        self.assertTrue(entered.wait(5))

    def test_work_held_back_by_a_fork_starts_as_soon_as_it_is_done(self):
        from tinyjobs.backends.sqlite.connection import FORK_POLL_SECONDS

        marked, done = threading.Event(), threading.Event()
        opened: list[float] = []

        def fork() -> None:
            with self.forking():
                marked.set()
                done.wait(5)
            opened.append(time.monotonic())

        forker = threading.Thread(target=fork)
        forker.start()
        marked.wait(5)
        entered = self.enter_from_another_thread()
        time.sleep(0.1)
        done.set()
        self.assertTrue(entered.wait(5))
        latency = time.monotonic() - opened[0]
        forker.join(5)
        self.assertLess(latency, FORK_POLL_SECONDS * 10)

    def test_nested_use_does_not_wait_for_a_fork_it_is_holding_up(self):
        inside, release = threading.Event(), threading.Event()
        took: list[float] = []

        def use() -> None:
            with self.gate.using():
                inside.set()
                release.wait(5)
                started = time.monotonic()
                with self.gate.using():
                    took.append(time.monotonic() - started)

        user = threading.Thread(target=use)
        user.start()
        inside.wait(5)
        with self.forking():
            release.set()
            self.gate.wait_quiet(threading.get_ident(), os.getpid())
        user.join(10)
        self.assertLess(took[0], 0.1)

    def test_the_forking_thread_itself_passes_the_gate(self):
        with self.forking():
            started = time.monotonic()
            with self.gate.using():
                waited = time.monotonic() - started
        self.assertLess(waited, 0.1)
        self.assertTrue(self.enter_from_another_thread().wait(5))

    def test_a_fork_does_not_wait_for_a_holder_that_is_forking_too(self):
        inside, release = threading.Event(), threading.Event()

        def fork_inside() -> None:
            with self.gate.using(), self.forking():
                inside.set()
                release.wait(5)

        other = threading.Thread(target=fork_inside)
        other.start()
        inside.wait(5)
        started = time.monotonic()
        with self.forking():
            self.gate.wait_quiet(threading.get_ident(), os.getpid())
        release.set()
        other.join(5)
        self.assertLess(time.monotonic() - started, 0.5)


class InterruptSweepTests(BackendTestCase):
    def settled(self, point: int) -> None:
        from tinyjobs.backends.sqlite.connection import FORK_GATE

        flush_control()
        deadline = time.monotonic() + 5
        while any(self.app._runner._awaiting.values()) or runtime._closing:
            self.assertLess(time.monotonic(), deadline, f"left unsettled at {point}")
            time.sleep(0.002)
        self.assertFalse(runtime.holds_tinyjobs(), point)
        self.assertEqual(runtime._fork_marks, {}, point)
        while True:
            gc.collect()
            held = [ref for ref in list(FORK_GATE._holders) if ref() is not None]
            if not held or time.monotonic() >= deadline:
                break
            time.sleep(0.002)
        self.assertEqual(held, [], point)
        with FORK_GATE.using():
            pass
        self.assertEqual(FORK_GATE._holders, {}, point)

    def sweep(self, action, check=None) -> int:
        def full_check(point: int) -> None:
            self.settled(point)
            if check is not None:
                check(point)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            points = sweep_interrupts(self, action, full_check)
            gc.collect()
        self.assertGreater(points, 5)
        self.assertEqual(
            [str(w.message) for w in caught if issubclass(w.category, RuntimeWarning)], []
        )
        return points

    def test_an_interrupt_anywhere_in_a_send_leaves_tinyjobs_whole(self):
        self.app.counts()
        self.sweep(
            lambda: self.app.send("noted"),
            lambda point: self.assertIsNotNone(self.app.send("noted")),
        )

    def test_an_interrupt_anywhere_in_an_enqueue_leaves_tinyjobs_whole(self):
        noted = self.app.task(name="noted")(lambda: None)
        self.app.counts()
        self.sweep(noted.enqueue, lambda point: self.assertIsNotNone(noted.enqueue()))

    def test_an_interrupt_anywhere_in_a_start_leaves_the_app_usable(self):
        def check(point: int) -> None:
            self.app.start()
            self.assertIsInstance(self.app.counts(), dict)
            self.app.close()

        self.app.close()
        self.sweep(self.app.start, check)

    def test_an_interrupt_anywhere_in_a_close_still_closes_the_app(self):
        def check(point: int) -> None:
            if not self.app._closed:
                self.assertIsInstance(self.app.counts(), dict)
                self.app.close()
            self.assertTrue(self.app._closed, point)
            self.assertIsNone(self.app._runner._loop, point)
            self.app.start()

        self.app.counts()
        self.sweep(self.app.close, check)

    def test_an_interrupt_anywhere_in_a_drain_leaves_no_job_running(self):
        noted = self.app.task(name="noted")(lambda: None)

        def check(point: int) -> None:
            deadline = time.monotonic() + 5
            while self.app.counts().get("running", 0):
                self.assertLess(time.monotonic(), deadline, point)
                time.sleep(0.01)
            noted.enqueue()

        noted.enqueue()
        self.sweep(self.app.drain, check)

    def test_an_interrupt_anywhere_in_a_transaction_leaves_nothing_open(self):
        inner = self.app.backend._inner
        keys = iter(range(100000))

        def write() -> None:
            with inner.transaction() as connection:
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES (?, 'v')", (f"k{next(keys)}",)
                )

        def check(point: int) -> None:
            started = time.monotonic()
            self.assertIsNotNone(self.app.send("noted"))
            self.assertLess(time.monotonic() - started, 1.0, point)
            self.assertFalse(inner._connections.connection.in_transaction, point)

        self.app.counts()
        self.sweep(write, check)

    @unittest.skipUnless(
        importlib.util.find_spec("django") and importlib.util.find_spec("asgiref"),
        "needs Django",
    )
    def test_an_interrupt_anywhere_in_shared_app_leaves_one_app_per_key(self):
        from tinyjobs.contrib.django import connections

        options = {"url": f"sqlite:///{self.database}-shared"}
        self.addCleanup(connections._apps.clear)
        prepared: list = []

        def share() -> None:
            connections.shared_app("sweep", options, prepared.append)

        def check(point: int) -> None:
            first = connections.shared_app("sweep", options, prepared.append)
            self.assertIs(connections.shared_app("sweep", options, prepared.append), first)
            self.assertIn(first, prepared)
            connections._apps.clear()

        self.sweep(share, check)


@unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
class ForkTests(BackendTestCase):
    def test_a_child_forked_while_another_thread_writes_can_write(self):
        @self.app.task
        def noop():
            return None

        holding = threading.Event()

        def write_slowly():
            with self.app.backend.transaction() as connection:
                connection.execute("UPDATE meta SET value = value")
                holding.set()
                time.sleep(0.5)

        writer = threading.Thread(target=write_slowly)
        writer.start()
        holding.wait()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(10)
            code = 1
            try:
                started = time.monotonic()
                noop.enqueue()
                code = 0 if time.monotonic() - started < 2.0 else 2
            finally:
                os._exit(code)
        writer.join()
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(self.app.counts()["queued"], 1)

    def test_a_child_forked_while_the_parent_closes_closes_at_once(self):
        @self.app.task
        def noop():
            return None

        inner = self.app.backend._inner
        enqueueing = inner._enqueue_sync
        entered = threading.Event()

        def _enqueue_sync(job):
            entered.set()
            time.sleep(1.0)
            return enqueueing(job)

        def enqueue():
            with suppress(Exception):
                noop.enqueue()

        with mock.patch.object(inner, "_enqueue_sync", _enqueue_sync):
            writer = threading.Thread(target=enqueue)
            writer.start()
            self.assertTrue(entered.wait(5))
            closer = threading.Thread(target=self.app.close)
            closer.start()
            time.sleep(0.2)
            child = self.fork_quietly()
            if child == 0:
                signal.alarm(10)
                code = 1
                try:
                    started = time.monotonic()
                    self.app.close()
                    asyncio.run(asyncio.wait_for(self.app.aclose(), 5))
                    code = 0 if time.monotonic() - started < 0.5 else 2
                finally:
                    os._exit(code)
            _, status = os.waitpid(child, 0)
            closer.join(5)
            writer.join(5)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_aclose_as_the_first_call_in_a_child_leaves_the_parent_loop_alone(self):
        child = self.fork_quietly()
        if child == 0:
            signal.alarm(10)
            code = 1
            try:
                failures = []
                threading.excepthook = failures.append
                asyncio.run(self.app.aclose())
                time.sleep(0.2)
                code = 0 if not failures else 2
            finally:
                os._exit(code)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(self.app.counts().get("queued", 0), 0)

    def fork_quietly(self) -> int:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return os.fork()

    def test_forking_inside_a_transaction_neither_stalls_nor_commits_in_the_child(self):
        @self.app.task
        def noop():
            return None

        inner = self.app.backend._inner
        waiting = threading.Thread(target=noop.enqueue)
        child = None
        try:
            with inner.transaction() as connection:
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES ('marker', 'parent')"
                )
                waiting.start()
                time.sleep(0.2)
                started = time.monotonic()
                child = self.fork_quietly()
                if child == 0:
                    signal.alarm(10)
                else:
                    forked_in = time.monotonic() - started
                    connection.execute(
                        "UPDATE meta SET value = 'committed' WHERE key = 'marker'"
                    )
        except RuntimeError as error:
            if child == 0:
                os._exit(0 if "forked inside" in str(error) else 3)
            raise
        if child == 0:
            os._exit(4)
        waiting.join(10)
        _, status = os.waitpid(child, 0)
        self.assertLess(forked_in, 1.0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        marker = inner._connections.connection.execute(
            "SELECT value FROM meta WHERE key = 'marker'"
        ).fetchone()[0]
        self.assertEqual(marker, "committed")
        self.assertEqual(self.app.counts()["queued"], 1)

    def test_a_child_that_fails_inside_its_parents_transaction_leaves_it_alone(self):
        inner = self.app.backend._inner
        child = None
        try:
            with inner.transaction() as connection:
                connection.execute(
                    "INSERT INTO meta (key, value) VALUES ('marker', 'parent')"
                )
                child = self.fork_quietly()
                if child == 0:
                    signal.alarm(10)
                    raise ValueError("child failed")
        except RuntimeError as error:
            if child == 0:
                os._exit(0 if "forked inside" in str(error) else 3)
            raise
        except ValueError:
            if child == 0:
                os._exit(5)
            raise
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        marker = inner._connections.connection.execute(
            "SELECT value FROM meta WHERE key = 'marker'"
        ).fetchone()[0]
        self.assertEqual(marker, "parent")

    def test_a_sync_call_from_a_handler_that_lands_in_a_fork_is_refused(self):
        import subprocess
        import sys

        script = (
            "import os, signal, sys, warnings\n"
            "warnings.simplefilter('ignore')\n"
            "where = sys.argv[2]\n"
            "def before():\n"
            "    if where == 'before': signal.raise_signal(signal.SIGUSR1)\n"
            "def parent():\n"
            "    if where == 'parent': signal.raise_signal(signal.SIGUSR1)\n"
            "os.register_at_fork(before=before, after_in_parent=parent)\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.exceptions import InterruptedCallError\n"
            "app = TinyJobs(sys.argv[1]).start()\n"
            "seen = []\n"
            "def handler(*_):\n"
            "    try:\n"
            "        app.send('t')\n"
            "        seen.append('served')\n"
            "    except InterruptedCallError:\n"
            "        seen.append('refused')\n"
            "signal.signal(signal.SIGUSR1, handler)\n"
            "child = os.fork()\n"
            "if child == 0:\n"
            "    os._exit(0)\n"
            "os.waitpid(child, 0)\n"
            "app.send('t')\n"
            "assert seen == ['refused'], seen\n"
            "assert app.counts() == {'queued': 1}, app.counts()\n"
            "app.close()\n"
        )
        for where in ("before", "parent"):
            with self.subTest(where=where):
                database = f"{self.database}-{where}"
                finished = subprocess.run(
                    [sys.executable, "-c", script, f"sqlite:///{database}", where],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(finished.returncode, 0, finished.stderr)

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_a_signal_handler_during_a_sync_call_is_served(self):
        @self.app.task(name="slow")
        def slow():
            time.sleep(0.5)

        sent: list = []

        def handler(signum, frame) -> None:
            sent.append(self.app.send("noted", queue="later"))

        slow.enqueue()
        previous = signal.signal(signal.SIGALRM, handler)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.2)
            self.app.drain(["default"])
        finally:
            disarm_alarm(previous)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].refresh().status, JobStatus.QUEUED)

    def test_a_call_from_a_thread_holding_a_tinyjobs_lock_is_refused(self):
        from tinyjobs.exceptions import InterruptedCallError

        self.app.counts()
        with self.app._runner._guard:
            with self.assertRaises(InterruptedCallError):
                self.app.counts()
        self.assertEqual(self.app.counts(), {})

    def test_every_tinyjobs_lock_refuses_a_call_from_its_holder(self):
        from tinyjobs.exceptions import InterruptedCallError
        from tinyjobs.ids import new_job_id

        self.app.counts()
        locks = {
            "app": self.app._lock_guard,
            "managed": self.app._backend._gate,
            "ids": new_job_id._guard,
            "runner": self.app._runner._guard,
        }
        for name, lock in locks.items():
            with self.subTest(name), lock:
                with self.assertRaises(InterruptedCallError):
                    self.app.counts()
        self.assertEqual(self.app.counts(), {})

    def test_a_refused_call_makes_no_coroutine(self):
        from tinyjobs.exceptions import InterruptedCallError

        made: list = []
        with self.app._runner._guard:
            with self.assertRaises(InterruptedCallError):
                self.app._runner.run(lambda: made.append(True))
        self.assertEqual(made, [])

    def test_a_backend_call_on_a_thread_holding_a_lock_is_refused(self):
        from tinyjobs import TinyJobs
        from tinyjobs.exceptions import InterruptedCallError

        async def call() -> None:
            app = TinyJobs("sqlite:///:memory:")
            await app.ready()
            try:
                with app._lock_guard:
                    with self.assertRaises(InterruptedCallError):
                        await app.asend("t")
                self.assertEqual(await app.acounts(), {})
            finally:
                await app.aclose()

        asyncio.run(call())

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_interrupts_during_calls_leave_no_lock_behind(self):
        from tinyjobs.runtime import LOOP_THREAD_PREFIX, holds_tinyjobs

        def loop_threads() -> int:
            return len([t for t in threading.enumerate() if t.name == LOOP_THREAD_PREFIX])

        self.app.counts()
        threads = loop_threads()
        self.assertGreater(interrupt_repeatedly(lambda: self.app.send("noted")), 0)
        self.assertFalse(holds_tinyjobs())
        self.assertEqual(loop_threads(), threads)
        sent: list = []
        other = threading.Thread(target=lambda: sent.append(self.app.send("noted")))
        other.start()
        other.join(5)
        self.assertEqual(len(sent), 1)
        self.assertIsNotNone(self.app.send("noted"))

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_interrupts_during_forks_leave_the_fork_hooks_settled(self):
        parent = os.getpid()

        def reap(options: int = 0) -> None:
            while True:
                try:
                    if os.waitpid(-1, options)[0] == 0:
                        return
                except ChildProcessError:
                    return

        def fork_once() -> None:
            if os.getpid() != parent:
                os._exit(1)
            reap(os.WNOHANG)
            child = os.fork()
            if child == 0:
                os._exit(0)
            os.waitpid(child, 0)

        import sys

        reported: list = []
        self.app.counts()
        self.addCleanup(reap)
        deadline = time.monotonic() + 5
        while not runtime._pause_wanted(threading.get_ident()):
            self.assertLess(
                time.monotonic(), deadline, [t.name for t in threading.enumerate()]
            )
            time.sleep(0.01)
        with warnings.catch_warnings(), mock.patch.object(
            sys, "unraisablehook", reported.append
        ):
            warnings.simplefilter("ignore", DeprecationWarning)
            interrupts = 0
            deadline = time.monotonic() + 10
            while interrupts + len(reported) == 0 and time.monotonic() < deadline:
                interrupts += interrupt_repeatedly(fork_once, 1.0, only_tinyjobs=True)
        if os.getpid() != parent:
            os._exit(1)
        self.assertGreater(interrupts + len(reported), 0)
        unexpected = [
            report for report in reported if not isinstance(report.exc_value, KeyboardInterrupt)
        ]
        self.assertEqual(
            unexpected,
            [],
            "".join(traceback.format_exception(unexpected[0].exc_value))[-6000:]
            if unexpected
            else "",
        )
        self.assertIsNotNone(self.app.send("noted"))
        assert_fork_hooks_settled(self, self.app)

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_a_signal_handler_on_a_thread_waiting_for_a_lock_is_served(self):
        held, release = threading.Event(), threading.Event()
        sent: list = []

        def hold() -> None:
            with self.app._runner._guard:
                held.set()
                release.wait(5)

        def handler(signum, frame) -> None:
            sent.append(self.app.send("noted"))

        self.app.counts()
        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        previous = signal.signal(signal.SIGALRM, handler)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.1)
            threading.Timer(0.4, release.set).start()
            self.app.counts()
        finally:
            disarm_alarm(previous)
            release.set()
            holder.join(5)
        self.assertEqual(len(sent), 1)

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_a_close_from_an_interrupting_handler_is_refused_at_once(self):
        from tinyjobs.exceptions import InterruptedCallError

        outcome: list = []

        def handler(signum, frame) -> None:
            started = time.monotonic()
            try:
                self.app.close()
            except InterruptedCallError:
                outcome.append(time.monotonic() - started)

        self.app.counts()
        previous = signal.signal(signal.SIGALRM, handler)
        try:
            with self.app._runner._guard:
                signal.setitimer(signal.ITIMER_REAL, 0.05)
                time.sleep(0.3)
        finally:
            disarm_alarm(previous)
        self.assertEqual(len(outcome), 1)
        self.assertLess(outcome[0], 0.5)
        self.assertFalse(self.app._closed)
        self.assertEqual(self.app.counts(), {})

    def test_an_interrupt_anywhere_in_a_fork_leaves_both_sides_whole(self):
        import sys

        parent = os.getpid()
        statuses: list[int] = []
        reported: list = []

        def fork_once() -> None:
            if os.getpid() != parent:
                os._exit(1)
            child = os.fork()
            if child == 0:
                code = 1
                try:
                    signal.alarm(10)
                    code = 0 if self.app.counts().get("queued", 0) >= 0 else 1
                finally:
                    os._exit(code)
            _, status = os.waitpid(child, 0)
            statuses.append(os.waitstatus_to_exitcode(status))

        def check(point: int) -> None:
            if os.getpid() != parent:
                os._exit(1)
            assert_fork_hooks_settled(self, self.app)
            self.assertIsNotNone(self.app.send("noted"))

        self.app.counts()
        with warnings.catch_warnings(), mock.patch.object(
            sys, "unraisablehook", reported.append
        ):
            warnings.simplefilter("ignore", DeprecationWarning)
            points = sweep_interrupts(self, fork_once, check)
        self.assertGreater(points, 20)
        self.assertEqual(set(statuses), {0})
        self.assertEqual({type(report.exc_value).__name__ for report in reported}, {"Injected"})

    def test_a_child_forked_while_another_thread_makes_an_id_can_enqueue(self):
        from tinyjobs.ids import new_job_id

        self.app.counts()
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with new_job_id._guard:
                held.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        try:
            child = self.fork_quietly()
            if child == 0:
                signal.alarm(5)
                try:
                    self.app.send("noted")
                finally:
                    os._exit(0)
        finally:
            release.set()
            holder.join(5)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_forking_an_idle_started_app_costs_no_wait(self):
        self.app.counts()
        started = time.monotonic()
        child = self.fork_quietly()
        if child == 0:
            os._exit(0)
        took = time.monotonic() - started
        os.waitpid(child, 0)
        self.assertLess(took, 0.25)

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_a_fork_does_not_wait_for_a_job_on_a_worker_thread(self):
        release = threading.Event()
        took: list[float] = []

        @self.app.task
        async def slow():
            await asyncio.to_thread(release.wait, 10)

        def handler(signum, frame) -> None:
            started = time.monotonic()
            child = self.fork_quietly()
            if child == 0:
                os._exit(0)
            took.append(time.monotonic() - started)
            os.waitpid(child, 0)
            release.set()

        slow.enqueue()
        previous = signal.signal(signal.SIGALRM, handler)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.5)
            self.assertEqual(self.app.drain(), 1)
        finally:
            disarm_alarm(previous)
        self.assertLess(took[0], 1.0)

    def test_a_fork_from_inside_the_loop_guard_does_not_deadlock(self):
        import subprocess
        import sys

        script = (
            "import os, sys, warnings\n"
            "warnings.simplefilter('ignore')\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.exceptions import InterruptedCallError\n"
            "app = TinyJobs(sys.argv[1]).start()\n"
            "with app._runner._guard:\n"
            "    child = os.fork()\n"
            "    if child == 0:\n"
            "        try:\n"
            "            TinyJobs(sys.argv[1]).start()\n"
            "            os._exit(1)\n"
            "        except InterruptedCallError:\n"
            "            pass\n"
            "if child == 0:\n"
            "    os._exit(0 if TinyJobs(sys.argv[1]).start().counts() == {} else 1)\n"
            "_, status = os.waitpid(child, 0)\n"
            "app.counts()\n"
            "app.close()\n"
            "sys.exit(os.waitstatus_to_exitcode(status))\n"
        )
        finished = subprocess.run(
            [sys.executable, "-c", script, f"sqlite:///{self.database}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(finished.returncode, 0, finished.stderr)

    def test_forking_an_idle_started_app_raises_no_thread_warning(self):
        fork_without_warning(self, f"sqlite:///{self.database}")

    def test_a_forked_child_connects_on_its_own(self):
        @self.app.task
        def noop():
            return None

        noop.enqueue()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(10)
            code = 1
            try:
                noop.enqueue()
                self.app.close()
                code = 0
            finally:
                os._exit(code)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(self.app.counts()["queued"], 2)
