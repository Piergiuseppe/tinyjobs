from __future__ import annotations

import asyncio
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from tinyjobs import Abandon, Retry, TinyJobs
from tinyjobs.exceptions import UnknownTaskError
from tinyjobs.services.execution import current_job
from tinyjobs.status import ExecutionStatus, JobStatus

SETTLE_TIMEOUT = 15.0


class WorkerTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")
        self.calls: list[str] = []

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def make_worker(self, **options):
        options.setdefault("concurrency", 4)
        options.setdefault("lease_seconds", 60.0)
        options.setdefault("reap_interval", 60.0)
        options.setdefault("shutdown_timeout", 5.0)
        return self.app.worker(**options)

    async def settle(self, worker, jobs, timeout: float = SETTLE_TIMEOUT) -> None:
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            statuses = [(await self.app.aget(job.id)).status for job in jobs]
            if all(status.is_terminal for status in statuses):
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def run_briefly(self, worker, seconds: float = 0.5) -> None:
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(seconds)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def status_of(self, job) -> JobStatus:
        return (await self.app.aget(job.id)).status


class ExecutionTests(WorkerTestCase):
    async def test_async_task_runs_and_succeeds(self):
        @self.app.task
        async def handler(value):
            self.calls.append(f"async:{value}")
            return value * 2

        job = await handler.aenqueue(21)
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual(self.calls, ["async:21"])

    async def test_sync_task_runs_in_a_thread(self):
        @self.app.task
        def handler(value):
            self.calls.append(f"sync:{value}")
            return value

        job = await handler.aenqueue("x")
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual(self.calls, ["sync:x"])

    async def test_async_and_sync_tasks_share_one_worker(self):
        @self.app.task
        async def first():
            self.calls.append("first")

        @self.app.task
        def second():
            self.calls.append("second")

        jobs = [await first.aenqueue(), await second.aenqueue()]
        await self.settle(self.make_worker(), jobs)
        self.assertEqual(sorted(self.calls), ["first", "second"])

    async def test_arguments_survive_the_round_trip(self):
        from datetime import datetime
        from decimal import Decimal

        received = {}

        @self.app.task
        def handler(when, amount, tags):
            received.update(when=when, amount=amount, tags=tags)

        job = await handler.aenqueue(
            datetime(2026, 8, 31, 12, 0), Decimal("19.99"), {"a", "b"}
        )
        await self.settle(self.make_worker(), [job])
        self.assertEqual(received["when"], datetime(2026, 8, 31, 12, 0))
        self.assertEqual(received["amount"], Decimal("19.99"))
        self.assertEqual(received["tags"], {"a", "b"})


class ForcedStopTests(WorkerTestCase):
    async def test_a_forced_stop_waits_for_the_releases_it_started(self):
        started = asyncio.Event()

        @self.app.task
        async def long_job():
            started.set()
            await asyncio.sleep(10)

        job = await long_job.aenqueue()
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())
        await asyncio.wait_for(started.wait(), 5)
        inner = self.app.backend._inner
        release = inner.release

        async def slow_release(job, delay=0.0):
            await asyncio.sleep(0.05)
            return await release(job, delay)

        with mock.patch.object(inner, "release", slow_release):
            worker.stop()
            worker._request_force()
            await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(await self.status_of(job), JobStatus.QUEUED)


class CurrentJobTests(WorkerTestCase):
    async def test_a_sync_task_sees_the_job_it_runs_as(self):
        @self.app.task
        def handler():
            self.calls.append(current_job.get().id)

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertEqual(self.calls, [job.id])

    async def test_an_async_task_sees_the_job_it_runs_as(self):
        @self.app.task
        async def handler():
            self.calls.append(current_job.get().id)

        jobs = [await handler.aenqueue() for _ in range(3)]
        await self.settle(self.make_worker(), jobs)
        self.assertEqual(sorted(self.calls), sorted(job.id for job in jobs))

    async def test_the_job_is_unset_once_a_drain_returns(self):
        @self.app.task
        def handler():
            return None

        await handler.aenqueue()
        await self.app.adrain()
        self.assertIsNone(current_job.get(None))


class FailureTests(WorkerTestCase):
    async def test_failing_task_without_retries_is_terminal(self):
        @self.app.task(max_attempts=1)
        def handler():
            raise ValueError("boom")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        stored = await self.app.aget(job.id)
        self.assertIs(stored.status, JobStatus.FAILED)
        self.assertIn("boom", stored.last_error)
        self.assertEqual(stored.attempts, 1)

    async def test_the_traceback_is_recorded_on_the_execution(self):
        @self.app.task(max_attempts=1)
        def handler():
            raise ValueError("boom")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        execution = (await self.app.aexecutions(job.id))[0]
        self.assertIs(execution.status, ExecutionStatus.FAILED)
        self.assertEqual(execution.error_type, "ValueError")
        self.assertIn("ValueError: boom", execution.traceback)

    async def test_task_retries_until_it_succeeds(self):
        attempts = []

        @self.app.task(max_attempts=3, retry_base=0.01, retry_jitter=False)
        def handler():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("flaky")
            return "ok"

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual((await self.app.aget(job.id)).attempts, 3)
        self.assertEqual(len(await self.app.aexecutions(job.id)), 3)

    async def test_retries_stop_when_the_budget_is_spent(self):
        @self.app.task(max_attempts=3, retry_base=0.01, retry_jitter=False)
        def handler():
            self.calls.append("try")
            raise ConnectionError("always")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertEqual(len(self.calls), 3)

    async def test_unlisted_exceptions_are_not_retried(self):
        @self.app.task(max_attempts=5, retry_on=(ConnectionError,), retry_base=0.01)
        def handler():
            self.calls.append("try")
            raise ValueError("permanent")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertEqual(len(self.calls), 1)

    async def test_abandon_skips_the_remaining_attempts(self):
        @self.app.task(max_attempts=5, retry_base=0.01)
        def handler():
            self.calls.append("try")
            raise Abandon("nothing to do")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertEqual(len(self.calls), 1)

    async def test_explicit_retry_controls_its_own_delay(self):
        @self.app.task(max_attempts=3, retry_base=100.0)
        def handler():
            self.calls.append("try")
            if len(self.calls) == 1:
                raise Retry(in_=0.01)
            return "ok"

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual(len(self.calls), 2)


class AttemptBudgetTests(WorkerTestCase):
    async def test_the_budget_recorded_on_the_job_wins_over_the_declaration(self):
        @self.app.task(max_attempts=5, retry_base=0.01, retry_jitter=False)
        def handler():
            self.calls.append("try")
            raise ConnectionError("always")

        job = await handler.with_options(max_attempts=2).aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual((await self.app.aget(job.id)).attempts, 2)

    async def test_a_job_sent_by_name_retries_like_the_registered_task(self):
        @self.app.task(name="known.task", max_attempts=3, retry_base=0.01)
        def handler():
            self.calls.append("try")
            raise ConnectionError("always")

        job = await self.app.asend("known.task")
        await self.settle(self.make_worker(), [job])
        self.assertEqual(len(self.calls), 3)


class TimeoutTests(WorkerTestCase):
    async def test_async_task_is_cancelled_at_the_timeout(self):
        @self.app.task(timeout=0.05, max_attempts=1)
        async def handler():
            await asyncio.sleep(30)

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIs(
            (await self.app.aexecutions(job.id))[0].status, ExecutionStatus.TIMEOUT
        )

    async def test_sync_task_timeout_fails_the_job_without_killing_the_thread(self):
        finished = []

        @self.app.task(timeout=0.05, max_attempts=1)
        def handler():
            time.sleep(0.4)
            finished.append(True)

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIs(
            (await self.app.aexecutions(job.id))[0].status, ExecutionStatus.TIMEOUT
        )
        await asyncio.sleep(0.5)
        self.assertEqual(finished, [True])


class UndecodableJobTests(WorkerTestCase):
    async def test_a_payload_the_worker_cannot_decode_fails_the_job(self):
        @self.app.task(max_attempts=1)
        def handler(value):
            self.calls.append(value)

        job = await handler.aenqueue(1)
        import sqlite3

        outside = sqlite3.connect(self.app.backend.database, isolation_level=None)
        outside.execute(
            "UPDATE jobs SET payload = ? WHERE id = ?",
            (b'{"args":[{"$t":"gone","$v":1}],"kwargs":{}}', job.id),
        )
        outside.close()

        await self.settle(self.make_worker(), [job])
        stored = await self.app.aget(job.id)
        self.assertIs(stored.status, JobStatus.FAILED)
        self.assertIn("gone", stored.last_error)
        self.assertEqual(self.calls, [])

    async def test_a_result_that_cannot_be_encoded_fails_the_job(self):
        class Unserializable:
            pass

        @self.app.task(store_result=True, max_attempts=1)
        def handler():
            return Unserializable()

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)


class RoutingTests(WorkerTestCase):
    async def test_a_worker_only_consumes_the_queues_it_subscribes_to(self):
        @self.app.task(queue="slow")
        def handler():
            self.calls.append("ran")

        job = await handler.aenqueue()
        await self.run_briefly(self.make_worker(queues=["default"]))
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)
        self.assertEqual(self.calls, [])

        await self.settle(self.make_worker(queues=["slow"]), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)

    async def test_higher_priority_jobs_run_first(self):
        order = []

        @self.app.task
        def handler(label):
            order.append(label)

        low = await handler.with_options(priority=0).aenqueue("low")
        high = await handler.with_options(priority=10).aenqueue("high")
        await self.settle(self.make_worker(concurrency=1), [low, high])
        self.assertEqual(order[0], "high")

    async def test_cancelled_jobs_are_never_executed(self):
        @self.app.task
        def handler():
            self.calls.append("ran")

        job = await handler.aenqueue()
        await self.app.acancel(job.id)
        await self.run_briefly(self.make_worker())
        self.assertEqual(self.calls, [])
        self.assertIs(await self.status_of(job), JobStatus.CANCELLED)

    async def test_an_unregistered_task_is_left_queued_not_failed(self):
        job = await self.app.asend("nobody.knows.this", args=[1])
        await self.run_briefly(self.make_worker(unknown_task_delay=30.0))
        stored = await self.app.aget(job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertGreater(stored.run_at, self.app.clock.now())

    async def test_a_sub_second_hand_back_delay_is_logged_as_it_is(self):
        await self.app.asend("nobody.knows.this")
        worker = self.make_worker(unknown_task_delay=0.25, unknown_task_max_delay=0.25)
        with self.assertLogs("tinyjobs.worker", "ERROR") as logs:
            await self.run_briefly(worker, seconds=0.1)
        self.assertIn("retrying it in 0.25s", logs.output[0])

    async def test_a_drain_hands_back_a_task_it_does_not_know(self):
        job = await self.app.asend("nobody.knows.this")
        with self.assertRaises(UnknownTaskError):
            await self.app.adrain()
        stored = await self.app.aget(job.id)
        self.assertEqual(
            (stored.status, stored.attempts, stored.hand_backs), (JobStatus.QUEUED, 0, 1)
        )
        self.assertEqual(await self.app.aexecutions(job.id), [])

    async def test_a_cancelled_drain_puts_its_job_back(self):
        @self.app.task
        async def slow():
            await asyncio.sleep(1.0)

        job = await slow.aenqueue()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.app.adrain(), 0.2)
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)

    async def test_a_stopped_drain_puts_back_a_job_that_outlasts_its_grace(self):
        from tinyjobs.worker import Drain

        @self.app.task
        async def slow():
            await asyncio.sleep(5.0)

        job = await slow.aenqueue()
        drain = Drain(self.app, shutdown_timeout=0.1)
        running = asyncio.create_task(drain.run())
        while await self.status_of(job) is not JobStatus.RUNNING:
            await asyncio.sleep(0.01)
        drain.stop()
        self.assertEqual(await asyncio.wait_for(running, 2.0), 0)
        self.assertTrue(drain.finished)
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)

    async def test_closing_the_app_under_a_worker_logs_no_errors(self):
        worker = self.make_worker(reap_interval=0.05, poll_maximum=0.02)
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.2)
        with self.assertNoLogs("tinyjobs.worker", "ERROR"):
            await self.app.aclose()
            await asyncio.wait_for(runner, timeout=10)

    async def test_an_unregistered_task_is_handed_back_less_and_less_often(self):
        job = await self.app.asend("nobody.knows.this")
        worker = self.make_worker(
            unknown_task_delay=0.1, unknown_task_max_delay=0.4, poll_maximum=0.02
        )
        await self.run_briefly(worker, seconds=2.0)
        stored = await self.app.aget(job.id)
        self.assertLessEqual(stored.hand_backs, 8)
        self.assertEqual(stored.attempts, 0)

    async def test_attempts_that_ran_before_a_hand_back_do_not_lengthen_it(self):
        @self.app.task(max_attempts=10)
        def flaky():
            raise Retry(in_=0.3)

        job = await flaky.aenqueue()
        knowing = self.make_worker(poll_maximum=0.02)
        runner = asyncio.create_task(knowing.start())
        while (await self.app.aget(job.id)).attempts < 3:
            await asyncio.sleep(0.02)
        knowing.stop()
        await asyncio.wait_for(runner, timeout=10)

        unaware = TinyJobs(self.app.url)
        try:
            await asyncio.sleep(0.3)
            await self.run_briefly(unaware.worker(poll_maximum=0.02), seconds=0.5)
        finally:
            await unaware.aclose()
        stored = await self.app.aget(job.id)
        self.assertEqual((stored.attempts, stored.hand_backs), (3, 1))
        self.assertLessEqual(stored.run_at - self.app.clock.now(), 60.0)

    async def test_a_hand_back_does_not_use_up_an_attempt(self):
        job = await self.app.asend("tests.flaky_once", max_attempts=2)
        unaware = self.make_worker(
            unknown_task_delay=0.05, unknown_task_max_delay=0.05, poll_maximum=0.02
        )
        await self.run_briefly(unaware, seconds=0.5)
        self.assertGreaterEqual((await self.app.aget(job.id)).hand_backs, 2)

        failures = []

        @self.app.task(name="tests.flaky_once", max_attempts=2, retry_base=0.05)
        def flaky_once():
            if not failures:
                failures.append(1)
                raise RuntimeError("first attempt")

        await asyncio.sleep(0.1)
        await self.settle(self.make_worker(poll_maximum=0.02), [job])
        stored = await self.app.aget(job.id)
        self.assertIs(stored.status, JobStatus.SUCCESS)
        self.assertEqual(stored.attempts, 2)
        self.assertEqual(
            [execution.attempt for execution in await self.app.aexecutions(job.id)],
            [1, 2],
        )

    async def test_a_hand_back_delay_above_the_maximum_is_kept(self):
        job = await self.app.asend("nobody.knows.this")
        await self.run_briefly(self.make_worker(unknown_task_delay=7200.0))
        stored = await self.app.aget(job.id)
        self.assertGreater(stored.run_at - self.app.clock.now(), 7000.0)

    async def test_the_hand_back_delay_never_exceeds_its_maximum(self):
        job = await self.app.asend("nobody.knows.this")
        worker = self.make_worker(
            unknown_task_delay=0.05, unknown_task_max_delay=0.1, poll_maximum=0.02
        )
        await self.run_briefly(worker, seconds=1.0)
        stored = await self.app.aget(job.id)
        self.assertLessEqual(stored.run_at - self.app.clock.now(), 0.1)


class SignalHandlerTests(WorkerTestCase):
    async def test_a_stopped_worker_puts_back_the_handlers_it_replaced(self):
        import signal

        def mine(signum, frame) -> None:
            return None

        numbers = (signal.SIGINT, signal.SIGTERM)
        for number in numbers:
            self.addCleanup(signal.signal, number, signal.signal(number, mine))
        await self.run_briefly(self.make_worker(), 0.2)
        self.assertEqual([signal.getsignal(number) for number in numbers], [mine, mine])

    async def wait_until(self, condition, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not condition():
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.01)

    async def test_a_worker_stopping_first_hands_the_signals_to_the_one_still_running(self):
        import os
        import signal

        received: list[int] = []
        previous = signal.signal(signal.SIGTERM, lambda number, frame: received.append(number))
        self.addCleanup(signal.signal, signal.SIGTERM, previous)
        mine = signal.getsignal(signal.SIGTERM)
        first, second = self.make_worker(queues=["a"]), self.make_worker(queues=["b"])
        running = asyncio.create_task(first.start())
        await asyncio.sleep(0.1)
        stopping = asyncio.create_task(second.start())
        await asyncio.sleep(0.1)
        second.stop()
        await asyncio.wait_for(stopping, timeout=10)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(running, timeout=10)
        self.assertEqual(received, [])
        self.assertIs(signal.getsignal(signal.SIGTERM), mine)

    async def test_a_stopped_worker_gives_the_loop_its_own_handler_back(self):
        import os
        import signal

        received: list[str] = []
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, received.append, "mine")
        self.addCleanup(loop.remove_signal_handler, signal.SIGTERM)
        await self.run_briefly(self.make_worker(), 0.2)
        os.kill(os.getpid(), signal.SIGTERM)
        await self.wait_until(lambda: received)
        self.assertEqual(received, ["mine"])

    @unittest.skipUnless(os.name == "posix", "needs sigaction")
    async def test_a_stopped_worker_puts_back_a_handler_set_outside_python(self):
        import ctypes
        import signal

        from tinyjobs.worker import signals

        libc = ctypes.CDLL(None)
        libc.signal.restype = ctypes.c_void_p
        libc.signal.argtypes = [ctypes.c_int, ctypes.c_void_p]
        before = signals._raw_action(signal.SIGTERM)
        self.addCleanup(signals._set_raw_action, signal.SIGTERM, before)
        libc.signal(signal.SIGTERM, ctypes.c_void_p(1))
        ignored = signals._raw_action(signal.SIGTERM)
        await self.run_briefly(self.make_worker(), 0.2)
        self.assertEqual(
            signals._pointer(signals._raw_action(signal.SIGTERM)), signals._pointer(ignored)
        )

    @unittest.skipUnless(os.name == "posix", "needs sigaction")
    async def test_a_stopped_worker_keeps_sigint_ignored_outside_python(self):
        import ctypes
        import signal

        from tinyjobs.worker import signals

        libc = ctypes.CDLL(None)
        libc.signal.restype = ctypes.c_void_p
        libc.signal.argtypes = [ctypes.c_int, ctypes.c_void_p]
        self.addCleanup(signal.signal, signal.SIGINT, signal.getsignal(signal.SIGINT))
        self.addCleanup(signals._set_raw_action, signal.SIGINT, signals._raw_action(signal.SIGINT))
        libc.signal(signal.SIGINT, ctypes.c_void_p(1))
        ignored = signals._raw_action(signal.SIGINT)
        await self.run_briefly(self.make_worker(), 0.2)
        self.assertEqual(
            signals._pointer(signals._raw_action(signal.SIGINT)), signals._pointer(ignored)
        )

    async def test_one_signal_stops_every_worker_on_the_loop(self):
        import os
        import signal

        self.addCleanup(signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        workers = [self.make_worker(queues=["a"]), self.make_worker(queues=["b"])]
        runners = [asyncio.create_task(worker.start()) for worker in workers]
        await asyncio.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(asyncio.gather(*runners), timeout=10)
        self.assertEqual([worker.finished for worker in workers], [True, True])

    async def test_a_handler_replaced_while_a_worker_ran_outlives_the_next_worker(self):
        import signal

        def mine(signum, frame) -> None:
            return None

        self.addCleanup(signal.signal, signal.SIGINT, signal.getsignal(signal.SIGINT))
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.2)
        signal.signal(signal.SIGINT, mine)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        await self.run_briefly(self.make_worker(), 0.2)
        self.assertIs(signal.getsignal(signal.SIGINT), mine)

    async def test_a_loop_handler_added_while_a_worker_ran_outlives_the_next_worker(self):
        import os
        import signal

        received: list[str] = []
        loop = asyncio.get_running_loop()
        fallback = signal.signal(signal.SIGTERM, lambda number, frame: None)
        self.addCleanup(signal.signal, signal.SIGTERM, fallback)
        self.addCleanup(loop.remove_signal_handler, signal.SIGTERM)
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.2)
        loop.add_signal_handler(signal.SIGTERM, received.append, "mine")
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        await self.run_briefly(self.make_worker(), 0.2)
        os.kill(os.getpid(), signal.SIGTERM)
        await self.wait_until(lambda: received)
        self.assertEqual(received, ["mine"])

    def test_a_worker_on_uvloop_stops_on_a_signal_and_puts_back_the_handler(self):
        import os
        import signal

        try:
            import uvloop
        except ImportError:
            self.skipTest("needs uvloop")
        received: list[int] = []
        self.addCleanup(signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        signal.signal(signal.SIGTERM, lambda number, frame: received.append(number))
        mine = signal.getsignal(signal.SIGTERM)
        worker = self.make_worker()

        async def main() -> None:
            asyncio.get_running_loop().call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(worker.start(), timeout=10)

        uvloop.run(main())
        self.assertEqual(received, [])
        self.assertIs(signal.getsignal(signal.SIGTERM), mine)
        os.kill(os.getpid(), signal.SIGTERM)
        deadline = time.monotonic() + 5
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(received, [signal.SIGTERM])

    def uvloop_or_skip(self):
        try:
            import uvloop
        except ImportError:
            self.skipTest("needs uvloop")
        return uvloop

    def run_uvloop_with_a_loop_handler(self, added_while_running: bool) -> list[str]:
        import os
        import signal

        uvloop = self.uvloop_or_skip()
        received: list[str] = []
        self.addCleanup(signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        worker = self.make_worker()

        async def main() -> None:
            loop = asyncio.get_running_loop()
            if not added_while_running:
                loop.add_signal_handler(signal.SIGTERM, received.append, "mine")
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.2)
            if added_while_running:
                loop.add_signal_handler(signal.SIGTERM, received.append, "mine")
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
            try:
                self.assertIsNot(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
                os.kill(os.getpid(), signal.SIGTERM)
                await self.wait_until(lambda: received)
            finally:
                loop.remove_signal_handler(signal.SIGTERM)

        uvloop.run(main())
        return received

    def test_a_loop_handler_added_on_uvloop_while_a_worker_ran_outlives_it(self):
        self.assertEqual(self.run_uvloop_with_a_loop_handler(added_while_running=True), ["mine"])

    def test_a_stopped_worker_on_uvloop_keeps_the_loop_handler_it_found(self):
        self.assertEqual(self.run_uvloop_with_a_loop_handler(added_while_running=False), ["mine"])

    def test_a_worker_on_uvloop_sees_a_signal_alongside_the_loop_handler_it_found(self):
        import os
        import signal

        uvloop = self.uvloop_or_skip()
        received: list[str] = []
        self.addCleanup(signal.signal, signal.SIGTERM, signal.getsignal(signal.SIGTERM))
        worker = self.make_worker()

        async def main() -> None:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, received.append, "mine")
            try:
                loop.call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)
                await asyncio.wait_for(worker.start(), timeout=10)
                await self.wait_until(lambda: received)
            finally:
                loop.remove_signal_handler(signal.SIGTERM)

        uvloop.run(main())
        self.assertEqual(received, ["mine"])

    async def test_without_loop_signals_the_worker_still_running_keeps_the_signals(self):
        import os
        import signal

        received: list[int] = []
        previous = signal.signal(signal.SIGTERM, lambda number, frame: received.append(number))
        self.addCleanup(signal.signal, signal.SIGTERM, previous)
        mine = signal.getsignal(signal.SIGTERM)
        loop = asyncio.get_running_loop()
        with mock.patch.object(loop, "add_signal_handler", side_effect=NotImplementedError):
            first, second = self.make_worker(queues=["a"]), self.make_worker(queues=["b"])
            stopping = asyncio.create_task(first.start())
            await asyncio.sleep(0.1)
            running = asyncio.create_task(second.start())
            await asyncio.sleep(0.1)
            first.stop()
            await asyncio.wait_for(stopping, timeout=10)
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(running, timeout=5)
        self.assertEqual(received, [])
        self.assertIs(signal.getsignal(signal.SIGTERM), mine)

    def test_a_worker_off_the_main_thread_runs_without_signal_handlers(self):
        import threading

        worker = self.make_worker()
        errors: list[BaseException] = []

        def run() -> None:
            try:
                worker.run()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        deadline = time.monotonic() + 5
        while worker._loop is None and time.monotonic() < deadline:
            time.sleep(0.01)
        worker.stop()
        thread.join(10)
        self.assertEqual(errors, [])


class AbandonedClaimTests(unittest.TestCase):
    def test_a_claim_cancelled_as_its_loop_ends_hands_its_job_back(self):
        import threading

        with tempfile.TemporaryDirectory() as directory:
            app = TinyJobs(f"sqlite:///{Path(directory) / 'jobs.db'}")
            self.addCleanup(app.close)
            app.task(name="noted")(lambda: None)
            job = app.send("noted")
            inner = app.backend._inner
            entered, proceed = threading.Event(), threading.Event()
            claim_sync = inner._claim_sync

            def held_claim(*args):
                entered.set()
                proceed.wait(5)
                return claim_sync(*args)

            async def main() -> None:
                caller = asyncio.ensure_future(
                    app.backend.claim(
                        worker_id="w", queues=("default",), limit=1, lease_seconds=3600.0
                    )
                )
                while not entered.is_set():
                    await asyncio.sleep(0.005)
                caller.cancel()

            with mock.patch.object(inner, "_claim_sync", held_claim):
                asyncio.run(main())
                proceed.set()
                deadline = time.monotonic() + 5
                while app.get(job.id).status is not JobStatus.QUEUED:
                    self.assertLess(time.monotonic(), deadline, app.get(job.id).status)
                    time.sleep(0.01)
            self.assertEqual(app.get(job.id).hand_backs, 1)

    def test_a_sync_drain_interrupted_as_the_process_exits_hands_its_job_back(self):
        import subprocess
        import sys

        script = (
            "import os, signal, sqlite3, sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "app = TinyJobs(sys.argv[1])\n"
            "app.task(name='noted')(lambda: None)\n"
            "app.send('noted')\n"
            "def block():\n"
            "    other = sqlite3.connect(sys.argv[2], isolation_level=None)\n"
            "    other.execute('BEGIN IMMEDIATE')\n"
            "    time.sleep(1.0)\n"
            "    other.execute('COMMIT')\n"
            "    other.close()\n"
            "threading.Thread(target=block).start()\n"
            "time.sleep(0.1)\n"
            "threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT)).start()\n"
            "app.drain()\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.db"
            url = f"sqlite:///{path}"
            finished = subprocess.run(
                [sys.executable, "-c", script, url, str(path)],
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertIn("KeyboardInterrupt", finished.stderr)
            app = TinyJobs(url)
            try:
                jobs = app.jobs().items
            finally:
                app.close()
        self.assertEqual([(job.status, job.attempts) for job in jobs], [(JobStatus.QUEUED, 0)])


    def test_a_worker_outliving_the_main_thread_fails_no_job_it_cannot_start(self):
        import subprocess
        import sys

        script = (
            "import sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "app = TinyJobs(sys.argv[1])\n"
            "app.task(name='slow', max_attempts=1)(lambda: time.sleep(0.02))\n"
            "for _ in range(200):\n"
            "    app.send('slow')\n"
            "worker = app.worker(handle_signals=False, concurrency=4, scheduler=False,\n"
            "                    poll_minimum=0.01, poll_maximum=0.05)\n"
            "threading.Thread(target=worker.run).start()\n"
            "threading.Timer(1.5, worker.stop).start()\n"
            "time.sleep(0.2)\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            url = f"sqlite:///{Path(directory) / 'jobs.db'}"
            finished = subprocess.run(
                [sys.executable, "-c", script, url],
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(finished.returncode, 0, finished.stderr)
            app = TinyJobs(url)
            try:
                counts = app.counts()
            finally:
                app.close()
        self.assertEqual(counts.get("failed", 0), 0, counts)
        self.assertEqual(counts.get("running", 0), 0, counts)


    def test_a_daemon_worker_alive_at_exit_does_not_crash_the_process(self):
        import subprocess
        import sys

        script = (
            "import sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "app = TinyJobs(sys.argv[1])\n"
            "@app.task\n"
            "async def hop():\n"
            "    await hop.aenqueue()\n"
            "for _ in range(8):\n"
            "    hop.enqueue()\n"
            "worker = app.worker(handle_signals=False, scheduler=False, concurrency=8)\n"
            "threading.Thread(target=worker.run, daemon=True).start()\n"
            "time.sleep(float(sys.argv[2]))\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            for run in range(5):
                url = f"sqlite:///{Path(directory) / f'jobs{run}.db'}"
                finished = subprocess.run(
                    [sys.executable, "-c", script, url, str(0.3 + run * 0.05)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                self.assertEqual(finished.returncode, 0, finished.stderr[-2000:])
                self.assertNotIn("Exception ignored", finished.stderr)


class CancelledClaimTests(WorkerTestCase):
    async def wait_for_status(self, job, status: JobStatus) -> None:
        deadline = time.monotonic() + 5
        while await self.status_of(job) is not status:
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.01)

    async def test_a_claim_cancelled_in_flight_hands_its_jobs_back(self):
        import threading

        @self.app.task
        async def noted():
            self.calls.append("ran")

        job = await noted.aenqueue()
        inner = self.app.backend._inner
        entered, proceed = threading.Event(), threading.Event()
        claim_sync = inner._claim_sync

        def held_claim(*args):
            entered.set()
            proceed.wait(5)
            return claim_sync(*args)

        with mock.patch.object(inner, "_claim_sync", held_claim):
            claiming = asyncio.ensure_future(
                self.app.backend.claim(
                    worker_id="w", queues=("default",), limit=1, lease_seconds=60.0
                )
            )
            while not entered.is_set():
                await asyncio.sleep(0.005)
            claiming.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await claiming
            proceed.set()
            await self.wait_for_status(job, JobStatus.QUEUED)
        stored = await self.app.aget(job.id)
        self.assertEqual((stored.attempts, stored.hand_backs), (0, 1))

    async def test_closing_waits_for_the_jobs_a_cancelled_claim_hands_back(self):
        import threading

        @self.app.task
        async def noted():
            self.calls.append("ran")

        await noted.aenqueue()
        backend = self.app.backend
        inner = backend._inner
        entered, proceed = threading.Event(), threading.Event()
        claim_sync, hand_back, close = inner._claim_sync, inner.hand_back, inner.close
        order: list[str] = []

        def held_claim(*args):
            entered.set()
            proceed.wait(5)
            return claim_sync(*args)

        async def slow_hand_back(*args, **kwargs):
            await asyncio.sleep(0.2)
            result = await hand_back(*args, **kwargs)
            order.append("handed back")
            return result

        async def noted_close():
            order.append("closed")
            await close()

        with mock.patch.object(inner, "_claim_sync", held_claim), mock.patch.object(
            inner, "hand_back", slow_hand_back
        ), mock.patch.object(inner, "close", noted_close):
            claiming = asyncio.ensure_future(
                backend.claim(
                    worker_id="w", queues=("default",), limit=1, lease_seconds=60.0
                )
            )
            while not entered.is_set():
                await asyncio.sleep(0.005)
            claiming.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await claiming
            proceed.set()
            await backend.close()
        self.assertEqual(order, ["handed back", "closed"])

    async def test_a_drain_cancelled_while_it_claims_puts_the_job_back(self):
        import threading

        @self.app.task
        async def noted():
            self.calls.append("ran")

        job = await noted.aenqueue()
        inner = self.app.backend._inner
        entered, proceed = threading.Event(), threading.Event()
        claim_sync = inner._claim_sync

        def held_claim(*args):
            entered.set()
            proceed.wait(5)
            return claim_sync(*args)

        with mock.patch.object(inner, "_claim_sync", held_claim):
            draining = asyncio.ensure_future(self.app.adrain())
            while not entered.is_set():
                await asyncio.sleep(0.005)
            draining.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await draining
            proceed.set()
            await self.wait_for_status(job, JobStatus.QUEUED)
        self.assertEqual(self.calls, [])
        self.assertEqual(await self.app.adrain(), 1)
        self.assertEqual(self.calls, ["ran"])

    async def test_a_worker_job_cancelled_before_it_starts_is_released(self):
        @self.app.task
        async def noted():
            self.calls.append("ran")

        job = await noted.aenqueue()
        worker = self.make_worker()
        launch, cancelled = worker._launch, []

        def launch_cancelled(claimed) -> None:
            launch(claimed)
            if not cancelled:
                cancelled.append(claimed.id)
                for task in worker._running:
                    task.cancel()
                worker.stop()

        with mock.patch.object(worker, "_launch", launch_cancelled):
            await asyncio.wait_for(worker.start(), 10)
        self.assertEqual(cancelled, [job.id])
        self.assertEqual(self.calls, [])
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)


class ShutdownTests(WorkerTestCase):
    async def test_in_flight_jobs_finish_during_a_graceful_stop(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(0.3)
            self.calls.append("finished")

        job = await handler.aenqueue()
        worker = self.make_worker(shutdown_timeout=5.0)
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.1)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(self.calls, ["finished"])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)

    async def test_a_claim_the_backend_never_answers_does_not_outlast_the_shutdown_timeout(self):
        await self.app.ready()
        claimed = asyncio.Event()
        given_up: list[bool] = []

        async def stuck(**_):
            claimed.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                given_up.append(True)
                raise

        worker = self.make_worker(shutdown_timeout=0.5)
        with mock.patch.object(self.app.backend, "claim", stuck):
            runner = asyncio.create_task(worker.start())
            await asyncio.wait_for(claimed.wait(), 5)
            started = time.monotonic()
            worker.stop()
            await asyncio.wait_for(runner, timeout=5)
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(given_up, [True])

    async def test_a_second_signal_gives_up_a_claim_still_waiting_at_once(self):
        await self.app.ready()
        claimed = asyncio.Event()

        async def stuck(**_):
            claimed.set()
            await asyncio.Event().wait()

        worker = self.make_worker(shutdown_timeout=10.0)
        with mock.patch.object(self.app.backend, "claim", stuck):
            runner = asyncio.create_task(worker.start())
            await asyncio.wait_for(claimed.wait(), 5)
            started = time.monotonic()
            worker.stop()
            await asyncio.sleep(0.1)
            worker._request_force()
            await asyncio.wait_for(runner, timeout=15)
        self.assertLess(time.monotonic() - started, 2)

    async def test_a_claim_answering_during_the_shutdown_timeout_still_runs_its_jobs(self):
        @self.app.task
        async def handler():
            self.calls.append("finished")

        job = await handler.aenqueue()
        claim = self.app.backend.claim
        stopping = asyncio.Event()

        async def slow(**options):
            await stopping.wait()
            await asyncio.sleep(0.2)
            return await claim(**options)

        worker = self.make_worker(shutdown_timeout=5.0)
        with mock.patch.object(self.app.backend, "claim", slow):
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.1)
            worker.stop()
            stopping.set()
            await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(self.calls, ["finished"])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)

    async def test_a_graceful_stop_keeps_heartbeating_the_jobs_it_waits_for(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(1.5)

        job = await handler.aenqueue()
        beats: list[float] = []
        heartbeat = self.app.backend.heartbeat

        async def counted(*args, **kwargs):
            beats.append(time.monotonic())
            return await heartbeat(*args, **kwargs)

        worker = self.make_worker(lease_seconds=3.0, shutdown_timeout=5.0)
        with mock.patch.object(self.app.backend, "heartbeat", counted):
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.1)
            stopped = time.monotonic()
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
        self.assertTrue([beat for beat in beats if beat > stopped])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)

    async def test_a_release_that_never_answers_ends_with_the_shutdown_timeout(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(30)

        async def silent(*args, **kwargs):
            await asyncio.Event().wait()

        await handler.aenqueue()
        worker = self.make_worker(shutdown_timeout=0.5)
        with mock.patch.object(self.app.backend, "release", silent):
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.3)
            stopped = time.monotonic()
            worker.stop()
            await asyncio.wait_for(runner, timeout=30)
        self.assertLess(time.monotonic() - stopped, 0.5 + 1.5)

    async def test_a_second_signal_gives_a_release_that_never_answers_a_second(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(30)

        async def silent(*args, **kwargs):
            await asyncio.Event().wait()

        await handler.aenqueue()
        worker = self.make_worker(shutdown_timeout=30.0)
        with mock.patch.object(self.app.backend, "release", silent):
            runner = asyncio.create_task(worker.start())
            await asyncio.sleep(0.3)
            worker.stop()
            await asyncio.sleep(0.1)
            forced = time.monotonic()
            worker._request_force()
            await asyncio.wait_for(runner, timeout=30)
        self.assertLess(time.monotonic() - forced, 1.5)

    async def test_a_job_that_outlives_the_grace_period_returns_to_the_queue(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(10)

        job = await handler.aenqueue()
        worker = self.make_worker(shutdown_timeout=0.2)
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.2)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)


if __name__ == "__main__":
    unittest.main()


class RefreshAgainstAWorkerTests(WorkerTestCase):
    async def test_arefresh_follows_a_job_all_the_way_to_success(self):
        @self.app.task(store_result=True)
        async def handler(value):
            return value * 2

        job = await handler.aenqueue(21)
        self.assertIs(job.status, JobStatus.QUEUED)

        await self.settle(self.make_worker(), [job])

        await job.arefresh()
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.result(), 42)

    async def test_arefresh_shows_the_attempts_climbing(self):
        @self.app.task(max_attempts=3, retry_base=0.05, retry_jitter=False)
        async def handler():
            raise ConnectionError("nope")

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])

        await job.arefresh()
        self.assertIs(job.status, JobStatus.FAILED)
        self.assertEqual(job.attempts, 3)
        self.assertIn("nope", job.last_error)

    async def test_the_sync_refresh_is_refused_inside_a_loop(self):
        from tinyjobs.exceptions import SynchronousCallInsideEventLoopError

        @self.app.task
        async def handler():
            return None

        job = await handler.aenqueue()
        with self.assertRaises(SynchronousCallInsideEventLoopError) as caught:
            job.refresh()
        self.assertIn("arefresh", str(caught.exception))


class WaitTests(WorkerTestCase):
    async def test_awaited_blocks_until_the_job_is_done_and_yields_the_result(self):
        @self.app.task(store_result=True)
        async def handler(value):
            await asyncio.sleep(0.2)
            return value * 2

        job = await handler.aenqueue(21)
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())

        settled = await job.awaited(timeout=10)

        self.assertIs(settled, job)
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(job.result(), 42)

        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def test_awaited_returns_on_failure_rather_than_raising(self):
        @self.app.task(max_attempts=1)
        async def handler():
            raise ValueError("boom")

        job = await handler.aenqueue()
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())

        await job.awaited(timeout=10)

        self.assertIs(job.status, JobStatus.FAILED)
        self.assertIn("boom", job.last_error)
        self.assertIsNone(job.result())

        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def test_awaited_survives_retries_and_reports_the_attempts(self):
        tries = []

        @self.app.task(max_attempts=3, retry_base=0.05, retry_jitter=False,
                       store_result=True)
        async def handler():
            tries.append(1)
            if len(tries) < 3:
                raise ConnectionError("nope")
            return "recovered"

        job = await handler.aenqueue()
        worker = self.make_worker()
        runner = asyncio.create_task(worker.start())

        await job.awaited(timeout=10)

        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(job.attempts, 3)
        self.assertEqual(job.result(), "recovered")

        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def test_awaited_times_out_when_nothing_is_consuming(self):
        from tinyjobs.exceptions import WaitTimeout

        @self.app.task
        async def handler():
            return None

        job = await handler.aenqueue()
        with self.assertRaises(WaitTimeout) as caught:
            await job.awaited(timeout=0.3)
        self.assertIn("queued", str(caught.exception))
        self.assertIsInstance(caught.exception, TimeoutError)

    async def test_awaited_returns_at_once_for_an_already_finished_job(self):
        @self.app.task
        async def handler():
            return None

        job = await handler.aenqueue()
        await self.app.acancel(job.id)
        started = time.monotonic()
        await job.awaited(timeout=5)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertIs(job.status, JobStatus.CANCELLED)

    async def test_the_sync_wait_is_refused_inside_a_loop(self):
        from tinyjobs.exceptions import SynchronousCallInsideEventLoopError

        @self.app.task
        async def handler():
            return None

        job = await handler.aenqueue()
        with self.assertRaises(SynchronousCallInsideEventLoopError) as caught:
            job.wait(timeout=1)
        self.assertIn("awaited", str(caught.exception))


class BackendOutageTests(WorkerTestCase):
    async def test_a_store_that_cannot_record_success_does_not_fail_the_job(self):
        @self.app.task
        def handler():
            self.calls.append("ran")

        job = await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        original = backend.complete

        async def unreachable(*args, **kwargs):
            raise ConnectionError("store unreachable")

        backend.complete = unreachable
        try:
            with self.assertLogs("tinyjobs.worker", "ERROR") as logs:
                await self.run_briefly(self.make_worker(), 0.5)
        finally:
            backend.complete = original
        stored = await self.app.aget(job.id)
        self.assertEqual(self.calls, ["ran"])
        self.assertIs(stored.status, JobStatus.RUNNING)
        self.assertIsNone(stored.last_error)
        self.assertIn("could not record the outcome", "\n".join(logs.output))

    async def flaky_store(self, method: str, failures: int, *, lost_reply: bool = False):
        await self.app.ready()
        backend = self.app.backend
        original = getattr(backend, method)
        calls = []

        async def flaky(*args, **kwargs):
            calls.append(1)
            if len(calls) <= failures:
                if lost_reply:
                    await original(*args, **kwargs)
                raise ConnectionError("store unreachable")
            return await original(*args, **kwargs)

        setattr(backend, method, flaky)
        backend.transient_errors = (ConnectionError,)
        self.addCleanup(vars(backend).pop, "transient_errors", None)
        self.addCleanup(setattr, backend, method, original)
        return calls

    async def test_a_success_survives_an_outage_shorter_than_the_lease(self):
        @self.app.task
        def handler():
            self.calls.append("ran")

        job = await handler.aenqueue()
        calls = await self.flaky_store("complete", 2)
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            await self.settle(self.make_worker(), [job])
        stored = await self.app.aget(job.id)
        self.assertEqual((self.calls, stored.status, stored.attempts), (["ran"], JobStatus.SUCCESS, 1))
        self.assertEqual(len(calls), 3)
        self.assertIn("trying again", "\n".join(logs.output))

    async def test_a_success_whose_reply_was_lost_still_counts_as_recorded(self):
        succeeded = []

        @self.app.task
        def handler():
            return None

        self.app.on_job_success(lambda job, result, duration: succeeded.append(job.id))
        job = await handler.aenqueue()
        await self.flaky_store("complete", 1, lost_reply=True)
        with self.assertLogs("tinyjobs.worker", "INFO") as logs:
            await self.settle(self.make_worker(), [job])
        self.assertEqual((await self.app.aget(job.id)).status, JobStatus.SUCCESS)
        self.assertNotIn("no longer held", "\n".join(logs.output))
        self.assertEqual(succeeded, [job.id])

    async def test_a_failure_survives_an_outage_shorter_than_the_lease(self):
        @self.app.task
        def handler():
            raise ValueError("bad input")

        job = await handler.aenqueue()
        calls = await self.flaky_store("fail", 1)
        await self.settle(self.make_worker(), [job])
        stored = await self.app.aget(job.id)
        self.assertEqual((stored.status, len(calls)), (JobStatus.FAILED, 2))
        self.assertIn("bad input", stored.last_error)

    async def test_an_attempt_cancelled_during_the_outage_is_not_counted_as_recorded(self):
        succeeded = []

        @self.app.task
        def handler():
            return None

        self.app.on_job_success(lambda job, result, duration: succeeded.append(job.id))
        job = await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        original = backend.complete
        calls = []

        async def cancelled_meanwhile(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                await self.app.acancel(job.id)
                raise ConnectionError("store unreachable")
            return await original(*args, **kwargs)

        backend.complete = cancelled_meanwhile
        backend.transient_errors = (ConnectionError,)
        self.addCleanup(vars(backend).pop, "transient_errors", None)
        self.addCleanup(setattr, backend, "complete", original)
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            await self.run_briefly(self.make_worker(), 1.0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(succeeded, [])
        self.assertIn("no longer held", "\n".join(logs.output))

    async def test_an_earlier_attempt_with_the_same_number_is_not_taken_for_this_one(self):
        failures = []

        @self.app.task(max_attempts=1)
        def handler():
            raise ValueError("bad input")

        self.app.on_job_failure(lambda job, error, will_retry: failures.append(job.attempts))
        job = await handler.aenqueue()
        await self.settle(self.make_worker(worker_id="host-a"), [job])
        self.assertTrue(await self.app.aretry(job.id))
        backend = self.app.backend
        original = backend.fail
        calls = []

        async def cancelled_meanwhile(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                await self.app.acancel(job.id)
                raise ConnectionError("store unreachable")
            return await original(*args, **kwargs)

        backend.fail = cancelled_meanwhile
        backend.transient_errors = (ConnectionError,)
        self.addCleanup(vars(backend).pop, "transient_errors", None)
        self.addCleanup(setattr, backend, "fail", original)
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            await self.run_briefly(self.make_worker(worker_id="host-a"), 1.0)
        self.assertEqual((await self.app.aget(job.id)).status, JobStatus.CANCELLED)
        self.assertEqual(failures, [1])
        self.assertIn("no longer held", "\n".join(logs.output))

    async def test_a_recorded_failure_stays_recorded_when_its_retry_has_already_started(self):
        failures, runs = [], []

        @self.app.task(max_attempts=3)
        async def handler():
            runs.append(1)
            if len(runs) == 1:
                raise Retry(in_=0)
            await asyncio.sleep(3)

        self.app.on_job_failure(lambda job, error, will_retry: failures.append(job.attempts))
        await handler.aenqueue()
        await self.flaky_store("fail", 1, lost_reply=True)
        with self.assertLogs("tinyjobs.worker", "INFO") as logs:
            await self.run_briefly(self.make_worker(), 1.0)
        self.assertEqual(failures, [1])
        self.assertNotIn("no longer held", "\n".join(logs.output))
        self.assertEqual(len(runs), 2)

    async def test_a_finished_job_stopped_while_its_outcome_waits_is_not_put_back(self):
        runs = []

        @self.app.task(max_attempts=1)
        def handler():
            runs.append(1)

        job = await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        original = backend.complete
        back = {"at": float("inf")}

        async def flaky(*args, **kwargs):
            if time.monotonic() < back["at"]:
                raise ConnectionError("store unreachable")
            return await original(*args, **kwargs)

        backend.complete = flaky
        backend.transient_errors = (ConnectionError,)
        self.addCleanup(vars(backend).pop, "transient_errors", None)
        self.addCleanup(setattr, backend, "complete", original)
        worker = self.make_worker(shutdown_timeout=1.0)
        runner = asyncio.create_task(worker.start())
        while not runs:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        back["at"] = time.monotonic() + 0.9
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
        self.assertIn("reaper takes it back", "\n".join(logs.output))
        self.assertIs((await self.app.aget(job.id)).status, JobStatus.RUNNING)
        await self.run_briefly(self.make_worker(), 0.5)
        self.assertEqual(len(runs), 1)

    async def test_a_recorded_job_stopped_inside_a_slow_hook_is_not_reported_unrecorded(self):
        started = []

        @self.app.task
        def handler():
            return None

        async def slow(job, result, duration):
            started.append(1)
            await asyncio.sleep(30)

        self.app.on_job_success(slow)
        job = await handler.aenqueue()
        worker = self.make_worker(shutdown_timeout=0.3)
        runner = asyncio.create_task(worker.start())
        while not started:
            await asyncio.sleep(0.01)
        with self.assertLogs("tinyjobs.worker", "INFO") as logs:
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
        self.assertIs((await self.app.aget(job.id)).status, JobStatus.SUCCESS)
        self.assertNotIn("while its outcome was being recorded", "\n".join(logs.output))

    async def test_a_history_read_that_fails_once_is_tried_again(self):
        succeeded = []

        @self.app.task
        def handler():
            return None

        self.app.on_job_success(lambda job, result, duration: succeeded.append(job.id))
        job = await handler.aenqueue()
        await self.flaky_store("complete", 1, lost_reply=True)
        backend = self.app.backend
        executions = backend.executions
        reads = []

        async def blip(*args, **kwargs):
            reads.append(1)
            if len(reads) == 1:
                raise ConnectionError("store unreachable")
            return await executions(*args, **kwargs)

        backend.executions = blip
        self.addCleanup(setattr, backend, "executions", executions)
        with self.assertNoLogs("tinyjobs.worker", "ERROR"):
            await self.settle(self.make_worker(), [job])
        self.assertEqual(succeeded, [job.id])

    async def test_the_retry_window_ends_before_the_lease_can_run_out(self):
        @self.app.task
        def handler():
            return None

        await handler.aenqueue()
        await self.app.ready()
        execution = self.app.execution
        real = execution.execute
        windows = []

        async def watched(*args, **kwargs):
            windows.append(kwargs["held_until"]() - time.monotonic())
            return await real(*args, **kwargs)

        execution.execute = watched
        self.addCleanup(vars(execution).pop, "execute", None)
        await self.run_briefly(self.make_worker(lease_seconds=30.0), 0.5)
        self.assertEqual(len(windows), 1)
        self.assertAlmostEqual(windows[0], 29.0, delta=0.5)

    async def test_an_outage_that_began_before_the_job_ended_is_waited_out_only_within_the_lease(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(2.5)

        job = await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        completes = []

        async def unreachable(*args, **kwargs):
            raise ConnectionError("store unreachable")

        async def never_recorded(*args, **kwargs):
            completes.append(1)
            raise ConnectionError("store unreachable")

        for method in ("complete", "heartbeat"):
            self.addCleanup(setattr, backend, method, getattr(backend, method))
        backend.complete = never_recorded
        backend.heartbeat = unreachable
        backend.transient_errors = (ConnectionError,)
        self.addCleanup(vars(backend).pop, "transient_errors", None)
        with self.assertLogs("tinyjobs.worker", "ERROR"):
            await self.run_briefly(self.make_worker(lease_seconds=3.0), 4.5)
        self.assertEqual((await self.app.aget(job.id)).status, JobStatus.RUNNING)
        self.assertEqual(completes, [1])

    async def test_an_error_the_backend_does_not_call_transient_is_not_retried(self):
        @self.app.task
        def handler():
            return None

        job = await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        original = backend.complete
        calls = []

        async def broken(*args, **kwargs):
            calls.append(1)
            raise ValueError("not a connection problem")

        backend.complete = broken
        self.addCleanup(setattr, backend, "complete", original)
        with self.assertLogs("tinyjobs.worker", "ERROR"):
            await self.run_briefly(self.make_worker(), 0.5)
        self.assertEqual(calls, [1])
        self.assertIs((await self.app.aget(job.id)).status, JobStatus.RUNNING)

    async def test_an_outage_longer_than_the_lease_gives_up_and_leaves_it_to_the_reaper(self):
        @self.app.task
        def handler():
            return None

        job = await handler.aenqueue()
        calls = await self.flaky_store("complete", 1000)
        started = time.monotonic()
        with self.assertLogs("tinyjobs.worker", "ERROR") as logs:
            await self.run_briefly(self.make_worker(lease_seconds=3.0), 3.5)
        self.assertLess(time.monotonic() - started, 7)
        self.assertGreater(len(calls), 1)
        self.assertIn("could not record the outcome", "\n".join(logs.output))
        self.assertIs((await self.app.aget(job.id)).status, JobStatus.RUNNING)


class HookTests(WorkerTestCase):
    async def test_start_and_success_see_the_job_the_result_and_the_duration(self):
        seen = []

        @self.app.task
        async def handler(value):
            return value * 2

        @self.app.on_job_start
        def started(job):
            seen.append(("start", job.task, job.attempts))

        @self.app.on_job_success
        async def succeeded(job, result, duration):
            seen.append(("success", result, duration >= 0))

        job = await handler.aenqueue(21)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(
            seen, [("start", handler.name, 1), ("success", 42, True)]
        )

    async def test_failure_says_whether_the_job_will_be_retried(self):
        seen = []

        @self.app.task(max_attempts=2, retry_base=0.01, retry_jitter=False)
        def flaky():
            raise ConnectionError("down")

        @self.app.on_job_failure
        def failed(job, error, will_retry):
            seen.append((job.attempts, type(error).__name__, will_retry))

        job = await flaky.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertEqual(
            seen, [(1, "ConnectionError", True), (2, "ConnectionError", False)]
        )

    async def test_a_hook_that_raises_leaves_the_job_alone(self):
        @self.app.task(store_result=True)
        def handler():
            return "done"

        @self.app.on_job_success
        def broken(job, result, duration):
            raise RuntimeError("metrics are down")

        job = await handler.aenqueue()
        with self.assertLogs("tinyjobs.hooks", "ERROR") as logs:
            await self.settle(self.make_worker(), [job])
        self.assertIn("metrics are down", "\n".join(logs.output))
        stored = await self.app.aget(job.id)
        self.assertIs(stored.status, JobStatus.SUCCESS)
        self.assertEqual(stored.result(), "done")

    async def test_hooks_get_a_bound_job(self):
        statuses = []

        @self.app.task
        async def handler():
            return None

        @self.app.on_job_success
        async def succeeded(job, result, duration):
            statuses.append((await job.arefresh()).status)

        job = await handler.aenqueue()
        await self.settle(self.make_worker(), [job])
        self.assertEqual(statuses, [JobStatus.SUCCESS])


    async def test_no_hook_fires_for_an_outcome_the_backend_did_not_record(self):
        seen = []

        @self.app.task
        async def cancelled_meanwhile():
            await self.app.acancel(current_job.get().id)
            return "late"

        @self.app.task
        async def failing_meanwhile():
            await self.app.acancel(current_job.get().id)
            raise ConnectionError("late")

        self.app.on_job_success(lambda job, result, duration: seen.append("success"))
        self.app.on_job_failure(lambda job, error, will_retry: seen.append("failure"))
        jobs = [await cancelled_meanwhile.aenqueue(), await failing_meanwhile.aenqueue()]
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            await self.settle(self.make_worker(), jobs)
        self.assertEqual(seen, [])
        self.assertEqual(
            sum("its outcome was not recorded" in line for line in logs.output), 2
        )
        self.assertNotIn("succeeded", "\n".join(logs.output))
        for job in jobs:
            self.assertIs(await self.status_of(job), JobStatus.CANCELLED)


class HeartbeatTests(WorkerTestCase):
    async def test_the_heartbeat_stops_once_the_app_is_closed(self):
        from unittest import mock

        from tinyjobs.exceptions import AppClosedError

        calls = []

        async def closed(*args, **kwargs):
            calls.append(args)
            raise AppClosedError("closed")

        @self.app.task
        async def slow():
            await asyncio.sleep(2.5)

        job = await slow.aenqueue()
        with mock.patch.object(self.app.backend, "heartbeat", closed):
            await self.settle(self.make_worker(lease_seconds=3.0), [job])
        self.assertEqual(len(calls), 1)

    async def test_a_heartbeat_that_never_answers_is_given_up_before_the_lease_lapses(self):
        from unittest import mock

        calls: list[float] = []

        @self.app.task
        async def slow():
            await asyncio.sleep(4.5)

        job = await slow.aenqueue()
        heartbeat = self.app.backend.heartbeat

        async def silent_once(*args, **kwargs):
            calls.append(time.monotonic())
            if len(calls) == 1:
                await asyncio.Event().wait()
            return await heartbeat(*args, **kwargs)

        worker = self.make_worker(lease_seconds=3.0, reap_interval=0.2, concurrency=1)
        with mock.patch.object(self.app.backend, "heartbeat", silent_once):
            await self.settle(worker, [job])
        self.assertLess(calls[1] - calls[0], 1.6)
        stored = await self.app.aget(job.id)
        self.assertEqual((stored.status, stored.attempts), (JobStatus.SUCCESS, 1))


class StaleHeartbeatTests(WorkerTestCase):
    async def test_a_worker_that_lost_its_job_does_not_keep_the_new_holder_alive(self):
        @self.app.task(max_attempts=3)
        async def slow():
            await asyncio.sleep(4.0)

        job = await slow.aenqueue()
        worker = self.make_worker(lease_seconds=3.0, reap_interval=0.2, concurrency=1)
        runner = asyncio.create_task(worker.start())
        while await self.status_of(job) is not JobStatus.RUNNING:
            await asyncio.sleep(0.02)
        await self.app.acancel(job.id)
        await self.app.aretry(job.id)
        await self.app.backend.claim(
            worker_id="crashed", queues=["default"], limit=1, lease_seconds=1.5
        )
        await asyncio.sleep(2.5)
        holder = (await self.app.aget(job.id)).worker_id
        worker.stop()
        await asyncio.wait_for(runner, timeout=15)
        self.assertNotEqual(holder, "crashed")


class HousekeepingTests(WorkerTestCase):
    async def test_the_worker_prunes_on_its_own(self):
        from tinyjobs.retention import RetentionPolicy

        @self.app.task
        async def handler(value):
            return value

        jobs = [await handler.aenqueue(n) for n in range(6)]
        worker = self.make_worker(
            retention=RetentionPolicy(
                success=0.05, cancelled=0.05, failed=0.05, interval=0.0
            )
        )
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 20
        remaining = len(jobs)
        while time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            remaining = len((await self.app.ajobs(limit=100)).items)
            if remaining == 0:
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(remaining, 0)

    async def test_a_disabled_policy_leaves_everything_alone(self):
        from tinyjobs.retention import KEEP_EVERYTHING

        @self.app.task
        async def handler(value):
            return value

        jobs = [await handler.aenqueue(n) for n in range(4)]
        worker = self.make_worker(retention=KEEP_EVERYTHING)
        await self.settle(worker, jobs)
        self.assertEqual(len((await self.app.ajobs(limit=100)).items), 4)

    async def test_pruning_never_removes_work_still_to_do(self):
        from tinyjobs.retention import RetentionPolicy

        running = asyncio.Event()

        @self.app.task
        async def handler(value):
            running.set()
            await asyncio.sleep(1.0)
            return value

        job = await handler.aenqueue(1)
        queued = await handler.aenqueue(2)
        worker = self.make_worker(
            retention=RetentionPolicy(
                success=1e-9, cancelled=1e-9, failed=1e-9, interval=0.0
            ),
            concurrency=1,
        )
        runner = asyncio.create_task(worker.start())
        await asyncio.wait_for(running.wait(), timeout=10)
        await asyncio.sleep(0.4)

        in_flight = await self.app.aget(job.id)
        waiting = await self.app.aget(queued.id)
        self.assertIsNotNone(in_flight, "a running job was pruned")
        self.assertIs(in_flight.status, JobStatus.RUNNING)
        self.assertIsNotNone(waiting, "a queued job was pruned")
        self.assertIs(waiting.status, JobStatus.QUEUED)

        worker.stop()
        await asyncio.wait_for(runner, timeout=15)
