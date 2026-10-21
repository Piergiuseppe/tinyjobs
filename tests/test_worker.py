from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from tinyjobs import Abandon, Retry, TinyJobs
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

    async def test_an_unregistered_task_is_handed_back_less_and_less_often(self):
        job = await self.app.asend("nobody.knows.this")
        worker = self.make_worker(
            unknown_task_delay=0.1, unknown_task_max_delay=0.4, poll_maximum=0.02
        )
        await self.run_briefly(worker, seconds=2.0)
        self.assertLessEqual((await self.app.aget(job.id)).attempts, 8)

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
        self.assertEqual(stored.attempts, 4)
        self.assertLessEqual(stored.run_at - self.app.clock.now(), 60.0)

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
