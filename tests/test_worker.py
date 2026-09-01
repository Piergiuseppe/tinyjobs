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
