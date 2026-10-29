from __future__ import annotations

import unittest

from tinyjobs import FrozenClock, JobStatus, TinyJobs, UnknownTaskError

START = 1_791_633_600.0


class MemoryBackendTests(unittest.TestCase):
    def test_memory_urls_get_a_private_store_each(self):
        first, second = TinyJobs("memory://"), TinyJobs("memory://")
        try:
            first.send("noop")
            self.assertEqual(len(first.jobs().items), 1)
            self.assertEqual(len(second.jobs().items), 0)
        finally:
            first.close()
            second.close()


class EagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = TinyJobs("memory://", eager=True)
        self.calls: list[str] = []

    def tearDown(self) -> None:
        self.app.close()

    def test_enqueue_returns_a_finished_job(self):
        @self.app.task(store_result=True)
        def double(value):
            return value * 2

        job = double.enqueue(21)
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(job.result(), 42)

    def test_a_failure_is_recorded_not_raised(self):
        @self.app.task
        def broken():
            raise ValueError("nope")

        job = broken.enqueue()
        self.assertIs(job.status, JobStatus.FAILED)
        self.assertEqual(job.last_error, "ValueError: nope")

    def test_a_task_that_enqueues_another_runs_both(self):
        @self.app.task
        def child(value):
            self.calls.append(f"child:{value}")

        @self.app.task
        def parent():
            self.calls.append("parent")
            child.enqueue(1)

        parent.enqueue()
        self.assertEqual(self.calls, ["parent", "child:1"])

    def test_hooks_fire(self):
        @self.app.task
        def handler():
            return None

        self.app.on_job_success(lambda job, result, duration: self.calls.append(job.task))
        handler.enqueue()
        self.assertEqual(self.calls, [handler.name])

    def test_send_by_name_runs_too(self):
        @self.app.task(name="named.task")
        def handler():
            self.calls.append("ran")

        self.assertIs(self.app.send("named.task").status, JobStatus.SUCCESS)
        self.assertEqual(self.calls, ["ran"])

    def test_a_delayed_job_waits_for_its_time(self):
        clock = FrozenClock(START)
        app = TinyJobs("memory://", eager=True, clock=clock)

        @app.task
        def later():
            return None

        try:
            job = later.with_options(delay=60).enqueue()
            self.assertIs(job.status, JobStatus.QUEUED)
            clock.advance(60)
            self.assertEqual(app.drain(), 1)
            self.assertIs(job.refresh().status, JobStatus.SUCCESS)
        finally:
            app.close()


class AsyncEagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_aenqueue_returns_a_finished_job(self):
        app = TinyJobs("memory://", eager=True)

        @app.task(store_result=True)
        async def double(value):
            return value * 2

        try:
            job = await double.aenqueue(4)
            self.assertIs(job.status, JobStatus.SUCCESS)
            self.assertEqual(job.result(), 8)
        finally:
            await app.aclose()


class DrainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FrozenClock(START)
        self.app = TinyJobs("memory://", clock=self.clock)
        self.calls: list[str] = []

        @self.app.task
        def record(label):
            self.calls.append(label)

        self.record = record

    def tearDown(self) -> None:
        self.app.close()

    def test_nothing_runs_until_drained(self):
        job = self.record.enqueue("a")
        self.assertIs(job.status, JobStatus.QUEUED)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.app.drain(), 1)
        self.assertEqual(self.calls, ["a"])
        self.assertEqual(self.app.drain(), 0)

    def test_jobs_run_by_priority_across_queues(self):
        self.record.enqueue("low")
        self.record.with_options(queue="mail", priority=5).enqueue("high")
        self.record.with_options(priority=1).enqueue("middle")
        self.app.drain()
        self.assertEqual(self.calls, ["high", "middle", "low"])

    def test_draining_some_queues_leaves_the_rest(self):
        self.record.enqueue("default")
        self.record.with_options(queue="mail").enqueue("mail")
        self.assertEqual(self.app.drain(["mail"]), 1)
        self.assertEqual(self.calls, ["mail"])

    def test_work_enqueued_into_a_new_queue_while_draining_runs_too(self):
        @self.app.task
        def fan_out():
            self.record.with_options(queue="fresh").enqueue("child")

        fan_out.enqueue()
        self.assertEqual(self.app.drain(), 2)
        self.assertEqual(self.calls, ["child"])

    def test_retries_wait_for_the_clock(self):
        @self.app.task(max_attempts=3, retry_base=10, retry_jitter=False)
        def flaky():
            self.calls.append("try")
            raise ConnectionError("down")

        job = flaky.enqueue()
        self.app.drain()
        self.assertEqual((job.refresh().status, job.attempts), (JobStatus.QUEUED, 1))
        self.clock.advance(10)
        self.app.drain()
        self.clock.advance(20)
        self.app.drain()
        self.assertEqual((job.refresh().status, job.attempts), (JobStatus.FAILED, 3))
        self.assertEqual(self.calls, ["try"] * 3)

    def test_a_drain_runs_the_next_job_beside_a_timed_out_thread_and_returns(self):
        import threading
        import time

        held = threading.Event()
        self.addCleanup(held.set)
        returned: list[str] = []

        @self.app.task(timeout=0.2, max_attempts=1)
        def runaway():
            held.wait(10)
            returned.append("runaway")

        @self.app.task(timeout=0.3)
        def charge():
            self.calls.append("charged")

        first = runaway.enqueue()
        job = charge.enqueue()
        started = time.monotonic()
        self.assertEqual(self.app.drain(), 2)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertIs(first.refresh().status, JobStatus.FAILED)
        self.assertIs(job.refresh().status, JobStatus.SUCCESS)
        self.assertEqual((self.calls, returned), (["charged"], []))

    def test_a_drain_adds_no_more_than_its_spare_threads_beside_timed_out_ones(self):
        import threading
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)
        peak = [0]

        def watch():
            while not held.is_set():
                live = sum(t.name.startswith("tinyjobs_") for t in threading.enumerate())
                peak[0] = max(peak[0], live)
                time.sleep(0.005)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        jobs = [runaway.enqueue() for _ in range(DRAIN_SPARE_THREADS + 4)]
        threading.Thread(target=watch, daemon=True).start()
        release = threading.Timer(1.0, held.set)
        release.start()
        self.addCleanup(release.cancel)
        self.assertEqual(self.app.drain(), len(jobs))
        self.assertLessEqual(peak[0], DRAIN_SPARE_THREADS + 1)
        cut = {job.refresh().status for job in jobs[: DRAIN_SPARE_THREADS + 1]}
        self.assertEqual(cut, {JobStatus.FAILED})

    def test_a_drain_warns_that_a_new_thread_takes_the_place_of_a_timed_out_one(self):
        import threading

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        runaway.enqueue()
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            self.assertEqual(self.app.drain(), 1)
        warning = next(line for line in logs.output if "still runs" in line)
        self.assertIn("a new thread takes its place", warning)

    def test_a_job_whose_thread_cannot_start_fails_and_the_next_one_runs(self):
        import threading
        from unittest import mock

        original = threading.Thread.start
        refused: list[str] = []

        def start(thread):
            if thread.name.startswith("tinyjobs_") and not refused:
                refused.append(thread.name)
                raise RuntimeError("can't start new thread")
            return original(thread)

        @self.app.task(max_attempts=1)
        def charge():
            self.calls.append("charged")

        first, second = charge.enqueue(), charge.enqueue()
        with mock.patch.object(threading.Thread, "start", start):
            self.assertEqual(self.app.drain(), 2)
        self.assertEqual(len(refused), 1)
        self.assertIs(first.refresh().status, JobStatus.FAILED)
        self.assertIs(second.refresh().status, JobStatus.SUCCESS)
        self.assertEqual(self.calls, ["charged"])

    def test_an_eager_job_past_its_timeout_returns_while_its_thread_still_runs(self):
        import threading
        import time

        held = threading.Event()
        self.addCleanup(held.set)
        app = TinyJobs("memory://", eager=True)
        self.addCleanup(app.close)

        @app.task(timeout=0.2, max_attempts=1)
        def partner():
            held.wait(10)

        started = time.monotonic()
        job = partner.enqueue()
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertIs(job.status, JobStatus.FAILED)

    def test_an_unknown_task_raises_and_stays_queued(self):
        job = self.app.send("elsewhere.task")
        with self.assertRaises(UnknownTaskError):
            self.app.drain()
        self.assertIs(job.refresh().status, JobStatus.QUEUED)


if __name__ == "__main__":
    unittest.main()
