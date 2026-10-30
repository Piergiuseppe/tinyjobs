from __future__ import annotations

import asyncio
import os
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

    def test_a_job_that_sends_its_own_dedup_key_gets_itself_back_at_once(self):
        import threading

        @self.app.task(store_result=True)
        def again():
            return str(self.app.send(again.name, key="k").status)

        @self.app.task(store_result=True)
        async def aagain():
            return str((await self.app.asend(aagain.name, key="ak")).status)

        for task, key in ((again, "k"), (aagain, "ak")):
            with self.subTest(task=task.name):
                returned: list = []
                calling = threading.Thread(
                    target=lambda: returned.append(task.with_options(key=key).enqueue()),
                    daemon=True,
                )
                calling.start()
                calling.join(5)
                self.assertFalse(calling.is_alive())
                self.assertIs(returned[0].status, JobStatus.SUCCESS)
                self.assertEqual(returned[0].result(), "running")

    def test_a_job_sending_its_own_dedup_key_from_a_thread_it_starts_gets_itself_back(self):
        import threading

        def sent():
            return str(self.app.send(viaexecutor.name, key="e").status)

        @self.app.task(store_result=True)
        async def viaexecutor():
            return await asyncio.get_running_loop().run_in_executor(None, sent)

        @self.app.task(store_result=True)
        def viathread():
            statuses: list = []
            started = threading.Thread(
                target=lambda: statuses.append(str(self.app.send(viathread.name, key="t").status))
            )
            started.start()
            started.join()
            return statuses[0]

        for task, key in ((viaexecutor, "e"), (viathread, "t")):
            with self.subTest(task=task.name):
                returned: list = []
                calling = threading.Thread(
                    target=lambda: returned.append(task.with_options(key=key).enqueue()),
                    daemon=True,
                )
                calling.start()
                calling.join(5)
                self.assertFalse(calling.is_alive())
                self.assertIs(returned[0].status, JobStatus.SUCCESS)
                self.assertEqual(returned[0].result(), "running")

    def test_closing_during_an_eager_call_runs_its_job_once_and_raises(self):
        import tempfile
        import threading
        import time
        from unittest import mock

        from tinyjobs.exceptions import AppClosedError
        from tinyjobs.worker.drain import Drain

        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        url = "sqlite:///" + os.path.join(folder.name, "jobs.db")
        app = TinyJobs(url, eager=True)
        runs: list[float] = []
        started = threading.Event()

        @app.task
        async def slow():
            runs.append(time.monotonic())
            started.set()
            await asyncio.sleep(3)

        outcome: list = []

        def call():
            try:
                outcome.append(slow.enqueue())
            except AppClosedError as error:
                outcome.append(error)

        defaults = dict(Drain.__init__.__kwdefaults__, shutdown_timeout=0.3)
        with mock.patch.object(Drain.__init__, "__kwdefaults__", defaults):
            calling = threading.Thread(target=call)
            calling.start()
            self.assertTrue(started.wait(5))
            app.close()
            calling.join(10)
        self.assertEqual(len(runs), 1)
        self.assertIsInstance(outcome[0], AppClosedError)
        check = TinyJobs(url)
        self.addCleanup(check.close)
        [job] = check.jobs().items
        self.assertEqual((job.status, job.attempts), (JobStatus.QUEUED, 1))

    def test_eager_calls_beside_a_drain_loop_wait_for_it_to_finish_their_jobs(self):
        import threading
        import time

        @self.app.task
        def slow():
            time.sleep(0.1)

        stop = threading.Event()

        def draining():
            while not stop.is_set():
                self.app.drain()

        beside = threading.Thread(target=draining)
        beside.start()
        try:
            statuses = [slow.enqueue().status for _ in range(20)]
        finally:
            stop.set()
            beside.join(10)
        self.assertEqual(set(statuses), {JobStatus.SUCCESS})

    def test_an_eager_call_waits_for_a_claim_in_flight_that_took_its_job(self):
        import threading
        from functools import partial
        from unittest import mock

        @self.app.task(store_result=True)
        def slow():
            return "done"

        self.app._eager = False
        job = slow.enqueue()
        self.app._eager = True
        claimed, go = threading.Event(), threading.Event()
        real = self.app.backend.claim

        async def paused(**options):
            jobs = await real(**options)
            if jobs and not claimed.is_set():
                claimed.set()
                while not go.is_set():
                    await asyncio.sleep(0.01)
            return jobs

        with mock.patch.object(self.app.backend, "claim", paused):
            beside = threading.Thread(target=self.app.drain)
            beside.start()
            self.assertTrue(claimed.wait(5))
            threading.Timer(0.3, go.set).start()
            settled = self.app.run_sync(partial(self.app.settle, job, inserted=True), name="settle")
            beside.join(5)
        self.assertEqual((settled.status, settled.result()), (JobStatus.SUCCESS, "done"))

    def test_a_job_whose_eager_call_loses_the_job_to_another_claim_gets_it_as_it_now_is(self):
        from unittest import mock

        @self.app.task
        def quick():
            return None

        seen: list = []

        @self.app.task(priority=1)
        async def outer():
            backend = self.app.backend
            real = backend.claim

            async def drained(**options):
                return []

            async def taken(job_id, **options):
                await real(worker_id="elsewhere", queues=["default"], limit=1, lease_seconds=60.0)
                return None

            with mock.patch.object(backend, "claim", drained), mock.patch.object(
                backend, "claim_job", taken
            ):
                seen.append((await self.app.settle(job)).status)

        self.app._eager = False
        job = quick.enqueue()
        outer.enqueue()
        self.app._eager = True
        self.app.drain()
        self.assertEqual(seen, [JobStatus.RUNNING])

    def test_an_eager_call_runs_only_its_own_job_and_before_older_ones(self):
        @self.app.task
        def record(label):
            self.calls.append(label)

        self.app._eager = False
        older = record.enqueue("older")
        self.app._eager = True
        self.assertIs(record.enqueue("sent").status, JobStatus.SUCCESS)
        self.assertEqual(self.calls, ["sent"])
        self.assertIs(older.refresh().status, JobStatus.QUEUED)
        self.assertEqual(self.app.drain(), 1)
        self.assertEqual(self.calls, ["sent", "older"])

    def test_a_dedup_hit_on_a_job_running_elsewhere_returns_it_at_once(self):
        import time

        @self.app.task
        def quick():
            return None

        self.app._eager = False
        held = quick.with_options(key="k").enqueue()
        self.app._eager = True
        self.app.run_sync(
            lambda: self.app.backend.claim(
                worker_id="elsewhere", queues=["default"], limit=1, lease_seconds=60.0
            ),
            name="claim",
        )
        started = time.monotonic()
        again = quick.with_options(key="k").enqueue()
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual((again.id, again.status), (held.id, JobStatus.RUNNING))

    def test_an_eager_call_on_a_backend_that_cannot_claim_by_id_leaves_its_job_queued(self):
        from unittest import mock

        @self.app.task
        async def quick():
            return None

        self.app.start()
        with mock.patch.object(
            self.app.backend, "claim_job", mock.AsyncMock(return_value=None)
        ):
            self.assertIs(quick.enqueue().status, JobStatus.QUEUED)

    def test_nested_eager_calls_run_quietly_at_any_depth(self):
        for timeout in (None, 30):
            with self.subTest(timeout=timeout):
                app = TinyJobs("memory://", eager=True)
                self.addCleanup(app.close)

                @app.task(store_result=True, timeout=timeout)
                def chain(n):
                    return [n, str(chain.enqueue(n - 1).status)] if n else [n]

                with self.assertNoLogs("tinyjobs", "WARNING"):
                    job = chain.enqueue(16)
                self.assertIs(job.status, JobStatus.SUCCESS)
                self.assertEqual(app.counts(), {"success": 17})
                seen = [job.result_payload for job in app.jobs(limit=50).items]
                self.assertEqual(sum(b"queued" in payload for payload in seen), 0)

    def test_a_timed_job_that_sends_stuck_jobs_fails_each_and_warns_per_thread_left(self):
        import threading

        held = threading.Event()
        self.addCleanup(held.set)
        entered: list[int] = []

        @self.app.task(timeout=0.05, max_attempts=1)
        def hang():
            entered.append(1)
            held.wait(10)

        statuses: list = []

        @self.app.task(timeout=60)
        def outer():
            statuses.extend(hang.enqueue().status for _ in range(12))

        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            self.assertIs(outer.enqueue().status, JobStatus.SUCCESS)
        self.assertEqual((len(entered), set(statuses)), (12, {JobStatus.FAILED}))
        self.assertEqual(sum("ended while its thread still runs" in line for line in logs.output), 12)

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


    async def test_concurrent_eager_calls_all_run_their_own_jobs(self):
        import time

        app = TinyJobs("memory://", eager=True)

        @app.task
        def plain(i):
            time.sleep(0.2)

        @app.task(timeout=5)
        def timed(i):
            time.sleep(0.2)

        @app.task
        async def coroutine(i):
            await asyncio.sleep(0.2)

        try:
            for task in (plain, timed, coroutine):
                with self.subTest(task=task.name):
                    threaded = [asyncio.to_thread(task.enqueue, i) for i in range(20)]
                    gathered = [task.aenqueue(i) for i in range(20)]
                    jobs = await asyncio.gather(*threaded, *gathered)
                    self.assertEqual({job.status for job in jobs}, {JobStatus.SUCCESS})
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

    def spend_the_spares(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        for _ in range(DRAIN_SPARE_THREADS):
            runaway.enqueue()
        self.assertEqual(self.app.drain(), DRAIN_SPARE_THREADS)
        return held

    def test_a_drain_renews_the_lease_of_a_job_while_it_runs(self):
        import time
        from unittest import mock

        from tinyjobs.worker import drain

        @self.app.task
        def slow():
            time.sleep(0.3)

        job = slow.enqueue()
        beats: list = []
        real = self.app.backend.heartbeat

        async def counted(job_ids, lease_seconds, worker_id=None):
            beats.append((list(job_ids), lease_seconds, worker_id))
            return await real(job_ids, lease_seconds, worker_id=worker_id)

        with mock.patch.object(drain, "DRAIN_RENEW_SECONDS", 0.05), mock.patch.object(
            self.app.backend, "heartbeat", counted
        ):
            self.assertEqual(self.app.drain(), 1)
        self.assertGreaterEqual(len(beats), 3)
        self.assertEqual(beats[0], ([job.id], 60.0, "drain"))
        self.assertIs(job.refresh().status, JobStatus.SUCCESS)

    def test_a_drain_given_up_by_its_caller_renews_the_lease_until_its_thread_ends(self):
        import time
        from unittest import mock

        from tinyjobs.worker import drain

        @self.app.task
        def slow():
            time.sleep(0.6)

        job = slow.enqueue()
        beats: list[float] = []
        real = self.app.backend.heartbeat

        async def counted(job_ids, lease_seconds, worker_id=None):
            beats.append(time.monotonic())
            return await real(job_ids, lease_seconds, worker_id=worker_id)

        async def given_up():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(self.app.adrain(), 0.1)
            left = time.monotonic()
            while (await job.arefresh()).status is not JobStatus.SUCCESS:
                await asyncio.sleep(0.02)
            ended = time.monotonic()
            await asyncio.sleep(0.2)
            return left, ended

        with mock.patch.object(drain, "DRAIN_RENEW_SECONDS", 0.05), mock.patch.object(
            self.app.backend, "heartbeat", counted
        ), self.assertLogs("tinyjobs", "WARNING"):
            left, ended = self.app.run_sync(given_up, name="given up")
        self.assertGreaterEqual(len([beat for beat in beats if beat > left + 0.1]), 3)
        self.assertEqual([beat for beat in beats if beat > ended + 0.06], [])

    def test_a_drain_writes_an_outcome_again_after_a_transient_error(self):
        from unittest import mock

        @self.app.task
        def quick():
            return None

        job = quick.enqueue()
        real = self.app.backend.complete
        calls: list[int] = []

        async def flaky(*arguments, **options):
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("the answer was lost")
            return await real(*arguments, **options)

        with mock.patch.object(self.app.backend, "complete", flaky), self.assertLogs(
            "tinyjobs", "WARNING"
        ):
            self.assertEqual(self.app.drain(), 1)
        self.assertEqual(len(calls), 2)
        self.assertIs(job.refresh().status, JobStatus.SUCCESS)

    def test_a_drain_inside_a_timed_job_runs_every_stuck_job_beside_their_threads(self):
        import threading
        import time

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def hang():
            held.wait(10)

        took: list[float] = []

        @self.app.task(timeout=60)
        def outer():
            started = time.monotonic()
            took.append(self.app.drain())
            took.append(time.monotonic() - started)

        for _ in range(12):
            hang.enqueue()
        outer.with_options(priority=1).enqueue()
        with self.assertLogs("tinyjobs.worker", "WARNING"):
            self.assertEqual(self.app.drain(), 1)
        self.assertEqual(took[0], 12)
        self.assertLess(took[1], 3)
        self.assertEqual(self.app.counts(), {"success": 1, "failed": 12})

    def test_a_drain_warns_that_a_new_thread_takes_the_place_of_a_timed_out_one(self):
        import threading
        import time

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
        held.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and any(
            thread.name.startswith("tinyjobs_") for thread in threading.enumerate()
        ):
            time.sleep(0.01)
        self.assertFalse(
            [t for t in threading.enumerate() if t.name.startswith("tinyjobs_")]
        )

    def test_a_drain_gives_back_the_thread_it_added_once_the_timed_out_one_returns(self):
        import threading
        import time

        held = threading.Event()
        self.addCleanup(held.set)
        seen: list[int] = []

        def job_threads() -> int:
            return sum(t.name.startswith("tinyjobs_") for t in threading.enumerate())

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        @self.app.task
        def release():
            held.set()
            time.sleep(0.3)

        @self.app.task
        def count():
            deadline = time.monotonic() + 2
            while job_threads() > 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            seen.append(job_threads())

        runaway.with_options(priority=3).enqueue()
        release.with_options(priority=2).enqueue()
        count.with_options(priority=1).enqueue()
        with self.assertLogs("tinyjobs.worker", "WARNING"):
            self.assertEqual(self.app.drain(), 3)
        self.assertEqual(seen, [1])

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


def _job_threads() -> int:
    import threading

    return sum(thread.name.startswith("tinyjobs_") for thread in threading.enumerate())


if __name__ == "__main__":
    unittest.main()
