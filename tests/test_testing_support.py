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
            settled = self.app.run_sync(partial(self.app.settle, job), name="settle")
            beside.join(5)
        self.assertEqual((settled.status, settled.result()), (JobStatus.SUCCESS, "done"))

    def test_an_eager_call_runs_its_own_job_past_one_left_for_a_spare(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def hang():
            held.wait(10)

        @self.app.task(timeout=5)
        def timed():
            return None

        @self.app.task(store_result=True)
        async def quick():
            return "quick"

        @self.app.task(store_result=True)
        def plain():
            return "plain"

        for _ in range(DRAIN_SPARE_THREADS):
            hang.enqueue()
        self.assertEqual(self.app.thread_spares.strayed, DRAIN_SPARE_THREADS)
        with self.assertLogs("tinyjobs.worker", "WARNING"):
            left = timed.enqueue()
        self.assertIs(left.status, JobStatus.QUEUED)
        self.assertEqual(quick.enqueue().result(), "quick")
        self.assertEqual(plain.enqueue().result(), "plain")
        self.assertIs(left.refresh().status, JobStatus.QUEUED)
        self.assertEqual(left.refresh().hand_backs, 3)

    def test_an_eager_call_on_a_backend_that_cannot_claim_by_id_leaves_its_job_queued(self):
        import threading
        from unittest import mock

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def hang():
            held.wait(10)

        @self.app.task
        async def quick():
            return None

        for _ in range(DRAIN_SPARE_THREADS + 1):
            hang.enqueue()
        with mock.patch.object(
            self.app.backend, "claim_job", mock.AsyncMock(return_value=None)
        ):
            self.assertIs(quick.enqueue().status, JobStatus.QUEUED)

    def test_nested_eager_calls_deeper_than_the_spares_run_quietly(self):
        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        @self.app.task(store_result=True)
        def chain(n):
            return [n, chain.enqueue(n - 1).result()] if n else [n]

        with self.assertNoLogs("tinyjobs", "WARNING"):
            job = chain.enqueue(DRAIN_SPARE_THREADS + 4)
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(job.result()[0], DRAIN_SPARE_THREADS + 4)

    def test_nested_timed_calls_past_the_spares_come_back_queued_and_run_once_a_thread_frees(self):
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        @self.app.task(store_result=True, timeout=30)
        def chain(n):
            return [n, str(chain.enqueue(n - 1).status)] if n else [n]

        started = time.monotonic()
        with self.assertLogs("tinyjobs.worker", "WARNING") as logs:
            job = chain.enqueue(DRAIN_SPARE_THREADS + 4)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertIn("are taken, 0 of them by threads past their timeout", logs.output[0])
        self.assertEqual(self.app.counts(), {"success": DRAIN_SPARE_THREADS + 5})
        seen = [job.result_payload for job in self.app.jobs(limit=50).items]
        self.assertEqual(sum(b"queued" in payload for payload in seen), 5)

    def test_a_timed_job_that_sends_stuck_jobs_never_outgrows_the_spares(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)
        entered: list[int] = []

        @self.app.task(timeout=0.05, max_attempts=1)
        def hang():
            entered.append(1)
            held.wait(10)

        @self.app.task(timeout=60)
        def outer():
            for _ in range(DRAIN_SPARE_THREADS * 5):
                hang.enqueue()

        self.assertIs(outer.enqueue().status, JobStatus.SUCCESS)
        self.assertEqual(len(entered), DRAIN_SPARE_THREADS)
        self.assertEqual(self.app.thread_spares.strayed, DRAIN_SPARE_THREADS)

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


    async def test_concurrent_eager_calls_of_ordinary_sync_jobs_all_run(self):
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        app = TinyJobs("memory://", eager=True)

        @app.task
        def plain(i):
            time.sleep(0.2)

        @app.task(timeout=5)
        def timed(i):
            time.sleep(0.2)

        try:
            for task in (plain, timed):
                with self.subTest(task=task.name):
                    calls = [
                        asyncio.to_thread(task.enqueue, i) for i in range(DRAIN_SPARE_THREADS + 4)
                    ]
                    jobs = await asyncio.gather(*calls)
                    self.assertEqual({job.status for job in jobs}, {JobStatus.SUCCESS})
        finally:
            await app.aclose()

    async def test_concurrent_eager_calls_run_no_more_stuck_threads_than_the_spares(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        app = TinyJobs("memory://", eager=True)
        held = threading.Event()
        entered: list[int] = []

        @app.task(timeout=0.05, max_attempts=1)
        def runaway():
            entered.append(1)
            held.wait(10)

        @app.task
        async def quick():
            return None

        try:
            await asyncio.gather(*(runaway.aenqueue() for _ in range(DRAIN_SPARE_THREADS * 3)))
            self.assertEqual(len(entered), DRAIN_SPARE_THREADS)
            self.assertIs((await quick.aenqueue()).status, JobStatus.SUCCESS)
        finally:
            held.set()
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

    def test_a_drain_hands_back_a_sync_job_once_its_spare_threads_are_taken(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        before = _job_threads()
        jobs = [runaway.enqueue() for _ in range(DRAIN_SPARE_THREADS + 2)]
        ran: list[int] = []
        draining = threading.Thread(target=lambda: ran.append(self.app.drain()), daemon=True)
        draining.start()
        draining.join(3)
        self.assertFalse(draining.is_alive())
        self.assertEqual(ran, [DRAIN_SPARE_THREADS])
        late = [job.refresh() for job in jobs[DRAIN_SPARE_THREADS:]]
        self.assertEqual({job.status for job in late}, {JobStatus.QUEUED})
        self.assertEqual({job.worker_id for job in late}, {None})
        self.assertLessEqual(_job_threads() - before, DRAIN_SPARE_THREADS)

    def test_eager_calls_share_the_apps_spare_threads(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)
        app = TinyJobs("memory://", eager=True)
        self.addCleanup(app.close)

        @app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        before = _job_threads()
        jobs = [runaway.enqueue() for _ in range(DRAIN_SPARE_THREADS + 4)]
        self.assertLessEqual(_job_threads() - before, DRAIN_SPARE_THREADS)
        self.assertEqual(jobs[-1].status, JobStatus.QUEUED)

    def test_a_drain_runs_the_jobs_before_one_it_has_no_spare_thread_for(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        @self.app.task
        async def quick():
            self.calls.append("quick")

        for _ in range(DRAIN_SPARE_THREADS):
            runaway.enqueue()
        job = quick.enqueue()
        late = [runaway.enqueue() for _ in range(2)]
        self.assertEqual(self.app.drain(), DRAIN_SPARE_THREADS + 1)
        self.assertIs(job.refresh().status, JobStatus.SUCCESS)
        self.assertEqual({job.refresh().status for job in late}, {JobStatus.QUEUED})

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_forked_child_has_every_spare_thread_its_parent_took(self):
        import signal
        import threading
        import warnings

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            held.wait(10)

        for _ in range(DRAIN_SPARE_THREADS):
            runaway.enqueue()
        self.assertEqual(self.app.drain(), DRAIN_SPARE_THREADS)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(10)
            try:
                self.record.enqueue("child")
                os._exit(0 if self.app.drain() == 1 else 1)
            finally:
                os._exit(2)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_concurrent_drains_run_no_more_stuck_threads_than_the_spares(self):
        import threading

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)
        entered: list[int] = []

        @self.app.task(timeout=0.05, max_attempts=1)
        def runaway():
            entered.append(1)
            held.wait(10)

        for _ in range(DRAIN_SPARE_THREADS * 3):
            runaway.enqueue()
        drains = [threading.Thread(target=self.app.drain) for _ in range(DRAIN_SPARE_THREADS * 3)]
        for draining in drains:
            draining.start()
        for draining in drains:
            draining.join(5)
        self.assertEqual(len(entered), DRAIN_SPARE_THREADS)
        self.assertEqual(
            self.app.counts(), {"failed": DRAIN_SPARE_THREADS, "queued": DRAIN_SPARE_THREADS * 2}
        )

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

    def test_a_drain_waiting_for_a_spare_thread_holds_no_job_meanwhile(self):
        import threading
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        @self.app.task(timeout=30)
        def later():
            return None

        spares = self.app.thread_spares
        held = [asyncio.run(spares.reserve()) for _ in range(DRAIN_SPARE_THREADS)]
        job = later.enqueue()
        draining = threading.Thread(target=self.app.drain)
        draining.start()
        deadline = time.monotonic() + 5
        while job.refresh().hand_backs == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.1)
        waiting = job.refresh()
        self.assertEqual(
            (waiting.status, waiting.hand_backs, waiting.run_at), (JobStatus.QUEUED, 1, START)
        )
        for spare in held:
            spare.end()
        draining.join(5)
        self.assertFalse(draining.is_alive())
        self.assertIs(job.refresh().status, JobStatus.SUCCESS)

    def test_a_drain_with_its_spares_spent_leases_no_job_it_cannot_run(self):
        self.spend_the_spares()

        @self.app.task(timeout=5)
        def timed(i):
            return i

        jobs = [timed.enqueue(i) for i in range(50)]
        self.clock.advance(5)
        self.assertEqual(self.app.drain(), 0)
        fresh = [job.refresh() for job in jobs]
        self.assertEqual({job.status for job in fresh}, {JobStatus.QUEUED})
        self.assertEqual({job.run_at for job in fresh}, {START})
        self.assertEqual(sum(job.hand_backs for job in fresh), 1)

    def test_a_drain_with_its_spares_spent_runs_sync_jobs_without_a_timeout(self):
        self.spend_the_spares()
        jobs = [self.record.enqueue(str(i)) for i in range(3)]
        self.assertEqual(self.app.drain(), 3)
        self.assertEqual({job.refresh().status for job in jobs}, {JobStatus.SUCCESS})

    def test_a_job_left_queued_runs_before_newer_ones_once_a_spare_returns(self):
        import time

        held = self.spend_the_spares()

        @self.app.task(timeout=5)
        def step(name):
            self.calls.append(name)

        @self.app.task
        async def pause():
            self.calls.append("pause")

        step.enqueue("first")
        pause.enqueue()
        step.enqueue("second")
        self.clock.advance(5)
        self.assertEqual(self.app.drain(), 0)
        held.set()
        while self.app.thread_spares.taken:
            time.sleep(0.01)
        self.assertEqual(self.app.drain(), 3)
        self.assertEqual(self.calls, ["first", "pause", "second"])

    def test_a_job_refused_a_spare_while_it_waited_keeps_its_turn(self):
        import threading
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

        held = threading.Event()
        self.addCleanup(held.set)
        entered: list[int] = []

        @self.app.task(timeout=0.5, max_attempts=1)
        def runaway():
            entered.append(1)
            held.wait(10)

        @self.app.task(timeout=5)
        def step(name):
            self.calls.append(name)

        for _ in range(DRAIN_SPARE_THREADS):
            runaway.enqueue()
        drains = [threading.Thread(target=self.app.drain) for _ in range(DRAIN_SPARE_THREADS)]
        for draining in drains:
            draining.start()
        started = time.monotonic()
        while len(entered) < DRAIN_SPARE_THREADS and time.monotonic() - started < 5:
            time.sleep(0.01)
        first = step.enqueue("first")
        second = step.enqueue("second")
        self.clock.advance(5)
        self.assertEqual(self.app.drain(), 0)
        for draining in drains:
            draining.join(5)
        self.assertEqual([job.refresh().run_at for job in (first, second)], [START, START])
        held.set()
        while self.app.thread_spares.taken:
            time.sleep(0.01)
        self.assertEqual(self.app.drain(), 2)
        self.assertEqual(self.calls, ["first", "second"])

    def test_a_backend_that_cannot_hand_back_in_place_gets_a_plain_hand_back(self):
        self.spend_the_spares()

        @self.app.task(timeout=5)
        def timed():
            return None

        job = timed.enqueue()
        self.app.backend.hands_back_in_place = False
        self.clock.advance(5)
        self.assertEqual(self.app.drain(), 0)
        stored = job.refresh()
        self.assertEqual((stored.status, stored.run_at), (JobStatus.QUEUED, START + 5))

    def test_a_drain_inside_a_timed_job_runs_stuck_jobs_on_spare_threads(self):
        import threading
        import time

        from tinyjobs.worker.drain import DRAIN_SPARE_THREADS

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

        for _ in range(DRAIN_SPARE_THREADS * 3):
            hang.enqueue()
        outer.with_options(priority=1).enqueue()
        self.assertEqual(self.app.drain(), 2)
        self.assertEqual(took[0], DRAIN_SPARE_THREADS - 1)
        self.assertLess(took[1], 2)
        self.assertEqual(self.app.thread_spares.strayed, DRAIN_SPARE_THREADS)

    def test_a_job_run_inside_a_drained_job_never_uses_the_outer_spare(self):
        import threading

        held = threading.Event()
        self.addCleanup(held.set)
        inner = TinyJobs("memory://", eager=True)
        self.addCleanup(inner.close)
        seen: list[tuple[int, int]] = []

        @inner.task(timeout=0.05, max_attempts=1)
        def stuck():
            held.wait(10)

        @self.app.task(timeout=5)
        def outer():
            stuck.enqueue()
            spares = self.app.thread_spares
            seen.append((spares.taken, spares.strayed))

        outer.enqueue()
        self.assertEqual(self.app.drain(), 1)
        self.assertEqual(seen, [(1, 0)])
        self.assertEqual(self.app.thread_spares.taken, 0)

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


def _job_threads() -> int:
    import threading

    return sum(thread.name.startswith("tinyjobs_") for thread in threading.enumerate())


if __name__ == "__main__":
    unittest.main()
