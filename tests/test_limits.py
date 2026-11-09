from __future__ import annotations

import asyncio
import threading
import time
import unittest
from dataclasses import replace
from unittest import mock

from tinyjobs import ConfigurationError, FrozenClock, JobStatus, TinyJobs
from tinyjobs.backends.sqlite.backend import SQLiteBackend
from tinyjobs.limiting import LIMITS_UNSUPPORTED_MESSAGE, limits_for, polling_under
from tinyjobs.limits import Limit, interval_of
from tests.support import BackendTestCase

START = 1_791_633_600.0


class DeclaringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = TinyJobs("memory://")
        self.addCleanup(self.app.close)

    def test_a_task_without_limits_has_none(self):
        @self.app.task
        def plain():
            return None

        self.assertIsNone(plain.limit)
        self.assertEqual(self.app.registry.limits(), {})

    def test_concurrency_and_rate_become_a_limit_on_the_task(self):
        @self.app.task(concurrency=2, rate="10/m")
        def both():
            return None

        @self.app.task(rate="1/30s")
        def spaced():
            return None

        self.assertEqual(both.limit, Limit(concurrency=2, interval=6.0))
        self.assertEqual(spaced.limit, Limit(concurrency=None, interval=30.0))
        self.assertEqual(
            self.app.registry.limits(), {both.name: both.limit, spaced.name: spaced.limit}
        )

    def test_rates_read_as_starts_per_period(self):
        self.assertEqual(interval_of("t", "10/m"), 6.0)
        self.assertEqual(interval_of("t", "100/h"), 36.0)
        self.assertEqual(interval_of("t", "2/d"), 43200.0)
        self.assertEqual(interval_of("t", "4 / 2 minutes"), 30.0)
        self.assertEqual(interval_of("t", "1/s"), 1.0)
        self.assertEqual(interval_of("t", "30/M"), 2.0)

    def test_a_malformed_rate_is_refused_with_the_format(self):
        huge = "9" * 5000
        for rate in ("fast", "10", "/m", "0/m", "10/0s", "10/fortnight", 10, "ten/m", f"1/{huge}d", f"{huge}/s", "1234567890/s"):
            with self.subTest(rate=rate), self.assertRaises(ConfigurationError) as caught:
                self.app.task(rate=rate)(lambda: None)
            self.assertIn('"10/m"', str(caught.exception))
            self.assertIn(repr(rate), str(caught.exception))

    def test_a_concurrency_below_one_or_not_a_number_is_refused(self):
        for concurrency in (0, -1, 1.5, "2", True):
            with self.subTest(concurrency=concurrency), self.assertRaises(
                ConfigurationError
            ) as caught:
                self.app.task(concurrency=concurrency)(lambda: None)
            self.assertIn("across every worker", str(caught.exception))


class StoreSupportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = TinyJobs("memory://")
        self.addCleanup(self.app.close)

        @self.app.task(concurrency=1, name="limited")
        def limited():
            return None

        self.limited = limited

    def unsupporting(self):
        backend = self.app.start().backend
        return mock.patch.object(
            backend, "capabilities", replace(SQLiteBackend.capabilities, limits=False)
        )

    def test_a_store_that_enforces_limits_receives_them_with_the_claim(self):
        backend = self.app.start().backend
        self.assertEqual(
            limits_for(backend, self.app.registry), {"limits": {"limited": Limit(concurrency=1)}}
        )

    def test_a_store_without_limits_is_never_asked_for_them(self):
        with self.unsupporting():
            self.assertEqual(limits_for(self.app.backend, self.app.registry), {})

    def test_a_worker_refuses_a_store_that_cannot_enforce_a_declared_limit(self):
        with self.unsupporting(), self.assertRaises(ConfigurationError) as caught:
            self.app.worker(handle_signals=False).run()
        self.assertEqual(
            str(caught.exception), LIMITS_UNSUPPORTED_MESSAGE.format(names="limited")
        )

    def test_a_drain_refuses_a_store_that_cannot_enforce_a_declared_limit(self):
        self.limited.enqueue()
        with self.unsupporting(), self.assertRaises(ConfigurationError):
            self.app.drain()
        self.assertEqual(self.app.counts(), {"queued": 1})

    def test_a_store_without_limits_still_runs_an_app_without_limited_tasks(self):
        app = TinyJobs("memory://")
        self.addCleanup(app.close)

        @app.task
        def plain():
            return None

        plain.enqueue()
        with mock.patch.object(
            app.start().backend, "capabilities", replace(SQLiteBackend.capabilities, limits=False)
        ):
            self.assertEqual(app.drain(), 1)


class PollingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = TinyJobs("memory://")
        self.addCleanup(self.app.close)

    def test_a_worker_polls_at_least_as_often_as_its_shortest_rate(self):
        @self.app.task(rate="10/s", name="quick")
        def quick():
            return None

        @self.app.task(rate="1/m", name="slow")
        def slow():
            return None

        @self.app.task(concurrency=2, name="capped")
        def capped():
            return None

        backend = self.app.start().backend
        interval = polling_under(backend, self.app.registry, 0.005, 0.5)
        self.assertEqual(interval._maximum, 0.1)
        self.assertEqual(interval._minimum, 0.005)

    def test_a_rate_longer_than_the_ceiling_leaves_the_ceiling_alone(self):
        @self.app.task(rate="1/m", name="slow")
        def slow():
            return None

        backend = self.app.start().backend
        self.assertEqual(polling_under(backend, self.app.registry, 0.005, 0.5)._maximum, 0.5)

    def test_a_rate_shorter_than_the_floor_stops_at_the_floor(self):
        @self.app.task(rate="1000/s", name="torrent")
        def torrent():
            return None

        backend = self.app.start().backend
        self.assertEqual(polling_under(backend, self.app.registry, 0.005, 0.5)._maximum, 0.05)
        self.assertEqual(polling_under(backend, self.app.registry, 0.2, 0.5)._maximum, 0.2)

    def test_without_rates_the_ceiling_is_the_configured_one(self):
        @self.app.task(concurrency=2, name="capped")
        def capped():
            return None

        backend = self.app.start().backend
        self.assertEqual(polling_under(backend, self.app.registry, 0.005, 0.5)._maximum, 0.5)


class DrainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FrozenClock(START)
        self.app = TinyJobs("memory://", clock=self.clock)
        self.addCleanup(self.app.close)
        self.ran: list[int] = []

    def test_a_drain_spaces_a_rate_limited_task_by_the_clock(self):
        @self.app.task(rate="10/m")
        def spaced(number: int):
            self.ran.append(number)

        for number in range(3):
            spaced.enqueue(number)
        self.assertEqual(self.app.drain(), 1)
        self.assertEqual(self.app.counts(), {"success": 1, "held": 2})
        self.assertEqual(self.app.drain(), 0)
        self.clock.advance(6)
        self.assertEqual(self.app.drain(), 1)
        self.clock.advance(6)
        self.assertEqual(self.app.drain(), 1)
        self.assertEqual(self.ran, [0, 1, 2])

    def test_a_drain_runs_a_concurrency_limited_task_to_the_end(self):
        @self.app.task(concurrency=1)
        def capped(number: int):
            self.ran.append(number)

        for number in range(3):
            capped.enqueue(number)
        self.assertEqual(self.app.drain(), 3)
        self.assertEqual(self.ran, [0, 1, 2])

    def test_an_eager_call_ignores_the_limits(self):
        app = TinyJobs("memory://", eager=True, clock=self.clock)
        self.addCleanup(app.close)

        @app.task(rate="1/h", concurrency=1)
        def inline(number: int):
            self.ran.append(number)

        for number in range(3):
            self.assertIs(inline.enqueue(number).status, JobStatus.SUCCESS)
        self.assertEqual(self.ran, [0, 1, 2])

    def test_a_held_job_is_waited_for_and_shows_as_held(self):
        @self.app.task(rate="1/h")
        def spaced():
            return None

        spaced.enqueue()
        held = spaced.enqueue()
        self.app.drain()
        self.assertIs(held.refresh().status, JobStatus.HELD)
        self.assertEqual(
            [job.id for job in self.app.jobs(status=JobStatus.HELD).items], [held.id]
        )
        with self.assertRaises(TimeoutError):
            held.wait(timeout=0.05)
        self.assertTrue(self.app.cancel(held.id))
        self.assertIs(held.wait(timeout=1.0).status, JobStatus.CANCELLED)

    def test_an_eager_call_on_a_store_without_claim_job_still_ignores_the_limits(self):
        app = TinyJobs("memory://", eager=True, clock=self.clock)
        self.addCleanup(app.close)

        @app.task(rate="1/h")
        def inline(number: int):
            self.ran.append(number)

        backend = app.start().backend
        with mock.patch.object(type(backend), "claims_by_id", mock.PropertyMock(return_value=False)):
            self.assertIs(inline.enqueue(1).status, JobStatus.SUCCESS)
            self.assertIs(inline.enqueue(2).status, JobStatus.SUCCESS)
        self.assertEqual(app.counts(), {"success": 2})
        self.assertEqual(self.ran, [1, 2])

    def test_an_eager_call_runs_on_a_store_that_cannot_enforce_limits(self):
        app = TinyJobs("memory://", eager=True, clock=self.clock)
        self.addCleanup(app.close)

        @app.task(rate="1/h")
        def inline(number: int):
            self.ran.append(number)

        backend = app.start().backend
        with mock.patch.object(
            backend, "capabilities", replace(SQLiteBackend.capabilities, limits=False)
        ), mock.patch.object(backend, "release_held", wraps=backend.release_held) as release:
            self.assertIs(inline.enqueue(1).status, JobStatus.SUCCESS)
            self.assertIs(inline.enqueue(2).status, JobStatus.SUCCESS)
        release.assert_not_called()
        self.assertEqual(self.ran, [1, 2])

    def test_held_jobs_count_as_waiting_in_stats_metrics_and_the_check(self):
        @self.app.task(rate="1/h")
        def spaced():
            return None

        spaced.enqueue()
        spaced.enqueue()
        self.app.drain()
        self.clock.advance(600)
        stats = self.app.stats()
        self.assertEqual(stats.queued_tasks, {spaced.name: 1})
        self.assertEqual(stats.queues[0].oldest_due_at, START)
        rendered = self.app.metrics.render(stats, now=self.clock.now())
        self.assertIn(f'tinyjobs_store_queued_jobs{{task="{spaced.name}"}} 1', rendered)
        self.assertIn('tinyjobs_store_oldest_due_seconds{queue="default"} 600', rendered)
        waiting = [finding for finding in self.app.check(max_wait=300.0) if "waited" in finding.message]
        self.assertEqual([finding.ok for finding in waiting], [False])
        self.assertIn("over the 5m00s allowed", waiting[0].message)


class ReleasingAtStartTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        import shutil

        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        self.url = f"sqlite:///{directory}/shared.db"
        self.clock = FrozenClock(START)
        self.ran: list[str] = []

    def app_with(self, *, limited: bool, registered: bool = True) -> TinyJobs:
        app = TinyJobs(self.url, clock=self.clock)
        self.addCleanup(app.close)
        if registered:
            options = {"rate": "1/h"} if limited else {}

            @app.task(name="shared", **options)
            def shared():
                self.ran.append("shared")

        @app.task(name="other")
        def other():
            self.ran.append("other")

        return app

    def held_two(self) -> TinyJobs:
        app = self.app_with(limited=True)
        app.send("shared")
        app.send("shared")
        self.assertEqual(app.drain(), 1)
        self.assertEqual(app.counts(), {"success": 1, "held": 1})
        return app

    def test_a_drain_releases_the_held_jobs_of_a_task_it_registers_without_a_limit(self):
        self.held_two()
        stranger = self.app_with(limited=False, registered=False)
        self.assertEqual(stranger.drain(), 0)
        self.assertEqual(stranger.counts(), {"success": 1, "held": 1})
        freed = self.app_with(limited=False)
        self.assertEqual(freed.drain(), 1)
        self.assertEqual(freed.counts(), {"success": 2})

    def test_a_worker_releases_the_held_jobs_of_a_task_it_registers_without_a_limit(self):
        self.held_two()
        freed = self.app_with(limited=False)
        worker = freed.worker(scheduler=False, handle_signals=False, poll_maximum=0.05)

        def stop_when_done() -> None:
            deadline = time.monotonic() + 10
            while freed.counts().get("success", 0) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            worker.stop()

        stopper = threading.Thread(target=stop_when_done, daemon=True)
        stopper.start()
        worker.run()
        stopper.join(5)
        self.assertEqual(freed.counts(), {"success": 2})

    def test_a_worker_keeps_releasing_what_an_older_build_holds_after_it_started(self):
        old_build = self.app_with(limited=True)
        freed = self.app_with(limited=False)
        gate = threading.Event()
        self.addCleanup(gate.set)

        @freed.task(name="blocking")
        def blocking():
            gate.wait(10)

        freed.send("blocking")
        backend = freed.start().backend
        real_release, returns = backend.release_held, []

        async def recording(*args, **kwargs):
            released = await real_release(*args, **kwargs)
            returns.append(released)
            return released

        worker = freed.worker(
            concurrency=1, scheduler=False, handle_signals=False, poll_maximum=0.05,
            reap_interval=0.2,
        )

        drained: list[int] = []

        def drive() -> None:
            deadline = time.monotonic() + 10
            try:
                while freed.counts().get("running", 0) < 1 and time.monotonic() < deadline:
                    time.sleep(0.01)
                for _ in range(3):
                    old_build.send("shared")
                drained.append(old_build.drain())
                gate.set()
                while freed.counts().get("success", 0) < 4 and time.monotonic() < deadline:
                    time.sleep(0.02)
            finally:
                gate.set()
                worker.stop()

        driver = threading.Thread(target=drive, daemon=True)
        with mock.patch.object(backend, "release_held", recording):
            driver.start()
            worker.run()
            driver.join(5)
        self.assertEqual(drained, [1])
        self.assertEqual(freed.counts(), {"success": 4})
        self.assertEqual(returns[0], 0)
        self.assertIn(2, returns[1:])

    def test_a_worker_releases_only_on_the_queues_it_serves(self):
        self.held_two()
        elsewhere = self.app_with(limited=False)
        worker = elsewhere.worker(
            queues=["other"], scheduler=False, handle_signals=False, poll_maximum=0.05,
            reap_interval=0.1,
        )
        stopper = threading.Thread(target=lambda: (time.sleep(0.5), worker.stop()), daemon=True)
        stopper.start()
        worker.run()
        stopper.join(5)
        self.assertEqual(elsewhere.counts(), {"success": 1, "held": 1})

    def test_a_drain_releases_only_on_the_queues_it_is_given(self):
        self.held_two()
        elsewhere = self.app_with(limited=False)
        self.assertEqual(elsewhere.drain(queues=["other"]), 0)
        self.assertEqual(elsewhere.counts(), {"success": 1, "held": 1})
        self.assertEqual(elsewhere.drain(queues=["default"]), 1)
        self.assertEqual(elsewhere.counts(), {"success": 2})

    def test_a_worker_whose_release_fails_still_starts_and_tries_again_later(self):
        self.held_two()
        freed = self.app_with(limited=False)
        backend = freed.start().backend
        real_release, calls = backend.release_held, []

        async def failing_once(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("odd")
            return await real_release(*args, **kwargs)

        worker = freed.worker(
            scheduler=False, handle_signals=False, poll_maximum=0.05, reap_interval=0.1
        )

        def stop_when_done() -> None:
            deadline = time.monotonic() + 10
            while freed.counts().get("success", 0) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            worker.stop()

        stopper = threading.Thread(target=stop_when_done, daemon=True)
        with mock.patch.object(backend, "release_held", failing_once):
            with self.assertLogs("tinyjobs.worker", level="WARNING") as logs:
                stopper.start()
                worker.run()
                stopper.join(5)
        self.assertIn("stay held for now (odd)", "\n".join(logs.output))
        self.assertEqual(freed.counts(), {"success": 2})
        self.assertGreaterEqual(len(calls), 2)

    def test_a_failing_release_at_start_is_logged_and_the_work_goes_on(self):
        self.held_two()
        freed = self.app_with(limited=False)
        freed.send("other")
        backend = freed.start().backend
        with mock.patch.object(backend, "release_held", side_effect=RuntimeError("odd")):
            with self.assertLogs("tinyjobs.worker", level="WARNING") as logs:
                self.assertEqual(freed.drain(), 1)
        self.assertIn("stay held for now (odd)", "\n".join(logs.output))
        self.assertEqual(freed.counts(), {"success": 2, "held": 1})


class WorkerTests(BackendTestCase):
    def run_worker(self, worker, *, until, seconds: float = 20.0) -> None:
        def stop_when_done() -> None:
            deadline = time.monotonic() + seconds
            while not until() and time.monotonic() < deadline:
                time.sleep(0.02)
            worker.stop()

        stopper = threading.Thread(target=stop_when_done, daemon=True)
        stopper.start()
        worker.run()
        stopper.join(5)

    def settled(self, total: int):
        return lambda: self.app.counts().get("success", 0) >= total

    def test_a_worker_never_runs_more_of_a_task_than_its_concurrency(self):
        lock = threading.Lock()
        inflight = {"now": 0, "peak": 0}
        release = threading.Event()
        self.addCleanup(release.set)

        @self.app.task(concurrency=2, name="capped")
        def capped():
            with lock:
                inflight["now"] += 1
                inflight["peak"] = max(inflight["peak"], inflight["now"])
            release.wait(2)
            with lock:
                inflight["now"] -= 1

        for _ in range(6):
            capped.enqueue()

        def free_when_two_run():
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and inflight["now"] < 2:
                time.sleep(0.01)
            time.sleep(0.2)
            release.set()

        threading.Thread(target=free_when_two_run, daemon=True).start()
        worker = self.app.worker(
            concurrency=6, poll_maximum=0.02, scheduler=False, handle_signals=False
        )
        self.run_worker(worker, until=self.settled(6))
        self.assertEqual(self.app.counts(), {"success": 6})
        self.assertEqual(inflight["peak"], 2)

    def test_a_worker_spaces_the_starts_of_a_rate_limited_task(self):
        starts: list[float] = []

        @self.app.task(rate="4/s", name="spaced")
        def spaced():
            starts.append(time.monotonic())

        for _ in range(4):
            spaced.enqueue()
        worker = self.app.worker(
            concurrency=4, poll_maximum=0.5, scheduler=False, handle_signals=False
        )
        began = time.monotonic()
        self.run_worker(worker, until=self.settled(4))
        self.assertEqual(self.app.counts(), {"success": 4})
        gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
        self.assertTrue(all(gap >= 0.15 for gap in gaps), gaps)
        self.assertLess(time.monotonic() - began, 3.0)

    def test_a_worker_claims_again_as_soon_as_a_capped_task_has_room(self):
        @self.app.task(concurrency=1, name="capped")
        def capped():
            time.sleep(0.1)

        for _ in range(3):
            capped.enqueue()
        worker = self.app.worker(
            concurrency=4, poll_minimum=1.0, poll_maximum=1.0, scheduler=False,
            handle_signals=False,
        )
        began = time.monotonic()
        self.run_worker(worker, until=self.settled(3))
        self.assertEqual(self.app.counts(), {"success": 3})
        self.assertLess(time.monotonic() - began, 1.5)

    def test_a_worker_claims_again_when_the_interval_of_a_rate_ends(self):
        @self.app.task(rate="4/s", name="spaced")
        def spaced():
            return None

        for _ in range(4):
            spaced.enqueue()
        worker = self.app.worker(
            concurrency=4, poll_minimum=1.0, poll_maximum=1.0, scheduler=False,
            handle_signals=False,
        )
        began = time.monotonic()
        self.run_worker(worker, until=self.settled(4))
        self.assertEqual(self.app.counts(), {"success": 4})
        self.assertLess(time.monotonic() - began, 2.0)

    def test_a_worker_claims_again_at_once_when_an_interval_ended_during_a_slow_claim(self):
        @self.app.task(rate="10/s", name="spaced")
        def spaced():
            return None

        for _ in range(4):
            spaced.enqueue()
        backend = self.app.start().backend
        real_claim = backend.claim

        async def slow_claim(*args, **kwargs):
            jobs = await real_claim(*args, **kwargs)
            await asyncio.sleep(0.2)
            return jobs

        worker = self.app.worker(
            concurrency=4, poll_minimum=1.0, poll_maximum=1.0, scheduler=False,
            handle_signals=False,
        )
        began = time.monotonic()
        with mock.patch.object(backend, "claim", slow_claim):
            self.run_worker(worker, until=self.settled(4))
        self.assertEqual(self.app.counts(), {"success": 4})
        self.assertLess(time.monotonic() - began, 3.0)

    def test_a_worker_bounds_its_poll_by_the_shortest_rate(self):
        @self.app.task(rate="10/s", name="quick")
        def quick():
            return None

        worker = self.app.worker(poll_maximum=0.5, scheduler=False, handle_signals=False)

        async def started():
            runner = asyncio.create_task(worker.start())
            for _ in range(200):
                if worker._serving:
                    break
                await asyncio.sleep(0.01)
            maximum = worker._interval._maximum
            worker.stop()
            await asyncio.wait_for(runner, 5)
            return maximum

        self.assertEqual(asyncio.run(started()), 0.1)


if __name__ == "__main__":
    unittest.main()
