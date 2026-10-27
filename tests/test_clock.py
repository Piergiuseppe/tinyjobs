from __future__ import annotations

import contextlib
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import tinyjobs.clock
from tinyjobs import Retry, TinyJobs
from tinyjobs.clock import (
    FOLLOW_SECONDS,
    JUMP_SECONDS,
    RETRY_SECONDS,
    FollowingClock,
    elapsed,
)
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.options import EnqueueOptions
from tinyjobs.status import JobStatus


class Utc:
    shift = 0.0

    def now(self) -> float:
        return time.time() + self.shift

    def monotonic(self) -> float:
        return time.monotonic()


def read(clock: FollowingClock, reference: float, source: str = "redis a db 0") -> None:
    started = elapsed()
    clock.follow(reference, started, started + 0.01, source)


def later_by(seconds: float):
    real_elapsed, real_time = tinyjobs.clock.elapsed, time.time
    patches = contextlib.ExitStack()
    patches.enter_context(
        mock.patch.object(tinyjobs.clock, "elapsed", lambda: real_elapsed() + seconds)
    )
    patches.enter_context(
        mock.patch.object(tinyjobs.clock.time, "time", lambda: real_time() + seconds)
    )
    return patches


class FollowingClockTests(unittest.TestCase):
    def test_before_any_reading_now_is_the_wall_clock(self):
        self.assertAlmostEqual(FollowingClock().now(), time.time(), delta=0.01)

    def test_before_any_reading_a_local_instant_is_kept_exactly(self):
        self.assertEqual(FollowingClock().from_wall(1_234.5678), 1_234.5678)

    def test_before_any_reading_a_reading_is_due(self):
        self.assertTrue(FollowingClock().due())

    def test_after_a_reading_it_counts_from_the_reference(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)
        self.assertFalse(clock.due())

    def test_an_older_reading_never_overwrites_a_newer_one(self):
        clock = FollowingClock()
        first = elapsed()
        clock.follow(1_000.0, first + 0.2, first + 0.25, "redis a db 0")
        clock.follow(5_000.0, first, first + 0.3, "redis a db 0")
        self.assertLess(abs(clock.now() - 1_000.0), 5.0)

    def test_a_reading_is_due_again_once_it_is_old(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with later_by(FOLLOW_SECONDS + 1):
            self.assertTrue(clock.due())

    def stepped(self):
        real_time = time.time
        return mock.patch.object(
            tinyjobs.clock.time, "time", lambda: real_time() + JUMP_SECONDS * 4
        )

    def test_a_step_of_the_wall_clock_does_not_move_now(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with self.stepped():
            self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_local_instant_after_a_step_of_the_wall_clock_keeps_its_distance(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with self.stepped():
            moved = clock.from_wall(time.time() + 60)
            self.assertAlmostEqual(moved, clock.now() + 60, delta=0.1)

    def test_a_step_of_the_wall_clock_makes_a_reading_due(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with self.stepped():
            self.assertTrue(clock.due())

    def slow_reading(self, clock: FollowingClock, off_by: float) -> None:
        time.sleep(0.001)
        started = tinyjobs.clock.elapsed()
        expected = clock.now() + 1.0
        clock.follow(expected + off_by, started, started + 2.0, "redis a db 0")

    def test_a_slow_reading_that_agrees_keeps_the_good_one(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        clock.forget()
        self.slow_reading(clock, 0.5)
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_slow_reading_that_agrees_counts_as_a_fresh_one(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        clock.forget()
        self.slow_reading(clock, 0.5)
        with later_by(2.0 + RETRY_SECONDS):
            self.assertFalse(clock.due())

    def test_a_slow_reading_on_an_old_one_that_agrees_renews_it(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with later_by(FOLLOW_SECONDS + 1):
            self.slow_reading(clock, 0.5)
        with later_by(FOLLOW_SECONDS + 3.0 + RETRY_SECONDS):
            self.assertFalse(clock.due())

    def test_a_slow_reading_that_disagrees_is_followed(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        clock.forget()
        self.slow_reading(clock, 120.0)
        self.assertLess(abs(clock.now() - 1_120.0), 0.1)

    def test_a_slow_reading_that_disagrees_is_checked_again_a_little_later(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        self.slow_reading(clock, 120.0)
        self.assertFalse(clock.due())
        with later_by(2.0 + RETRY_SECONDS):
            self.assertTrue(clock.due())

    def test_a_first_slow_reading_is_used(self):
        clock = FollowingClock()
        started = elapsed()
        clock.follow(1_000.0, started, started + 2.0, "redis a db 0")
        self.assertLess(abs(clock.now() - 999.0), 0.1)

    def test_a_first_slow_reading_is_checked_again_a_little_later(self):
        clock = FollowingClock()
        started = elapsed()
        clock.follow(1_000.0, started, started + 2.0, "redis a db 0")
        self.assertFalse(clock.due())
        with later_by(2.0 + RETRY_SECONDS):
            self.assertTrue(clock.due())

    def test_a_second_server_that_agrees_is_accepted(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        read(clock, 1_000.0, source="redis b db 0")
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_second_server_that_agrees_refreshes_the_reading(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        clock.forget()
        time.sleep(0.001)
        read(clock, 1_000.0, source="redis b db 0")
        self.assertFalse(clock.due())

    def test_a_forget_after_a_slow_reading_asks_again_at_once(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        self.slow_reading(clock, 120.0)
        clock.forget()
        self.assertTrue(clock.due())

    def test_a_second_server_read_slowly_is_judged_by_its_round_trip(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        started = elapsed()
        expected = clock.now() + 1.5
        clock.follow(expected - 1.3, started, started + 3.0, "redis b db 0")
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_renewed_reading_is_not_replaced_by_one_that_started_before_it(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        time.sleep(0.001)
        earlier = elapsed()
        time.sleep(0.001)
        self.slow_reading(clock, 0.5)
        clock.follow(clock.now() + 3.0, earlier, elapsed() + 0.1, "redis a db 0")
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_second_server_that_disagrees_is_refused(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        with self.assertRaises(ConfigurationError):
            read(clock, 1_120.0, source="redis b db 0")

    def test_a_reading_that_started_before_a_forget_is_ignored(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        started = elapsed()
        clock.forget()
        clock.follow(9_000.0, started, elapsed(), "redis a db 0")
        self.assertLess(abs(clock.now() - 1_000.0), 0.1)

    def test_a_reading_that_started_before_a_forget_leaves_one_due(self):
        clock = FollowingClock()
        read(clock, 1_000.0)
        started = elapsed()
        clock.forget()
        clock.follow(9_000.0, started, elapsed(), "redis a db 0")
        self.assertTrue(clock.due())

    def test_equal_local_instants_map_to_equal_instants(self):
        clock = FollowingClock()
        read(clock, time.time() + 900)
        local = time.time() + 60
        self.assertEqual(len({clock.from_wall(local) for _ in range(200)}), 1)

    def test_a_local_instant_moves_by_the_difference_to_the_reference(self):
        clock = FollowingClock()
        read(clock, time.time() + 900)
        local = time.time() + 60
        self.assertAlmostEqual(clock.from_wall(local), local + 900, delta=0.1)


class ClockEdgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def app(self, **options) -> TinyJobs:
        app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}", **options)
        self.addCleanup(app.close)
        return app

    def test_a_run_at_is_stored_exactly_as_given_on_sqlite(self):
        app = self.app()

        @app.task(name="exact")
        def exact() -> None:
            return None

        asked = [time.time() + 60, 1_791_633_600.123456, time.time() + 0.5]
        stored = [exact.with_options(run_at=at).enqueue().run_at for at in asked]
        self.assertEqual(stored, asked)

    def test_a_datetime_run_at_is_stored_as_its_instant_on_sqlite(self):
        app = self.app()

        @app.task(name="dated")
        def dated() -> None:
            return None

        moment = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        job = dated.with_options(run_at=moment).enqueue()
        self.assertEqual(app.get(job.id).run_at, moment.timestamp())

    def test_a_retry_at_a_datetime_waits_until_then(self):
        app = self.app()
        moment = datetime.now(timezone.utc) + timedelta(minutes=10)

        @app.task(name="again", max_attempts=3)
        def again() -> None:
            raise Retry(at=moment)

        job = again.enqueue()
        app.drain()
        stored = app.get(job.id)
        self.assertEqual(stored.status, JobStatus.QUEUED)
        self.assertAlmostEqual(stored.run_at, moment.timestamp(), delta=0.01)

    def test_the_cron_slot_is_not_an_enqueue_option(self):
        app = self.app()

        @app.task(name="plain")
        def plain() -> None:
            return None

        with self.assertRaises(TypeError):
            EnqueueOptions(due_at=1.0)
        with self.assertRaises(TypeError):
            plain.with_options(due_at=1.0)

    def test_each_app_dates_its_job_ids_by_its_own_clock(self):
        ahead = Utc()
        ahead.shift = 86_400.0
        first = self.app(clock=ahead)
        second = TinyJobs(
            f"sqlite:///{Path(self.directory.name) / 'other.db'}", clock=Utc()
        )
        self.addCleanup(second.close)
        for app in (first, second):
            app.task(name="plain")(lambda: None)
        first.send("plain")
        job = second.send("plain")
        minted = int(uuid.UUID(job.id).hex[:12], 16) / 1000
        self.assertAlmostEqual(minted, time.time(), delta=5)

    def test_a_clock_with_only_now_and_monotonic_takes_a_run_at(self):
        app = self.app(clock=Utc())
        later = time.time() + 60

        @app.task(name="timed")
        def timed() -> None:
            return None

        self.assertEqual(timed.with_options(run_at=later).enqueue().run_at, later)

    def test_a_clock_with_only_now_and_monotonic_takes_a_retry_at(self):
        app = self.app(clock=Utc())
        later = time.time() + 60

        @app.task(name="again", max_attempts=2)
        def again() -> None:
            raise Retry(at=later)

        retried = again.enqueue()
        app.drain()
        self.assertEqual(app.get(retried.id).run_at, later)

    def test_a_clock_with_only_now_and_monotonic_fires_cron(self):
        clock = Utc()
        app = self.app(clock=clock)

        @app.task(cron="* * * * *")
        def tick() -> None:
            return None

        app.start()
        app.run_sync(app.scheduling.register, name="register")
        clock.shift = 120.0
        app.run_sync(lambda: app.scheduling.fire_due(60.0), name="fire")
        self.assertEqual(app.counts().get("queued"), 1)

    def test_a_naive_datetime_run_at_is_local_time(self):
        app = self.app()

        @app.task(name="local")
        def local() -> None:
            return None

        moment = datetime(2030, 1, 2, 3, 4, 5)
        job = local.with_options(run_at=moment).enqueue()
        self.assertEqual(job.run_at, moment.timestamp())

    def test_a_frozen_clock_at_any_time_still_makes_job_ids(self):
        from tinyjobs import FrozenClock

        for at in (-1.0, 1e13, float("inf")):
            with self.subTest(at=at):
                app = TinyJobs(
                    f"sqlite:///{Path(self.directory.name) / f'{at}.db'}",
                    clock=FrozenClock(at),
                )
                self.addCleanup(app.close)
                app.task(name="plain")(lambda: None)
                self.assertEqual(uuid.UUID(app.send("plain").id).version, 7)

    def test_a_frozen_clock_far_ahead_leaves_ids_working_for_other_apps(self):
        from tinyjobs import FrozenClock

        far = self.app(clock=FrozenClock(1e13))
        far.task(name="plain")(lambda: None)
        far.send("plain")
        near = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'near.db'}")
        self.addCleanup(near.close)
        near.task(name="plain")(lambda: None)
        self.assertEqual(uuid.UUID(near.send("plain").id).version, 7)


if __name__ == "__main__":
    unittest.main()
