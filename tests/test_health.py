from __future__ import annotations

import unittest

from tinyjobs import TinyJobs
from tinyjobs.clock import FrozenClock
from tinyjobs.duration import format_age

START = 1_791_633_600.0


class HealthTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FrozenClock(START)
        self.app = TinyJobs("sqlite://", clock=self.clock)

        @self.app.task(name="health.echo")
        def echo(value=None):
            return value

        self.echo = echo
        self.app.start()

    def tearDown(self) -> None:
        self.app.close()

    def claim(self, lease_seconds: float = 30.0):
        return self.app.run_sync(
            lambda: self.app.backend.claim(
                worker_id="w", queues=("default",), limit=1, lease_seconds=lease_seconds
            ),
            name="claim",
        )

    def failures(self, **options) -> list[str]:
        return [f.message for f in self.app.check(**options) if not f.ok]


class StatsTests(HealthTestCase):
    def test_counts_are_split_by_queue_and_status(self):
        self.echo.enqueue(1)
        self.echo.enqueue(2)
        self.echo.with_options(queue="mail").enqueue(3)
        self.claim()
        stats = self.app.stats()
        by_queue = {queue.queue: queue.counts for queue in stats.queues}
        self.assertEqual(
            by_queue, {"default": {"queued": 1, "running": 1}, "mail": {"queued": 1}}
        )
        self.assertEqual(stats.queued_tasks, {"health.echo": 2})

    def test_the_oldest_due_job_ignores_jobs_scheduled_for_later(self):
        self.echo.with_options(delay=600).enqueue()
        self.assertIsNone(self.app.stats().queues[0].oldest_due_at)
        self.clock.advance(5)
        self.echo.enqueue()
        self.clock.advance(40)
        self.assertEqual(self.app.stats().queues[0].oldest_due_at, START + 5)

    def test_expired_leases_are_counted(self):
        self.echo.enqueue()
        self.claim(lease_seconds=30)
        self.assertEqual(self.app.stats().expired_leases, 0)
        self.clock.advance(31)
        self.assertEqual(self.app.stats().expired_leases, 1)


class CheckTests(HealthTestCase):
    def test_a_healthy_app_passes(self):
        self.echo.enqueue()
        self.assertEqual(self.failures(max_wait=60), [])

    def test_an_app_without_tasks_fails(self):
        empty = TinyJobs("sqlite://")
        try:
            self.assertIn("no tasks registered", " ".join(
                f.message for f in empty.check() if not f.ok
            ))
        finally:
            empty.close()

    def test_queued_jobs_for_an_unregistered_task_fail(self):
        self.app.send("elsewhere.task")
        self.app.send("elsewhere.task")
        self.assertEqual(len(self.failures()), 1)
        self.assertIn("2 queued job(s) for elsewhere.task", self.failures()[0])

    def test_a_job_past_its_lease_fails(self):
        self.echo.enqueue()
        self.claim(lease_seconds=30)
        self.clock.advance(31)
        self.assertIn("past their lease", self.failures()[0])

    def test_a_queue_that_waited_too_long_fails_only_with_a_limit(self):
        self.echo.enqueue()
        self.clock.advance(300)
        self.assertEqual(self.failures(), [])
        self.assertIn("over the 1m00s allowed", self.failures(max_wait=60)[0])

    def test_a_schedule_nothing_fires_fails(self):
        @self.app.task(name="health.tick", cron="* * * * *")
        def tick():
            return None

        self.app.run_sync(lambda: self.app.scheduling.register(), name="register")
        self.assertEqual(self.failures(), [])
        self.clock.advance(180)
        self.assertIn("schedule health.tick is", self.failures()[0])


class AgeFormattingTests(unittest.TestCase):
    def test_ages_read_naturally(self):
        cases = {0: "0s", 59.9: "59s", 61: "1m01s", 3600: "1h00m", 3 * 86400 + 7200: "3d2h"}
        for seconds, text in cases.items():
            self.assertEqual(format_age(seconds), text)


if __name__ == "__main__":
    unittest.main()
