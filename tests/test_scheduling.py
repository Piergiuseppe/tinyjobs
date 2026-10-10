from __future__ import annotations

import asyncio
import multiprocessing
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.clock import FrozenClock
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.services.scheduling import MAXIMUM_CATCH_UP
from tinyjobs.status import JobStatus

START = datetime(2026, 10, 10, 9, 59, 30, tzinfo=timezone.utc).timestamp()
TEN = datetime(2026, 10, 10, 10, 0, tzinfo=timezone.utc).timestamp()
GRACE = 60.0
RACERS = 6


def declare(app: TinyJobs, cron: str = "0 * * * *", **options):
    @app.task(name="report.hourly", cron=cron, **options)
    def hourly():
        return None

    return hourly


def race(database: str, at: float, barrier, sink) -> None:
    app = TinyJobs(f"sqlite:///{database}", clock=FrozenClock(at))
    declare(app)
    app.start()
    barrier.wait()
    tick = app.run_sync(app.scheduling.fire_due(GRACE), name="tick")
    sink.put([job.id for job in tick.created])
    app.close()


class SchedulingTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.db"
        self.clock = FrozenClock(START)
        self.app = TinyJobs(f"sqlite:///{self.database}", clock=self.clock)
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await self.app.aclose()
        self.directory.cleanup()

    async def state(self, name: str = "report.hourly"):
        return {state.name: state for state in await self.app.aschedules()}[name]

    async def tick(self):
        return await self.app.scheduling.fire_due(GRACE)

    async def jobs(self):
        return (await self.app.ajobs(limit=1000)).items


class RegistrationTests(SchedulingTestCase):
    async def test_registering_stores_the_next_firing(self):
        declare(self.app)
        await self.app.scheduling.register()
        state = await self.state()
        self.assertEqual(state.next_run_at, TEN)
        self.assertEqual(state.cron, "0 * * * *")
        self.assertEqual(state.timezone, "UTC")

    async def test_registering_again_keeps_the_stored_firing(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(7200)
        await self.app.scheduling.register()
        self.assertEqual((await self.state()).next_run_at, TEN)

    async def test_changing_the_cron_recomputes_the_firing(self):
        declare(self.app)
        await self.app.scheduling.register()
        other = TinyJobs(f"sqlite:///{self.database}", clock=self.clock)
        declare(other, cron="*/5 * * * *")
        await other.ready()
        await other.scheduling.register()
        await other.aclose()
        state = await self.state()
        self.assertEqual(state.cron, "*/5 * * * *")
        self.assertEqual(state.next_run_at, TEN)

    async def test_changing_the_timezone_recomputes_the_firing(self):
        declare(self.app)
        await self.app.scheduling.register()
        other = TinyJobs(f"sqlite:///{self.database}", clock=self.clock)
        declare(other, cron="30 * * * *", timezone="Asia/Kolkata")
        await other.ready()
        await other.scheduling.register()
        await other.aclose()
        self.assertEqual((await self.state()).next_run_at, TEN)

    async def test_a_deleted_row_is_registered_again_on_the_next_tick(self):
        declare(self.app)
        await self.app.scheduling.register()
        await self.app.backend._call(
            lambda: self.app.backend._connections.connection.execute(
                "DELETE FROM schedules"
            )
        )
        await self.tick()
        self.assertEqual((await self.state()).next_run_at, TEN)


class FiringTests(SchedulingTestCase):
    async def test_nothing_happens_before_the_time(self):
        declare(self.app)
        await self.app.scheduling.register()
        tick = await self.tick()
        self.assertEqual(tick.created, [])
        self.assertEqual(tick.wake_at, TEN)

    async def test_an_on_time_tick_creates_one_job(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(31)
        tick = await self.tick()
        self.assertEqual((len(tick.created), tick.missed), (1, []))
        job = tick.created[0]
        self.assertEqual(job.task, "report.hourly")
        self.assertEqual(job.run_at, TEN)
        self.assertIs((await self.app.aget(job.id)).status, JobStatus.QUEUED)
        state = await self.state()
        self.assertEqual(state.next_run_at, TEN + 3600)
        self.assertEqual(state.last_job_id, job.id)
        self.assertEqual(tick.wake_at, TEN + 3600)

    async def test_a_second_tick_at_the_same_moment_does_nothing(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(31)
        await self.tick()
        self.assertEqual((await self.tick()).created, [])
        self.assertEqual(len(await self.jobs()), 1)

    async def test_the_job_carries_the_task_defaults(self):
        @self.app.task(name="report.slow", cron="0 * * * *", queue="slow",
                       priority=7, max_attempts=4)
        def slow():
            return None

        await self.app.scheduling.register()
        self.clock.advance(31)
        job = (await self.tick()).created[0]
        self.assertEqual((job.queue, job.priority, job.max_attempts), ("slow", 7, 4))

    async def test_lateness_inside_the_grace_still_counts_as_on_time(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(30 + GRACE)
        self.assertEqual(len((await self.tick()).created), 1)


class MisfireTests(SchedulingTestCase):
    async def outage(self, hours: float, **options):
        declare(self.app, **options)
        await self.app.scheduling.register()
        self.clock.advance(30 + hours * 3600 + 600)
        return await self.tick()

    async def test_skip_drops_missed_firings(self):
        tick = await self.outage(3)
        self.assertEqual(tick.created, [])
        self.assertEqual(tick.missed, ["report.hourly"])
        self.assertEqual((await self.state()).next_run_at, TEN + 4 * 3600)

    async def test_skip_still_fires_one_that_falls_inside_the_grace(self):
        declare(self.app, cron="*/5 * * * *")
        await self.app.scheduling.register()
        self.clock.advance(30 + 3600 + 20)
        tick = await self.tick()
        self.assertEqual([job.run_at for job in tick.created], [TEN + 3600])
        self.assertEqual(tick.missed, ["report.hourly"])

    async def test_run_once_creates_a_single_catch_up_job(self):
        tick = await self.outage(3, misfire="run_once")
        self.assertEqual(len(tick.created), 1)
        self.assertEqual(tick.missed, ["report.hourly"])
        self.assertEqual(tick.created[0].run_at, self.clock.now())
        self.assertEqual((await self.state()).next_run_at, TEN + 4 * 3600)

    async def test_run_all_creates_every_missed_firing_in_order(self):
        tick = await self.outage(3, misfire="run_all", overlap=True)
        self.assertEqual(
            [job.run_at for job in tick.created],
            [TEN, TEN + 3600, TEN + 7200, TEN + 10800],
        )

    async def test_run_all_catches_up_in_bounded_batches(self):
        declare(self.app, cron="* * * * *", misfire="run_all", overlap=True)
        await self.app.scheduling.register()
        self.clock.advance(30 + (MAXIMUM_CATCH_UP * 2 + 10) * 60)
        first = await self.tick()
        self.assertEqual(len(first.created), MAXIMUM_CATCH_UP)
        self.assertLessEqual(first.wake_at, self.clock.now())
        second = await self.tick()
        third = await self.tick()
        self.assertEqual(len(second.created), MAXIMUM_CATCH_UP)
        self.assertEqual(len(third.created), 11)
        self.assertEqual(len({job.run_at for job in await self.jobs()}), 211)

    async def test_run_all_requires_overlap(self):
        with self.assertRaises(ConfigurationError):
            declare(self.app, misfire="run_all")


class ClockTests(SchedulingTestCase):
    async def test_a_clock_that_jumps_back_does_not_silence_the_schedule(self):
        declare(self.app, cron="* * * * *", overlap=True)
        await self.app.scheduling.register()
        self.clock.advance(3600)
        await self.tick()
        self.clock.advance(-3600)
        tick = await self.tick()
        self.assertEqual(tick.rewound, ["report.hourly"])
        self.assertEqual((await self.state()).next_run_at, TEN)
        self.clock.advance(31)
        self.assertEqual(len((await self.tick()).created), 1)

    async def test_skew_inside_the_grace_is_not_mistaken_for_a_jump(self):
        declare(self.app, cron="* * * * *")
        await self.app.scheduling.register()
        self.clock.advance(31)
        await self.tick()
        self.clock.advance(-GRACE + 5)
        tick = await self.tick()
        self.assertEqual((tick.rewound, tick.created), ([], []))
        self.assertEqual((await self.state()).next_run_at, TEN + 60)


class OverlapTests(SchedulingTestCase):
    async def test_a_new_run_waits_for_the_previous_one(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(31)
        first = (await self.tick()).created[0]
        self.clock.advance(3600)
        tick = await self.tick()
        self.assertEqual(tick.created, [])
        self.assertEqual(tick.skipped, ["report.hourly"])
        self.assertEqual((await self.state()).next_run_at, TEN + 7200)

        claimed = await self.app.backend.claim(
            worker_id="w", queues=("default",), limit=1, lease_seconds=60
        )
        await self.app.backend.complete(claimed[0], None)
        self.clock.advance(3600)
        follow_up = (await self.tick()).created
        self.assertEqual(len(follow_up), 1)
        self.assertNotEqual(follow_up[0].id, first.id)

    async def test_overlap_lets_runs_pile_up(self):
        declare(self.app, overlap=True)
        await self.app.scheduling.register()
        self.clock.advance(31)
        await self.tick()
        self.clock.advance(3600)
        self.assertEqual(len((await self.tick()).created), 1)
        self.assertEqual(len(await self.jobs()), 2)


class LeaderFreeTests(SchedulingTestCase):
    async def test_many_apps_racing_on_one_file_create_one_job(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(31)
        racers = []
        for _ in range(8):
            racer = TinyJobs(f"sqlite:///{self.database}", clock=self.clock)
            declare(racer)
            await racer.ready()
            racers.append(racer)
        ticks = await asyncio.gather(
            *(racer.scheduling.fire_due(GRACE) for racer in racers)
        )
        for racer in racers:
            await racer.aclose()
        self.assertEqual(sum(len(tick.created) for tick in ticks), 1)
        self.assertEqual(len(await self.jobs()), 1)

    async def test_many_processes_racing_create_one_job(self):
        declare(self.app)
        await self.app.scheduling.register()
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(RACERS)
        sink = context.Queue()
        processes = [
            context.Process(
                target=race, args=(str(self.database), TEN + 1, barrier, sink)
            )
            for _ in range(RACERS)
        ]
        for process in processes:
            process.start()
        created = [sink.get(timeout=60) for _ in processes]
        for process in processes:
            process.join(timeout=30)
        self.assertEqual(sum(len(ids) for ids in created), 1)
        self.assertEqual(len(await self.jobs()), 1)


class WorkerTests(SchedulingTestCase):
    async def run_worker(self, **options):
        worker = self.app.worker(
            concurrency=2, reap_interval=60.0, shutdown_timeout=5.0, **options
        )
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            jobs = await self.jobs()
            if jobs and all(job.status.is_terminal for job in jobs):
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def test_the_worker_fires_and_runs_a_cron_task(self):
        ran = []

        @self.app.task(name="report.hourly", cron="0 * * * *")
        async def hourly():
            ran.append(self.clock.now())

        await self.app.scheduling.register()
        self.clock.advance(31)
        await self.run_worker()
        jobs = await self.jobs()
        self.assertEqual([job.status for job in jobs], [JobStatus.SUCCESS])
        self.assertEqual(len(ran), 1)

    async def test_a_worker_without_a_scheduler_leaves_cron_alone(self):
        declare(self.app)
        await self.app.scheduling.register()
        self.clock.advance(31)
        worker = self.app.worker(scheduler=False)
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.3)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(await self.jobs(), [])

    async def test_a_restarted_scheduler_adopts_a_changed_cron(self):
        old = TinyJobs(f"sqlite:///{self.database}", clock=self.clock)
        declare(old, cron="0 0 * * *")
        await old.ready()
        await old.scheduling.register()
        await old.aclose()

        declare(self.app, cron="0 * * * *")
        scheduler = self.app.scheduler()
        runner = asyncio.create_task(scheduler.start())
        await asyncio.sleep(0.3)
        scheduler.stop()
        await asyncio.wait_for(runner, timeout=10)
        state = await self.state()
        self.assertEqual((state.cron, state.next_run_at), ("0 * * * *", TEN))

    async def test_a_standalone_scheduler_only_creates_jobs(self):
        declare(self.app)
        self.clock.advance(31)
        await self.app.scheduling.register()
        self.clock.advance(3600)
        scheduler = self.app.scheduler()
        runner = asyncio.create_task(scheduler.start())
        await asyncio.sleep(0.3)
        scheduler.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(
            [job.status for job in await self.jobs()], [JobStatus.QUEUED]
        )


if __name__ == "__main__":
    unittest.main()
