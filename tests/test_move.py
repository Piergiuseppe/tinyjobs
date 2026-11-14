from __future__ import annotations

import asyncio
import contextlib
import io
import os
import shutil
import signal
import tempfile
import threading
import time
import unittest
import uuid
from dataclasses import replace
from threading import Event
from unittest import mock

from tinyjobs import TinyJobs
from tinyjobs.cli.main import main
from tinyjobs.exceptions import ConfigurationError, JobNotFoundError
from tinyjobs.ids import new_job_id
from tinyjobs.models import ExecutionError
from tinyjobs.moving import (
    NEEDS_AFTER,
    PROBE_QUEUE,
    PROBE_TASK,
    Move,
    Report,
    _families,
    _groups,
)
from tinyjobs.status import JobStatus

REDIS_URL = os.environ.get("TINYJOBS_REDIS_URL")
POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")

ADD = "tests.move.add"


def with_tasks(app: TinyJobs) -> TinyJobs:
    @app.task(name=ADD, max_attempts=3, store_result=True)
    def add(first, second):
        return first + second

    app.adding = add
    return app


class Stores(unittest.TestCase):
    old_kind = "sqlite"
    new_kind = "sqlite"

    def setUp(self) -> None:
        self.folder = tempfile.mkdtemp(prefix="tinyjobs-move-test-")
        self.addCleanup(shutil.rmtree, self.folder, True)
        self.old_url, self.old_options = self.address(self.old_kind, "old")
        self.new_url, self.new_options = self.address(self.new_kind, "new")
        self.old = self.opened(self.old_url, self.old_options)
        self.new = self.opened(self.new_url, self.new_options)

    def address(self, kind: str, name: str) -> tuple[str, dict]:
        if kind == "sqlite":
            return f"sqlite:///{self.folder}/{name}.db", {}
        if kind == "redis":
            prefix = f"tinyjobs-move-{uuid.uuid4().hex}"
            self.addCleanup(asyncio.run, forget_redis(prefix))
            return REDIS_URL, {"prefix": prefix}
        prefix = f"tinyjobs_move_{uuid.uuid4().hex[:16]}"
        self.addCleanup(forget_postgres, prefix)
        return POSTGRES_URL, {"prefix": prefix}

    def opened(self, url: str, options: dict) -> TinyJobs:
        app = with_tasks(TinyJobs(url, **options))
        self.addCleanup(app.close)
        return app

    def move(self, **options) -> Report:
        return Move(self.old, self.new, **options).run()

    def add(self, *numbers, **options):
        task = self.old.adding
        return task.with_options(**options).enqueue(*numbers) if options else task.enqueue(*numbers)

    def on(self, app: TinyJobs, call):
        async def connected():
            await app.ensure_connected()
            return await call()

        return app.run_sync(connected, name="test")

    def claimed(self, app: TinyJobs, lease: float) -> list:
        return self.on(app, lambda: app.backend.claim(
            worker_id="host/1/test", queues=["default"], limit=10, lease_seconds=lease
        ))

    def statuses(self, app: TinyJobs) -> dict[str, str]:
        return {
            job.id: job.status.value for job in app.jobs(limit=500).items if job.task == ADD
        }


async def forget_redis(prefix: str) -> None:
    from redis.asyncio import Redis

    client = Redis.from_url(REDIS_URL)
    try:
        keys = [key async for key in client.scan_iter(match=f"{{{prefix}}}:*", count=10_000)]
        if keys:
            await client.delete(*keys)
    finally:
        await client.aclose()


def forget_postgres(prefix: str) -> None:
    import psycopg

    with psycopg.connect(POSTGRES_URL, autocommit=True) as connection:
        for table in ("executions", "jobs", "schedules", "meta", "ran"):
            connection.execute(f'DROP TABLE IF EXISTS "{prefix}_{table}" CASCADE')


class MoveCases:
    def test_jobs_still_to_run_move_with_everything_they_carry(self):
        plain = self.add(1, 2)
        later = self.add(3, 4, delay=3600, priority=7, key="once", queue="slow")
        tried = self.claimed(self.old, 30)[0]
        self.on(self.old, lambda: self.old.backend.release(tried, 0.0))
        held = replace(self.old.get(plain.id), id=new_job_id(), status=JobStatus.HELD, _app=None)
        self.on(self.old, lambda: self.old.backend.enqueue(held))
        before = {job.id: job for job in self.old.jobs().items}

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.found, report.moved), (3, 3))
        self.assertEqual(self.old.jobs().items, [])
        after = {job.id: job for job in self.new.jobs().items}
        self.assertEqual(set(after), set(before))
        for job_id, job in after.items():
            was = before[job_id]
            self.assertEqual(job.status, JobStatus.QUEUED)
            for name in ("task", "payload", "codec", "queue", "priority", "attempts",
                         "max_attempts", "run_at", "created_at", "dedup_key", "claims"):
                self.assertEqual(getattr(job, name), getattr(was, name), name)
            self.assertIsNone(job.worker_id)
            self.assertIsNone(job.lease_expires_at)
        self.assertEqual(after[later.id].queue, "slow")
        self.assertEqual(after[later.id].dedup_key, "once")
        self.assertEqual(after[plain.id].claims, 1)
        self.assertEqual(self.new.drain(), 2)
        self.assertEqual(self.new.get(plain.id).result(), 3)
        self.assertEqual(self.new.get(later.id).status, JobStatus.QUEUED)
        again = self.add(5, 6, key="once")
        self.assertNotEqual(again.id, later.id)

    def test_a_chain_moves_whole_and_runs_in_its_order(self):
        first = self.add(1, 1)
        second = self.add(2, 2, after=first)
        third = self.add(3, 3, after=second)
        other = self.add(4, 4, after=first)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 4)
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), {
            first.id: "queued", second.id: "waiting", third.id: "waiting", other.id: "waiting",
        })
        self.assertEqual(self.new.get(third.id).after, second.id)
        while self.new.drain():
            pass
        self.assertEqual(set(self.statuses(self.new).values()), {"success"})
        self.assertEqual(self.new.get(third.id).result(), 6)

    def test_a_job_whose_parent_already_succeeded_moves_on_its_own(self):
        parent = self.add(1, 1)
        child = self.add(2, 2, after=parent, delay=3600)
        self.assertEqual(self.old.drain(), 1)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 1)
        moved = self.new.get(child.id)
        self.assertEqual((moved.status, moved.after), (JobStatus.QUEUED, None))
        self.assertIsNone(self.new.get(parent.id))
        self.assertEqual(self.statuses(self.old), {parent.id: "success"})

    def test_jobs_behind_a_running_job_stay_until_it_ends(self):
        parent = self.add(1, 1)
        child = self.add(2, 2, after=parent)
        grandchild = self.add(3, 3, after=child)
        running = self.claimed(self.old, 30)[0]

        report = self.move()

        self.assertFalse(report.ok)
        self.assertEqual((report.moved, report.behind), (0, 2))
        self.assertEqual((report.running, report.pending), (1, 2))
        self.assertEqual(self.statuses(self.old), {
            parent.id: "running", child.id: "waiting", grandchild.id: "waiting",
        })
        self.assertEqual(self.statuses(self.new), {})
        self.assertIn("still running in the old store", "\n".join(report.lines()))

        self.on(self.old, lambda: self.old.backend.complete(running, None))
        again = self.move()

        self.assertTrue(again.ok, again.lines())
        self.assertEqual(again.moved, 2)
        self.assertEqual(self.statuses(self.new), {child.id: "queued", grandchild.id: "waiting"})

    def test_a_job_whose_worker_died_goes_back_to_the_queue_and_moves(self):
        job = self.add(1, 2)
        self.claimed(self.old, 0.05)
        time.sleep(0.3)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.reaped, report.moved), (1, 1))
        self.assertEqual(self.new.get(job.id).status, JobStatus.QUEUED)
        self.assertEqual(self.new.get(job.id).attempts, 1)

    def test_a_run_stopped_after_its_copies_is_finished_by_the_next(self):
        first = self.add(1, 1)
        second = self.add(2, 2, after=first)
        lone = self.add(3, 3)

        with mock.patch.object(Move, "_take", side_effect=RuntimeError("the lights went out")):
            with self.assertRaises(RuntimeError):
                self.move()
        self.assertEqual(set(self.statuses(self.old).values()), {"queued", "waiting"})
        self.assertEqual(len(self.statuses(self.new)), 3)
        self.assertEqual(self.new.drain(), 3)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 3)
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), {
            first.id: "success", lone.id: "success", second.id: "success",
        })

    def test_a_run_stopped_between_two_deletes_of_a_chain_is_finished_by_the_next(self):
        first = self.add(1, 1, delay=3600)
        second = self.add(2, 2, after=first)
        third = self.add(3, 3, after=second)
        real = self.old.backend.delete
        calls = []

        async def failing(job_id):
            if len(calls) == 2:
                raise RuntimeError("the lights went out")
            calls.append(job_id)
            return await real(job_id)

        with mock.patch.object(self.old.backend, "delete", failing):
            with self.assertRaises(RuntimeError):
                self.move()
        self.assertEqual(calls, [third.id, second.id])
        self.assertEqual(self.statuses(self.old), {first.id: "queued"})

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 1)
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), {
            first.id: "queued", second.id: "waiting", third.id: "waiting",
        })

    def test_a_run_stopped_between_two_copies_of_a_chain_is_finished_by_the_next(self):
        first = self.add(1, 1, delay=3600)
        second = self.add(2, 2, after=first)
        self.new.counts()
        real = self.new.backend.enqueue
        calls = []

        async def failing(job):
            if job.task == ADD and calls:
                raise RuntimeError("the lights went out")
            if job.task == ADD:
                calls.append(job.id)
            return await real(job)

        with mock.patch.object(self.new.backend, "enqueue", failing):
            with self.assertRaises(RuntimeError):
                self.move()
        self.assertEqual(self.statuses(self.old), {first.id: "queued", second.id: "waiting"})
        self.assertEqual(self.statuses(self.new), {first.id: "queued"})

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 2)
        self.assertEqual(self.statuses(self.new), {first.id: "queued", second.id: "waiting"})
        self.assertEqual(self.statuses(self.old), {})

    def test_a_copy_that_failed_in_the_new_store_takes_its_chain_out_of_the_old_one(self):
        parent = self.add(1, 1)
        child = self.add(2, 2, after=parent)
        grandchild = self.add(3, 3, after=child)
        with mock.patch.object(Move, "_take", side_effect=RuntimeError("the lights went out")):
            with self.assertRaises(RuntimeError):
                self.move()
        [running] = self.claimed(self.new, 30)
        self.on(self.new, lambda: self.new.backend.fail(
            running, ExecutionError("ValueError", "no", ""), None
        ))
        ended = {parent.id: "failed", child.id: "cancelled", grandchild.id: "cancelled"}
        self.assertEqual(self.statuses(self.new), ended)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.moved, 3)
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), ended)

    def test_a_copy_someone_cancelled_in_the_new_store_stays_cancelled(self):
        job = self.add(1, 1, delay=3600)
        with mock.patch.object(Move, "_take", side_effect=RuntimeError("the lights went out")):
            with self.assertRaises(RuntimeError):
                self.move()
        self.assertTrue(self.new.cancel(job.id))

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), {job.id: "cancelled"})

    def test_there_and_back_and_there_again_needs_nothing_in_between(self):
        first = self.add(1, 1, delay=3600)
        second = self.add(2, 2, after=first)
        there = {first.id: "queued", second.id: "waiting"}

        self.assertTrue(self.move().ok)
        self.assertEqual((self.statuses(self.old), self.statuses(self.new)), ({}, there))
        back = Move(self.new, self.old).run()
        self.assertTrue(back.ok, back.lines())
        self.assertEqual(back.moved, 2)
        self.assertEqual((self.statuses(self.old), self.statuses(self.new)), (there, {}))
        again = self.move()
        self.assertTrue(again.ok, again.lines())
        self.assertEqual((self.statuses(self.old), self.statuses(self.new)), ({}, there))
        self.assertEqual(self.new.get(second.id).after, first.id)

    def test_a_job_sent_behind_a_moving_job_keeps_it_back_for_the_next_run(self):
        parent = self.add(1, 1, delay=3600)
        known = self.add(2, 2, after=parent)
        test = self
        late = []

        class Raced(Move):
            async def _take(self, families):
                for after in (known.id, parent.id):
                    sent = replace(
                        await test.old.backend.get(parent.id), id=new_job_id(), after=after
                    )
                    late.append((await test.old.backend.enqueue(sent)).id)
                return await super()._take(families)

        report = Raced(self.old, self.new).run()

        self.assertFalse(report.ok)
        self.assertEqual((report.moved, report.joined, report.pending), (0, 2, 4))
        self.assertEqual(self.statuses(self.old), {
            parent.id: "queued", known.id: "waiting", late[0]: "waiting", late[1]: "waiting",
        })
        self.assertIn(
            "2 job(s) left: a job was sent to the old store behind them while they moved; "
            "the next run takes them together",
            report.lines(),
        )

        again = self.move()

        self.assertTrue(again.ok, again.lines())
        self.assertEqual(again.moved, 4)
        self.assertEqual(self.statuses(self.old), {})
        self.assertEqual(self.statuses(self.new), {
            parent.id: "queued", known.id: "waiting", late[0]: "waiting", late[1]: "waiting",
        })
        self.assertEqual(self.new.get(late[0]).after, known.id)

    def test_sending_behind_a_job_that_has_moved_is_refused_by_the_old_store(self):
        parent = self.add(1, 1, delay=3600)
        self.assertTrue(self.move().ok)

        with self.assertRaises(JobNotFoundError):
            self.add(2, 2, after=parent.id)

        self.assertEqual(self.statuses(self.old), {})

    def test_a_job_a_worker_starts_between_the_copy_and_the_delete_keeps_running(self):
        started = self.add(1, 1)
        ended = self.add(2, 2)
        test = self
        claimed = {}

        class Raced(Move):
            async def _take(self, families):
                for job in await test.old.backend.claim(
                    worker_id="host/1/late", queues=["default"], limit=2, lease_seconds=30
                ):
                    claimed[job.id] = job
                await test.old.backend.complete(claimed[ended.id], None)
                return await super()._take(families)

        report = Raced(self.old, self.new).run()

        self.assertFalse(report.ok)
        self.assertEqual((report.moved, report.running), (0, 1))
        self.assertEqual(report.doubled, sorted([started.id, ended.id]))
        self.assertEqual(self.statuses(self.new), {started.id: "queued", ended.id: "queued"})
        self.assertEqual(self.statuses(self.old), {started.id: "running", ended.id: "success"})
        self.assertTrue(
            self.on(self.old, lambda: self.old.backend.complete(claimed[started.id], None))
        )
        self.assertIn(
            "2 job(s) were started by a worker of the old store while they moved, and run "
            "in both stores",
            "\n".join(report.lines()),
        )

    def test_a_job_another_run_took_first_is_not_counted_or_reported(self):
        early = self.add(1, 1, delay=3600)
        behind = self.add(2, 2, after=early)
        late = self.add(3, 3, delay=3600)
        test = self

        class Raced(Move):
            async def _plan(self):
                plan = await super()._plan()
                for job in (early, behind):
                    await test.new.backend.enqueue(replace(await test.old.backend.get(job.id)))
                for job in (behind, early):
                    await test.old.backend.delete(job.id)
                return plan

            async def _take(self, families):
                await test.new.backend.enqueue(replace(await test.old.backend.get(late.id)))
                await test.old.backend.delete(late.id)
                return await super()._take(families)

        with mock.patch("tinyjobs.moving.GROUP", 1):
            report = Raced(self.old, self.new).run()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.moved, report.changed), (0, 0))
        self.assertEqual((report.doubled, report.joined, report.cancelled), ([], 0, []))
        self.assertEqual(len(self.statuses(self.new)), 3)
        self.assertEqual(report.lines(), [
            "moved 0 job(s) from the old store to the new store",
            "nothing left to run in the old store",
        ])

    def test_a_job_cancelled_in_the_old_store_while_it_moves_is_named(self):
        job = self.add(1, 1, delay=3600)
        test = self

        class Raced(Move):
            async def _take(self, families):
                await test.old.backend.cancel(job.id)
                return await super()._take(families)

        report = Raced(self.old, self.new).run()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.moved, report.cancelled, report.doubled), (0, [job.id], []))
        self.assertEqual(self.new.get(job.id).status, JobStatus.QUEUED)
        self.assertIn(
            f"1 job(s) were cancelled in the old store while they moved, and their copies "
            f"are still to run in the new store: {job.id}",
            report.lines(),
        )

    def test_a_probe_left_by_a_run_that_was_killed_is_cleared_and_never_moved(self):
        job = self.add(1, 1, delay=3600)
        for app, age in ((self.old, 7200), (self.new, 7200), (self.new, 5)):
            probe = replace(
                self.old.get(job.id), id=new_job_id(), task=PROBE_TASK, queue=PROBE_QUEUE,
                created_at=time.time() - age, _app=None,
            )
            self.on(app, lambda: app.backend.enqueue(probe))

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.found, report.moved), (1, 1))
        self.assertEqual(self.old.jobs().items, [])
        left = [job for job in self.new.jobs().items if job.task == PROBE_TASK]
        self.assertEqual(len(left), 1)
        self.assertGreater(left[0].created_at, time.time() - 60)

    def test_a_copy_left_by_a_run_that_stopped_and_then_started_in_the_old_store_is_reported(self):
        job = self.add(1, 1)
        with mock.patch.object(Move, "_take", side_effect=RuntimeError("the lights went out")):
            with self.assertRaises(RuntimeError):
                self.move()
        self.claimed(self.old, 30)

        report = self.move()

        self.assertFalse(report.ok)
        self.assertEqual((report.moved, report.running, report.doubled), (0, 1, [job.id]))
        self.assertEqual(self.old.get(job.id).status, JobStatus.RUNNING)
        self.assertEqual(self.new.get(job.id).status, JobStatus.QUEUED)

    def test_a_key_already_held_in_the_new_store_stands_for_both(self):
        keyed = self.add(1, 1, key="once")
        child = self.add(2, 2, after=keyed)
        holder = self.new.adding.with_options(key="once").enqueue(5, 5)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.moved, report.merged), (2, 1))
        self.assertIsNone(self.new.get(keyed.id))
        self.assertEqual(self.new.get(child.id).after, holder.id)
        self.assertEqual(self.new.get(child.id).status, JobStatus.WAITING)
        self.assertEqual(self.statuses(self.old), {})

    def test_a_dry_run_counts_what_would_move_now_and_moves_nothing(self):
        first = self.add(1, 1)
        self.add(2, 2, after=first)
        self.claimed(self.old, 0.05)
        time.sleep(0.3)
        self.add(3, 3)
        before = self.statuses(self.old)

        report = self.move(dry_run=True)

        self.assertTrue(report.ok)
        self.assertEqual((report.found, report.moved, report.behind), (2, 1, 1))
        self.assertEqual((report.reaped, report.running, report.expired), (0, 1, 1))
        self.assertEqual(self.statuses(self.old), before)
        self.assertEqual(self.new.jobs().items, [])
        lines = report.lines()
        self.assertEqual(lines[0], "1 job(s) would move from the old store to the new store")
        self.assertIn("1 job(s) whose worker has stopped reporting are not counted", lines[1])
        self.assertIn("1 job(s) left: they wait for a job that is not moving", lines[2])
        self.assertEqual(lines[-1], "dry run, no job moved")
        self.assertEqual(self.move().moved, 3)

    def test_finished_jobs_stay_where_they_are(self):
        done = self.add(1, 1)
        dropped = self.add(2, 2)
        self.old.cancel(dropped.id)
        self.assertEqual(self.old.drain(), 1)

        report = self.move()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.found, report.moved), (0, 0))
        self.assertEqual(self.new.jobs().items, [])
        self.assertEqual(self.statuses(self.old), {done.id: "success", dropped.id: "cancelled"})

    def test_one_store_reached_twice_is_refused_before_a_job_is_deleted(self):
        first = self.add(1, 1)
        second = self.add(2, 2, after=first)
        same = self.opened(self.old_url, self.old_options)

        with self.assertRaises(ConfigurationError) as caught:
            Move(self.old, same, old_name="here", new_name="there").run()

        self.assertEqual(
            str(caught.exception),
            "here and there are one store under two addresses, so there is nothing to move; "
            "no job was touched",
        )
        self.assertEqual(
            {job.id: job.status.value for job in self.old.jobs().items},
            {first.id: "queued", second.id: "waiting"},
        )

    def test_an_old_store_that_never_held_a_job_is_refused(self):
        with self.assertRaises(ConfigurationError) as caught:
            self.move()
        self.assertIn("the old store holds no job of any status", str(caught.exception))
        with self.assertRaises(ConfigurationError):
            self.move(dry_run=True)

    def test_a_move_leaves_nothing_of_its_own_in_the_new_store(self):
        job = self.add(1, 1, delay=3600)

        self.assertTrue(self.move().ok)

        self.assertEqual([job.id for job in self.new.jobs().items], [job.id])
        self.assertEqual(self.new.counts(), {"queued": 1})
        self.assertEqual(self.old.counts(), {})
        self.assertNotIn(PROBE_TASK, [job.task for job in self.old.jobs().items])


class SQLiteToSQLiteTests(MoveCases, Stores):
    def test_a_job_a_worker_took_before_it_was_read_is_left(self):
        taken = self.add(1, 1)
        child = self.add(2, 2, after=taken)
        lone = self.add(3, 3)
        test = self

        class Raced(Move):
            async def _plan(self):
                plan = await super()._plan()
                await test.old.backend.claim(
                    worker_id="host/1/late", queues=["default"], limit=1, lease_seconds=30
                )
                return plan

        report = Raced(self.old, self.new).run()

        self.assertFalse(report.ok)
        self.assertEqual((report.moved, report.changed), (1, 1))
        self.assertEqual((report.running, report.pending, report.doubled), (1, 1, []))
        self.assertEqual(report.lines(), [
            "moved 1 job(s) from the old store to the new store",
            "1 job(s) left: they wait for a job a worker of the old store took before they "
            "were read",
            "1 job(s) still running in the old store; they finish there, or come back when "
            "their lease ends",
            "2 job(s) left to run in the old store; run it again",
        ])
        self.assertEqual(self.statuses(self.new), {lone.id: "queued"})
        self.assertEqual(self.old.get(taken.id).status, JobStatus.RUNNING)
        self.assertEqual(self.old.get(child.id).status, JobStatus.WAITING)

    def test_a_store_in_memory_is_refused_at_either_end_before_anything_moves(self):
        job = self.add(1, 1)
        for address in ("sqlite:///", "sqlite://", "sqlite:///:memory:", "memory://"):
            gone = self.opened(address, {})
            for old, new in ((self.old, gone), (gone, self.new)):
                with self.assertRaises(ConfigurationError) as caught:
                    Move(old, new).run()
                self.assertIn("is in memory", str(caught.exception))
        self.assertEqual(self.statuses(self.old), {job.id: "queued"})
        self.assertEqual(self.new.jobs().items, [])

    def test_a_copy_whose_job_was_cancelled_before_it_was_read_is_named_as_cancelled(self):
        job = self.add(1, 1, delay=3600)
        with mock.patch.object(Move, "_take", side_effect=RuntimeError("the lights went out")):
            with self.assertRaises(RuntimeError):
                self.move()
        test = self

        class Raced(Move):
            async def _plan(self):
                plan = await super()._plan()
                await test.old.backend.cancel(job.id)
                return plan

        report = Raced(self.old, self.new).run()

        self.assertTrue(report.ok, report.lines())
        self.assertEqual((report.cancelled, report.doubled, report.changed), ([job.id], [], 0))
        self.assertEqual(self.new.get(job.id).status, JobStatus.QUEUED)

    def test_a_stop_between_two_groups_leaves_the_rest_for_the_next_run(self):
        for number in range(4):
            self.add(number, number)
        stopped = Event()
        said = []

        def say(line: str) -> None:
            said.append(line)
            stopped.set()

        with mock.patch.multiple("tinyjobs.moving", GROUP=2, PROGRESS_SECONDS=0):
            report = Move(self.old, self.new, stopped=stopped, say=say).run()

        self.assertFalse(report.ok)
        self.assertTrue(report.cut_short)
        self.assertEqual((report.moved, report.pending), (2, 2))
        self.assertEqual(said, ["moved 2 of 4"])
        self.assertEqual(
            report.lines()[-1], "stopped before the end; run it again to move the rest"
        )
        said.clear()
        stopped.clear()
        with mock.patch.multiple("tinyjobs.moving", GROUP=1, PROGRESS_SECONDS=0):
            counted = Move(self.old, self.new, dry_run=True, stopped=stopped, say=say).run()
        self.assertFalse(counted.ok)
        self.assertEqual(said, ["counted 1 of 2"])
        self.assertEqual(
            counted.lines()[-1],
            "stopped before the end: the count is partial, and no job moved",
        )
        self.assertTrue(self.move().ok)
        self.assertEqual(len(self.statuses(self.new)), 4)

    def test_what_the_new_store_cannot_hold_is_left_and_named(self):
        first = self.add(1, 1)
        second = self.add(2, 2, after=first)
        lone = self.add(3, 3)
        self.new.counts()
        limited = replace(self.new.backend.capabilities, after=False)

        with mock.patch.object(self.new.backend, "capabilities", limited):
            report = self.move()

        self.assertFalse(report.ok)
        self.assertEqual(report.unsupported, {NEEDS_AFTER: 2})
        self.assertEqual((report.moved, report.pending), (1, 2))
        self.assertEqual(self.statuses(self.new), {lone.id: "queued"})
        self.assertEqual(self.statuses(self.old), {first.id: "queued", second.id: "waiting"})
        self.assertIn("2 job(s) left: they run after another job", "\n".join(report.lines()))
        self.assertEqual(report.lines()[-1], "2 job(s) left to run in the old store")

    def test_families_keep_parents_before_children_and_groups_keep_families_whole(self):
        plan = {"a": None, "b": "a", "c": "b", "d": "a", "e": None, "f": "gone", "g": "f"}
        self.assertEqual(_families(plan), [["a", "b", "c", "d"], ["e"], ["f", "g"]])
        with mock.patch("tinyjobs.moving.GROUP", 3):
            self.assertEqual(
                _groups(_families(plan)), [[["a", "b", "c", "d"]], [["e"], ["f", "g"]]]
            )
        self.assertEqual(_groups([]), [])


SECRETS = (
    "postgresql://me:hunter2@db/jobs",
    "postgresql://me:hun?ter2@db/jobs",
    "postgresql://me:hun@ter/2@db/jobs",
    "postgresql://db:5432/jobs?application_name=api@prod&password=hunter2",
    "postgresql://me@db/jobs?password=hun?ter2",
    "postgresql://me@db/jobs?sslpassword=hunter2&pass%77ord=hunter2",
    "redis://:hunter2@[::1]:6379/0",
    "redis://cache:6379?client_name=a@b&password=hunter2",
    "redis://me:hunter2@cache:6379/0?Password=hunter2",
)


class MoveCommandTests(Stores):
    def run_cli(self, *arguments: str, app: TinyJobs | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = main(["move", *arguments], app=app)
            except SystemExit as stopped:
                code = stopped.code
        return code, out.getvalue(), err.getvalue()

    def test_the_command_moves_reports_and_exits_zero_when_nothing_is_left(self):
        first = self.add(1, 1)
        self.add(2, 2, after=first)

        code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url, "--dry-run")
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines(), [
            "2 job(s) would move from the old store (sqlite) to the new store (sqlite)",
            "dry run, no job moved",
        ])

        code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines(), [
            "moved 2 job(s) from the old store (sqlite) to the new store (sqlite)",
            "nothing left to run in the old store (sqlite)",
        ])
        self.assertEqual(sorted(self.statuses(self.new).values()), ["queued", "waiting"])

    def test_the_command_exits_one_while_a_job_is_still_running_in_the_old_store(self):
        self.add(1, 1)
        self.claimed(self.old, 30)

        code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)

        self.assertEqual(code, 1)
        self.assertEqual(
            out.splitlines()[-1],
            "1 job(s) left to run in the old store (sqlite); run it again",
        )

    def test_the_new_store_can_be_the_one_of_the_app(self):
        job = self.add(1, 1)

        code, out, _ = self.run_cli("--from", self.old_url, app=self.new)

        self.assertEqual(code, 0)
        self.assertIn("to the store of the app (sqlite)", out)
        self.assertEqual(self.new.get(job.id).status, JobStatus.QUEUED)
        self.assertEqual(self.new.drain(), 1)

    def test_a_missing_or_identical_new_store_is_an_error(self):
        job = self.add(1, 1)

        code, _, err = self.run_cli("--from", self.old_url)
        self.assertEqual(code, 2)
        self.assertIn("name the store to move to", err)

        code, _, err = self.run_cli("--from", self.old_url, "--to", self.old_url)
        self.assertEqual(code, 2)
        self.assertIn("the same address and prefix", err)

        code, _, err = self.run_cli("--from", self.old_url, app=self.old)
        self.assertEqual(code, 2)
        self.assertIn("are one store under two addresses", err)
        self.assertEqual(self.old.get(job.id).status, JobStatus.QUEUED)

    def test_a_refusal_halfway_and_an_error_after_the_last_job_are_told_apart(self):
        self.add(1, 1, delay=3600)
        self.add(2, 2, delay=3600)
        real = Move._copy
        calls = []

        async def refusing(move, families):
            if calls:
                raise ConfigurationError("the new store went read-only")
            calls.append(1)
            return await real(move, families)

        with mock.patch("tinyjobs.moving.GROUP", 1), mock.patch.object(Move, "_copy", refusing):
            code, out, err = self.run_cli("--from", self.old_url, "--to", self.new_url)
        self.assertEqual((code, err), (2, "error: the new store went read-only\n"))
        self.assertEqual(out.splitlines(), [
            "moved 1 job(s) from the old store (sqlite) to the new store (sqlite)",
            "stopped before the end; run it again to move the rest",
        ])

        counts = Move._counts
        asked = []

        def failing(move):
            asked.append(1)
            if len(asked) == 2:
                raise RuntimeError("the lights went out")
            return counts(move)

        with mock.patch.object(Move, "_counts", failing):
            code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)
        self.assertEqual(code, 2)
        self.assertEqual(out.splitlines(), [
            "moved 1 job(s) from the old store (sqlite) to the new store (sqlite)",
            "stopped while counting what is left in the old store (sqlite); run it again to "
            "know",
        ])
        self.assertEqual(len(self.statuses(self.new)), 2)

    def test_a_prefix_or_an_address_the_app_would_override_is_refused(self):
        job = self.add(1, 1)

        code, _, err = self.run_cli("--from", self.old_url, "--to-prefix", "fresh", app=self.new)
        self.assertEqual(code, 2)
        self.assertIn("--to-prefix goes with --to", err)

        code, _, err = self.run_cli(
            "--from", self.old_url, "--to", self.new_url, "--app", "tasks:app"
        )
        self.assertEqual(code, 2)
        self.assertIn("--to and --app both name a store to move to", err)
        self.assertEqual(self.old.get(job.id).status, JobStatus.QUEUED)
        self.assertEqual(self.new.jobs().items, [])

    def signalled(self, times: int):
        real = Move._take
        calls = []

        async def taking(move, families):
            taken = await real(move, families)
            if not calls:
                calls.append(1)
                for _ in range(times):
                    signal.raise_signal(signal.SIGINT)
                    await asyncio.sleep(0.2)
            return taken

        if (
            threading.current_thread() is not threading.main_thread()
            or signal.getsignal(signal.SIGINT) is not signal.default_int_handler
        ):
            self.skipTest("needs the main thread and the default interrupt handler")
        return mock.patch.multiple(Move, _take=taking), mock.patch("tinyjobs.moving.GROUP", 1)

    def test_one_stop_signal_ends_the_run_after_the_jobs_it_is_on(self):
        for number in range(3):
            self.add(number, number, delay=3600)
        taking, grouped = self.signalled(1)

        with taking, grouped:
            code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)

        self.assertEqual(code, 1)
        self.assertEqual(out.splitlines(), [
            "moved 1 job(s) from the old store (sqlite) to the new store (sqlite)",
            "stopped before the end; run it again to move the rest",
        ])
        self.assertIs(signal.getsignal(signal.SIGINT), signal.default_int_handler)
        self.assertEqual(len(self.statuses(self.old)), 2)

    def test_a_second_stop_signal_ends_the_command_at_once(self):
        for number in range(3):
            self.add(number, number, delay=3600)
        taking, grouped = self.signalled(2)

        with taking, grouped:
            code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)

        self.assertEqual(code, 130)
        self.assertEqual(
            out.splitlines()[-1], "stopped before the end; run it again to move the rest"
        )
        self.assertIs(signal.getsignal(signal.SIGINT), signal.default_int_handler)
        self.assertEqual(len(self.statuses(self.old)) + len(self.statuses(self.new)), 3)
        self.assertTrue(self.move().ok)
        self.assertEqual(len(self.statuses(self.new)), 3)

    def test_a_new_store_in_memory_is_refused(self):
        job = self.add(1, 1)
        for address in ("sqlite:///", "memory://"):
            code, out, err = self.run_cli("--from", self.old_url, "--to", address)
            self.assertEqual((code, out), (2, ""))
            self.assertIn("the new store", err)
            self.assertIn("is in memory", err)
        self.assertEqual(self.old.get(job.id).status, JobStatus.QUEUED)

    def test_a_file_that_is_not_there_is_refused_and_not_created(self):
        missing = f"{self.folder}/elsewhere/jobs.db"

        code, out, err = self.run_cli("--from", f"sqlite:///{missing}", "--to", self.new_url)

        self.assertEqual((code, out), (2, ""))
        self.assertIn("the file --from names does not exist", err)
        self.assertFalse(os.path.exists(os.path.dirname(missing)))

    def test_a_prefix_that_names_no_store_is_refused(self):
        self.add(1, 1)

        code, _, err = self.run_cli(
            "--from", self.old_url, "--from-prefix", "elsewhere", "--to", self.new_url
        )

        self.assertEqual(code, 2)
        self.assertIn("the old store (sqlite) holds no job of any status", err)

    def test_prefixes_name_two_stores_in_one_file(self):
        url = f"sqlite:///{self.folder}/shared.db"
        legacy = self.opened(url, {"prefix": "legacy"})
        job = legacy.adding.enqueue(1, 1)

        code, out, _ = self.run_cli(
            "--from", url, "--from-prefix", "legacy", "--to", url, "--to-prefix", "fresh"
        )

        self.assertEqual(code, 0)
        self.assertIn("moved 1 job(s)", out)
        fresh = self.opened(url, {"prefix": "fresh"})
        self.assertEqual(fresh.get(job.id).status, JobStatus.QUEUED)
        self.assertIsNone(legacy.get(job.id))

    def test_an_error_halfway_still_says_what_had_moved(self):
        self.add(1, 1)
        self.add(2, 2)
        real = Move._take
        calls = []

        async def failing(move, families):
            if calls:
                raise RuntimeError("the lights went out")
            calls.append(1)
            return await real(move, families)

        with mock.patch("tinyjobs.moving.GROUP", 1), mock.patch.object(Move, "_take", failing):
            code, out, err = self.run_cli("--from", self.old_url, "--to", self.new_url)

        self.assertEqual(code, 2)
        self.assertEqual(out.splitlines(), [
            "moved 1 job(s) from the old store (sqlite) to the new store (sqlite)",
            "stopped before the end; run it again to move the rest",
        ])
        self.assertEqual(
            err, "error: the move stopped on an error, RuntimeError: the lights went out; "
            "--log-level DEBUG shows where\n"
        )

        with mock.patch.object(Move, "_read", side_effect=RuntimeError("the lights went out")):
            code, out, err = self.run_cli(
                "--from", self.old_url, "--to", self.new_url, "--dry-run"
            )
        self.assertEqual(code, 2)
        self.assertEqual(out.splitlines(), [
            "0 job(s) would move from the old store (sqlite) to the new store (sqlite)",
            "stopped before the end: the count is partial, and no job moved",
        ])
        code, out, _ = self.run_cli("--from", self.old_url, "--to", self.new_url)
        self.assertEqual((code, out.splitlines()[0][:14]), (0, "moved 1 job(s)"))

    def test_nothing_of_an_address_past_its_scheme_is_ever_printed(self):
        self.add(0, 0)
        self.assertEqual(self.old.drain(), 1)
        for old_url in SECRETS:
            self.add(1, 1)
            new_url = SECRETS[0] + "/new"
            stores = {old_url: self.old, new_url: self.new}
            with mock.patch(
                "tinyjobs.cli.commands.move._opened", lambda url, prefix, opened: stores[url]
            ):
                for extra in ((), ("--dry-run",)):
                    code, out, err = self.run_cli("--from", old_url, "--to", new_url, *extra)
                    self.assertEqual(code, 0, err)
                    for part in ("hunter2", "ter2", "hun", "me", "db", "cache", "api", "prod"):
                        self.assertNotIn(part, out + err, old_url)
            code, out, err = self.run_cli("--from", old_url, "--to", old_url)
            self.assertEqual(code, 2)
            self.assertNotIn("hun", out + err)

    def test_an_address_in_the_wrong_place_is_not_echoed(self):
        for arguments in (
            ("--from", self.old_url, SECRETS[0]),
            ("--from", self.old_url, "--to", self.new_url, SECRETS[3], "--dry-run"),
            ("--from", self.old_url, "--too", SECRETS[7]),
            ("--from", self.old_url, f"--dry-run={SECRETS[0]}"),
            ("--from", self.old_url, "--to", self.new_url, "--log-level", SECRETS[8]),
        ):
            code, out, err = self.run_cli(*arguments)
            self.assertEqual(code, 2)
            self.assertNotIn("hunter2", out + err)
            self.assertIn("<an address>", err)


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
class SQLiteToRedisTests(MoveCases, Stores):
    new_kind = "redis"


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
class RedisToSQLiteTests(MoveCases, Stores):
    old_kind = "redis"


@unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")
class SQLiteToPostgresTests(MoveCases, Stores):
    new_kind = "postgres"


@unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")
class PostgresToSQLiteTests(MoveCases, Stores):
    old_kind = "postgres"


@unittest.skipUnless(REDIS_URL and POSTGRES_URL, "needs redis and postgres")
class RedisToPostgresTests(MoveCases, Stores):
    old_kind = "redis"
    new_kind = "postgres"


@unittest.skipUnless(REDIS_URL and POSTGRES_URL, "needs redis and postgres")
class PostgresToRedisTests(MoveCases, Stores):
    old_kind = "postgres"
    new_kind = "redis"


if __name__ == "__main__":
    unittest.main()
