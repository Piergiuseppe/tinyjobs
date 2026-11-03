from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace

from ..backends.base import Backend
from ..clock import Clock, FrozenClock
from ..ids import new_job_id
from ..models import ExecutionError, Job, ScheduleState
from ..retention import KEEP_EVERYTHING, RetentionPolicy
from ..status import ExecutionStatus, JobStatus

START = 1_791_633_600.0
ERROR = ExecutionError(type="ValueError", message="boom", traceback="trace")
RACERS = 8
RACED_JOBS = 200


def make_job(**overrides) -> Job:
    fields = dict(
        id=new_job_id(),
        task="tests.noop",
        payload=b'{"args":[],"kwargs":{}}',
        codec="json",
        queue="default",
        status=JobStatus.QUEUED,
        max_attempts=1,
        run_at=0.0,
        created_at=0.0,
    )
    fields.update(overrides)
    return Job(**fields)


def pruning(**horizons: float) -> RetentionPolicy:
    settings = dict(success=0, cancelled=0, failed=0, interval=0.0)
    settings.update(horizons)
    return RetentionPolicy(**settings)


class BackendContract:
    independent_clients = True

    async def make_backend(self, clock: Clock) -> Backend:
        raise NotImplementedError

    async def asyncSetUp(self) -> None:
        self.clock = FrozenClock(START)
        self.opened: list[Backend] = []
        self.backend = await self.open()

    async def asyncTearDown(self) -> None:
        for backend in self.opened:
            await backend.close()

    async def open(self) -> Backend:
        backend = await self.make_backend(self.clock)
        await backend.setup()
        self.opened.append(backend)
        return backend

    async def racers(self) -> list[Backend]:
        if not self.independent_clients:
            return [self.backend] * RACERS
        return [self.backend] + [await self.open() for _ in range(RACERS - 1)]

    def requires(self, capability: str) -> None:
        if not getattr(self.backend.capabilities, capability):
            raise unittest.SkipTest(f"the backend does not declare {capability}")

    async def enqueue(self, **overrides) -> Job:
        return await self.backend.enqueue(make_job(**overrides))

    async def claim(self, limit=10, lease_seconds=60.0, worker="worker-a", queues=("default",)):
        return await self.backend.claim(
            worker_id=worker, queues=queues, limit=limit, lease_seconds=lease_seconds
        )

    async def claimed_one(self, **overrides) -> Job:
        await self.enqueue(**overrides)
        return (await self.claim())[0]

    async def stored(self, job: Job) -> Job:
        return await self.backend.get(job.id)

    async def reap_after_the_lease(self) -> int:
        self.clock.advance(61)
        return await self.backend.reap_expired()

    async def test_enqueued_job_is_readable(self):
        job = await self.enqueue(payload=b'{"args":[1],"kwargs":{}}', priority=3)
        stored = await self.stored(job)
        self.assertEqual(stored, job)

    async def test_a_missing_job_is_none(self):
        self.assertIsNone(await self.backend.get(new_job_id()))

    async def test_claim_marks_running_and_counts_the_attempt(self):
        claimed = await self.claimed_one()
        self.assertIs(claimed.status, JobStatus.RUNNING)
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "worker-a")
        self.assertEqual(claimed.lease_expires_at, START + 60.0)

    async def test_claim_opens_an_execution(self):
        claimed = await self.claimed_one()
        executions = await self.backend.executions(claimed.id)
        self.assertEqual(len(executions), 1)
        self.assertIs(executions[0].status, ExecutionStatus.RUNNING)
        self.assertEqual((executions[0].attempt, executions[0].worker_id), (1, "worker-a"))

    async def test_claim_never_returns_the_same_job_twice(self):
        await self.enqueue()
        self.assertEqual(len(await self.claim()), 1)
        self.assertEqual(await self.claim(), [])

    async def test_claim_respects_run_at(self):
        self.requires("delayed")
        await self.enqueue(run_at=START + 3600)
        self.assertEqual(await self.claim(), [])
        self.clock.advance(3600)
        self.assertEqual(len(await self.claim()), 1)

    async def test_claim_respects_the_queue_filter(self):
        await self.enqueue(queue="other")
        self.assertEqual(await self.claim(), [])
        self.assertEqual(len(await self.claim(queues=("default", "other"))), 1)

    async def test_claim_skips_cancelled_jobs(self):
        job = await self.enqueue()
        self.assertTrue(await self.backend.cancel(job.id))
        self.assertEqual(await self.claim(), [])
        self.assertIs((await self.stored(job)).status, JobStatus.CANCELLED)

    async def test_claim_orders_by_priority_then_run_at(self):
        self.requires("priority")
        late = await self.enqueue(priority=0, run_at=2.0)
        early = await self.enqueue(priority=0, run_at=1.0)
        high = await self.enqueue(priority=9, run_at=3.0, queue="other")
        claimed = await self.claim(limit=3, queues=("default", "other"))
        self.assertEqual([job.id for job in claimed], [high.id, early.id, late.id])

    async def test_claim_honours_the_limit(self):
        for _ in range(5):
            await self.enqueue()
        self.assertEqual(len(await self.claim(limit=2)), 2)
        self.assertEqual(len(await self.claim(limit=10)), 3)

    async def test_concurrent_claims_never_share_a_job(self):
        await self.backend.enqueue_many([make_job() for _ in range(RACED_JOBS)])
        racers = await self.racers()

        async def drain(backend: Backend, name: str) -> list[str]:
            taken = []
            while batch := await backend.claim(
                worker_id=name, queues=("default",), limit=3, lease_seconds=60
            ):
                taken.extend(job.id for job in batch)
            return taken

        results = await asyncio.gather(
            *(drain(backend, f"w{index}") for index, backend in enumerate(racers))
        )
        everything = [job_id for taken in results for job_id in taken]
        self.assertEqual(len(everything), RACED_JOBS)
        self.assertEqual(len(set(everything)), RACED_JOBS)

    async def test_complete_stores_the_result_and_closes_the_execution(self):
        claimed = await self.claimed_one()
        self.clock.advance(2)
        self.assertTrue(await self.backend.complete(claimed, b'"done"'))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.SUCCESS)
        self.assertEqual(stored.result_payload, b'"done"')
        self.assertEqual(stored.finished_at, START + 2)
        self.assertIsNone(stored.lease_expires_at)
        execution = (await self.backend.executions(claimed.id))[0]
        self.assertIs(execution.status, ExecutionStatus.SUCCESS)
        self.assertEqual(execution.duration, 2.0)

    async def test_fail_without_retry_is_terminal(self):
        claimed = await self.claimed_one()
        self.assertTrue(
            await self.backend.fail(claimed, ERROR, None, ExecutionStatus.TIMEOUT)
        )
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.FAILED)
        self.assertEqual(stored.last_error, "ValueError: boom")
        self.assertEqual(stored.finished_at, START)
        execution = (await self.backend.executions(claimed.id))[0]
        self.assertIs(execution.status, ExecutionStatus.TIMEOUT)
        self.assertEqual((execution.error_type, execution.traceback), ("ValueError", "trace"))

    async def test_fail_with_retry_requeues_at_the_given_time(self):
        claimed = await self.claimed_one(max_attempts=3)
        self.assertTrue(await self.backend.fail(claimed, ERROR, retry_at=START + 30))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual((stored.run_at, stored.attempts), (START + 30, 1))
        self.assertIsNone(stored.worker_id)
        self.assertEqual(await self.claim(), [])
        self.clock.advance(30)
        again = (await self.claim())[0]
        self.assertEqual(again.attempts, 2)
        self.assertEqual(len(await self.backend.executions(claimed.id)), 2)

    async def test_a_worker_that_lost_the_lease_cannot_report(self):
        claimed = await self.claimed_one()
        impostor = replace(claimed, worker_id="worker-b")
        self.assertFalse(await self.backend.complete(impostor, None))
        self.assertFalse(await self.backend.fail(impostor, ERROR, None))
        self.assertFalse(await self.backend.release(impostor))
        self.assertIs((await self.stored(claimed)).status, JobStatus.RUNNING)

    async def test_reporting_twice_is_a_no_op(self):
        claimed = await self.claimed_one()
        self.assertTrue(await self.backend.complete(claimed, None))
        self.assertFalse(await self.backend.complete(claimed, None))
        self.assertFalse(await self.backend.fail(claimed, ERROR, None))

    async def test_heartbeat_extends_the_lease(self):
        claimed = await self.claimed_one()
        self.clock.advance(50)
        await self.backend.heartbeat([claimed.id], 600.0)
        self.assertEqual((await self.stored(claimed)).lease_expires_at, START + 650)
        self.clock.advance(100)
        self.assertEqual(await self.backend.reap_expired(), 0)

    async def test_heartbeat_ignores_jobs_no_longer_running(self):
        claimed = await self.claimed_one()
        await self.backend.complete(claimed, None)
        await self.backend.heartbeat([claimed.id, new_job_id()], 600.0)
        self.assertIsNone((await self.stored(claimed)).lease_expires_at)

    async def test_heartbeat_names_the_leases_it_renewed(self):
        held, done = await self.claim_two()
        await self.backend.complete(done, None)
        renewed = await self.backend.heartbeat(
            [held.id, done.id, new_job_id()], 600.0, worker_id=held.worker_id
        )
        self.assertEqual(list(renewed), [held.id])
        self.assertEqual(
            list(await self.backend.heartbeat([held.id], 600.0, worker_id="someone-else")), []
        )

    async def claim_two(self) -> list[Job]:
        await self.enqueue()
        await self.enqueue()
        return await self.claim(limit=2)

    async def test_a_heartbeat_from_another_worker_leaves_the_lease_alone(self):
        claimed = await self.claimed_one()
        self.clock.advance(50)
        await self.backend.heartbeat([claimed.id], 600.0, worker_id="someone-else")
        self.assertEqual((await self.stored(claimed)).lease_expires_at, START + 60)
        await self.backend.heartbeat([claimed.id], 600.0, worker_id=claimed.worker_id)
        self.assertEqual((await self.stored(claimed)).lease_expires_at, START + 650)

    async def test_an_expired_lease_returns_the_job_to_the_queue(self):
        claimed = await self.claimed_one(max_attempts=3)
        self.clock.advance(61)
        self.assertEqual(await self.backend.reap_expired(), 1)
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertIsNone(stored.worker_id)
        self.assertIn("lease expired", stored.last_error)
        self.assertIs(
            (await self.backend.executions(claimed.id))[0].status, ExecutionStatus.LOST
        )
        self.assertEqual(len(await self.claim()), 1)

    async def test_an_expired_lease_fails_the_job_when_attempts_are_exhausted(self):
        claimed = await self.claimed_one(max_attempts=1)
        self.clock.advance(61)
        self.assertEqual(await self.backend.reap_expired(), 1)
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.FAILED)
        self.assertEqual(stored.finished_at, START + 61)

    async def test_the_reaper_ignores_healthy_leases(self):
        await self.claimed_one()
        self.clock.advance(59)
        self.assertEqual(await self.backend.reap_expired(), 0)

    async def test_release_returns_a_running_job_to_the_queue(self):
        claimed = await self.claimed_one()
        self.assertTrue(await self.backend.release(claimed, delay=10))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual(stored.run_at, START + 10)
        self.assertIs(
            (await self.backend.executions(claimed.id))[0].status, ExecutionStatus.LOST
        )

    async def test_release_many_returns_the_jobs_still_held_in_one_call(self):
        for _ in range(3):
            await self.enqueue()
        first, second, third = await self.claim(limit=3)
        await self.backend.complete(third, None)
        impostor = replace(second, worker_id="worker-b")
        self.assertEqual(await self.backend.release_many([first, impostor, third]), 1)
        self.assertIs((await self.stored(first)).status, JobStatus.QUEUED)
        self.assertIs((await self.stored(second)).status, JobStatus.RUNNING)
        self.assertIs((await self.stored(third)).status, JobStatus.SUCCESS)
        self.assertIs(
            (await self.backend.executions(first.id))[0].status, ExecutionStatus.LOST
        )
        self.assertEqual((await self.claim())[0].id, first.id)
        self.assertEqual(await self.backend.release_many([]), 0)

    async def test_a_job_claimed_by_id_is_leased_once_and_only_when_due(self):
        if not hasattr(self.backend, "claim_job"):
            raise unittest.SkipTest("the backend cannot claim a job by its id")
        first = await self.enqueue(run_at=START - 2)
        wanted = await self.enqueue(run_at=START - 1)
        later = await self.enqueue(run_at=START + 5)

        async def by_id(job: Job) -> Job | None:
            return await self.backend.claim_job(job.id, worker_id="worker-b", lease_seconds=60.0)

        self.assertIsNone(await by_id(later))
        claimed = await by_id(wanted)
        self.assertEqual(
            (claimed.id, claimed.status, claimed.worker_id, claimed.attempts, claimed.claims),
            (wanted.id, JobStatus.RUNNING, "worker-b", 1, 1),
        )
        self.assertEqual(claimed.lease_expires_at, START + 60)
        self.assertIsNone(await by_id(wanted))
        self.assertEqual([job.id for job in await self.claim()], [first.id])
        self.assertIsNone(await by_id(first))
        self.assertEqual(len(await self.backend.executions(wanted.id)), 1)
        self.assertTrue(await self.backend.complete(claimed, None))
        self.assertIsNone(await by_id(wanted))
        self.clock.advance(5)
        self.assertEqual((await by_id(later)).id, later.id)

    async def test_hand_back_returns_a_job_as_if_it_was_never_claimed(self):
        claimed = await self.claimed_one(max_attempts=2)
        self.assertTrue(await self.backend.hand_back(claimed, delay=10))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual(stored.run_at, START + 10)
        self.assertEqual((stored.attempts, stored.hand_backs), (0, 1))
        self.assertIsNone(stored.worker_id)
        self.assertEqual(await self.backend.executions(claimed.id), [])
        self.clock.advance(10)
        again = (await self.claim())[0]
        self.assertEqual((again.attempts, again.hand_backs), (1, 1))
        self.assertEqual(len(await self.backend.executions(claimed.id)), 1)

    async def test_only_the_holder_can_hand_a_job_back(self):
        claimed = await self.claimed_one()
        impostor = replace(claimed, worker_id="worker-b")
        self.assertFalse(await self.backend.hand_back(impostor))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.RUNNING)
        self.assertEqual((stored.attempts, stored.hand_backs), (1, 0))

    async def test_a_stale_claim_cannot_report_for_the_next_one(self):
        stale = await self.claimed_one(max_attempts=3)
        await self.reap_after_the_lease()
        current = (await self.claim())[0]
        self.assertEqual((current.worker_id, current.attempts), (stale.worker_id, 2))
        self.assertFalse(await self.backend.complete(stale, None))
        self.assertFalse(await self.backend.fail(stale, ERROR, None))
        self.assertFalse(await self.backend.fail(stale, ERROR, retry_at=START))
        self.assertFalse(await self.backend.release(stale))
        self.assertFalse(await self.backend.hand_back(stale))
        stored = await self.stored(current)
        self.assertEqual((stored.status, stored.attempts), (JobStatus.RUNNING, 2))
        statuses = [e.status for e in await self.backend.executions(current.id)]
        self.assertEqual(statuses, [ExecutionStatus.LOST, ExecutionStatus.RUNNING])

    async def test_a_run_cancelled_and_retried_cannot_report_for_the_retry(self):
        stale = await self.claimed_one()
        self.assertTrue(await self.backend.cancel(stale.id))
        self.assertTrue(await self.backend.requeue(stale.id))
        current = (await self.claim())[0]
        self.assertEqual((current.worker_id, current.attempts), (stale.worker_id, 1))
        self.assertFalse(await self.backend.complete(stale, b"stale"))
        self.assertFalse(await self.backend.fail(stale, ERROR, None))
        self.assertFalse(await self.backend.release(stale))
        self.assertFalse(await self.backend.hand_back(stale))
        self.assertTrue(await self.backend.complete(current, b"fresh"))
        stored = await self.stored(current)
        self.assertEqual((stored.status, stored.result_payload), (JobStatus.SUCCESS, b"fresh"))
        history = [(e.attempt, e.status) for e in await self.backend.executions(current.id)]
        self.assertEqual(
            history, [(1, ExecutionStatus.CANCELLED), (1, ExecutionStatus.SUCCESS)]
        )

    async def test_a_retried_job_keeps_the_history_of_its_earlier_runs(self):
        first = await self.claimed_one(max_attempts=2)
        await self.backend.fail(first, ERROR, retry_at=START)
        second = (await self.claim())[0]
        await self.backend.fail(second, ERROR, None)
        self.assertTrue(await self.backend.requeue(first.id))
        third = (await self.claim())[0]
        await self.backend.complete(third, None)
        history = [(e.attempt, e.status) for e in await self.backend.executions(first.id)]
        self.assertEqual(
            history,
            [
                (1, ExecutionStatus.FAILED),
                (2, ExecutionStatus.FAILED),
                (1, ExecutionStatus.SUCCESS),
            ],
        )

    async def test_cancelling_a_running_job_closes_its_execution(self):
        claimed = await self.claimed_one()
        self.assertTrue(await self.backend.cancel(claimed.id))
        execution = (await self.backend.executions(claimed.id))[0]
        self.assertIs(execution.status, ExecutionStatus.CANCELLED)
        self.assertEqual(execution.finished_at, START)

    async def test_any_other_outcome_resets_the_hand_backs(self):
        outcomes = {
            "complete": lambda job: self.backend.complete(job, None),
            "fail": lambda job: self.backend.fail(job, ERROR, None),
            "retry": lambda job: self.backend.fail(job, ERROR, retry_at=START),
            "release": lambda job: self.backend.release(job),
            "reap": lambda job: self.reap_after_the_lease(),
            "cancel": lambda job: self.backend.cancel(job.id),
        }
        for name, outcome in outcomes.items():
            with self.subTest(name):
                await self.enqueue(queue=name, max_attempts=3)
                job = (await self.claim(queues=(name,)))[0]
                self.assertTrue(await self.backend.hand_back(job))
                again = (await self.claim(queues=(name,)))[0]
                self.assertEqual(again.hand_backs, 1)
                await outcome(again)
                self.assertEqual((await self.stored(again)).hand_backs, 0)

    async def test_an_exhausted_lease_resets_the_hand_backs(self):
        job = await self.claimed_one(max_attempts=2)
        self.assertTrue(await self.backend.hand_back(job))
        again = (await self.claim())[0]
        await self.backend.fail(again, ERROR, retry_at=START)
        last = (await self.claim())[0]
        self.assertTrue(await self.backend.hand_back(last))
        final = (await self.claim())[0]
        self.assertEqual((final.attempts, final.hand_backs), (2, 1))
        await self.reap_after_the_lease()
        stored = await self.stored(final)
        self.assertEqual((stored.status, stored.hand_backs), (JobStatus.FAILED, 0))

    async def test_cancel_stops_a_running_job_from_reporting(self):
        claimed = await self.claimed_one()
        self.assertTrue(await self.backend.cancel(claimed.id))
        self.assertFalse(await self.backend.complete(claimed, None))
        self.assertIs((await self.stored(claimed)).status, JobStatus.CANCELLED)

    async def test_cancel_refuses_a_terminal_job(self):
        claimed = await self.claimed_one()
        await self.backend.complete(claimed, None)
        self.assertFalse(await self.backend.cancel(claimed.id))
        self.assertFalse(await self.backend.cancel(new_job_id()))

    async def test_requeue_resets_a_failed_job(self):
        claimed = await self.claimed_one()
        await self.backend.hand_back(claimed)
        claimed = (await self.claim())[0]
        await self.backend.fail(claimed, ERROR, None)
        self.assertTrue(await self.backend.requeue(claimed.id))
        stored = await self.stored(claimed)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual((stored.attempts, stored.hand_backs), (0, 0))
        self.assertIsNone(stored.last_error)
        self.assertIsNone(stored.finished_at)
        self.assertEqual(len(await self.claim()), 1)

    async def test_requeue_refuses_a_successful_job(self):
        claimed = await self.claimed_one()
        await self.backend.complete(claimed, None)
        self.assertFalse(await self.backend.requeue(claimed.id))

    async def test_requeue_refuses_a_job_whose_dedup_key_another_job_holds(self):
        self.requires("dedup")
        failed = await self.claimed_one(dedup_key="invoice:1")
        await self.backend.fail(failed, ERROR, None)
        active = await self.enqueue(dedup_key="invoice:1")
        self.assertFalse(await self.backend.requeue(failed.id))
        self.assertIs((await self.stored(failed)).status, JobStatus.FAILED)
        self.assertEqual(
            (await self.enqueue(dedup_key="invoice:1")).id, active.id
        )

    async def test_a_retried_job_holds_its_dedup_key_again(self):
        self.requires("dedup")
        failed = await self.claimed_one(dedup_key="invoice:1")
        await self.backend.fail(failed, ERROR, None)
        self.assertTrue(await self.backend.requeue(failed.id))
        self.assertEqual((await self.enqueue(dedup_key="invoice:1")).id, failed.id)

    async def test_a_dedup_key_returns_the_active_job(self):
        self.requires("dedup")
        first = await self.enqueue(dedup_key="invoice:1")
        second = await self.enqueue(dedup_key="invoice:1")
        self.assertEqual(first.id, second.id)
        self.assertEqual(len((await self.backend.list(limit=100)).items), 1)

    async def test_a_dedup_key_holds_while_the_job_runs(self):
        self.requires("dedup")
        claimed = await self.claimed_one(dedup_key="invoice:1")
        self.assertEqual((await self.enqueue(dedup_key="invoice:1")).id, claimed.id)

    async def test_a_dedup_key_is_free_again_once_the_job_is_terminal(self):
        self.requires("dedup")
        for finish in ("complete", "fail", "cancel"):
            with self.subTest(finish=finish):
                claimed = await self.claimed_one(dedup_key=f"key:{finish}")
                if finish == "complete":
                    await self.backend.complete(claimed, None)
                elif finish == "fail":
                    await self.backend.fail(claimed, ERROR, None)
                else:
                    await self.backend.cancel(claimed.id)
                again = await self.enqueue(dedup_key=f"key:{finish}")
                self.assertNotEqual(again.id, claimed.id)

    async def test_concurrent_enqueues_with_one_key_store_one_job(self):
        self.requires("dedup")
        racers = await self.racers()
        stored = await asyncio.gather(
            *(backend.enqueue(make_job(dedup_key="once")) for backend in racers)
        )
        self.assertEqual(len({job.id for job in stored}), 1)

    async def test_enqueue_many_writes_every_job(self):
        await self.backend.enqueue_many([make_job() for _ in range(20)])
        self.assertEqual(len((await self.backend.list(limit=100)).items), 20)

    async def test_listing_is_newest_first_and_paginates(self):
        made = [await self.enqueue(queue="a" if index % 2 else "b") for index in range(5)]
        page = await self.backend.list(limit=2)
        self.assertEqual([job.id for job in page.items], [made[4].id, made[3].id])
        self.assertIsNotNone(page.cursor)
        rest = await self.backend.list(limit=10, cursor=page.cursor)
        self.assertEqual([job.id for job in rest.items], [job.id for job in made[2::-1]])
        self.assertIsNone(rest.cursor)

    async def test_listing_filters_by_queue_and_status(self):
        await self.enqueue(queue="a")
        await self.enqueue(queue="b")
        claimed = await self.claimed_one(queue="default")
        await self.backend.complete(claimed, None)
        self.assertEqual(len((await self.backend.list(queue="a", limit=10)).items), 1)
        done = await self.backend.list(status=JobStatus.SUCCESS, limit=10)
        self.assertEqual([job.id for job in done.items], [claimed.id])
        both = await self.backend.list(status=JobStatus.QUEUED, queue="b", limit=10)
        self.assertEqual(len(both.items), 1)

    async def test_counts_and_stats_follow_every_transition(self):
        await self.enqueue()
        await self.enqueue(queue="mail", task="tests.mail")
        await self.enqueue(run_at=START + 600)
        running = (await self.claim(limit=1))[0]
        self.clock.advance(5)
        finished = await self.claimed_one()
        await self.backend.fail(finished, ERROR, None)
        self.assertEqual(
            await self.backend.counts_by_status(),
            {"queued": 2, "running": 1, "failed": 1},
        )
        stats = await self.backend.stats(self.clock.now())
        by_queue = {queue.queue: queue for queue in stats.queues}
        self.assertEqual(by_queue["default"].counts, {"queued": 1, "running": 1, "failed": 1})
        self.assertEqual(by_queue["mail"].counts, {"queued": 1})
        self.assertEqual(by_queue["mail"].oldest_due_at, 0.0)
        self.assertIsNone(by_queue["default"].oldest_due_at)
        self.assertEqual(stats.queued_tasks, {"tests.noop": 1, "tests.mail": 1})
        self.assertEqual(stats.expired_leases, 0)
        self.clock.advance(60)
        self.assertEqual((await self.backend.stats(self.clock.now())).expired_leases, 1)
        await self.backend.complete(running, None)
        self.assertEqual((await self.backend.counts_by_status())["success"], 1)

    async def test_purge_prunes_past_the_horizon_oldest_first(self):
        self.requires("retention")
        doomed = [await self.finished(JobStatus.SUCCESS, at) for at in (300, 100, 200)]
        kept = await self.finished(JobStatus.SUCCESS, 990)
        policy = replace(pruning(success=50), batch=2, maximum_per_sweep=2)
        self.assertEqual(await self.backend.purge(policy, 1000.0), 2)
        self.assertIsNone(await self.stored(doomed[1]))
        self.assertIsNone(await self.stored(doomed[2]))
        self.assertIsNotNone(await self.stored(doomed[0]))
        self.assertEqual(await self.backend.purge(policy, 1000.0), 1)
        self.assertIsNotNone(await self.stored(kept))

    async def test_purge_keeps_each_status_to_its_own_horizon(self):
        self.requires("retention")
        success = await self.finished(JobStatus.SUCCESS, 100)
        failed = await self.finished(JobStatus.FAILED, 100)
        removed = await self.backend.purge(pruning(success=50, failed=5000), 1000.0)
        self.assertEqual(removed, 1)
        self.assertIsNone(await self.stored(success))
        self.assertIsNotNone(await self.stored(failed))

    async def test_purge_never_touches_queued_or_running_jobs(self):
        self.requires("retention")
        await self.enqueue()
        await self.claimed_one()
        everything = pruning(success=1e-9, cancelled=1e-9, failed=1e-9)
        self.assertEqual(await self.backend.purge(everything, START + 1e6), 0)
        self.assertEqual(len((await self.backend.list(limit=10)).items), 2)

    async def test_purge_takes_the_executions_and_indexes_with_the_job(self):
        self.requires("retention")
        claimed = await self.claimed_one(dedup_key="gone")
        await self.backend.complete(claimed, None)
        await self.backend.purge(pruning(success=1e-9), START + 10)
        self.assertEqual(await self.backend.executions(claimed.id), [])
        self.assertEqual((await self.backend.list(limit=10)).items, [])
        self.assertEqual(await self.backend.counts_by_status(), {})

    async def test_purge_runs_once_per_interval(self):
        self.requires("retention")
        for _ in range(3):
            await self.finished(JobStatus.SUCCESS, 100)
        policy = replace(pruning(success=50), interval=3600.0)
        self.assertEqual(await self.backend.purge(policy, 1000.0), 3)
        self.assertIsNone(await self.backend.purge(policy, 1000.0))
        self.assertEqual(await self.backend.purge(policy, 4601.0), 0)

    async def test_a_disabled_policy_purges_nothing(self):
        self.requires("retention")
        job = await self.finished(JobStatus.SUCCESS, 100)
        self.assertIsNone(await self.backend.purge(KEEP_EVERYTHING, 1e9))
        self.assertIsNotNone(await self.stored(job))

    async def finished(self, status: JobStatus, at: float) -> Job:
        claimed = await self.claimed_one()
        now = self.clock.now()
        self.clock.advance(at - now)
        try:
            if status is JobStatus.SUCCESS:
                await self.backend.complete(claimed, None)
            else:
                await self.backend.fail(claimed, ERROR, None)
        finally:
            self.clock.advance(now - at)
        return claimed

    async def test_registering_a_schedule_keeps_the_stored_firing(self):
        self.requires("schedules")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 999.0)])
        self.assertEqual((await self.backend.schedules())[0].next_run_at, 100.0)

    async def test_changing_the_cron_or_timezone_replaces_the_firing(self):
        self.requires("schedules")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        await self.backend.register_schedules([ScheduleState("s", "5 * * * *", "UTC", 200.0)])
        await self.backend.register_schedules([ScheduleState("s", "5 * * * *", "Europe/Rome", 300.0)])
        state = (await self.backend.schedules())[0]
        self.assertEqual((state.cron, state.timezone, state.next_run_at), ("5 * * * *", "Europe/Rome", 300.0))

    async def test_firing_advances_the_schedule_and_stores_the_jobs(self):
        self.requires("schedules")
        await self.backend.register_schedules(
            [ScheduleState("b", "0 * * * *", "UTC", 100.0), ScheduleState("a", "0 * * * *", "UTC", 50.0)]
        )
        job = make_job(task="b")
        stored = await self.backend.fire_schedule("b", expected=100.0, next_run_at=3700.0, jobs=[job])
        self.assertEqual([j.id for j in stored], [job.id])
        self.assertIsNotNone(await self.stored(job))
        states = await self.backend.schedules()
        self.assertEqual([state.name for state in states], ["a", "b"])
        self.assertEqual(
            (states[1].next_run_at, states[1].last_run_at, states[1].last_job_id),
            (3700.0, START, job.id),
        )

    async def test_a_stale_firing_creates_nothing(self):
        self.requires("schedules")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        await self.backend.fire_schedule("s", expected=100.0, next_run_at=3700.0, jobs=[])
        job = make_job()
        self.assertIsNone(
            await self.backend.fire_schedule("s", expected=100.0, next_run_at=3700.0, jobs=[job])
        )
        self.assertIsNone(await self.stored(job))

    async def test_racing_firings_create_one_job(self):
        self.requires("schedules")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        racers = await self.racers()
        outcomes = await asyncio.gather(
            *(
                backend.fire_schedule("s", expected=100.0, next_run_at=3700.0, jobs=[make_job()])
                for backend in racers
            )
        )
        self.assertEqual(sum(outcome is not None for outcome in outcomes), 1)
        self.assertEqual(len((await self.backend.list(limit=100)).items), 1)

    async def test_a_firing_respects_the_dedup_key(self):
        self.requires("schedules")
        self.requires("dedup")
        running = await self.enqueue(dedup_key="tinyjobs:schedule:s")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        stored = await self.backend.fire_schedule(
            "s", expected=100.0, next_run_at=3700.0, jobs=[make_job(dedup_key="tinyjobs:schedule:s")]
        )
        self.assertEqual([job.id for job in stored], [running.id])
