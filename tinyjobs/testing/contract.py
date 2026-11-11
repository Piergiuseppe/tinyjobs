from __future__ import annotations

import asyncio
import inspect
import time
import unittest
from dataclasses import replace

from ..backends.base import Backend
from ..clock import Clock, FrozenClock
from ..exceptions import JobNotFoundError
from ..following import PARENT_ENDED_MESSAGE
from ..ids import new_job_id
from ..limits import Limit
from ..models import NEVER, ExecutionError, Job, ScheduleState
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
    wide_cancel_seconds = 1.0

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

    async def test_claimed_jobs_keep_bytes_that_look_like_separators(self):
        payload = b'{"args":["a=b,c\\n"],"kwargs":{}}\n=,\x00\xff'
        first = await self.enqueue(payload=payload, task="tests.a=b,c", dedup_key="k=1,2\n3")
        second = await self.enqueue(payload=payload, task="tests.a=b,c", dedup_key="k=4,5\n6")
        [claimed] = await self.claim(limit=1)
        by_id = await self.backend.claim_job(second.id, worker_id="worker-b", lease_seconds=60.0)
        for job, expected in ((claimed, first), (by_id, second)):
            self.assertEqual(job.id, expected.id)
            self.assertEqual(job.payload, payload)
            self.assertEqual(job.task, "tests.a=b,c")
            self.assertEqual(job.dedup_key, expected.dedup_key)

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

    async def test_expire_results_drops_results_older_than_the_ttl(self):
        self.requires("result_ttl")
        old = await self.completed_with(b'"old"', at=100)
        fresh = await self.completed_with(b'"fresh"', at=900)
        other = await self.completed_with(b'"other"', at=100, task="tests.mail")
        bare = await self.finished(JobStatus.SUCCESS, 100)
        self.assertEqual(await self.backend.expire_results("tests.noop", 500.0, 1000.0), 1)
        stored = await self.stored(old)
        self.assertIsNone(stored.result_payload)
        self.assertIs(stored.status, JobStatus.SUCCESS)
        self.assertEqual(stored.finished_at, 100.0)
        self.assertEqual((await self.stored(fresh)).result_payload, b'"fresh"')
        self.assertEqual((await self.stored(other)).result_payload, b'"other"')
        self.assertIsNone((await self.stored(bare)).result_payload)
        self.assertEqual(await self.backend.expire_results("tests.noop", 500.0, 1000.0), 0)

    async def test_expire_results_works_in_batches_oldest_first(self):
        self.requires("result_ttl")
        jobs = [await self.completed_with(b'"r"', at=at) for at in (300, 100, 200)]
        self.assertEqual(await self.backend.expire_results("tests.noop", 1.0, 1000.0, batch=2), 2)
        self.assertIsNone((await self.stored(jobs[1])).result_payload)
        self.assertIsNone((await self.stored(jobs[2])).result_payload)
        self.assertEqual((await self.stored(jobs[0])).result_payload, b'"r"')
        self.assertEqual(await self.backend.expire_results("tests.noop", 1.0, 1000.0, batch=2), 1)

    async def test_expire_results_starts_from_the_floor_it_is_given(self):
        self.requires("result_ttl")
        before = await self.completed_with(b'"before"', at=100)
        after = await self.completed_with(b'"after"', at=300)
        self.assertEqual(
            await self.backend.expire_results("tests.noop", 1.0, 1000.0, since=200.0), 1
        )
        self.assertEqual((await self.stored(before)).result_payload, b'"before"')
        self.assertIsNone((await self.stored(after)).result_payload)

    async def test_purge_takes_the_result_index_with_the_job(self):
        self.requires("result_ttl")
        self.requires("retention")
        job = await self.completed_with(b'"r"', at=100)
        self.assertEqual(await self.backend.purge(pruning(success=50), 1000.0), 1)
        self.assertIsNone(await self.stored(job))
        self.assertEqual(await self.backend.expire_results("tests.noop", 1.0, 1000.0), 0)

    async def completed_with(self, result: bytes, at: float, **overrides) -> Job:
        claimed = await self.claimed_one(**overrides)
        now = self.clock.now()
        self.clock.advance(at - now)
        try:
            await self.backend.complete(claimed, result)
        finally:
            self.clock.advance(now - at)
        return claimed

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

    async def test_a_schedule_moved_to_never_round_trips_and_moves_back(self):
        self.requires("schedules")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        self.assertIsNotNone(
            await self.backend.fire_schedule("s", expected=100.0, next_run_at=NEVER, jobs=())
        )
        stored = {state.name: state for state in await self.backend.schedules()}
        self.assertEqual(stored["s"].next_run_at, NEVER)
        self.assertIsNone(
            await self.backend.fire_schedule("s", expected=100.0, next_run_at=3700.0, jobs=())
        )
        self.assertIsNotNone(
            await self.backend.fire_schedule("s", expected=NEVER, next_run_at=3700.0, jobs=())
        )
        stored = {state.name: state for state in await self.backend.schedules()}
        self.assertEqual(stored["s"].next_run_at, 3700.0)

    async def test_a_firing_respects_the_dedup_key(self):
        self.requires("schedules")
        self.requires("dedup")
        running = await self.enqueue(dedup_key="tinyjobs:schedule:s")
        await self.backend.register_schedules([ScheduleState("s", "0 * * * *", "UTC", 100.0)])
        stored = await self.backend.fire_schedule(
            "s", expected=100.0, next_run_at=3700.0, jobs=[make_job(dedup_key="tinyjobs:schedule:s")]
        )
        self.assertEqual([job.id for job in stored], [running.id])

    async def test_an_audit_answers_with_findings(self):
        audit = getattr(self.backend, "audit", None)
        if audit is None:
            raise unittest.SkipTest("the backend audits nothing")
        findings = audit()
        if inspect.isawaitable(findings):
            findings = await findings
        self.assertTrue(findings)
        for finding in findings:
            self.assertIsInstance(finding.ok, bool)
            self.assertIsInstance(finding.message, str)
            self.assertTrue(finding.message)

    async def limited(self, limit=10, worker="worker-a", queues=("default",), **limits) -> list[Job]:
        return await self.backend.claim(
            worker_id=worker,
            queues=queues,
            limit=limit,
            lease_seconds=60.0,
            limits={name: value for name, value in limits.items()},
        )

    async def statuses(self, *jobs: Job) -> list[JobStatus]:
        return [(await self.stored(job)).status for job in jobs]

    async def test_a_concurrency_limit_caps_the_running_jobs_of_a_task_across_claims(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=2)}
        first, second, third = [await self.enqueue(run_at=float(index)) for index in range(3)]
        mail = await self.enqueue(task="tests.mail")
        claimed = await self.limited(**limits)
        self.assertEqual(
            sorted(job.id for job in claimed), sorted([first.id, second.id, mail.id])
        )
        self.assertEqual(await self.statuses(third), [JobStatus.HELD])
        self.assertEqual(await self.limited(**limits), [])
        await self.backend.complete(await self.stored(first), None)
        admitted = await self.limited(**limits)
        self.assertEqual(
            [(job.id, job.status, job.claims) for job in admitted],
            [(third.id, JobStatus.RUNNING, 1)],
        )
        self.assertEqual(await self.limited(**limits), [])

    async def test_a_rate_limit_spaces_the_starts_of_a_task(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(interval=10.0)}
        jobs = [await self.enqueue(run_at=float(index)) for index in range(3)]
        self.assertEqual([job.id for job in await self.limited(**limits)], [jobs[0].id])
        self.assertEqual(await self.statuses(*jobs[1:]), [JobStatus.HELD, JobStatus.HELD])
        self.assertEqual(await self.limited(**limits), [])
        self.clock.advance(9.9)
        self.assertEqual(await self.limited(**limits), [])
        self.clock.advance(0.1)
        self.assertEqual([job.id for job in await self.limited(**limits)], [jobs[1].id])
        self.clock.advance(10)
        self.assertEqual([job.id for job in await self.limited(**limits)], [jobs[2].id])
        self.assertEqual(await self.statuses(*jobs), [JobStatus.RUNNING] * 3)

    async def test_a_rate_limit_admits_one_job_of_a_task_per_claim_however_much_room(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=5, interval=1.0)}
        for index in range(3):
            await self.enqueue(run_at=float(index))
        self.clock.advance(100)
        self.assertEqual(len(await self.limited(**limits)), 1)
        self.assertEqual((await self.backend.counts_by_status())["held"], 2)

    async def test_held_jobs_are_admitted_in_the_order_the_queue_promised(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        first = await self.enqueue(run_at=0.0)
        self.assertEqual([job.id for job in await self.limited(**limits)], [first.id])
        second = await self.enqueue(run_at=1.0)
        self.assertEqual(await self.limited(**limits), [])
        self.assertEqual(await self.statuses(second), [JobStatus.HELD])
        urgent = await self.enqueue(run_at=2.0, priority=5)
        await self.backend.complete(await self.stored(first), None)
        self.assertEqual([job.id for job in await self.limited(**limits)], [urgent.id])
        self.assertEqual(await self.statuses(second), [JobStatus.HELD])
        await self.backend.complete((await self.stored(urgent)), None)
        self.assertEqual([job.id for job in await self.limited(**limits)], [second.id])

    async def test_a_held_job_waits_for_a_worker_on_its_queue(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        first = await self.enqueue()
        mail = await self.enqueue(queue="mail")
        self.assertEqual([job.id for job in await self.limited(**limits)], [first.id])
        self.assertEqual(await self.limited(queues=("mail",), **limits), [])
        self.assertEqual(await self.statuses(mail), [JobStatus.HELD])
        await self.backend.complete(await self.stored(first), None)
        self.assertEqual(await self.limited(queues=("default",), **limits), [])
        self.assertEqual(await self.statuses(mail), [JobStatus.HELD])
        self.assertEqual([job.id for job in await self.limited(queues=("mail",), **limits)], [mail.id])

    async def test_a_held_job_counts_as_held_and_can_be_cancelled(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        running = await self.enqueue()
        held = await self.enqueue(run_at=1.0)
        await self.limited(**limits)
        self.assertEqual(await self.backend.counts_by_status(), {"running": 1, "held": 1})
        stats = await self.backend.stats(self.clock.now())
        self.assertEqual(stats.queues[0].counts, {"running": 1, "held": 1})
        self.assertEqual(stats.queues[0].oldest_due_at, 1.0)
        listed = await self.backend.list(status=JobStatus.HELD, limit=10)
        self.assertEqual([job.id for job in listed.items], [held.id])
        self.assertTrue(await self.backend.cancel(held.id))
        self.assertEqual(await self.statuses(held), [JobStatus.CANCELLED])
        await self.backend.complete(running, None)
        self.assertEqual(await self.limited(**limits), [])

    async def test_every_way_out_of_running_frees_the_room_of_a_task(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}

        async def complete(job):
            await self.backend.complete(job, None)

        async def fail(job):
            await self.backend.fail(job, ERROR, None)

        async def retry(job):
            await self.backend.fail(job, ERROR, self.clock.now() + 3600)

        async def release(job):
            await self.backend.release(job)

        async def release_many(job):
            await self.backend.release_many([job])

        async def hand_back(job):
            await self.backend.hand_back(job, 3600)

        async def cancel(job):
            await self.backend.cancel(job.id)

        async def reap(job):
            self.clock.advance(61)
            await self.backend.reap_expired()

        for way in (complete, fail, retry, release, release_many, hand_back, cancel, reap):
            with self.subTest(way=way.__name__):
                running = await self.enqueue(max_attempts=2, run_at=0.0)
                waiting = await self.enqueue(run_at=1.0)
                self.assertEqual([job.id for job in await self.limited(**limits)], [running.id])
                self.assertEqual(await self.limited(**limits), [])
                await way(await self.stored(running))
                self.clock.advance(1)
                admitted = await self.limited(**limits)
                self.assertEqual([job.id for job in admitted], [waiting.id], way.__name__)
                for job in admitted:
                    await self.backend.complete(job, None)
                for status in (JobStatus.QUEUED, JobStatus.HELD):
                    for leftover in (await self.backend.list(status=status, limit=10)).items:
                        await self.backend.cancel(leftover.id)

    async def test_a_claim_by_id_takes_room_from_the_task(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        first = await self.enqueue()
        second = await self.enqueue(run_at=1.0)
        taken = await self.backend.claim_job(first.id, worker_id="worker-a", lease_seconds=60.0)
        self.assertEqual(taken.id, first.id)
        self.assertEqual(await self.limited(**limits), [])
        self.assertEqual(await self.statuses(second), [JobStatus.HELD])
        await self.backend.complete(taken, None)
        self.assertEqual([job.id for job in await self.limited(**limits)], [second.id])

    async def test_a_raised_limit_admits_as_many_held_jobs_as_the_room_allows(self):
        self.requires("limits")
        jobs = [await self.enqueue(run_at=float(index)) for index in range(4)]
        self.assertEqual(len(await self.limited(**{"tests.noop": Limit(concurrency=1)})), 1)
        self.assertEqual((await self.backend.counts_by_status())["held"], 3)
        admitted = await self.limited(**{"tests.noop": Limit(concurrency=3)})
        self.assertEqual([job.id for job in admitted], [jobs[1].id, jobs[2].id])
        self.assertEqual(await self.statuses(jobs[3]), [JobStatus.HELD])

    async def test_a_held_job_released_to_the_queue_is_decided_again(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        first = await self.enqueue(run_at=0.0)
        second = await self.enqueue(run_at=1.0)
        await self.limited(**limits)
        await self.backend.hand_back(await self.stored(first), 0.0)
        self.assertEqual(await self.statuses(first, second), [JobStatus.QUEUED, JobStatus.HELD])
        admitted = await self.limited(**limits)
        self.assertEqual([job.id for job in admitted], [second.id])
        self.assertEqual(await self.statuses(first), [JobStatus.HELD])
        await self.backend.complete(await self.stored(second), None)
        self.assertEqual([job.id for job in await self.limited(**limits)], [first.id])

    async def test_jobs_of_a_saturated_task_do_not_hide_the_work_behind_them(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        await self.enqueue(run_at=0.0)
        flood = [await self.enqueue(run_at=1.0 + index, priority=5) for index in range(30)]
        other = await self.enqueue(task="tests.mail", run_at=100.0)
        self.assertEqual(len(await self.limited(limit=4, **limits)), 1)
        claimed = []
        for _ in range(12):
            claimed += await self.limited(limit=4, **limits)
            if any(job.id == other.id for job in claimed):
                break
        self.assertIn(other.id, [job.id for job in claimed])
        self.assertEqual(len([job for job in claimed if job.task == "tests.noop"]), 0)
        held = (await self.backend.counts_by_status())["held"]
        self.assertGreaterEqual(held, 1)
        self.assertLessEqual(held, len(flood))

    async def test_concurrent_claims_never_exceed_a_concurrency_limit(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=3)}
        await self.backend.enqueue_many(
            [make_job(run_at=float(index)) for index in range(RACED_JOBS)]
        )
        racers = await self.racers()
        results = await asyncio.gather(
            *(
                backend.claim(
                    worker_id=f"w{index}", queues=("default",), limit=10,
                    lease_seconds=60, limits=limits,
                )
                for index, backend in enumerate(racers)
            )
        )
        taken = [job.id for batch in results for job in batch]
        self.assertEqual(len(taken), 3)
        self.assertEqual(len(set(taken)), 3)
        self.assertEqual((await self.backend.counts_by_status())["running"], 3)

    async def test_unlimited_tasks_pass_a_claim_with_limits_untouched(self):
        self.requires("limits")
        limits = {"tests.other": Limit(concurrency=1)}
        jobs = [await self.enqueue(run_at=float(index)) for index in range(3)]
        claimed = await self.limited(limit=2, **limits)
        self.assertEqual([job.id for job in claimed], [jobs[0].id, jobs[1].id])
        self.assertEqual(await self.statuses(jobs[2]), [JobStatus.QUEUED])

    async def test_held_jobs_of_tasks_named_to_release_held_return_to_the_queue(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1), "tests.mail": Limit(concurrency=1)}
        for task in ("tests.noop", "tests.mail"):
            await self.enqueue(task=task, run_at=0.0)
            await self.enqueue(task=task, run_at=1.0, priority=3)
        self.assertEqual(len(await self.limited(**limits)), 2)
        self.assertEqual((await self.backend.counts_by_status())["held"], 2)
        self.assertEqual(await self.backend.release_held(tasks=()), 0)
        self.assertEqual(await self.backend.release_held(tasks=("tests.other",)), 0)
        self.assertEqual(await self.backend.release_held(tasks=("tests.mail",)), 1)
        held = (await self.backend.list(status=JobStatus.HELD, limit=10)).items
        self.assertEqual([job.task for job in held], ["tests.noop"])
        queued = (await self.backend.list(status=JobStatus.QUEUED, limit=10)).items
        self.assertEqual([(job.task, job.run_at, job.priority) for job in queued], [("tests.mail", 0.0, 0)])
        self.assertEqual(len(await self.limited(**limits)), 0)
        self.assertEqual((await self.backend.counts_by_status())["held"], 2)
        self.assertEqual(await self.backend.release_held(tasks=("tests.noop", "tests.mail")), 2)
        self.assertEqual((await self.backend.counts_by_status()).get("held", 0), 0)
        self.assertEqual(sorted(job.task for job in await self.claim()), ["tests.mail", "tests.noop"])

    async def test_release_held_stays_within_the_queues_it_is_given(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1)}
        await self.enqueue(run_at=0.0)
        in_default = await self.enqueue(run_at=1.0)
        in_mail = await self.enqueue(run_at=2.0, queue="mail")
        self.assertEqual(len(await self.limited(queues=("default", "mail"), **limits)), 1)
        self.assertEqual(await self.statuses(in_default, in_mail), [JobStatus.HELD, JobStatus.HELD])
        self.assertEqual(await self.backend.release_held(tasks=("tests.noop",), queues=("other",)), 0)
        self.assertEqual(await self.backend.release_held(tasks=("tests.noop",), queues=("mail",)), 1)
        self.assertEqual(await self.statuses(in_default, in_mail), [JobStatus.HELD, JobStatus.QUEUED])
        self.assertEqual(await self.backend.release_held(tasks=("tests.noop",), queues=None), 1)
        self.assertEqual(await self.statuses(in_default, in_mail), [JobStatus.QUEUED, JobStatus.QUEUED])

    async def test_a_held_job_with_room_waits_behind_fresh_work_the_queue_put_before_it(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(concurrency=1), "tests.video": Limit(concurrency=1)}
        first_noop = await self.enqueue(run_at=0.0)
        first_video = await self.enqueue(task="tests.video", run_at=0.0)
        second_video = await self.enqueue(task="tests.video", run_at=1.0)
        claimed = await self.limited(limit=3, **limits)
        self.assertEqual(sorted(job.id for job in claimed), sorted([first_noop.id, first_video.id]))
        self.assertEqual(await self.statuses(second_video), [JobStatus.HELD])
        await self.backend.complete(await self.stored(first_video), None)
        second_noop = await self.enqueue(run_at=2.0, priority=9)
        mail = await self.enqueue(task="tests.mail", run_at=2.0, priority=5)
        self.assertEqual([job.id for job in await self.limited(limit=1, **limits)], [mail.id])
        self.assertEqual(
            await self.statuses(second_noop, second_video), [JobStatus.HELD, JobStatus.HELD]
        )
        self.assertEqual([job.id for job in await self.limited(limit=1, **limits)], [second_video.id])
        await self.backend.complete(await self.stored(first_noop), None)
        self.assertEqual([job.id for job in await self.limited(limit=1, **limits)], [second_noop.id])

    async def test_held_jobs_count_as_queued_in_the_stats(self):
        self.requires("limits")
        limits = {"tests.noop": Limit(interval=3600.0)}
        await self.enqueue(run_at=0.0)
        await self.enqueue(run_at=1.0)
        await self.enqueue(run_at=2.0, queue="mail")
        self.assertEqual(len(await self.limited(queues=("default", "mail"), **limits)), 1)
        self.assertEqual((await self.backend.counts_by_status())["held"], 2)
        stats = await self.backend.stats(self.clock.now())
        by_queue = {queue.queue: queue for queue in stats.queues}
        self.assertEqual(by_queue["default"].oldest_due_at, 1.0)
        self.assertEqual(by_queue["mail"].oldest_due_at, 2.0)
        self.assertEqual(stats.queued_tasks, {"tests.noop": 2})
        await self.backend.cancel((await self.backend.list(status=JobStatus.HELD, queue="mail", limit=1)).items[0].id)
        stats = await self.backend.stats(self.clock.now())
        by_queue = {queue.queue: queue for queue in stats.queues}
        self.assertIsNone(by_queue["mail"].oldest_due_at)
        self.assertEqual(stats.queued_tasks, {"tests.noop": 1})

    async def test_a_claim_counts_the_running_jobs_of_tasks_it_has_no_limit_for(self):
        self.requires("limits")
        first = await self.enqueue(run_at=0.0)
        capped = {"tests.noop": Limit(concurrency=1)}
        self.assertEqual([job.id for job in await self.limited(**capped)], [first.id])
        second = await self.enqueue(run_at=1.0)
        mail = await self.enqueue(task="tests.mail", run_at=2.0)
        elsewhere = await self.limited(worker="worker-b", **{"tests.mail": Limit(concurrency=1)})
        self.assertEqual(sorted(job.id for job in elsewhere), sorted([second.id, mail.id]))
        self.assertTrue(await self.backend.hand_back(await self.stored(second)))
        self.assertEqual(await self.limited(**capped), [])
        self.assertEqual(await self.statuses(first, second), [JobStatus.RUNNING, JobStatus.HELD])
        await self.backend.complete(await self.stored(first), None)
        self.assertEqual([job.id for job in await self.limited(**capped)], [second.id])


    async def child_of(self, parent: Job, **overrides) -> Job:
        return await self.enqueue(after=parent.id, **overrides)

    async def finish(self, job: Job) -> None:
        [claimed] = [found for found in await self.claim() if found.id == job.id]
        self.assertTrue(await self.backend.complete(claimed, None))

    async def test_a_job_after_another_waits_out_of_the_claims_way(self):
        self.requires("after")
        parent = await self.enqueue()
        child = await self.child_of(parent)
        self.assertEqual((child.status, child.after), (JobStatus.WAITING, parent.id))
        stored = await self.stored(child)
        self.assertEqual((stored.status, stored.after), (JobStatus.WAITING, parent.id))
        self.assertEqual([job.id for job in await self.claim()], [parent.id])
        self.assertEqual(await self.claim(), [])
        self.assertIsNone(
            await self.backend.claim_job(child.id, worker_id="worker-b", lease_seconds=60.0)
        )
        self.assertEqual((await self.backend.counts_by_status())["waiting"], 1)
        listed = await self.backend.list(status=JobStatus.WAITING)
        self.assertEqual([job.id for job in listed.items], [child.id])

    async def test_a_waiting_job_is_queued_when_the_job_before_it_succeeds(self):
        self.requires("after")
        parent = await self.enqueue()
        children = [await self.child_of(parent, run_at=0.0) for _ in range(3)]
        later = await self.child_of(parent, run_at=START + 500)
        other = await self.child_of(await self.enqueue(queue="mail"))
        self.clock.advance(10)
        await self.finish(parent)
        self.assertEqual(await self.statuses(*children), [JobStatus.QUEUED] * 3)
        self.assertEqual(
            [(await self.stored(child)).run_at for child in children], [START + 10] * 3
        )
        self.assertEqual((await self.stored(later)).run_at, START + 500)
        self.assertEqual(await self.statuses(other), [JobStatus.WAITING])
        self.assertEqual(
            sorted(job.id for job in await self.claim()), sorted(child.id for child in children)
        )

    async def test_a_waiting_job_keeps_waiting_while_the_job_before_it_retries(self):
        self.requires("after")
        parent = await self.enqueue(max_attempts=2)
        child = await self.child_of(parent)
        [claimed] = await self.claim()
        self.assertTrue(await self.backend.fail(claimed, ERROR, retry_at=self.clock.now()))
        self.assertEqual(await self.statuses(child), [JobStatus.WAITING])
        [claimed] = await self.claim()
        self.assertTrue(await self.backend.release(claimed))
        self.assertEqual(await self.statuses(child), [JobStatus.WAITING])
        [claimed] = await self.claim()
        self.assertTrue(await self.backend.hand_back(claimed))
        self.assertEqual(await self.statuses(child), [JobStatus.WAITING])
        await self.finish(parent)
        self.assertEqual(await self.statuses(child), [JobStatus.QUEUED])

    async def chain(self, length: int = 3) -> list[Job]:
        jobs = [await self.enqueue()]
        for _ in range(length):
            jobs.append(await self.child_of(jobs[-1]))
        return jobs

    async def assert_cancelled_with_the_parent(self, *jobs: Job) -> None:
        for job in jobs:
            stored = await self.stored(job)
            self.assertEqual(
                (stored.status, stored.last_error, stored.finished_at),
                (JobStatus.CANCELLED, PARENT_ENDED_MESSAGE, self.clock.now()),
            )
        self.assertEqual(await self.backend.executions(jobs[-1].id), [])

    async def test_a_failure_cancels_every_job_waiting_down_the_line(self):
        self.requires("after")
        parent, *descendants = await self.chain()
        aside = await self.child_of(parent)
        [claimed] = await self.claim()
        self.clock.advance(5)
        self.assertTrue(await self.backend.fail(claimed, ERROR, retry_at=None))
        await self.assert_cancelled_with_the_parent(*descendants, aside)
        self.assertEqual(await self.claim(), [])
        self.assertNotIn("waiting", await self.backend.counts_by_status())

    async def test_cancelling_a_job_cancels_every_job_waiting_down_the_line(self):
        self.requires("after")
        for cancelled in range(3):
            jobs = await self.chain()
            self.clock.advance(5)
            self.assertTrue(await self.backend.cancel(jobs[cancelled].id))
            self.assertEqual(await self.statuses(*jobs[:cancelled]), [
                JobStatus.QUEUED, *[JobStatus.WAITING] * 3
            ][:cancelled])
            self.assertEqual(await self.statuses(jobs[cancelled]), [JobStatus.CANCELLED])
            await self.assert_cancelled_with_the_parent(*jobs[cancelled + 1 :])
            await self.backend.cancel(jobs[0].id)

    async def test_cancelling_a_running_job_cancels_the_jobs_waiting_on_it(self):
        self.requires("after")
        parent, *descendants = await self.chain(2)
        await self.claim()
        self.assertTrue(await self.backend.cancel(parent.id))
        await self.assert_cancelled_with_the_parent(*descendants)

    async def test_a_job_lost_for_good_cancels_the_jobs_waiting_on_it(self):
        self.requires("after")
        lost, *descendants = await self.chain(2)
        kept = await self.enqueue(max_attempts=2)
        waiting = await self.child_of(kept)
        self.assertEqual(len(await self.claim()), 2)
        self.assertEqual(await self.reap_after_the_lease(), 2)
        self.assertEqual(
            await self.statuses(lost, kept, waiting),
            [JobStatus.FAILED, JobStatus.QUEUED, JobStatus.WAITING],
        )
        await self.assert_cancelled_with_the_parent(*descendants)

    async def test_a_job_sent_after_one_that_ended_is_placed_by_how_it_ended(self):
        self.requires("after")
        done = await self.enqueue()
        await self.finish(done)
        failed = await self.claimed_one()
        await self.backend.fail(failed, ERROR, retry_at=None)
        cancelled = await self.enqueue()
        await self.backend.cancel(cancelled.id)
        self.clock.advance(5)
        ready = await self.child_of(done)
        self.assertEqual((ready.status, ready.finished_at), (JobStatus.QUEUED, None))
        self.assertEqual([job.id for job in await self.claim()], [ready.id])
        for ended in (failed, cancelled):
            child = await self.child_of(ended)
            self.assertEqual(
                (child.status, child.last_error, child.finished_at),
                (JobStatus.CANCELLED, PARENT_ENDED_MESSAGE, self.clock.now()),
            )
            await self.assert_cancelled_with_the_parent(child)
        self.assertEqual(await self.claim(), [])

    async def test_a_job_cannot_be_sent_after_one_the_store_does_not_hold(self):
        self.requires("after")
        before = await self.backend.counts_by_status()
        with self.assertRaises(JobNotFoundError) as raised:
            await self.backend.enqueue_many(
                [make_job(), make_job(after="no-such-job"), make_job()]
            )
        self.assertIn("no-such-job", str(raised.exception))
        self.assertEqual(await self.backend.counts_by_status(), before)

    async def test_a_parent_sent_in_the_same_call_is_not_yet_in_the_store(self):
        self.requires("after")
        parent = make_job()
        with self.assertRaises(JobNotFoundError):
            await self.backend.enqueue_many([parent, make_job(after=parent.id)])
        self.assertEqual(await self.backend.counts_by_status(), {})
        many = [make_job() for _ in range(250)] + [make_job(after="no-such-job")]
        with self.assertRaises(JobNotFoundError):
            await self.backend.enqueue_many(many)
        self.assertEqual(await self.backend.counts_by_status(), {})
        first, last = make_job(), make_job()
        fillers = [make_job() for _ in range(1100)]
        for parent in (first, last):
            with self.assertRaises(JobNotFoundError):
                await self.backend.enqueue_many(
                    [first, make_job(after=parent.id), *fillers, last]
                )
            self.assertEqual(await self.backend.counts_by_status(), {})

    async def test_a_job_placed_cancelled_takes_no_dedup_key(self):
        self.requires("after")
        self.requires("dedup")
        ended = await self.enqueue()
        await self.backend.cancel(ended.id)
        holder = await self.enqueue(dedup_key="report:1")
        child = await self.child_of(ended, dedup_key="report:1")
        self.assertNotEqual(child.id, holder.id)
        self.assertEqual(child.status, JobStatus.CANCELLED)
        self.assertEqual(
            await self.backend.counts_by_status(), {"cancelled": 2, "queued": 1}
        )
        self.assertEqual((await self.enqueue(dedup_key="report:1")).id, holder.id)

    async def test_a_waiting_job_holds_its_dedup_key(self):
        self.requires("after")
        self.requires("dedup")
        parent = await self.enqueue()
        child = await self.child_of(parent, dedup_key="report:1")
        again = await self.child_of(parent, dedup_key="report:1")
        plain = await self.enqueue(dedup_key="report:1")
        self.assertEqual((again.id, plain.id), (child.id, child.id))
        self.assertEqual(plain.status, JobStatus.WAITING)
        await self.backend.cancel(child.id)
        fresh = await self.enqueue(dedup_key="report:1")
        self.assertNotEqual(fresh.id, child.id)
        self.assertFalse(await self.backend.requeue(child.id))

    async def test_requeue_places_a_job_by_the_state_of_the_job_before_it(self):
        self.requires("after")
        parent, child, grandchild = await self.chain(2)
        [claimed] = await self.claim()
        await self.backend.fail(claimed, ERROR, retry_at=None)
        self.assertFalse(await self.backend.requeue(child.id))
        self.assertTrue(await self.backend.requeue(parent.id))
        self.assertEqual(
            await self.statuses(child, grandchild), [JobStatus.CANCELLED] * 2
        )
        self.assertFalse(await self.backend.requeue(grandchild.id))
        self.assertTrue(await self.backend.requeue(child.id))
        self.assertTrue(await self.backend.requeue(grandchild.id))
        self.assertEqual(await self.statuses(child, grandchild), [JobStatus.WAITING] * 2)
        waiting = await self.stored(child)
        self.assertEqual((waiting.finished_at, waiting.last_error), (None, None))
        await self.finish(parent)
        self.assertEqual(
            await self.statuses(child, grandchild), [JobStatus.QUEUED, JobStatus.WAITING]
        )
        [claimed] = await self.claim()
        await self.backend.fail(claimed, ERROR, retry_at=None)
        self.assertTrue(await self.backend.requeue(child.id))
        self.assertEqual(await self.statuses(child), [JobStatus.QUEUED])

    async def test_requeue_runs_a_job_whose_parent_was_purged(self):
        self.requires("after")
        self.requires("retention")
        parent = await self.enqueue()
        child = await self.child_of(parent)
        await self.finish(parent)
        [claimed] = await self.claim()
        await self.backend.fail(claimed, ERROR, retry_at=None)
        self.assertEqual(
            await self.backend.purge(pruning(success=1e-9), self.clock.now() + 10), 1
        )
        self.assertIsNone(await self.stored(parent))
        self.assertTrue(await self.backend.requeue(child.id))
        self.assertEqual(await self.statuses(child), [JobStatus.QUEUED])

    async def test_waiting_jobs_stay_out_of_the_queue_figures(self):
        self.requires("after")
        parent = await self.enqueue(run_at=5.0)
        await self.child_of(parent, run_at=1.0)
        await self.child_of(parent, run_at=2.0, queue="mail")
        stats = await self.backend.stats(self.clock.now())
        by_queue = {queue.queue: queue for queue in stats.queues}
        self.assertEqual(by_queue["default"].counts, {"queued": 1, "waiting": 1})
        self.assertEqual(by_queue["default"].oldest_due_at, 5.0)
        self.assertEqual(by_queue["mail"].counts, {"waiting": 1})
        self.assertIsNone(by_queue["mail"].oldest_due_at)
        self.assertEqual(stats.queued_tasks, {"tests.noop": 1})
        await self.finish(parent)
        stats = await self.backend.stats(self.clock.now())
        by_queue = {queue.queue: queue for queue in stats.queues}
        self.assertEqual(by_queue["mail"].counts, {"queued": 1})
        self.assertEqual(by_queue["mail"].oldest_due_at, START)
        self.assertEqual(stats.queued_tasks, {"tests.noop": 2})

    async def test_a_wide_line_of_waiting_jobs_is_cancelled_without_rereading_them_for_each(self):
        self.requires("after")
        parent = await self.enqueue()
        aside = await self.child_of(await self.enqueue(run_at=1.0))
        for _ in range(40):
            await self.backend.enqueue_many(
                [make_job(after=parent.id) for _ in range(100)]
            )
        started = time.monotonic()
        self.assertTrue(await self.backend.cancel(parent.id))
        self.assertLess(time.monotonic() - started, self.wide_cancel_seconds)
        self.assertEqual(
            await self.backend.counts_by_status(),
            {"cancelled": 4001, "queued": 1, "waiting": 1},
        )
        self.assertEqual(await self.statuses(aside), [JobStatus.WAITING])

    async def test_a_long_line_of_waiting_jobs_is_cancelled_at_once(self):
        self.requires("after")
        parent, *descendants = await self.chain(300)
        self.assertTrue(await self.backend.cancel(parent.id))
        self.assertEqual(await self.backend.counts_by_status(), {"cancelled": 301})
        self.assertEqual(await self.statuses(descendants[-1]), [JobStatus.CANCELLED])
