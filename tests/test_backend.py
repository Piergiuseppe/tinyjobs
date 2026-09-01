from __future__ import annotations

import unittest

from dataclasses import replace

from tinyjobs.models import ExecutionError, Job
from tinyjobs.status import ExecutionStatus, JobStatus
from tests.support import BackendTestCase

ERROR = ExecutionError(type="ValueError", message="boom", traceback="trace")


class SQLiteBackendTests(BackendTestCase):
    def enqueue(self, **overrides) -> Job:
        defaults = dict(
            id=overrides.pop("id", None) or self.next_id(),
            task="tests.noop",
            payload=b"{}",
            codec="json",
            queue="default",
            status=JobStatus.QUEUED,
            max_attempts=1,
            run_at=0.0,
            created_at=0.0,
        )
        defaults.update(overrides)
        return self.backend.enqueue(Job(**defaults))

    def next_id(self) -> str:
        from tinyjobs.ids import new_job_id

        return new_job_id()

    def claim(self, limit=10, lease_seconds=60.0, worker="worker-a"):
        return self.backend.claim(
            worker_id=worker,
            queues=("default",),
            limit=limit,
            lease_seconds=lease_seconds,
        )

    def test_enqueued_job_is_readable(self):
        job = self.enqueue()
        stored = self.backend.get(job.id)
        self.assertEqual(stored.id, job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)

    def test_claim_marks_running_and_counts_the_attempt(self):
        self.enqueue()
        claimed = self.claim()
        self.assertEqual(len(claimed), 1)
        self.assertIs(claimed[0].status, JobStatus.RUNNING)
        self.assertEqual(claimed[0].attempts, 1)
        self.assertEqual(claimed[0].worker_id, "worker-a")

    def test_claim_creates_an_execution_row(self):
        job = self.enqueue()
        self.claim()
        executions = self.backend.executions(job.id)
        self.assertEqual(len(executions), 1)
        self.assertIs(executions[0].status, ExecutionStatus.RUNNING)

    def test_claim_never_returns_the_same_job_twice(self):
        self.enqueue()
        self.assertEqual(len(self.claim()), 1)
        self.assertEqual(len(self.claim()), 0)

    def test_claim_respects_run_at(self):
        self.enqueue(run_at=self.app.clock.now() + 3600)
        self.assertEqual(self.claim(), [])

    def test_claim_respects_the_queue_filter(self):
        self.enqueue(queue="other")
        self.assertEqual(self.claim(), [])

    def test_claim_skips_cancelled_jobs(self):
        job = self.enqueue()
        self.assertTrue(self.backend.cancel(job.id))
        self.assertEqual(self.claim(), [])

    def test_claim_orders_by_priority_then_age(self):
        low = self.enqueue(priority=0, run_at=1.0)
        high = self.enqueue(priority=9, run_at=2.0)
        claimed = self.claim(limit=2)
        self.assertEqual([j.id for j in claimed], [high.id, low.id])

    def test_claim_honours_the_limit(self):
        for _ in range(5):
            self.enqueue()
        self.assertEqual(len(self.claim(limit=2)), 2)

    def test_complete_stores_the_result_and_closes_the_execution(self):
        job = self.enqueue()
        claimed = self.claim()[0]
        self.assertTrue(self.backend.complete(claimed, b'"done"'))
        stored = self.backend.get(job.id)
        self.assertIs(stored.status, JobStatus.SUCCESS)
        self.assertEqual(stored.result, b'"done"')
        self.assertIsNone(stored.lease_expires_at)
        self.assertIs(
            self.backend.executions(job.id)[0].status, ExecutionStatus.SUCCESS
        )

    def test_fail_without_retry_is_terminal(self):
        job = self.enqueue()
        claimed = self.claim()[0]
        self.assertTrue(self.backend.fail(claimed, ERROR, None))
        stored = self.backend.get(job.id)
        self.assertIs(stored.status, JobStatus.FAILED)
        self.assertIn("boom", stored.last_error)

    def test_fail_with_retry_requeues_at_the_given_time(self):
        job = self.enqueue(max_attempts=3)
        claimed = self.claim()[0]
        self.assertTrue(self.backend.fail(claimed, ERROR, retry_at=1234.0))
        stored = self.backend.get(job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual(stored.run_at, 1234.0)
        self.assertEqual(stored.attempts, 1)
        self.assertIsNone(stored.worker_id)

    def test_a_worker_that_lost_the_lease_cannot_complete(self):
        self.enqueue()
        claimed = self.claim()[0]
        impostor = replace(claimed, worker_id="worker-b")
        self.assertFalse(self.backend.complete(impostor, None))
        self.assertIs(self.backend.get(claimed.id).status, JobStatus.RUNNING)

    def test_completing_twice_is_a_no_op(self):
        self.enqueue()
        claimed = self.claim()[0]
        self.assertTrue(self.backend.complete(claimed, None))
        self.assertFalse(self.backend.complete(claimed, None))

    def test_heartbeat_extends_the_lease(self):
        self.enqueue()
        claimed = self.claim(lease_seconds=10)[0]
        self.backend.heartbeat([claimed.id], 600.0)
        self.assertGreater(
            self.backend.get(claimed.id).lease_expires_at, claimed.lease_expires_at
        )

    def test_expired_lease_returns_the_job_to_the_queue(self):
        job = self.enqueue(max_attempts=3)
        self.claim(lease_seconds=-5)
        self.assertEqual(self.backend.reap_expired(), 1)
        stored = self.backend.get(job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertIsNone(stored.worker_id)
        self.assertIn("lease expired", stored.last_error)

    def test_expired_lease_fails_the_job_when_attempts_are_exhausted(self):
        job = self.enqueue(max_attempts=1)
        self.claim(lease_seconds=-5)
        self.assertEqual(self.backend.reap_expired(), 1)
        self.assertIs(self.backend.get(job.id).status, JobStatus.FAILED)

    def test_reaping_marks_the_abandoned_execution_lost(self):
        job = self.enqueue(max_attempts=3)
        self.claim(lease_seconds=-5)
        self.backend.reap_expired()
        self.assertIs(self.backend.executions(job.id)[0].status, ExecutionStatus.LOST)

    def test_reaper_ignores_healthy_leases(self):
        self.enqueue()
        self.claim(lease_seconds=600)
        self.assertEqual(self.backend.reap_expired(), 0)

    def test_release_returns_a_running_job_to_the_queue(self):
        self.enqueue()
        claimed = self.claim()[0]
        self.assertTrue(self.backend.release(claimed))
        self.assertIs(self.backend.get(claimed.id).status, JobStatus.QUEUED)

    def test_requeue_resets_a_failed_job(self):
        job = self.enqueue()
        claimed = self.claim()[0]
        self.backend.fail(claimed, ERROR, None)
        self.assertTrue(self.backend.requeue(job.id))
        stored = self.backend.get(job.id)
        self.assertIs(stored.status, JobStatus.QUEUED)
        self.assertEqual(stored.attempts, 0)
        self.assertIsNone(stored.last_error)

    def test_requeue_refuses_a_successful_job(self):
        self.enqueue()
        claimed = self.claim()[0]
        self.backend.complete(claimed, None)
        self.assertFalse(self.backend.requeue(claimed.id))

    def test_cancel_refuses_a_terminal_job(self):
        self.enqueue()
        claimed = self.claim()[0]
        self.backend.complete(claimed, None)
        self.assertFalse(self.backend.cancel(claimed.id))

    def test_dedup_key_returns_the_existing_job(self):
        first = self.enqueue(dedup_key="invoice:1")
        second = self.enqueue(dedup_key="invoice:1")
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(self.backend.list(limit=100).items), 1)

    def test_dedup_key_is_reusable_once_the_job_is_terminal(self):
        first = self.enqueue(dedup_key="invoice:1")
        claimed = self.claim()[0]
        self.backend.complete(claimed, None)
        second = self.enqueue(dedup_key="invoice:1")
        self.assertNotEqual(first.id, second.id)

    def test_listing_filters_and_paginates(self):
        for index in range(5):
            self.enqueue(queue="a" if index % 2 else "b")
        page = self.backend.list(limit=2)
        self.assertEqual(len(page.items), 2)
        self.assertIsNotNone(page.cursor)
        following = self.backend.list(limit=10, cursor=page.cursor)
        self.assertEqual(len(following.items), 3)
        self.assertEqual(len(self.backend.list(queue="a", limit=10).items), 2)

    def test_listing_filters_by_status(self):
        self.enqueue()
        self.enqueue()
        self.backend.complete(self.claim(limit=1)[0], None)
        self.assertEqual(
            len(self.backend.list(status=JobStatus.SUCCESS, limit=10).items), 1
        )

    def test_enqueue_many_writes_every_job(self):
        jobs = [
            Job(
                id=self.next_id(),
                task="tests.noop",
                payload=b"{}",
                codec="json",
                queue="default",
                status=JobStatus.QUEUED,
                run_at=0.0,
                created_at=0.0,
            )
            for _ in range(20)
        ]
        self.backend.enqueue_many(jobs)
        self.assertEqual(len(self.backend.list(limit=100).items), 20)


if __name__ == "__main__":
    unittest.main()
