from __future__ import annotations

import multiprocessing
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.ids import new_job_id
from tinyjobs.models import Job
from tinyjobs.status import JobStatus

JOB_COUNT = 600
PROCESS_COUNT = 6
THREAD_COUNT = 8
IDLE_ROUNDS_BEFORE_STOP = 40
CLAIM_BATCH = 1
PACING_SECONDS = 0.001


def seed(database: str, count: int) -> None:
    app = TinyJobs(f"sqlite:///{database}").start()
    app.run_sync(
        app.backend.enqueue_many(
        [
            Job(
                id=new_job_id(),
                task="tests.noop",
                payload=b"{}",
                codec="json",
                queue="default",
                status=JobStatus.QUEUED,
                run_at=0.0,
                created_at=0.0,
            )
            for _ in range(count)
            ]
        ),
        name="seed",
    )
    app.close()


def claim_once(app, worker_id: str):
    return app.run_sync(
        app.backend.claim(
            worker_id=worker_id,
            queues=("default",),
            limit=CLAIM_BATCH,
            lease_seconds=300,
        ),
        name="claim",
    )


def drain(database: str, worker_id: str, sink, barrier=None) -> None:
    app = TinyJobs(f"sqlite:///{database}").start()
    claimed: list[str] = []
    idle = 0
    if barrier is not None:
        barrier.wait(timeout=60)
    while idle < IDLE_ROUNDS_BEFORE_STOP:
        jobs = claim_once(app, worker_id)
        if not jobs:
            idle += 1
            time.sleep(0.005)
            continue
        idle = 0
        claimed.extend(job.id for job in jobs)
        time.sleep(PACING_SECONDS)
    sink.put(claimed)
    app.close()


class ClaimExclusivityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "jobs.db")
        seed(self.database, JOB_COUNT)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def assert_contention_was_real(self, batches, participants: int) -> None:
        active = sum(1 for batch in batches if batch)
        self.assertGreaterEqual(
            active,
            max(participants // 2, 2),
            f"only {active}/{participants} consumers claimed anything, so this run "
            f"did not actually exercise concurrent claims",
        )

    def assert_claimed_exactly_once(self, batches: list[list[str]]) -> None:
        everything = [job_id for batch in batches for job_id in batch]
        duplicates = [k for k, v in Counter(everything).items() if v > 1]
        self.assertEqual(duplicates, [], f"{len(duplicates)} job(s) claimed twice")
        self.assertEqual(len(everything), JOB_COUNT)
        self.assertEqual(len(set(everything)), JOB_COUNT)

    def test_competing_processes_never_claim_the_same_job(self):
        context = multiprocessing.get_context("spawn")
        sink = context.Queue()
        gate = context.Barrier(PROCESS_COUNT)
        workers = [
            context.Process(
                target=drain, args=(self.database, f"process-{index}", sink, gate)
            )
            for index in range(PROCESS_COUNT)
        ]
        for worker in workers:
            worker.start()
        batches = [sink.get(timeout=120) for _ in workers]
        for worker in workers:
            worker.join(timeout=30)
        self.assert_claimed_exactly_once(batches)
        self.assert_contention_was_real(batches, PROCESS_COUNT)

    def test_competing_threads_never_claim_the_same_job(self):
        app = TinyJobs(f"sqlite:///{self.database}").start()
        batches: list[list[str]] = []
        guard = threading.Lock()
        start = threading.Barrier(THREAD_COUNT)

        def consume(index: int) -> None:
            claimed: list[str] = []
            idle = 0
            start.wait()
            while idle < IDLE_ROUNDS_BEFORE_STOP:
                jobs = claim_once(app, f"thread-{index}")
                if not jobs:
                    idle += 1
                    time.sleep(0.005)
                    continue
                idle = 0
                claimed.extend(job.id for job in jobs)
                time.sleep(PACING_SECONDS)
            with guard:
                batches.append(claimed)

        threads = [
            threading.Thread(target=consume, args=(index,))
            for index in range(THREAD_COUNT)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)
        self.assert_claimed_exactly_once(batches)
        self.assert_contention_was_real(batches, THREAD_COUNT)
        app.close()

    def test_every_job_ends_up_running_exactly_once(self):
        app = TinyJobs(f"sqlite:///{self.database}").start()
        drained = app.run_sync(
            app.backend.claim(
                worker_id="solo",
                queues=("default",),
                limit=JOB_COUNT,
                lease_seconds=300,
            ),
            name="claim",
        )
        self.assertEqual(len(drained), JOB_COUNT)
        self.assertTrue(all(job.attempts == 1 for job in drained))
        app.close()


if __name__ == "__main__":
    unittest.main()
