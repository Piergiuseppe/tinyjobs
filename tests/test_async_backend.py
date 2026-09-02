from __future__ import annotations

import asyncio
import time
import unittest
from dataclasses import replace

from tinyjobs import TinyJobs
from tinyjobs.backends import Backend, BaseBackend, Capabilities, register_builtin
from tinyjobs.exceptions import SynchronousCallInsideEventLoopError
from tinyjobs.models import ExecutionError, Job, Page
from tinyjobs.status import ExecutionStatus, JobStatus


class RamBackend(BaseBackend):
    capabilities = Capabilities(dedup=True)

    def __init__(self, url: str, **options) -> None:
        self._jobs: dict[str, Job] = {}
        self._attempts: dict[str, list[str]] = {}
        self._lock = asyncio.Lock()
        self.thread_hops = 0

    @classmethod
    def from_url(cls, url: str, **options) -> "RamBackend":
        return cls(url, **options)

    async def enqueue(self, job: Job) -> Job:
        async with self._lock:
            self._jobs[job.id] = job
            return job

    async def claim(self, *, worker_id, queues, limit, lease_seconds) -> list[Job]:
        now = time.time()
        async with self._lock:
            ready = [
                job
                for job in self._jobs.values()
                if job.status is JobStatus.QUEUED
                and job.queue in queues
                and job.run_at <= now
            ]
            ready.sort(key=lambda job: (-job.priority, job.run_at))
            claimed = []
            for job in ready[:limit]:
                updated = replace(
                    job,
                    status=JobStatus.RUNNING,
                    worker_id=worker_id,
                    attempts=job.attempts + 1,
                    lease_expires_at=now + lease_seconds,
                )
                self._jobs[job.id] = updated
                self._attempts.setdefault(job.id, []).append("running")
                claimed.append(updated)
            return claimed

    async def heartbeat(self, job_ids, lease_seconds) -> None:
        return None

    async def complete(self, job: Job, result: bytes | None) -> bool:
        async with self._lock:
            current = self._jobs.get(job.id)
            if current is None or current.worker_id != job.worker_id:
                return False
            self._jobs[job.id] = replace(
                current, status=JobStatus.SUCCESS, result_payload=result
            )
            self._attempts[job.id][-1] = "success"
            return True

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        async with self._lock:
            current = self._jobs[job.id]
            self._jobs[job.id] = replace(
                current,
                status=JobStatus.QUEUED if retry_at else JobStatus.FAILED,
                run_at=retry_at or current.run_at,
                worker_id=None,
                last_error=error.summary,
            )
            self._attempts[job.id][-1] = str(execution_status)
            return True

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        async with self._lock:
            self._jobs[job.id] = replace(
                self._jobs[job.id], status=JobStatus.QUEUED, worker_id=None
            )
            return True

    async def cancel(self, job_id: str) -> bool:
        async with self._lock:
            self._jobs[job_id] = replace(
                self._jobs[job_id], status=JobStatus.CANCELLED
            )
            return True

    async def requeue(self, job_id: str) -> bool:
        return True

    async def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    async def list(self, *, status=None, queue=None, limit=100, cursor=None) -> Page:
        items = sorted(self._jobs.values(), key=lambda job: job.id, reverse=True)
        if status is not None:
            items = [job for job in items if job.status is status]
        return Page(items=items[:limit])

    async def executions(self, job_id: str) -> list:
        return self._attempts.get(job_id, [])

    async def reap_expired(self) -> int:
        return 0


register_builtin("ram", RamBackend.from_url)


class NativeAsyncBackendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = TinyJobs("ram://")
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await self.app.aclose()

    async def test_a_pure_async_backend_satisfies_the_protocol(self):
        self.assertIsInstance(self.app.backend, Backend)

    async def test_jobs_run_end_to_end_without_a_single_thread_hop(self):
        done = []

        @self.app.task
        async def handler(value):
            done.append(value)
            return value

        jobs = [await handler.aenqueue(n) for n in range(5)]
        worker = self.app.worker(concurrency=4, reap_interval=60)
        runner = asyncio.create_task(worker.start())
        for _ in range(200):
            await asyncio.sleep(0.01)
            statuses = [(await self.app.aget(job.id)).status for job in jobs]
            if all(status.is_terminal for status in statuses):
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

        self.assertEqual(sorted(done), [0, 1, 2, 3, 4])
        self.assertEqual(self.app.backend.thread_hops, 0)

    async def test_retries_work_against_a_third_party_backend(self):
        tries = []

        @self.app.task(max_attempts=3, retry_base=0.01, retry_jitter=False)
        async def flaky():
            tries.append(1)
            if len(tries) < 3:
                raise ConnectionError("nope")

        job = await flaky.aenqueue()
        worker = self.app.worker(concurrency=2, reap_interval=60)
        runner = asyncio.create_task(worker.start())
        for _ in range(200):
            await asyncio.sleep(0.01)
            if (await self.app.aget(job.id)).status.is_terminal:
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

        self.assertIs((await self.app.aget(job.id)).status, JobStatus.SUCCESS)
        self.assertEqual(len(tries), 3)


class SyncFacadeTests(unittest.IsolatedAsyncioTestCase):
    async def test_calling_the_sync_api_inside_a_loop_is_refused_clearly(self):
        app = TinyJobs("ram://")

        @app.task
        async def handler():
            return None

        with self.assertRaises(SynchronousCallInsideEventLoopError) as caught:
            handler.enqueue()
        message = str(caught.exception)
        self.assertIn("aenqueue", message)
        self.assertIn("running event loop", message)
        await app.aclose()


class SyncOnlyUsageTests(unittest.TestCase):
    def test_everything_works_from_plain_sync_code(self):
        app = TinyJobs("ram://")

        @app.task
        async def handler(value):
            return value

        job = handler.enqueue(1)
        self.assertIs(app.get(job.id).status, JobStatus.QUEUED)
        self.assertEqual(len(app.jobs(limit=10).items), 1)
        self.assertTrue(app.cancel(job.id))
        self.assertIs(app.get(job.id).status, JobStatus.CANCELLED)
        app.close()


if __name__ == "__main__":
    unittest.main()
