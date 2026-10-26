from __future__ import annotations

import asyncio
import copy
import time
import unittest
from dataclasses import replace

from tinyjobs import TinyJobs
from tinyjobs.backends import Backend, BaseBackend, Capabilities, register_builtin
from tinyjobs.backends.managed import ManagedBackend
from tinyjobs.exceptions import AppClosedError, SynchronousCallInsideEventLoopError
from tinyjobs.models import ExecutionError, Job, Page, QueueStats, Stats
from tinyjobs.runtime import LoopRunner
from tinyjobs.status import ExecutionStatus, JobStatus


class RamBackend(BaseBackend):
    capabilities = Capabilities(dedup=True, retention=True)

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
                    claims=job.claims + 1,
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
            current = self._held(job)
            if current is None:
                return False
            self._jobs[job.id] = replace(
                current, status=JobStatus.SUCCESS, result_payload=result, hand_backs=0
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
            current = self._held(job)
            if current is None:
                return False
            self._jobs[job.id] = replace(
                current,
                status=JobStatus.QUEUED if retry_at else JobStatus.FAILED,
                run_at=retry_at or current.run_at,
                worker_id=None,
                last_error=error.summary,
                hand_backs=0,
            )
            self._attempts[job.id][-1] = str(execution_status)
            return True

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        async with self._lock:
            current = self._held(job)
            if current is None:
                return False
            self._jobs[job.id] = replace(
                current, status=JobStatus.QUEUED, worker_id=None, hand_backs=0
            )
            return True

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        async with self._lock:
            current = self._held(job)
            if current is None:
                return False
            self._jobs[job.id] = replace(
                current,
                status=JobStatus.QUEUED,
                worker_id=None,
                attempts=current.attempts - 1,
                hand_backs=current.hand_backs + 1,
            )
            self._attempts[job.id].pop()
            return True

    def _held(self, job: Job) -> Job | None:
        current = self._jobs.get(job.id)
        if (
            current is None
            or current.status is not JobStatus.RUNNING
            or current.worker_id != job.worker_id
            or current.claims != job.claims
        ):
            return None
        return current

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

    async def counts_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for job in self._jobs.values():
            counts[str(job.status)] = counts.get(str(job.status), 0) + 1
        return counts

    async def stats(self, now: float) -> Stats:
        queues: dict[str, dict[str, int]] = {}
        for job in self._jobs.values():
            counts = queues.setdefault(job.queue, {})
            counts[str(job.status)] = counts.get(str(job.status), 0) + 1
        return Stats(queues=[QueueStats(queue, counts) for queue, counts in queues.items()])

    async def purge(self, policy, now: float) -> int | None:
        if not policy.enabled:
            return None
        horizons = policy.horizons(now)
        async with self._lock:
            doomed = [
                job.id for job in self._jobs.values()
                if job.status in horizons
                and job.finished_at is not None
                and job.finished_at < horizons[job.status]
            ]
            for job_id in doomed:
                del self._jobs[job_id]
        return len(doomed)


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



class ThirdPartyRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = TinyJobs("ram://")
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await self.app.aclose()

    async def test_a_plugin_can_implement_its_own_pruning(self):
        from tinyjobs.retention import RetentionPolicy

        @self.app.task
        async def handler(value):
            return value

        job = await handler.aenqueue(1)
        claimed = await self.app.backend.claim(
            worker_id="w", queues=("default",), limit=1, lease_seconds=60
        )
        await self.app.backend.complete(claimed[0], None)
        self.app.backend._jobs[job.id] = replace(
            self.app.backend._jobs[job.id], finished_at=100.0
        )

        removed = await self.app.apurge(
            RetentionPolicy(success=10.0, cancelled=0, failed=0, interval=0.0)
        )
        self.assertEqual(removed, 1)
        self.assertIsNone(await self.app.aget(job.id))

    async def test_counts_come_from_the_plugin(self):
        @self.app.task
        async def handler(value):
            return value

        await handler.aenqueue(1)
        await handler.aenqueue(2)
        self.assertEqual(await self.app.acounts(), {"queued": 2})


class Releasing:
    capabilities = Capabilities()

    def __init__(self) -> None:
        self.released: list[tuple[Job, float]] = []

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        self.released.append((job, delay))
        return True


class HandBackFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_base_backend_without_hand_back_releases_instead(self):
        class Legacy(RamBackend):
            hand_back = BaseBackend.hand_back

        backend = Legacy("legacy://")
        await backend.enqueue(Job("id", "task", b"", "json", "default", JobStatus.QUEUED))
        claimed = (
            await backend.claim(
                worker_id="w", queues=["default"], limit=1, lease_seconds=60.0
            )
        )[0]
        self.assertTrue(await backend.hand_back(claimed, 5.0))
        stored = await backend.get("id")
        self.assertEqual(
            (stored.status, stored.attempts, stored.hand_backs), (JobStatus.QUEUED, 1, 0)
        )

    async def test_a_plugin_without_hand_back_is_released_instead(self):
        plugin = Releasing()
        runner = LoopRunner()
        job = Job("id", "task", b"", "json", "default", JobStatus.RUNNING)
        try:
            self.assertTrue(await ManagedBackend(plugin, runner).hand_back(job, 5.0))
        finally:
            runner.close()
        self.assertEqual(plugin.released, [(job, 5.0)])


class ManagedBackendTests(unittest.TestCase):
    def test_a_half_built_wrapper_has_no_attributes_to_forward(self):
        with self.assertRaises(AttributeError):
            ManagedBackend.__new__(ManagedBackend).anything

    def test_a_plugin_heartbeat_without_a_worker_id_still_runs(self):
        beats = []

        class Legacy(Releasing):
            async def heartbeat(self, job_ids, lease_seconds):
                beats.append((list(job_ids), lease_seconds))

        class Aware(Releasing):
            async def heartbeat(self, job_ids, lease_seconds, worker_id=None):
                beats.append((list(job_ids), lease_seconds, worker_id))

        class KeywordOnly(Releasing):
            async def heartbeat(self, job_ids, lease_seconds, *, worker_id=None):
                beats.append((list(job_ids), lease_seconds, worker_id))

        def plain(method):
            async def wrapped(*args, **kwargs):
                return await method(*args, **kwargs)

            return wrapped

        class Wrapped(Releasing):
            heartbeat = plain(Legacy.heartbeat)

        runner = LoopRunner()
        try:
            for plugin in (Legacy(), Aware(), KeywordOnly(), Wrapped()):
                wrapper = ManagedBackend(plugin, runner)
                runner.run(lambda: wrapper.heartbeat(["a"], 30.0, worker_id="w"))
        finally:
            runner.close()
        self.assertEqual(
            beats,
            [(["a"], 30.0), (["a"], 30.0, "w"), (["a"], 30.0, "w"), (["a"], 30.0)],
        )

    def test_calls_that_fail_or_are_refused_leave_nothing_counted(self):
        class Broken(Releasing):
            def cancel(self, job_id):
                raise RuntimeError("broken")

            async def close(self):
                return None

        runner = LoopRunner()
        try:
            wrapper = ManagedBackend(Broken(), runner)
            with self.assertRaises(RuntimeError):
                runner.run(lambda: wrapper.cancel("id"))
            self.assertEqual(wrapper._active, 0)
            runner.run(lambda: wrapper.close())
            with self.assertRaises(AppClosedError):
                runner.run(lambda: wrapper.cancel("id"))
            self.assertEqual(wrapper._active, 0)
        finally:
            runner.close()

    def test_a_wrapper_can_be_copied(self):
        runner = LoopRunner()
        try:
            wrapper = ManagedBackend(Releasing(), runner)
            self.assertIs(copy.copy(wrapper).capabilities, wrapper.capabilities)
        finally:
            runner.close()


if __name__ == "__main__":
    unittest.main()
