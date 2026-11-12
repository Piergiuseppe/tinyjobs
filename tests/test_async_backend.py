from __future__ import annotations

import asyncio
import copy
import threading
import time
import unittest
from collections import Counter
from contextlib import ExitStack
from dataclasses import replace
from functools import wraps
from itertools import count
from unittest import mock

from tinyjobs import TinyJobs
from tinyjobs.backends import Backend, BaseBackend, Capabilities, register_builtin
from tinyjobs.backends.base import LEASE_EXPIRED_MESSAGE
from tinyjobs.backends.managed import ManagedBackend
from tinyjobs.clock import Clock
from tinyjobs.exceptions import AppClosedError, SynchronousCallInsideEventLoopError
from tinyjobs.models import Execution, ExecutionError, Job, Page, QueueStats, Stats
from tinyjobs.runtime import LoopRunner
from tinyjobs.status import ExecutionStatus, JobStatus
from tinyjobs.testing import BackendContract


def locked(method):
    @wraps(method)
    async def called(self, *args, **kwargs):
        with self._lock:
            return await method(self, *args, **kwargs)

    return called


class RamBackend(BaseBackend):
    capabilities = Capabilities(retention=True)

    def __init__(self, url: str, *, clock: Clock | None = None, **options) -> None:
        self._clock = clock or Clock()
        self._jobs: dict[str, Job] = {}
        self._order: dict[str, int] = {}
        self._sent = count()
        self._runs: dict[str, list[Execution]] = {}
        self._purged_at: float | None = None
        self._lock = threading.Lock()

    @classmethod
    def from_url(cls, url: str, **options) -> "RamBackend":
        return cls(url, **options)

    @locked
    async def enqueue(self, job: Job) -> Job:
        self._jobs[job.id] = replace(job)
        self._order[job.id] = next(self._sent)
        self._runs[job.id] = []
        return job

    @locked
    async def claim(
        self, *, worker_id, queues, limit, lease_seconds, limits=None
    ) -> list[Job]:
        now = self._clock.now()
        due = [
            job
            for job in self._jobs.values()
            if job.status is JobStatus.QUEUED and job.queue in queues and job.run_at <= now
        ]
        due.sort(key=lambda job: (-job.priority, job.run_at, self._order[job.id]))
        claimed = []
        for job in due[:limit]:
            taken = self._put(
                job,
                status=JobStatus.RUNNING,
                worker_id=worker_id,
                attempts=job.attempts + 1,
                claims=job.claims + 1,
                lease_expires_at=now + lease_seconds,
            )
            self._runs[job.id].append(
                Execution(
                    job.id,
                    taken.attempts,
                    ExecutionStatus.RUNNING,
                    worker_id,
                    started_at=now,
                    claim=taken.claims,
                )
            )
            claimed.append(replace(taken))
        return claimed

    @locked
    async def heartbeat(self, job_ids, lease_seconds, worker_id=None) -> list[str]:
        renewed = []
        for job_id in job_ids:
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.RUNNING:
                continue
            if worker_id is not None and job.worker_id != worker_id:
                continue
            self._put(job, lease_expires_at=self._clock.now() + lease_seconds)
            renewed.append(job_id)
        return renewed

    @locked
    async def complete(self, job: Job, result: bytes | None) -> bool:
        held = self._held(job)
        if held is None:
            return False
        self._end(
            held,
            JobStatus.SUCCESS,
            ExecutionStatus.SUCCESS,
            result_payload=result,
            last_error=None,
        )
        return True

    @locked
    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        held = self._held(job)
        if held is None:
            return False
        if retry_at is None:
            self._end(held, JobStatus.FAILED, execution_status, error, last_error=error.summary)
        else:
            self._requeue(held, execution_status, error, run_at=retry_at, last_error=error.summary)
        return True

    @locked
    async def release(self, job: Job, delay: float = 0.0) -> bool:
        held = self._held(job)
        if held is None:
            return False
        self._requeue(held, ExecutionStatus.LOST, run_at=self._clock.now() + delay)
        return True

    @locked
    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        held = self._held(job)
        if held is None:
            return False
        self._runs[held.id].pop()
        self._put(
            held,
            status=JobStatus.QUEUED,
            run_at=self._clock.now() + delay,
            attempts=held.attempts - 1,
            hand_backs=held.hand_backs + 1,
            worker_id=None,
            lease_expires_at=None,
        )
        return True

    @locked
    async def cancel(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status.is_terminal:
            return False
        if job.status is JobStatus.RUNNING:
            self._close(job, ExecutionStatus.CANCELLED)
        self._put(
            job,
            status=JobStatus.CANCELLED,
            finished_at=self._clock.now(),
            lease_expires_at=None,
            hand_backs=0,
        )
        return True

    @locked
    async def requeue(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status not in (JobStatus.FAILED, JobStatus.CANCELLED):
            return False
        self._put(
            job,
            status=JobStatus.QUEUED,
            run_at=job.created_at if job.claims == 0 else self._clock.now(),
            attempts=0,
            last_error=None,
            finished_at=None,
            worker_id=None,
            lease_expires_at=None,
        )
        return True

    @locked
    async def get(self, job_id: str) -> Job | None:
        job = self._jobs.get(job_id)
        return None if job is None else replace(job)

    @locked
    async def list(self, *, status=None, queue=None, limit=100, cursor=None) -> Page:
        found = [
            job
            for job in sorted(self._jobs.values(), key=lambda job: -self._order[job.id])
            if (cursor is None or self._order[job.id] < int(cursor))
            and (status is None or job.status is status)
            and (queue is None or job.queue == queue)
        ]
        page = found[:limit]
        more = len(found) > limit
        return Page(
            items=[replace(job) for job in page],
            cursor=str(self._order[page[-1].id]) if more else None,
        )

    @locked
    async def executions(self, job_id: str) -> list[Execution]:
        return [replace(run) for run in self._runs.get(job_id, [])]

    @locked
    async def reap_expired(self) -> int:
        now = self._clock.now()
        expired = [
            job
            for job in self._jobs.values()
            if job.status is JobStatus.RUNNING and job.lease_expires_at < now
        ]
        for job in expired:
            if job.attempts < job.max_attempts:
                self._requeue(
                    job, ExecutionStatus.LOST, run_at=now, last_error=LEASE_EXPIRED_MESSAGE
                )
            else:
                self._end(
                    job,
                    JobStatus.FAILED,
                    ExecutionStatus.LOST,
                    run_at=now,
                    worker_id=None,
                    last_error=LEASE_EXPIRED_MESSAGE,
                )
        return len(expired)

    @locked
    async def counts_by_status(self) -> dict[str, int]:
        return dict(Counter(str(job.status) for job in self._jobs.values()))

    @locked
    async def stats(self, now: float) -> Stats:
        queues = []
        for queue in sorted({job.queue for job in self._jobs.values()}):
            jobs = [job for job in self._jobs.values() if job.queue == queue]
            due = [
                job.run_at
                for job in jobs
                if job.status is JobStatus.QUEUED and job.run_at <= now
            ]
            queues.append(
                QueueStats(
                    queue,
                    dict(Counter(str(job.status) for job in jobs)),
                    min(due, default=None),
                )
            )
        running = [job for job in self._jobs.values() if job.status is JobStatus.RUNNING]
        queued = [job for job in self._jobs.values() if job.status is JobStatus.QUEUED]
        return Stats(
            queues=queues,
            expired_leases=sum(job.lease_expires_at < now for job in running),
            queued_tasks=dict(Counter(job.task for job in queued)),
        )

    @locked
    async def purge(self, policy, now: float) -> int | None:
        if not policy.enabled:
            return None
        if self._purged_at is not None and now - self._purged_at < policy.interval:
            return None
        self._purged_at = now
        doomed: list[Job] = []
        for status, horizon in policy.horizons(now).items():
            past = [
                job
                for job in self._jobs.values()
                if job.status is status
                and job.finished_at is not None
                and job.finished_at < horizon
            ]
            past.sort(key=lambda job: job.finished_at)
            doomed.extend(past[: policy.maximum_per_sweep - len(doomed)])
        for job in doomed:
            del self._jobs[job.id], self._runs[job.id], self._order[job.id]
        return len(doomed)

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

    def _put(self, job: Job, **changes) -> Job:
        self._jobs[job.id] = replace(job, **changes)
        return self._jobs[job.id]

    def _close(
        self, job: Job, status: ExecutionStatus, error: ExecutionError | None = None
    ) -> None:
        run = self._runs[job.id][-1]
        run.status = status
        run.finished_at = self._clock.now()
        if error is not None:
            run.error_type, run.traceback = error.type, error.traceback

    def _end(
        self,
        job: Job,
        status: JobStatus,
        ran: ExecutionStatus,
        error: ExecutionError | None = None,
        **changes,
    ) -> None:
        self._close(job, ran, error)
        self._put(
            job,
            status=status,
            finished_at=self._clock.now(),
            lease_expires_at=None,
            hand_backs=0,
            **changes,
        )

    def _requeue(
        self,
        job: Job,
        ran: ExecutionStatus,
        error: ExecutionError | None = None,
        **changes,
    ) -> None:
        self._close(job, ran, error)
        self._put(
            job,
            status=JobStatus.QUEUED,
            worker_id=None,
            lease_expires_at=None,
            hand_backs=0,
            **changes,
        )


register_builtin("ram", RamBackend.from_url)


class RamContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    independent_clients = False

    async def make_backend(self, clock):
        return RamBackend("ram://", clock=clock)


class NativeAsyncBackendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = TinyJobs("ram://")
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await self.app.aclose()

    async def test_a_pure_async_backend_satisfies_the_protocol(self):
        self.assertIsInstance(self.app.backend, Backend)

    async def test_claims_run_on_the_apps_loop_and_every_other_call_where_it_was_made(self):
        done = []

        @self.app.task
        async def handler(value):
            done.append(value)
            return value

        threads: dict[str, set[str]] = {}

        def watched(name, method):
            async def called(*args, **kwargs):
                threads.setdefault(name, set()).add(threading.current_thread().name)
                return await method(*args, **kwargs)

            return called

        with ExitStack() as watching:
            for name in ("enqueue", "claim", "complete", "get"):
                watching.enter_context(
                    mock.patch.object(RamBackend, name, watched(name, getattr(RamBackend, name)))
                )
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

        here = {threading.current_thread().name}
        self.assertEqual(sorted(done), [0, 1, 2, 3, 4])
        self.assertEqual(threads.pop("claim"), {"tinyjobs-loop"})
        self.assertEqual(threads, {"enqueue": here, "complete": here, "get": here})

    async def test_eager_mode_runs_jobs_on_a_backend_that_cannot_claim_one_by_id(self):
        self.assertFalse(hasattr(RamBackend, "claim_job"))
        app = TinyJobs("ram://", eager=True)
        await app.ready()
        self.addAsyncCleanup(app.aclose)

        ran: list[int] = []

        @app.task
        async def handler(value):
            ran.append(value)

        job = await handler.aenqueue(21)
        self.assertIs(job.status, JobStatus.SUCCESS)
        self.assertEqual(ran, [21])

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


class ClaimHandBackTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_given_up_claim_hands_back_until_its_callers_end_and_names_what_failed(self):
        from tinyjobs import deadlines

        class Timed(Releasing):
            def __init__(self) -> None:
                super().__init__()
                self.left: list[float] = []

            async def release(self, job: Job, delay: float = 0.0) -> bool:
                self.left.append(deadlines.left())
                if job.id == "broken":
                    raise OSError("gone")
                return await super().release(job, delay)

        plugin = Timed()
        runner = LoopRunner()
        jobs = [
            Job(name, "task", b"", "json", "default", JobStatus.RUNNING)
            for name in ("kept", "broken")
        ]
        try:
            wrapper = ManagedBackend(plugin, runner)
            with deadlines.until(time.monotonic() - 1):
                with self.assertLogs("tinyjobs.backend", "WARNING") as logs:
                    end = deadlines.Deadline(time.monotonic() + 30)
                    await wrapper._hand_back_all(jobs, end)
        finally:
            runner.close()
        self.assertEqual(plugin.released, [(jobs[0], 0.0)])
        self.assertTrue(all(20 < left <= 30 for left in plugin.left))
        self.assertIn("broken", logs.output[0])
        self.assertIn("gone", logs.output[0])

    async def test_a_cancelled_hand_back_still_names_the_jobs_that_failed(self):
        handing = asyncio.Event()

        class Stuck(Releasing):
            async def release(self, job: Job, delay: float = 0.0) -> bool:
                if job.id == "broken":
                    raise OSError("gone")
                handing.set()
                await asyncio.sleep(60)
                return True

        runner = LoopRunner()
        jobs = [
            Job(name, "task", b"", "json", "default", JobStatus.RUNNING)
            for name in ("broken", "stuck")
        ]
        try:
            wrapper = ManagedBackend(Stuck(), runner)
            with self.assertLogs("tinyjobs.backend", "WARNING") as logs:
                task = asyncio.ensure_future(wrapper._hand_back_all(jobs, None))
                await asyncio.wait_for(handing.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        finally:
            runner.close()
        self.assertEqual(len(logs.output), 2)
        self.assertIn("stuck", logs.output[0])
        self.assertIn("cancelled", logs.output[0])
        self.assertIn("broken", logs.output[1])
        self.assertIn("gone", logs.output[1])


class GivenUpClaimCloseTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_slow_hand_back_holds_neither_close_nor_the_loop(self):
        for loop_bound in (True, False):
            with self.subTest(loop_bound=loop_bound):
                await self.closes_at_once(loop_bound)

    async def closes_at_once(self, loop_bound: bool) -> None:
        from tinyjobs import deadlines

        caller = asyncio.current_task()
        loop = asyncio.get_running_loop()
        job = Job("id", "task", b"", "json", "default", JobStatus.RUNNING)
        handing = threading.Event()

        class Sleepy(Releasing):
            capabilities = Capabilities(loop_bound=loop_bound)

            async def claim(self, **_) -> list[Job]:
                await asyncio.sleep(0.3)
                loop.call_soon_threadsafe(caller.cancel)
                return [job]

            async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
                handing.set()
                await asyncio.sleep(60)
                return True

            async def close(self) -> None:
                return None

        runner = LoopRunner()
        wrapper = ManagedBackend(Sleepy(), runner)
        try:
            with self.assertRaises(asyncio.CancelledError):
                with deadlines.until(time.monotonic() + 0.2):
                    await wrapper.claim(
                        worker_id="w", queues=["default"], limit=1, lease_seconds=60
                    )
            caller.uncancel()
            self.assertTrue(await asyncio.to_thread(handing.wait, 5))
            started = time.monotonic()
            await asyncio.wait_for(wrapper.close(), 15)
            self.assertLess(time.monotonic() - started, 1.0)
        finally:
            started = time.monotonic()
            with self.assertLogs("tinyjobs.backend", "WARNING") as logs:
                await asyncio.to_thread(runner.close)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertIn("id", logs.output[0])
        self.assertIn("cancelled", logs.output[0])


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
