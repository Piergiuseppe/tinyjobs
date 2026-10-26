from __future__ import annotations

import asyncio
import json
import os
import time
import unittest
import uuid
from functools import partial
from contextlib import suppress

from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.runtime import run_on
from tinyjobs.status import ExecutionStatus
from tinyjobs.testing import BackendContract
from tinyjobs.testing.contract import ERROR
from tests.support import fork_without_warning, race_open_and_close

REDIS_URL = os.environ.get("TINYJOBS_REDIS_URL")

try:
    from redis.exceptions import ConnectionError as RedisConnectionError

    from tinyjobs.backends.redis import RedisBackend
except ImportError:
    RedisBackend = None


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
class RedisContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.prefix = f"tinyjobs-test-{uuid.uuid4().hex}"

    async def make_backend(self, clock):
        return RedisBackend.from_url(REDIS_URL, prefix=self.prefix, clock=clock)

    async def asyncTearDown(self) -> None:
        client = self.backend._client
        keys = [key async for key in client.scan_iter(match=f"{{{self.prefix}}}:*", count=10_000)]
        if keys:
            await client.delete(*keys)
        await super().asyncTearDown()

    async def test_a_retried_job_takes_its_dedup_key_over_from_a_vanished_holder(self):
        failed = await self.claimed_one(dedup_key="invoice:1")
        await self.backend.fail(failed, ERROR, None)
        await self.backend._client.set(self.backend._keys("dedup", "invoice:1"), "ghost")
        self.assertTrue(await self.backend.requeue(failed.id))
        self.assertEqual((await self.enqueue(dedup_key="invoice:1")).id, failed.id)

    async def test_a_store_written_by_0_6_is_moved_past_what_0_6_accepts(self):
        version = self.backend._keys("schema_version")
        await self.backend._client.set(version, 1)
        await self.backend.setup()
        self.assertEqual(int(await self.backend._client.get(version)), 2)

    async def test_a_store_from_a_newer_release_is_refused_and_left_alone(self):
        from tinyjobs.backends.redis.backend import SCHEMA_VERSION

        version = self.backend._keys("schema_version")
        await self.backend._client.set(version, SCHEMA_VERSION + 1)
        with self.assertRaisesRegex(RuntimeError, "newer than this tinyjobs"):
            await self.backend.setup()
        self.assertEqual(
            int(await self.backend._client.get(version)), SCHEMA_VERSION + 1
        )

    async def test_a_job_stored_before_claims_existed_keeps_its_history(self):
        claimed = await self.claimed_one()
        await self.backend.fail(claimed, ERROR, None)
        await self.backend._client.hdel(self.backend._keys("job", claimed.id), "claims")
        self.assertTrue(await self.backend.requeue(claimed.id))
        again = (await self.claim())[0]
        self.assertTrue(await self.backend.complete(again, None))
        history = [(e.attempt, e.status) for e in await self.backend.executions(claimed.id)]
        self.assertEqual(
            history, [(1, ExecutionStatus.FAILED), (1, ExecutionStatus.SUCCESS)]
        )

    async def legacy(self, job_id: str, executions: dict[int, tuple[str, float]]) -> None:
        client = self.backend._client
        await client.hdel(self.backend._keys("job", job_id), "claims")
        hash = self.backend._keys("executions", job_id)
        await client.delete(hash)
        for attempt, (status, started_at) in executions.items():
            await client.hset(
                hash,
                attempt,
                json.dumps(
                    {"status": status, "worker_id": "old", "started_at": started_at}
                ),
            )

    async def test_a_job_retried_before_claims_existed_keeps_every_row_in_order(self):
        claimed = await self.claimed_one(max_attempts=2)
        await self.backend.fail(claimed, ERROR, None)
        self.assertTrue(await self.backend.requeue(claimed.id))
        await self.legacy(claimed.id, {1: ("failed", 30.0), 2: ("failed", 20.0)})
        again = (await self.claim())[0]
        self.assertEqual(again.claims, 3)
        self.assertTrue(await self.backend.complete(again, None))
        history = [
            (e.attempt, e.status, e.started_at)
            for e in await self.backend.executions(claimed.id)
        ]
        self.assertEqual(
            history,
            [
                (2, ExecutionStatus.FAILED, 20.0),
                (1, ExecutionStatus.FAILED, 30.0),
                (1, ExecutionStatus.SUCCESS, history[2][2]),
            ],
        )

    async def test_history_written_before_claims_existed_reads_in_order_at_once(self):
        claimed = await self.claimed_one(max_attempts=3)
        await self.backend.fail(claimed, ERROR, None)
        await self.legacy(claimed.id, {1: ("failed", 30.0), 2: ("failed", 20.0)})
        history = [e.started_at for e in await self.backend.executions(claimed.id)]
        self.assertEqual(history, [20.0, 30.0])
        await self.legacy(claimed.id, {1: ("failed", 30.0), 2: ("failed", 20.0)})
        stored = await self.backend.get(claimed.id)
        self.assertEqual(stored.claims, 2)
        await self.legacy(claimed.id, {1: ("failed", 30.0), 2: ("failed", 20.0)})
        listed = (await self.backend.list(limit=10)).items
        self.assertEqual([job.claims for job in listed if job.id == claimed.id], [2])

    async def test_history_that_started_together_keeps_its_field_order(self):
        claimed = await self.claimed_one(max_attempts=5)
        await self.backend.fail(claimed, ERROR, None)
        await self.legacy(
            claimed.id, {number: ("failed", 5.0) for number in (5, 4, 3, 2, 1)}
        )
        history = [e.attempt for e in await self.backend.executions(claimed.id)]
        self.assertEqual(history, [1, 2, 3, 4, 5])

    async def test_a_job_running_before_claims_existed_is_reaped_as_lost(self):
        claimed = await self.claimed_one(max_attempts=3)
        await self.legacy(
            claimed.id, {2: ("failed", 40.0), 1: ("running", 50.0)}
        )
        self.clock.advance(61)
        self.assertEqual(await self.backend.reap_expired(), 1)
        history = [(e.attempt, e.status) for e in await self.backend.executions(claimed.id)]
        self.assertEqual(
            history, [(2, ExecutionStatus.FAILED), (1, ExecutionStatus.LOST)]
        )
        self.assertEqual((await self.stored(claimed)).claims, 2)


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
@unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
class RedisForkTests(unittest.TestCase):
    def test_forking_an_idle_started_app_raises_no_thread_warning(self):
        fork_without_warning(self, REDIS_URL)


async def on_backend_loop(app: TinyJobs, coroutine):
    return await run_on(app.backend._loop, coroutine, "test")


async def forget(app: TinyJobs, prefix: str) -> None:
    await on_backend_loop(app, delete_prefix(app.backend._client, prefix))


async def delete_prefix(client, prefix: str) -> None:
    keys = [key async for key in client.scan_iter(match=f"{{{prefix}}}:*", count=10_000)]
    if keys:
        await client.delete(*keys)


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
class RedisAppTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        self.app = TinyJobs(REDIS_URL, prefix=self.prefix)
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await forget(self.app, self.prefix)
        await self.app.aclose()

    async def run_worker(self, jobs, **options) -> None:
        worker = self.app.worker(concurrency=4, poll_maximum=0.05, **options)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            statuses = [(await job.arefresh()).status for job in jobs]
            if all(status.is_terminal for status in statuses):
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def test_a_worker_runs_jobs_and_retries_against_redis(self):
        attempts = []

        @self.app.task(store_result=True)
        async def double(value):
            return value * 2

        @self.app.task(max_attempts=3, retry_base=0.01, retry_jitter=False)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("down")

        jobs = [await double.aenqueue(21), await flaky.aenqueue()]
        await self.run_worker(jobs)
        self.assertEqual(jobs[0].result(), 42)
        self.assertEqual((jobs[1].status, jobs[1].attempts), (JobStatus.SUCCESS, 3))
        self.assertEqual(len(await self.app.aexecutions(jobs[1].id)), 3)

    async def test_the_worker_fires_cron_against_redis(self):
        ran = []

        @self.app.task(cron="* * * * *")
        def tick():
            ran.append(1)

        await self.app.scheduling.register()
        state = (await self.app.aschedules())[0]
        await on_backend_loop(
            self.app,
            self.app.backend._client.hset(
                self.app.backend._keys("schedule", state.name),
                "next_run_at",
                repr(time.time() - 1),
            )
        )
        worker = self.app.worker(concurrency=2, poll_maximum=0.05)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 10
        while not ran and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(ran, [1])

    async def test_check_and_stats_work_against_redis(self):
        @self.app.task
        def noop():
            return None

        await noop.aenqueue()
        await self.app.asend("elsewhere.task")
        stats = await self.app.astats()
        self.assertEqual(stats.queues[0].counts, {"queued": 2})
        failures = [f.message for f in await self.app.acheck() if not f.ok]
        self.assertEqual(len(failures), 1)
        self.assertIn("elsewhere.task", failures[0])

    async def test_a_burst_larger_than_the_pool_waits_instead_of_failing(self):
        small = TinyJobs(REDIS_URL, prefix=self.prefix, max_connections=4)

        @small.task
        async def noop(value):
            return value

        try:
            jobs = await asyncio.gather(*(noop.aenqueue(n) for n in range(200)))
            self.assertEqual(len({job.id for job in jobs}), 200)
        finally:
            await small.aclose()

    async def test_a_client_option_still_reaches_the_client(self):
        single = TinyJobs(REDIS_URL, prefix=self.prefix, single_connection_client=True)
        try:
            await single.ready()
            self.assertEqual(await single.acounts(), {})
            self.assertTrue(single.backend._client.single_connection_client)
        finally:
            await single.aclose()

    async def test_an_exhausted_pool_gives_up_after_its_timeout(self):
        small = TinyJobs(
            REDIS_URL, prefix=self.prefix, max_connections=1, pool_timeout=0.2
        )

        @small.task
        async def noop():
            return None

        await small.ready()
        holding = asyncio.create_task(
            on_backend_loop(small, small.backend._client.blpop("tinyjobs-test-nothing", 2))
        )
        try:
            await asyncio.sleep(0.1)
            with self.assertRaises(RedisConnectionError):
                await noop.aenqueue()
        finally:
            await holding
            await small.aclose()

    async def test_two_prefixes_on_one_server_do_not_see_each_other(self):
        other_prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        other = TinyJobs(REDIS_URL, prefix=other_prefix)
        await other.ready()
        try:
            await self.app.asend("a.task")
            self.assertEqual(len((await other.ajobs()).items), 0)
        finally:
            await forget(other, other_prefix)
            await other.aclose()


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
class RedisEventLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        self.app = TinyJobs(REDIS_URL, prefix=self.prefix)

        @self.app.task
        def handler(value):
            return value

        self.handler = handler

    def tearDown(self) -> None:
        self.app.start()
        self.app.run_sync(partial(forget, self.app, self.prefix), name="forget")
        self.app.close()

    def test_one_app_serves_one_event_loop_after_another(self):
        asyncio.run(self.handler.aenqueue(1))
        asyncio.run(self.handler.aenqueue(2))
        self.handler.enqueue(3)
        self.assertEqual(self.app.counts()["queued"], 3)

    def test_a_loop_reaches_an_app_hosted_on_another_running_loop(self):
        import threading

        hosted = threading.Event()
        release = threading.Event()

        async def host() -> None:
            await self.app.ready()
            hosted.set()
            while not release.is_set():
                await asyncio.sleep(0.01)

        thread = threading.Thread(target=asyncio.run, args=(host(),))
        thread.start()
        try:
            hosted.wait(5)
            job = asyncio.run(self.handler.aenqueue(4))
            stored = asyncio.run(self.app.aget(job.id))
            self.assertEqual(self.app.decode(stored).args, (4,))
        finally:
            release.set()
            thread.join(5)

    def test_a_worker_on_a_second_loop_shares_an_app_hosted_elsewhere(self):
        import threading

        hosted = threading.Event()
        release = threading.Event()
        jobs = []

        async def host() -> None:
            await self.app.ready()
            hosted.set()
            while not release.is_set():
                await asyncio.sleep(0.01)
            jobs.append(await self.handler.aenqueue(8))

        async def work() -> None:
            job = await self.handler.aenqueue(7)
            worker = self.app.worker(handle_signals=False, scheduler=False)
            running = asyncio.create_task(worker.start())
            deadline = time.monotonic() + 10
            while (await job.arefresh()).status is not JobStatus.SUCCESS:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.02)
            worker.stop()
            await running

        thread = threading.Thread(target=asyncio.run, args=(host(),))
        thread.start()
        try:
            hosted.wait(5)
            asyncio.run(work())
        finally:
            release.set()
            thread.join(5)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(self.app.counts(), {"success": 1, "queued": 1})

    def test_an_app_dropped_without_closing_stops_its_loop_thread(self):
        import gc
        import threading

        def loop_threads() -> int:
            return sum(t.name == "tinyjobs-loop" for t in threading.enumerate())

        before = loop_threads()
        dropped = TinyJobs(REDIS_URL, prefix=self.prefix)
        handler = dropped.task(name="handler")(lambda value: value)
        asyncio.run(handler.aenqueue(9))
        self.assertEqual(loop_threads(), before + 1)
        del dropped, handler
        gc.collect()
        deadline = time.monotonic() + 5
        while loop_threads() > before and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(loop_threads(), before)

    def test_an_app_readied_on_a_stopped_loop_keeps_working(self):
        stopped = asyncio.new_event_loop()
        try:
            stopped.run_until_complete(self.app.ready())
            self.handler.enqueue(10)

            async def host() -> None:
                await self.app.ready()
                await self.handler.aenqueue(11)

            asyncio.run(host())
            self.assertEqual(self.app.counts()["queued"], 2)
        finally:
            stopped.close()

    def test_an_app_readied_on_a_finished_loop_keeps_working(self):
        async def host() -> None:
            await self.app.ready()
            await self.handler.aenqueue(5)

        asyncio.run(host())
        asyncio.run(self.handler.aenqueue(6))
        self.assertEqual(self.app.counts()["queued"], 2)

    def test_event_loops_and_threads_connecting_at_once_share_one_connection(self):
        import threading

        errors: list[BaseException] = []
        start = threading.Barrier(8)

        def from_a_loop() -> None:
            async def enqueue() -> None:
                start.wait()
                await self.handler.aenqueue(1)

            try:
                asyncio.run(enqueue())
            except BaseException as error:
                errors.append(error)

        def from_a_thread() -> None:
            start.wait()
            try:
                self.handler.enqueue(2)
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=from_a_loop) for _ in range(4)]
        threads += [threading.Thread(target=from_a_thread) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(self.app.counts()["queued"], 8)


    def slow_setup(self, seconds: float):
        from unittest import mock

        setup = RedisBackend.setup

        async def slowly(backend):
            await asyncio.sleep(seconds)
            await setup(backend)

        patcher = mock.patch.object(RedisBackend, "setup", slowly)
        patcher.start()
        self.addCleanup(patcher.stop)

    def finish(self, loop: asyncio.AbstractEventLoop) -> None:
        abandoned = asyncio.all_tasks(loop)
        for task in abandoned:
            task.cancel()
        if abandoned:
            loop.run_until_complete(
                asyncio.gather(*abandoned, return_exceptions=True)
            )
        loop.close()

    def test_a_worker_uses_the_connection_a_sync_call_made(self):
        ran: list[int] = []

        @self.app.task
        def record(value):
            ran.append(value)

        record.enqueue(1)

        async def work() -> None:
            running = asyncio.create_task(
                self.app.worker(handle_signals=False).start()
            )
            for _ in range(50):
                if ran:
                    break
                await asyncio.sleep(0.05)
            running.cancel()
            with suppress(asyncio.CancelledError):
                await running

        asyncio.run(work())
        self.assertEqual(ran, [1])

    def test_cancelling_the_first_connection_closes_the_half_made_backend(self):
        closed = self.track_closes()
        self.slow_setup(0.5)

        with self.assertRaises(TimeoutError):
            asyncio.run(asyncio.wait_for(self.handler.aenqueue(1), 0.2))
        deadline = time.monotonic() + 5
        while not closed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(closed), 1)
        self.assertIsNone(self.app._backend)
        self.handler.enqueue(2)
        self.assertEqual(self.app.counts()["queued"], 1)

    def test_a_host_loop_that_ends_leaves_the_connection_in_place(self):
        async def host() -> None:
            await self.app.ready()
            await self.handler.aenqueue(1)

        asyncio.run(host())
        connected = self.app.backend
        self.handler.enqueue(2)
        asyncio.run(self.handler.aenqueue(3))
        self.assertIs(self.app.backend, connected)
        self.assertEqual(self.app.counts()["queued"], 3)

    def test_a_sync_drain_survives_another_loop_connecting(self):
        import threading

        ran: list[int] = []

        @self.app.task
        async def step(value):
            await asyncio.sleep(0.2)
            ran.append(value)

        for value in range(3):
            step.enqueue(value)
        errors: list[BaseException] = []

        def drain() -> None:
            try:
                self.app.drain()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=drain)
        thread.start()
        time.sleep(0.1)

        async def host() -> None:
            await self.app.ready()
            await self.handler.aenqueue(9)

        asyncio.run(host())
        thread.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(sorted(ran), [0, 1, 2])

    def test_a_caller_takes_over_a_connection_whose_loop_stopped(self):
        import threading

        self.slow_setup(0.5)
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        asyncio.run_coroutine_threadsafe(self.handler.aenqueue(1), loop)
        time.sleep(0.2)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        self.addCleanup(self.finish, loop)

        asyncio.run(asyncio.wait_for(self.handler.aenqueue(2), 5))
        self.assertGreaterEqual(self.app.counts()["queued"], 1)

    def track_closes(self) -> list:
        from unittest import mock

        closed: list = []
        close = RedisBackend.close

        async def record(backend):
            closed.append(backend)
            await close(backend)

        patcher = mock.patch.object(RedisBackend, "close", record)
        patcher.start()
        self.addCleanup(patcher.stop)
        return closed

    def test_an_owner_whose_loop_stops_mid_connection_still_connects_once(self):
        import threading

        closed = self.track_closes()
        self.slow_setup(0.5)
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever)
        thread.start()
        asyncio.run_coroutine_threadsafe(self.app.ready(), loop)
        time.sleep(0.2)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        asyncio.run(self.handler.aenqueue(1))
        winner = self.app.backend

        resumed = threading.Thread(target=loop.run_forever)
        resumed.start()
        time.sleep(1)
        loop.call_soon_threadsafe(loop.stop)
        resumed.join(5)
        self.addCleanup(self.finish, loop)

        self.assertIs(self.app.backend, winner)
        self.assertEqual(closed, [])

    def test_an_explicit_host_reuses_the_connection_a_sync_call_made(self):
        closed = self.track_closes()
        self.handler.enqueue(1)
        implicit = self.app.backend

        async def host() -> None:
            await self.app.ready()
            await self.handler.aenqueue(2)

        asyncio.run(host())
        self.assertIs(self.app.backend, implicit)
        self.assertEqual(closed, [])

    def test_a_dropped_app_closes_its_connection(self):
        import gc

        closed = self.track_closes()
        dropped = TinyJobs(REDIS_URL, prefix=self.prefix)
        handler = dropped.task(name="handler")(lambda value: value)
        handler.enqueue(1)
        del dropped, handler
        gc.collect()
        deadline = time.monotonic() + 5
        while not closed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(closed), 1)

    def test_sync_and_async_callers_racing_start_and_close_leave_nothing_behind(self):
        race_open_and_close(self, TinyJobs(REDIS_URL, prefix=self.prefix))

    def test_enqueues_racing_open_and_close_cycles_see_only_closed_errors(self):
        import threading

        from tinyjobs.exceptions import AppClosedError

        unexpected: list[BaseException] = []
        stop = threading.Event()

        def hammer() -> None:
            while not stop.is_set():
                try:
                    self.handler.enqueue(1)
                except AppClosedError:
                    time.sleep(0.001)
                except BaseException as error:
                    unexpected.append(error)

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for thread in threads:
            thread.start()

        async def cycle() -> None:
            for _ in range(15):
                await self.app.ready()
                await asyncio.sleep(0.02)
                await self.app.aclose()

        try:
            for _ in range(2):
                asyncio.run(cycle())
        finally:
            stop.set()
            for thread in threads:
                thread.join(10)
        self.assertEqual(unexpected, [])

    def test_closing_during_the_first_connection_does_not_hang_a_caller(self):
        import threading

        from tinyjobs.exceptions import AppClosedError

        self.slow_setup(0.5)
        outcome: list[BaseException] = []

        def enqueue() -> None:
            try:
                self.handler.enqueue(1)
            except BaseException as error:
                outcome.append(error)

        thread = threading.Thread(target=enqueue)
        thread.start()
        time.sleep(0.2)
        self.app.close()
        thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome[0], AppClosedError)

    def test_aclose_stops_the_loop_an_implicit_connection_started(self):
        asyncio.run(self.handler.aenqueue(1))
        own = self.app._runner._thread
        asyncio.run(self.app.aclose())
        own.join(5)
        self.assertFalse(own.is_alive())


class RedisInstallTests(unittest.TestCase):
    def test_redis_urls_are_known_without_importing_redis(self):
        from tinyjobs.backends import available_schemes

        self.assertIn("redis", available_schemes())
        self.assertIn("rediss", available_schemes())

    def test_a_missing_client_library_says_how_to_install_it(self):
        import builtins
        import sys

        real_import = builtins.__import__

        def without_redis(name, globals=None, locals=None, fromlist=(), level=0):
            if level == 0 and name.split(".")[0] == "redis":
                raise ImportError("No module named 'redis'")
            return real_import(name, globals, locals, fromlist, level)

        saved = {name: module for name, module in sys.modules.items() if "redis" in name}
        for name in saved:
            del sys.modules[name]
        builtins.__import__ = without_redis
        try:
            with self.assertRaises(ConfigurationError) as caught:
                TinyJobs("redis://localhost:1/0").start()
        finally:
            builtins.__import__ = real_import
            sys.modules.update(saved)
        self.assertIn("pip install 'tinyjobs[redis]'", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
