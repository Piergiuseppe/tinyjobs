from __future__ import annotations

import asyncio
import os
import time
import unittest
import uuid

from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.testing import BackendContract

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


async def forget(app: TinyJobs, prefix: str) -> None:
    client = app.backend._client
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
        await self.app.backend._client.hset(
            self.app.backend._keys("schedule", state.name),
            "next_run_at",
            repr(time.time() - 1),
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
            small.backend._client.blpop("tinyjobs-test-nothing", 2)
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
        try:
            await self.app.asend("a.task")
            self.assertEqual(len((await other.ajobs()).items), 0)
        finally:
            await forget(other, other_prefix) if other._backend else None
            await other.aclose()


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
