from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import types
import unittest
import uuid
from functools import partial
from contextlib import contextmanager, suppress
from unittest import mock

import tinyjobs.clock
from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.runtime import run_on
from tinyjobs.status import ExecutionStatus
from tinyjobs.testing import BackendContract
from tinyjobs.testing.contract import ERROR, START, make_job
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

    async def test_a_job_retried_before_any_claim_is_due_from_when_it_was_sent(self):
        job = await self.enqueue(run_at=START + 3600, created_at=START)
        self.assertTrue(await self.backend.cancel(job.id))
        self.clock.advance(10)
        self.assertTrue(await self.backend.requeue(job.id))
        self.assertEqual((await self.stored(job)).run_at, START)
        self.assertEqual(len(await self.claim()), 1)

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


SKEWED_HOST = """
import os, sys, time, types
import tinyjobs.clock
real = time.time
tinyjobs.clock.time = types.SimpleNamespace(
    time=lambda: real() + float(sys.argv[3]), monotonic=time.monotonic
)
import redis
from tinyjobs import TinyJobs
url, prefix = sys.argv[1], sys.argv[2]
app = TinyJobs(url, prefix=prefix)
marks = redis.Redis.from_url(url)

@app.task(name="mark")
def mark(value):
    time.sleep(0.2)
    marks.rpush("{" + prefix + "}:ran", f"{value}:skewed")

print("ready", flush=True)
app.worker(concurrency=4, poll_maximum=0.05, lease_seconds=5.0, reap_interval=0.2).run()
app.close()
"""


@contextmanager
def skewed(seconds: float):
    import tinyjobs.clock

    real = time.time
    fake = types.SimpleNamespace(time=lambda: real() + seconds, monotonic=time.monotonic)
    with mock.patch.object(tinyjobs.clock, "time", fake):
        yield


@contextmanager
def later_by(seconds: float):
    import tinyjobs.clock
    from tinyjobs.backends.redis import backend as module

    real = tinyjobs.clock.elapsed

    def shifted() -> float:
        return real() + seconds

    with (
        mock.patch.object(tinyjobs.clock, "elapsed", shifted),
        mock.patch.object(module, "elapsed", shifted),
        skewed(seconds),
    ):
        yield


class Untouched:
    def now(self) -> float:
        return time.time() + 900.0

    def monotonic(self) -> float:
        return time.monotonic()


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
class RedisClockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        self.opened: list = []

    async def asyncTearDown(self) -> None:
        if not self.opened:
            await self.open()
        for backend in self.opened:
            with suppress(Exception):
                await delete_prefix(backend._client, self.prefix)
        for backend in self.opened:
            await backend.close()

    async def open(self, clock=None, url: str = REDIS_URL):
        backend = RedisBackend.from_url(url, prefix=self.prefix, clock=clock)
        self.opened.append(backend)
        await backend.setup()
        return backend

    async def server_time(self, backend) -> float:
        seconds, microseconds = await backend._client.time()
        return seconds + microseconds / 1_000_000

    async def counted_reads(self, backend) -> list:
        reads: list = []
        original = backend._time

        async def counted(connection):
            reads.append(1)
            return await original(connection)

        backend._time = counted
        return reads

    async def test_a_host_whose_clock_runs_an_hour_ahead_leaves_live_leases_alone(self):
        here = await self.open()
        job = await here.enqueue(make_job(run_at=time.time(), created_at=time.time()))
        [claimed] = await here.claim(
            worker_id="here", queues=["default"], limit=1, lease_seconds=30.0
        )
        self.assertEqual(claimed.id, job.id)
        with skewed(3600.0):
            ahead = await self.open()
            self.assertEqual(await ahead.reap_expired(), 0)
        self.assertEqual((await here.get(job.id)).status, JobStatus.RUNNING)

    async def test_a_host_whose_clock_runs_behind_still_claims_jobs_that_are_due(self):
        with skewed(600.0):
            ahead = await self.open()
            job = await ahead.enqueue(make_job(run_at=ahead._clock.now() + 1.0))
        await asyncio.sleep(1.5)
        with skewed(-600.0):
            behind = await self.open()
            claimed = await behind.claim(
                worker_id="behind", queues=["default"], limit=1, lease_seconds=30.0
            )
        self.assertEqual([job.id for job in claimed], [job.id])

    async def test_every_host_stamps_times_from_the_redis_clock(self):
        for skew in (-7200.0, 0.0, 7200.0):
            with skewed(skew):
                backend = await self.open()
                await backend.enqueue(make_job(run_at=0.0))
                [claimed] = await backend.claim(
                    worker_id=f"w{skew}", queues=["default"], limit=1, lease_seconds=30.0
                )
                now = backend._clock.now()
            server = await self.server_time(backend)
            self.assertAlmostEqual(claimed.lease_expires_at, server + 30.0, delta=2.0)
            self.assertAlmostEqual(now, server, delta=2.0)

    async def test_a_step_of_the_local_clock_moves_nothing_even_before_the_next_call(self):
        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="noted")
        def noted() -> None:
            return None

        await app.ready()
        self.opened.append(await self.open())
        for step in (3600.0, -3600.0):
            with skewed(step):
                job = await noted.with_options(delay=60).aenqueue()
            server = await self.server_time(self.opened[0])
            self.assertAlmostEqual(job.run_at, server + 60.0, delta=2.0)
            self.assertAlmostEqual(job.created_at, server, delta=2.0)

    async def test_a_step_while_the_clock_is_read_leaves_no_error_behind(self):
        backend = await self.open()
        original = backend._time
        stepped = []

        async def stepping(connection):
            reply = await original(connection)
            stepped.append(skewed(3600.0))
            stepped[-1].__enter__()
            return reply

        backend._time = stepping
        backend._clock.forget()
        try:
            await backend.reap_expired()
            now = backend._clock.now()
        finally:
            for step in stepped:
                step.__exit__(None, None, None)
        backend._time = original
        self.assertEqual(len(stepped), 1)
        self.assertAlmostEqual(now, await self.server_time(backend), delta=1.0)

    async def test_a_slow_reading_does_not_replace_a_good_one(self):
        backend = await self.open()
        original = backend._time

        async def slow(connection):
            reply = await original(connection)
            await asyncio.sleep(1.0)
            return reply

        backend._time = slow
        backend._clock.forget()
        await backend.reap_expired()
        backend._time = original
        self.assertAlmostEqual(
            backend._clock.now(), await self.server_time(backend), delta=0.3
        )

    async def test_a_slow_reading_is_not_retried_on_every_call(self):
        backend = await self.open()
        original = backend._time

        async def slow(connection):
            reply = await original(connection)
            await asyncio.sleep(0.6)
            return reply

        backend._time = slow
        backend._clock.forget()
        await backend.reap_expired()
        reads = await self.counted_reads(backend)
        await backend.reap_expired()
        await backend.reap_expired()
        self.assertEqual(reads, [])

    async def test_the_clock_is_read_again_once_it_is_old(self):
        from tinyjobs.clock import FOLLOW_SECONDS

        backend = await self.open()
        reads = await self.counted_reads(backend)
        await backend.reap_expired()
        self.assertEqual(reads, [])
        with later_by(FOLLOW_SECONDS + 1):
            await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def test_a_lost_connection_reads_the_clock_again(self):
        backend = await self.open()
        reads = await self.counted_reads(backend)
        original = backend._scripts.run

        async def broken(*args, **kwargs):
            raise RedisConnectionError("gone")

        backend._scripts.run = broken
        with self.assertRaises(RedisConnectionError):
            await backend.reap_expired()
        backend._scripts.run = original
        reads.clear()
        await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def test_a_client_on_one_connection_reads_the_clock_on_it(self):
        backend = RedisBackend.from_url(
            REDIS_URL,
            prefix=self.prefix,
            max_connections=1,
            pool_timeout=2,
            single_connection_client=True,
        )
        self.opened.append(backend)
        await backend.setup()
        backend._clock.forget()
        self.assertEqual(await backend.reap_expired(), 0)

    async def test_a_reading_waiting_on_a_client_closed_meanwhile_fails_as_a_lost_connection(self):
        backend = RedisBackend.from_url(
            REDIS_URL, prefix=self.prefix, single_connection_client=True
        )
        self.opened.append(backend)
        await backend.setup()
        backend._clock.forget()
        client = backend._client
        lock = client._single_conn_lock
        real = client.initialize

        async def close_soon() -> None:
            await asyncio.sleep(0.05)
            await client.aclose()
            lock.release()

        async def initialize():
            found = await real()
            await lock.acquire()
            closing.append(asyncio.create_task(close_soon()))
            return found

        closing: list[asyncio.Task] = []
        with mock.patch.object(client, "initialize", initialize):
            with self.assertRaises(RedisConnectionError):
                await backend.reap_expired()
        await asyncio.gather(*closing)

    async def test_opening_a_connection_is_not_counted_in_a_reading(self):
        from redis.asyncio.connection import Connection

        backend = await self.open()
        await backend._client.connection_pool.disconnect()
        backend._clock.forget()
        real = Connection._connect

        async def slowly(connection, *args, **kwargs):
            await asyncio.sleep(0.3)
            return await real(connection, *args, **kwargs)

        with mock.patch.object(Connection, "_connect", slowly):
            await backend.reap_expired()
        self.assertAlmostEqual(
            backend._clock.now(), await self.server_time(backend), delta=0.1
        )

    async def test_a_health_check_is_not_counted_in_a_reading(self):
        from redis.asyncio.connection import AbstractConnection

        backend = await self.open()
        backend._clock.forget()
        real = AbstractConnection.check_health

        async def slowly(connection):
            await asyncio.sleep(0.3)
            return await real(connection)

        with mock.patch.object(AbstractConnection, "check_health", slowly):
            await backend.reap_expired()
        self.assertAlmostEqual(
            backend._clock.now(), await self.server_time(backend), delta=0.1
        )

    async def test_one_clock_cannot_follow_two_servers_whose_times_differ(self):
        from tinyjobs.clock import FollowingClock

        clock = FollowingClock()
        await self.open(clock)
        second = REDIS_URL.rsplit("/", 1)[0] + "/1"
        other = RedisBackend.from_url(second, prefix=self.prefix, clock=clock)
        self.opened.append(other)
        original = other._time

        async def elsewhere(connection):
            seconds, microseconds = await original(connection)
            return seconds + 120, microseconds

        other._time = elsewhere
        with self.assertRaises(ConfigurationError):
            await other.setup()

    async def test_one_clock_follows_one_server_however_its_url_is_spelled(self):
        from tinyjobs.clock import FollowingClock

        clock = FollowingClock()
        await self.open(clock)
        spelled = REDIS_URL.replace("127.0.0.1", "localhost").rsplit("/", 1)[0] + "/1"
        other = await self.open(clock, url=spelled)
        reads = await self.counted_reads(other)
        clock.forget()
        for _ in range(3):
            await other.reap_expired()
        self.assertEqual(reads, [1])

    async def test_readings_due_at_once_share_one_round_trip(self):
        backend = await self.open()
        reads = await self.counted_reads(backend)
        backend._clock.forget()
        await asyncio.gather(*(backend.reap_expired() for _ in range(50)))
        self.assertEqual(reads, [1])

    async def test_a_timeout_reads_the_clock_again(self):
        from redis.exceptions import TimeoutError as RedisTimeoutError

        backend = await self.open()
        reads = await self.counted_reads(backend)
        original = backend._scripts.run

        async def stalled(*args, **kwargs):
            raise RedisTimeoutError("stalled")

        backend._scripts.run = stalled
        with self.assertRaises(RedisTimeoutError):
            await backend.reap_expired()
        backend._scripts.run = original
        reads.clear()
        await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def test_cron_fired_by_a_host_behind_is_due_at_its_slot(self):
        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(cron="* * * * *")
        def tick() -> None:
            return None

        backend = await self.open()
        with skewed(-900.0):
            await app.ready()
            await app.scheduling.register()
            [state] = await app.aschedules()
            slot = await self.server_time(backend) - 1
            await on_backend_loop(
                app,
                app.backend._client.hset(
                    app.backend._keys("schedule", state.name), "next_run_at", repr(slot)
                ),
            )
            await app.scheduling.fire_due(60.0)
        [job] = (await app.ajobs()).items
        self.assertAlmostEqual(job.run_at, slot, delta=0.01)
        claimed = await backend.claim(
            worker_id="due", queues=["default"], limit=1, lease_seconds=30.0
        )
        self.assertEqual([found.id for found in claimed], [job.id])

    async def test_jobs_list_newest_first_whichever_host_sent_them(self):
        apps = [TinyJobs(REDIS_URL, prefix=self.prefix) for _ in range(2)]
        for app in apps:
            self.addAsyncCleanup(app.aclose)

            @app.task(name="listed")
            def listed() -> None:
                return None

        await apps[0].ready()
        first = await apps[0].asend("listed")
        await asyncio.sleep(0.1)
        with skewed(-900.0):
            await apps[1].ready()
            second = await apps[1].asend("listed")
        listing = [job.id for job in (await apps[0].ajobs()).items]
        self.assertEqual(listing, [second.id, first.id])

    async def test_a_clock_given_by_the_caller_is_used_as_it_is(self):
        backend = await self.open(Untouched())
        await backend.enqueue(make_job(run_at=0.0))
        [claimed] = await backend.claim(
            worker_id="given", queues=["default"], limit=1, lease_seconds=30.0
        )
        self.assertAlmostEqual(claimed.lease_expires_at, time.time() + 930.0, delta=2.0)

    async def wrong_by(self, backend, seconds: float) -> None:
        from tinyjobs.clock import elapsed

        server = await self.server_time(backend)
        started = elapsed()
        backend._clock.follow(server + seconds, started, started, backend._server())

    async def test_a_host_still_on_an_old_time_ahead_cannot_reap_a_live_lease(self):
        here = await self.open()
        job = await here.enqueue(make_job(run_at=0.0))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=60.0)
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        self.assertGreater(stale._clock.now() - await self.server_time(stale), 100)
        self.assertEqual(await stale.reap_expired(), 0)
        self.assertEqual((await here.get(job.id)).status, JobStatus.RUNNING)

    async def test_a_host_still_on_an_old_time_ahead_runs_no_job_before_it_is_due(self):
        here = await self.open()
        server = await self.server_time(here)
        delayed = await here.enqueue(make_job(run_at=server + 60.0))
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        entered = await stale.enqueue(make_job(run_at=server + 60.0))
        claimed = await stale.claim(
            worker_id="stale", queues=["default"], limit=5, lease_seconds=30.0
        )
        self.assertEqual(claimed, [])
        for job in (delayed, entered):
            self.assertEqual((await here.get(job.id)).status, JobStatus.QUEUED)

    async def test_a_host_still_on_an_old_time_ahead_stamps_reaped_jobs_with_redis_time(self):
        here = await self.open()
        retried = await here.enqueue(make_job(run_at=0.0, max_attempts=2))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=0.2)
        failed = await here.enqueue(make_job(run_at=0.0, max_attempts=1))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=0.2)
        await asyncio.sleep(0.5)
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        self.assertEqual(await stale.reap_expired(), 2)
        server = await self.server_time(here)
        self.assertAlmostEqual((await here.get(retried.id)).run_at, server, delta=2.0)
        self.assertAlmostEqual((await here.get(failed.id)).finished_at, server, delta=2.0)

    async def test_a_host_still_on_an_old_time_ahead_keeps_results_as_long_as_asked(self):
        from tinyjobs.retention import RetentionPolicy

        here = await self.open()
        job = await here.enqueue(make_job(run_at=0.0))
        [claimed] = await here.claim(
            worker_id="here", queues=["default"], limit=1, lease_seconds=30.0
        )
        await here.complete(claimed, None)
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        policy = RetentionPolicy(success=60.0, cancelled=0, failed=0, interval=1.0)
        self.assertEqual(await stale.purge(policy, stale._clock.now()), 0)
        self.assertIsNotNone(await here.get(job.id))

    async def test_a_host_still_on_an_old_time_behind_stamps_leases_from_redis_time(self):
        behind = await self.open()
        await self.wrong_by(behind, -120.0)
        job = await behind.enqueue(make_job(run_at=0.0))
        [claimed] = await behind.claim(
            worker_id="behind", queues=["default"], limit=1, lease_seconds=60.0
        )
        server = await self.server_time(behind)
        self.assertGreater(claimed.lease_expires_at, server + 55.0)
        await behind.heartbeat([job.id], 60.0, worker_id="behind")
        renewed = await behind.get(job.id)
        self.assertGreater(renewed.lease_expires_at, server + 55.0)
        here = await self.open()
        self.assertEqual(await here.reap_expired(), 0)

    async def test_a_clock_that_must_be_read_again_is_read_before_a_job_is_built(self):
        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="built")
        def built() -> None:
            return None

        await app.ready()
        from tinyjobs.clock import elapsed

        started = elapsed()
        server = await self.server_time(await self.open())
        app.clock.follow(server - 30.0, started, started, app.backend._server())
        with skewed(30.0):
            job = await built.aenqueue()
        self.assertAlmostEqual(job.created_at, await self.server_time(self.opened[0]), delta=2.0)

    async def test_two_sentinel_services_are_two_servers_to_one_clock(self):
        from tinyjobs.clock import FollowingClock

        clock = FollowingClock()
        first = RedisBackend.from_url(REDIS_URL, prefix=self.prefix, clock=clock)
        second = RedisBackend.from_url(REDIS_URL, prefix=self.prefix, clock=clock)
        self.opened.extend([first, second])
        first._client.connection_pool.service_name = "one"
        second._client.connection_pool.service_name = "two"
        await first.setup()
        original = second._time

        async def elsewhere(connection):
            seconds, microseconds = await original(connection)
            return seconds + 120, microseconds

        second._time = elsewhere
        with self.assertRaises(ConfigurationError):
            await second.setup()

    async def test_a_frozen_clock_still_decides_what_now_is(self):
        from tinyjobs import FrozenClock

        clock = FrozenClock(1_000_000.0)
        backend = await self.open(clock)
        await backend.enqueue(make_job(run_at=999_999.0))
        [claimed] = await backend.claim(
            worker_id="frozen", queues=["default"], limit=1, lease_seconds=30.0
        )
        self.assertEqual(claimed.lease_expires_at, 1_000_030.0)
        self.assertEqual(clock.now(), 1_000_000.0)

    async def test_times_given_by_the_caller_are_read_on_the_local_clock(self):
        from datetime import datetime, timezone

        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="later")
        def later() -> None:
            return None

        with skewed(900.0):
            await app.ready()
            local = tinyjobs.clock.time.time()
            by_number = await later.with_options(run_at=local + 60).aenqueue()
            moment = datetime.fromtimestamp(local + 120, tz=timezone.utc)
            by_datetime = await later.with_options(run_at=moment).aenqueue()
        server = await self.server_time(await self.open())
        self.assertAlmostEqual(by_number.run_at, server + 60.0, delta=2.0)
        self.assertAlmostEqual(by_datetime.run_at, server + 120.0, delta=2.0)

    async def test_a_retry_at_a_local_time_waits_as_long_as_asked(self):
        from tinyjobs import Retry

        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="again", max_attempts=3)
        def again() -> None:
            raise Retry(at=tinyjobs.clock.time.time() + 30)

        with skewed(900.0):
            await app.ready()
            job = await again.aenqueue()
            worker = app.worker(concurrency=1, poll_maximum=0.05)
            runner = asyncio.create_task(worker.start())
            deadline = time.monotonic() + 10
            while (await app.aget(job.id)).attempts == 0:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
        stored = await app.aget(job.id)
        server = await self.server_time(await self.open())
        self.assertEqual(stored.status, JobStatus.QUEUED)
        self.assertAlmostEqual(stored.run_at, server + 30.0, delta=3.0)

    async def test_a_host_fifteen_minutes_ahead_runs_every_job_once_beside_this_one(self):
        backend = await self.open()
        app = TinyJobs(REDIS_URL, prefix=self.prefix)
        marks = f"{{{self.prefix}}}:ran"

        @app.task(name="mark")
        async def mark(value: int) -> None:
            await asyncio.sleep(1.0)
            await backend._client.rpush(marks, f"{value}:here")

        await app.ready()
        jobs = [await mark.aenqueue(value) for value in range(24)]
        worker = app.worker(
            concurrency=8, poll_maximum=0.05, lease_seconds=5.0, reap_interval=0.2
        )
        runner = asyncio.create_task(worker.start())
        host = None
        try:
            deadline = time.monotonic() + 10
            while (await app.acounts()).get("running", 0) < 8:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.02)
            host = await asyncio.create_subprocess_exec(
                sys.executable, "-c", SKEWED_HOST, REDIS_URL, self.prefix, "900",
                stdout=asyncio.subprocess.PIPE,
            )
            self.assertEqual(await asyncio.wait_for(host.stdout.readline(), 20), b"ready\n")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                await asyncio.sleep(0.1)
                if (await app.acounts()).get("success", 0) >= 24:
                    break
            await asyncio.sleep(2.0)
            self.assertIsNone(host.returncode)
            marked = [value.decode() for value in await backend._client.lrange(marks, 0, -1)]
            ran = sorted(int(value.split(":")[0]) for value in marked)
            self.assertEqual(ran, list(range(24)))
            self.assertIn("skewed", {value.split(":")[1] for value in marked})
            stored = [await app.aget(job.id) for job in jobs]
            self.assertEqual([job.status for job in stored], [JobStatus.SUCCESS] * 24)
            self.assertEqual([job.attempts for job in stored], [1] * 24)
        finally:
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
            if host is not None:
                if host.returncode is None:
                    host.send_signal(signal.SIGTERM)
                await asyncio.wait_for(host.wait(), 20)
            await forget(app, self.prefix)
            await app.aclose()


@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
@unittest.skipIf(RedisBackend is None, "pip install 'tinyjobs[redis]' to run the redis tests")
@unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
class RedisForkTests(unittest.TestCase):
    def test_forking_an_idle_started_app_raises_no_thread_warning(self):
        import redis

        prefix = f"tinyjobs-test-{uuid.uuid4().hex}"
        client = redis.Redis.from_url(REDIS_URL)
        self.addCleanup(client.close)
        self.addCleanup(
            lambda: [client.delete(key) for key in client.scan_iter(match=f"{{{prefix}}}:*")]
        )
        fork_without_warning(self, REDIS_URL, f"prefix={prefix}")


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
