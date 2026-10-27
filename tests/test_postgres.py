from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
import unittest
import uuid
from contextlib import suppress
from unittest import mock

import tinyjobs.clock
from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import ConfigurationError, UnsupportedCapabilityError
from tinyjobs.runtime import run_on
from tinyjobs.testing import BackendContract
from tinyjobs.testing.contract import ERROR, make_job
from tests.support import (
    LoopBoundAppCases,
    Untouched,
    fork_without_warning,
    later_by,
    race_open_and_close,
    skewed,
)

POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")

try:
    import psycopg
    from psycopg import errors
    from psycopg_pool import PoolClosed, PoolTimeout
    import psycopg.rows
    import psycopg.sql

    from tinyjobs.backends.postgres import PostgresBackend
except ImportError:
    PostgresBackend = None


def new_prefix() -> str:
    return f"tinyjobs_test_{uuid.uuid4().hex[:16]}"


async def sql(query: str, parameters: dict | None = None) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(POSTGRES_URL, autocommit=True) as connection:
        cursor = await connection.execute(query, parameters)
        return await cursor.fetchall() if cursor.description else []


async def drop(prefix: str, schema: str | None = None) -> None:
    qualify = f'"{schema}".' if schema else ""
    for table in ("executions", "jobs", "schedules", "meta", "ran"):
        await sql(f'DROP TABLE IF EXISTS {qualify}"{prefix}_{table}" CASCADE')
    if schema:
        await sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def server_time() -> float:
    [(now,)] = await sql("SELECT extract(epoch FROM clock_timestamp())::double precision")
    return now


async def on_backend_loop(app: TinyJobs, coroutine):
    return await run_on(app.backend._loop, coroutine, "test")


async def _rows_of(connection, query: str, parameters: dict) -> list[tuple]:
    cursor = connection.cursor(row_factory=psycopg.rows.tuple_row)
    await cursor.execute(query, parameters)
    return await cursor.fetchall()


def requires_postgres(case):
    case = unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")(case)
    return unittest.skipIf(
        PostgresBackend is None, "pip install 'tinyjobs[postgres]' to run the postgres tests"
    )(case)


@requires_postgres
class PostgresContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.prefix = new_prefix()

    async def make_backend(self, clock):
        return PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, clock=clock)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await drop(self.prefix)

    async def test_a_store_from_a_newer_release_is_refused_and_left_alone(self):
        from tinyjobs.backends.postgres.schema import SCHEMA_VERSION

        newer = str(SCHEMA_VERSION + 1)
        await sql(
            f"UPDATE \"{self.prefix}_meta\" SET value = %(v)s WHERE key = 'schema_version'",
            {"v": newer},
        )
        with self.assertRaisesRegex(RuntimeError, "newer than this tinyjobs"):
            await self.backend.setup()
        stored = await sql(
            f"SELECT value FROM \"{self.prefix}_meta\" WHERE key = 'schema_version'"
        )
        self.assertEqual(stored, [(newer,)])

    async def test_backends_setting_up_one_store_at_once_all_succeed(self):
        await drop(self.prefix)
        racers = [await self.make_backend(self.clock) for _ in range(8)]
        try:
            await asyncio.gather(*(backend.setup() for backend in racers))
            job = await racers[3].enqueue(make_job())
            self.assertEqual((await racers[5].get(job.id)).id, job.id)
        finally:
            for backend in racers:
                await backend.close()

    async def test_setting_up_again_keeps_every_job(self):
        job = await self.enqueue()
        await self.backend.setup()
        self.assertEqual((await self.stored(job)).status, JobStatus.QUEUED)

    async def test_enqueueing_the_same_id_again_returns_the_stored_job(self):
        job = await self.enqueue(priority=3)
        again = await self.backend.enqueue(make_job(id=job.id, priority=9))
        self.assertEqual((again.id, again.priority), (job.id, 3))
        self.assertEqual((await self.backend.list(limit=10)).items, [await self.stored(job)])

    async def test_a_large_batch_keeps_its_order_and_its_dedup_keys(self):
        jobs = [make_job(created_at=float(n)) for n in range(2500)]
        for position in (10, 1500, 2400):
            jobs[position].dedup_key = "shared"
        stored = await self.backend.enqueue_many(jobs)
        self.assertEqual(len(stored), 2500)
        self.assertEqual([job.id for job in stored[:10]], [job.id for job in jobs[:10]])
        self.assertEqual({stored[n].id for n in (10, 1500, 2400)}, {jobs[10].id})
        self.assertEqual((await self.backend.counts_by_status())["queued"], 2498)

    async def test_a_large_batch_inside_a_caller_transaction_lives_and_dies_with_it(self):
        jobs = [make_job(created_at=float(n)) for n in range(2500)]
        jobs[2400].dedup_key = jobs[10].dedup_key = "shared"
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            async with connection.transaction(force_rollback=True):
                stored = await self.backend.aenqueue_in(connection, jobs)
                self.assertEqual(stored[2400].id, jobs[10].id)
                self.assertEqual(await self.backend.counts_by_status(), {})
            self.assertEqual(await self.backend.counts_by_status(), {})
            async with connection.transaction():
                await self.backend.aenqueue_in(connection.cursor(), jobs)
        self.assertEqual((await self.backend.counts_by_status())["queued"], 2499)

    async def test_a_large_batch_that_fails_halfway_stores_nothing(self):
        jobs = [make_job() for _ in range(1500)]
        jobs[1200].priority = 2**40
        with self.assertRaises(psycopg.DataError):
            await self.backend.enqueue_many(jobs)
        self.assertEqual(await self.backend.counts_by_status(), {})

    async def test_a_holder_that_finishes_before_it_is_read_lets_the_new_job_in(self):
        holder = await self.claimed_one(dedup_key="invoice:1")
        real = self.backend._holders

        def finishing_first(missing, resolved):
            yield (
                f"UPDATE \"{self.prefix}_jobs\" SET status = 'success' WHERE id = %(id)s RETURNING id",
                {"id": holder.id},
            )
            return (yield from real(missing, resolved))

        with mock.patch.object(self.backend, "_holders", finishing_first):
            entered = await self.enqueue(dedup_key="invoice:1")
        self.assertNotEqual(entered.id, holder.id)
        self.assertEqual((await self.stored(entered)).status, JobStatus.QUEUED)

    async def test_holders_that_keep_vanishing_end_in_an_error(self):
        await self.enqueue(dedup_key="invoice:1")

        def vanished(missing, resolved):
            return missing
            yield

        with mock.patch.object(self.backend, "_holders", vanished):
            with self.assertRaisesRegex(RuntimeError, "dedup holders"):
                await self.enqueue(dedup_key="invoice:1")

    async def test_a_deadlock_is_retried(self):
        job = await self.enqueue()
        real = self.backend._rows
        raised = []

        async def once(query, parameters, *, rows=True):
            if not raised:
                raised.append(1)
                raise errors.DeadlockDetected("deadlock detected")
            return await real(query, parameters, rows=rows)

        with mock.patch.object(self.backend, "_rows", once):
            with self.assertLogs("tinyjobs.postgres", "WARNING"):
                claimed = await self.claim()
        self.assertEqual([found.id for found in claimed], [job.id])

    async def test_a_deadlock_that_keeps_happening_is_raised(self):
        await self.enqueue()

        async def always(query, parameters, *, rows=True):
            raise errors.SerializationFailure("could not serialize access")

        with mock.patch.object(self.backend, "_rows", always):
            with self.assertRaises(errors.SerializationFailure):
                await self.claim()

    async def test_two_jobs_retried_at_once_never_share_a_dedup_key(self):
        racers = [await self.open(), await self.open()]
        for round in range(20):
            key = f"race:{round}"
            failed = []
            for _ in range(2):
                claimed = await self.claimed_one(dedup_key=key)
                await self.backend.fail(claimed, ERROR, None)
                failed.append(claimed)
            outcomes = await asyncio.gather(
                *(backend.requeue(job.id) for backend, job in zip(racers, failed))
            )
            self.assertEqual(sorted(outcomes), [False, True])

    async def test_a_transaction_cancelled_at_any_point_leaves_its_connection_clean(self):
        with self.assertNoLogs("psycopg", "WARNING"):
            for steps in range(60):
                task = asyncio.create_task(
                    self.backend.fire_schedule("missing", expected=0.0, next_run_at=1.0, jobs=[])
                )
                for _ in range(steps):
                    await asyncio.sleep(0)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        self.assertEqual(self.backend._pool.get_stats().get("returns_bad", 0), 0)
        self.assertEqual(await self.backend.counts_by_status(), {})

    async def test_a_store_already_at_this_version_starts_beside_an_open_writer(self):
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as writer:
            await writer.execute(f'LOCK TABLE "{self.prefix}_jobs" IN ROW EXCLUSIVE MODE')
            late = await self.make_backend(self.clock)
            self.addAsyncCleanup(late.close)
            await asyncio.wait_for(late.setup(), 3)
            await writer.rollback()

    async def test_creating_the_store_gives_up_behind_a_lock_held_too_long(self):
        from tinyjobs.backends.postgres import backend as module

        await drop(self.prefix)
        held = await self.make_backend(self.clock)
        self.addAsyncCleanup(held.close)
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as holder:
            await holder.execute(
                "SELECT pg_advisory_xact_lock(%(key)s)", {"key": held._tables.lock}
            )
            started = time.monotonic()
            with mock.patch.object(module, "SETUP_LOCK_SECONDS", 0.2):
                with self.assertRaises(errors.LockNotAvailable):
                    await held.setup()
            self.assertLess(time.monotonic() - started, 3)
            await holder.rollback()

    async def test_ids_list_in_byte_order_whatever_the_database_collation(self):
        for job_id in ("B-1", "a-1", "_-1"):
            await self.backend.enqueue(make_job(id=job_id))
        listed = [job.id for job in (await self.backend.list(limit=10)).items]
        self.assertEqual(listed, sorted(listed, reverse=True))
        self.assertEqual(listed, ["a-1", "_-1", "B-1"])

    async def test_due_jobs_are_claimed_past_a_backlog_of_delayed_higher_priority_jobs(self):
        delayed = [
            make_job(priority=5, queue="a", run_at=self.clock.now() + 3600)
            for _ in range(50000)
        ]
        await self.backend.enqueue_many(delayed)
        due = [await self.enqueue(priority=p, queue=q) for p, q in ((1, "a"), (0, "b"), (1, "b"))]
        claimed = await self.claim(limit=10, queues=("a", "b", "default"))
        self.assertEqual(
            [job.id for job in claimed], [due[0].id, due[2].id, due[1].id]
        )
        await self.enqueue(priority=1, queue="a")
        async with self.backend._connection() as connection:
            await connection.execute("SET plan_cache_mode = force_custom_plan")
            cursor = await connection.execute(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {self.backend._queries.claim}",
                {
                    "now": self.clock.now(),
                    "queues": ["a", "b", "default"],
                    "limit": 10,
                    "worker": "explain",
                    "lease": self.clock.now() + 60,
                    "seconds": 60.0,
                },
            )
            [[plan]] = (await cursor.fetchone()).values()
        touched = plan["Plan"]["Shared Hit Blocks"] + plan["Plan"]["Shared Read Blocks"]
        self.assertLess(touched, 80)

    async def test_heartbeats_rewrite_rows_in_place(self):
        [(version,)] = await sql("SHOW server_version_num")
        if int(version) < 150000:
            self.skipTest("pg_stat_force_next_flush needs postgres 15")
        await self.backend.close()
        self.backend = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, clock=self.clock, max_connections=1
        )
        await self.backend.setup()
        claimed = [await self.claimed_one() for _ in range(20)]
        query = (
            "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relname = %(name)s"
        )

        async def counted() -> tuple[int, int]:
            async with self.backend._connection() as connection:
                await connection.execute("SELECT pg_stat_force_next_flush()")
                rows = await _rows_of(connection, query, {"name": f"{self.prefix}_jobs"})
            return rows[0]

        before = await counted()
        for _ in range(10):
            self.clock.advance(1)
            await self.backend.heartbeat([job.id for job in claimed], 60.0, worker_id="worker-a")
        after = await counted()
        updated, hot = after[0] - before[0], after[1] - before[1]
        self.assertEqual(updated, 200)
        self.assertGreaterEqual(hot, 150)

    async def test_a_failed_wake_up_still_stores_the_job(self):
        self.backend._queries.notify = "SELECT this_is_not_sql("
        job = await self.enqueue()
        self.assertEqual((await self.stored(job)).status, JobStatus.QUEUED)

    async def test_a_burst_of_jobs_wakes_listeners_a_few_times_and_once_more_at_the_end(self):
        listener = await self.listening(self.backend)
        for _ in range(30):
            await self.enqueue(queue="mail")
        heard = await self.heard(listener)
        self.assertGreaterEqual(len(heard), 1)
        self.assertLessEqual(len(heard), 4)
        await self.enqueue(queue="mail")
        self.assertEqual(await self.heard(listener), ["mail"])

    async def test_closing_with_a_wake_up_pending_leaves_nothing_running(self):
        backend = await self.open()
        await backend.enqueue(make_job(queue="mail"))
        await backend.enqueue(make_job(queue="mail"))
        self.assertIsNotNone(backend._waking)
        waking = backend._waking
        await backend.close()
        self.assertTrue(waking.done())

    async def test_a_reaped_job_wakes_listeners_on_its_queue(self):
        await self.enqueue(queue="mail", max_attempts=2)
        await self.claim(queues=("mail",))
        listener = await self.listening(self.backend)
        self.assertEqual(await self.reap_after_the_lease(), 1)
        self.assertEqual(await self.heard(listener), ["mail"])

    async def test_a_released_job_due_now_wakes_listeners(self):
        await self.enqueue(queue="mail")
        [claimed] = await self.claim(queues=("mail",))
        listener = await self.listening(self.backend)
        self.assertTrue(await self.backend.release(claimed))
        self.assertEqual(await self.heard(listener), ["mail"])

    async def test_a_retry_due_later_wakes_nobody(self):
        await self.enqueue(queue="mail")
        [claimed] = await self.claim(queues=("mail",))
        listener = await self.listening(self.backend)
        self.assertTrue(await self.backend.fail(claimed, ERROR, self.clock.now() + 60))
        self.assertEqual(await self.heard(listener), [])

    async def listening(self, backend):
        connection = await psycopg.AsyncConnection.connect(POSTGRES_URL, autocommit=True)
        self.addAsyncCleanup(connection.close)
        await connection.execute(f'LISTEN "{backend._tables.channel}"')
        return connection

    async def heard(self, connection, seconds: float = 0.5) -> list[str]:
        return [
            notice.payload
            async for notice in connection.notifies(timeout=seconds, stop_after=10)
        ]

    async def test_a_job_due_now_wakes_listeners_on_its_queue(self):
        listener = await self.listening(self.backend)
        await self.enqueue(queue="mail")
        self.assertEqual(await self.heard(listener), ["mail"])

    async def test_a_batch_wakes_each_queue_once(self):
        listener = await self.listening(self.backend)
        await self.backend.enqueue_many(
            [make_job(queue=queue) for queue in ("a", "b", "a", "a", "b")]
        )
        self.assertEqual(sorted(await self.heard(listener)), ["a", "b"])

    async def test_a_delayed_job_wakes_nobody(self):
        listener = await self.listening(self.backend)
        await self.enqueue(run_at=self.clock.now() + 60)
        self.assertEqual(await self.heard(listener), [])

    async def test_a_job_that_already_existed_wakes_nobody(self):
        job = await self.enqueue()
        listener = await self.listening(self.backend)
        await self.backend.enqueue(make_job(id=job.id))
        self.assertEqual(await self.heard(listener), [])

    async def test_a_retried_job_due_now_wakes_listeners(self):
        await self.enqueue(queue="mail")
        [failed] = await self.claim(queues=("mail",))
        await self.backend.fail(failed, ERROR, None)
        listener = await self.listening(self.backend)
        self.assertTrue(await self.backend.requeue(failed.id))
        self.assertEqual(await self.heard(listener), ["mail"])

    async def test_notifying_can_be_turned_off(self):
        quiet = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, clock=self.clock, notify=False
        )
        self.addAsyncCleanup(quiet.close)
        await quiet.setup()
        listener = await self.listening(quiet)
        await quiet.enqueue(make_job())
        failed = (
            await quiet.claim(
                worker_id="w", queues=["default"], limit=1, lease_seconds=30.0
            )
        )[0]
        await quiet.fail(failed, ERROR, None)
        await quiet.requeue(failed.id)
        self.assertEqual(await self.heard(listener), [])


@requires_postgres
class PostgresConfigurationTests(unittest.IsolatedAsyncioTestCase):
    def test_names_that_are_not_plain_identifiers_are_refused(self):
        for options in (
            {"prefix": "Jobs"},
            {"prefix": "jobs-1"},
            {"prefix": "1jobs"},
            {"prefix": "j" * 41},
            {"prefix": 'jobs"; DROP TABLE x; --'},
            {"schema": "Public"},
            {"schema": "a.b"},
        ):
            with self.subTest(options=options):
                with self.assertRaises(ConfigurationError):
                    PostgresBackend.from_url(POSTGRES_URL, **options)

    async def test_a_schema_keeps_its_tables_apart_from_the_same_prefix_elsewhere(self):
        prefix = new_prefix()
        schema = f"tj_{uuid.uuid4().hex[:12]}"
        self.addAsyncCleanup(drop, prefix)
        self.addAsyncCleanup(drop, prefix, schema)
        inside = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix, schema=schema)
        outside = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        for backend in (inside, outside):
            self.addAsyncCleanup(backend.close)
            await backend.setup()
        job = await inside.enqueue(make_job())
        self.assertIsNone(await outside.get(job.id))
        [(found,)] = await sql(
            "SELECT to_regclass(%(name)s) IS NOT NULL", {"name": f'"{schema}"."{prefix}_jobs"'}
        )
        self.assertTrue(found)
        self.assertNotEqual(inside._tables.channel, outside._tables.channel)

    async def test_both_url_schemes_reach_the_same_store(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        spelled = "postgres://" + POSTGRES_URL.split("://", 1)[1]
        apps = [TinyJobs(url, prefix=prefix) for url in (POSTGRES_URL, spelled)]
        for app in apps:
            self.addAsyncCleanup(app.aclose)
        job = await apps[0].asend("anything")
        self.assertEqual((await apps[1].aget(job.id)).id, job.id)
        self.assertIsInstance(apps[1].backend._inner, PostgresBackend)

    async def show(self, backend, name: str) -> str:
        async with backend._connection() as connection:
            row = await (await connection.execute(f"SHOW {name}")).fetchone()
        return row[name]

    async def test_connections_run_read_committed_with_timeouts_whatever_the_role_default(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        backend = PostgresBackend.from_url(
            POSTGRES_URL,
            prefix=prefix,
            options="-c default_transaction_isolation=serializable",
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        self.assertEqual(await self.show(backend, "transaction_isolation"), "read committed")
        self.assertEqual(await self.show(backend, "statement_timeout"), "30s")
        self.assertEqual(await self.show(backend, "idle_in_transaction_session_timeout"), "1min")
        self.assertEqual(await self.show(backend, "application_name"), "tinyjobs")

    async def test_the_statement_timeout_can_be_turned_off(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix, statement_timeout=None)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        self.assertEqual(await self.show(backend, "statement_timeout"), "0")

    def test_connection_settings_in_the_url_win_over_the_defaults(self):
        from tinyjobs.backends.postgres.backend import _connection_kwargs

        spelled = f"{POSTGRES_URL}?keepalives_idle=5&application_name=billing"
        kwargs = _connection_kwargs(spelled, {})
        self.assertNotIn("keepalives_idle", kwargs)
        self.assertNotIn("application_name", kwargs)
        self.assertEqual(kwargs["keepalives"], 1)
        given = _connection_kwargs(POSTGRES_URL, {"keepalives_count": 9})
        self.assertEqual(given["keepalives_count"], 9)
        self.assertEqual(given["application_name"], "tinyjobs")

    async def test_closing_before_setup_refuses_to_open_and_starts_nothing(self):
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=new_prefix())
        before = asyncio.all_tasks()
        await backend.close()
        with self.assertRaises(PoolClosed):
            await backend.setup()
        await asyncio.sleep(0.05)
        self.assertEqual(asyncio.all_tasks() - before, set())

    async def test_closing_while_the_first_connection_waits_stops_at_once(self):
        backend = PostgresBackend.from_url(
            "postgresql://postgres@127.0.0.1:1/nothing", pool_timeout=30
        )
        before = asyncio.all_tasks()
        setting_up = asyncio.create_task(backend.setup())
        await asyncio.sleep(0.2)
        started = time.monotonic()
        await backend.close()
        self.assertLess(time.monotonic() - started, 2)
        with self.assertRaises(PoolClosed):
            await setting_up
        await asyncio.sleep(0.05)
        self.assertEqual(asyncio.all_tasks() - before - {asyncio.current_task()}, set())

    async def test_closing_twice_at_once_closes_once(self):
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=new_prefix())
        await backend.setup()
        workers = list(backend._pool._workers)
        await asyncio.gather(backend.close(), backend.close())
        self.assertTrue(all(worker.done() for worker in workers))

    async def test_a_server_that_cannot_be_reached_fails_setup_in_time(self):
        backend = PostgresBackend.from_url(
            "postgresql://postgres@127.0.0.1:1/nothing", pool_timeout=0.5
        )
        started = time.monotonic()
        with self.assertRaises(PoolTimeout):
            await backend.setup()
        self.assertLess(time.monotonic() - started, 5)
        await backend.close()


SKEWED_HOST = """
import sys, time, types
import tinyjobs.clock
real = time.time
tinyjobs.clock.time = types.SimpleNamespace(
    time=lambda: real() + float(sys.argv[3]), monotonic=time.monotonic
)
import psycopg
from tinyjobs import TinyJobs
url, prefix = sys.argv[1], sys.argv[2]
app = TinyJobs(url, prefix=prefix)
marks = psycopg.connect(url, autocommit=True)

@app.task(name="mark")
def mark(value):
    time.sleep(0.2)
    marks.execute(f'INSERT INTO "{prefix}_ran" VALUES (%s, %s)', (value, "skewed"))

print("ready", flush=True)
app.worker(concurrency=4, poll_maximum=0.05, lease_seconds=5.0, reap_interval=0.2).run()
app.close()
"""


@requires_postgres
class PostgresClockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.prefix = new_prefix()
        self.opened: list = []

    async def asyncTearDown(self) -> None:
        for backend in self.opened:
            await backend.close()
        await drop(self.prefix)

    async def open(self, clock=None, url: str | None = None, **options):
        backend = PostgresBackend.from_url(
            url or POSTGRES_URL, prefix=self.prefix, clock=clock, **options
        )
        self.opened.append(backend)
        await backend.setup()
        return backend

    def counted_reads(self, backend) -> list:
        reads: list = []
        original = backend._timed

        async def counted():
            reads.append(1)
            return await original()

        backend._timed = counted
        return reads

    def slow_readings(self, backend, seconds: float) -> None:
        backend._queries.server_time = (
            "SELECT extract(epoch FROM clock_timestamp())::double precision AS now, "
            f"pg_sleep({seconds})"
        )

    async def wrong_by(self, backend, seconds: float) -> None:
        from tinyjobs.clock import elapsed

        server = await server_time()
        started = elapsed()
        backend._clock.follow(server + seconds, started, started, backend._server())

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
        self.assertEqual([found.id for found in claimed], [job.id])

    async def test_every_host_stamps_times_from_the_postgres_clock(self):
        for skew in (-7200.0, 0.0, 7200.0):
            with skewed(skew):
                backend = await self.open()
                await backend.enqueue(make_job(run_at=0.0))
                [claimed] = await backend.claim(
                    worker_id=f"w{skew}", queues=["default"], limit=1, lease_seconds=30.0
                )
                now = backend._clock.now()
            server = await server_time()
            self.assertAlmostEqual(claimed.lease_expires_at, server + 30.0, delta=2.0)
            self.assertAlmostEqual(now, server, delta=2.0)

    async def test_a_step_of_the_local_clock_moves_nothing_even_before_the_next_call(self):
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="noted")
        def noted() -> None:
            return None

        await app.ready()
        for step in (3600.0, -3600.0):
            with skewed(step):
                job = await noted.with_options(delay=60).aenqueue()
            server = await server_time()
            self.assertAlmostEqual(job.run_at, server + 60.0, delta=2.0)
            self.assertAlmostEqual(job.created_at, server, delta=2.0)

    async def test_a_step_while_the_clock_is_read_leaves_no_error_behind(self):
        backend = await self.open()
        original = backend._timed
        stepped = []

        async def stepping():
            reply = await original()
            stepped.append(skewed(3600.0))
            stepped[-1].__enter__()
            return reply

        backend._timed = stepping
        backend._clock.forget()
        try:
            await backend.reap_expired()
            now = backend._clock.now()
        finally:
            for step in stepped:
                step.__exit__(None, None, None)
        backend._timed = original
        self.assertEqual(len(stepped), 1)
        self.assertAlmostEqual(now, await server_time(), delta=1.0)

    async def test_a_slow_reading_does_not_replace_a_good_one(self):
        backend = await self.open()
        original = backend._queries.server_time
        self.slow_readings(backend, 1.0)
        backend._clock.forget()
        await backend.reap_expired()
        backend._queries.server_time = original
        self.assertAlmostEqual(backend._clock.now(), await server_time(), delta=0.3)

    async def test_a_slow_reading_is_not_retried_on_every_call(self):
        backend = await self.open()
        self.slow_readings(backend, 0.6)
        backend._clock.forget()
        await backend.reap_expired()
        reads = self.counted_reads(backend)
        await backend.reap_expired()
        await backend.reap_expired()
        self.assertEqual(reads, [])

    async def test_the_clock_is_read_again_once_it_is_old(self):
        from tinyjobs.backends.postgres import backend as module
        from tinyjobs.clock import FOLLOW_SECONDS

        backend = await self.open()
        reads = self.counted_reads(backend)
        await backend.reap_expired()
        self.assertEqual(reads, [])
        with later_by(FOLLOW_SECONDS + 1, module):
            await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def terminate(self, backend) -> None:
        async with backend._pool.connection() as connection:
            [pid] = (await (await connection.execute("SELECT pg_backend_pid() AS pid")).fetchone()).values()
        await sql("SELECT pg_terminate_backend(%(pid)s)", {"pid": pid})

    async def test_a_lost_connection_reads_the_clock_again(self):
        backend = await self.open(max_connections=1)
        reads = self.counted_reads(backend)
        await self.terminate(backend)
        with self.assertRaises(psycopg.OperationalError):
            await backend.reap_expired()
        reads.clear()
        self.assertEqual(await backend.reap_expired(), 0)
        self.assertEqual(reads, [1])

    async def test_an_exhausted_pool_reads_the_clock_again(self):
        backend = await self.open(max_connections=1, pool_timeout=0.2)
        reads = self.counted_reads(backend)
        async with backend._pool.connection():
            with self.assertRaises(PoolTimeout):
                await backend.reap_expired()
        reads.clear()
        await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def test_a_cancelled_statement_reads_the_clock_again(self):
        backend = await self.open(statement_timeout=0.1)
        reads = self.counted_reads(backend)
        original = backend._queries.reap
        backend._queries.reap = "SELECT pg_sleep(1) AS reaped"
        with self.assertRaises(errors.QueryCanceled):
            await backend.reap_expired()
        backend._queries.reap = original
        reads.clear()
        await backend.reap_expired()
        self.assertEqual(reads, [1])

    async def test_opening_a_connection_is_not_counted_in_a_reading(self):
        backend = await self.open()
        backend._clock.forget()
        real = psycopg.AsyncConnection.connect.__func__

        async def slowly(cls, *args, **kwargs):
            await asyncio.sleep(0.3)
            return await real(cls, *args, **kwargs)

        with mock.patch.object(psycopg.AsyncConnection, "connect", classmethod(slowly)):
            async with backend._pool.connection():
                await backend.reap_expired()
        self.assertAlmostEqual(backend._clock.now(), await server_time(), delta=0.1)

    async def test_one_clock_cannot_follow_two_servers_whose_times_differ(self):
        from tinyjobs.clock import FollowingClock

        clock = FollowingClock()
        await self.open(clock)
        other = PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, clock=clock)
        self.opened.append(other)
        other._server = lambda: "postgres elsewhere:5432 db other"
        other._queries.server_time = (
            "SELECT extract(epoch FROM clock_timestamp())::double precision + 120 AS now"
        )
        with self.assertRaises(ConfigurationError):
            await other.setup()
            await other.reap_expired()

    async def test_one_clock_follows_one_server_however_its_url_is_spelled(self):
        from tinyjobs.clock import FollowingClock

        clock = FollowingClock()
        await self.open(clock)
        other = await self.open(clock, url=POSTGRES_URL.replace("127.0.0.1", "localhost"))
        reads = self.counted_reads(other)
        clock.forget()
        for _ in range(3):
            await other.reap_expired()
        self.assertEqual(reads, [1])

    async def test_readings_due_at_once_share_one_round_trip(self):
        backend = await self.open()
        reads = self.counted_reads(backend)
        backend._clock.forget()
        await asyncio.gather(*(backend.reap_expired() for _ in range(50)))
        self.assertEqual(reads, [1])

    async def test_cron_fired_by_a_host_behind_is_due_at_its_slot(self):
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(cron="* * * * *")
        def tick() -> None:
            return None

        backend = await self.open()
        with skewed(-900.0):
            await app.ready()
            await app.scheduling.register()
            [state] = await app.aschedules()
            slot = await server_time() - 1
            await sql(
                f'UPDATE "{self.prefix}_schedules" SET next_run_at = %(slot)s WHERE name = %(name)s',
                {"slot": slot, "name": state.name},
            )
            await app.scheduling.fire_due(60.0)
        [job] = (await app.ajobs()).items
        self.assertAlmostEqual(job.run_at, slot, delta=0.01)
        claimed = await backend.claim(
            worker_id="due", queues=["default"], limit=1, lease_seconds=30.0
        )
        self.assertEqual([found.id for found in claimed], [job.id])

    async def test_jobs_list_newest_first_whichever_host_sent_them(self):
        apps = [TinyJobs(POSTGRES_URL, prefix=self.prefix) for _ in range(2)]
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

    async def test_a_host_still_on_an_old_time_ahead_cannot_reap_a_live_lease(self):
        here = await self.open()
        job = await here.enqueue(make_job(run_at=0.0))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=60.0)
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        self.assertGreater(stale._clock.now() - await server_time(), 100)
        self.assertEqual(await stale.reap_expired(), 0)
        self.assertEqual((await here.get(job.id)).status, JobStatus.RUNNING)

    async def test_a_host_still_on_an_old_time_ahead_runs_no_job_before_it_is_due(self):
        here = await self.open()
        server = await server_time()
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

    async def test_a_host_still_on_an_old_time_ahead_wakes_nobody_for_a_job_not_due(self):
        here = await self.open()
        listener = await psycopg.AsyncConnection.connect(POSTGRES_URL, autocommit=True)
        self.addAsyncCleanup(listener.close)
        await listener.execute(f'LISTEN "{here._tables.channel}"')
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        await stale.enqueue(make_job(run_at=await server_time() + 60.0))
        heard = [n async for n in listener.notifies(timeout=0.5, stop_after=1)]
        self.assertEqual(heard, [])

    async def test_a_host_still_on_an_old_time_ahead_stamps_reaped_jobs_with_postgres_time(self):
        here = await self.open()
        retried = await here.enqueue(make_job(run_at=0.0, max_attempts=2))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=0.2)
        failed = await here.enqueue(make_job(run_at=0.0, max_attempts=1))
        await here.claim(worker_id="here", queues=["default"], limit=1, lease_seconds=0.2)
        await asyncio.sleep(0.5)
        stale = await self.open()
        await self.wrong_by(stale, 120.0)
        self.assertEqual(await stale.reap_expired(), 2)
        server = await server_time()
        self.assertAlmostEqual((await here.get(retried.id)).run_at, server, delta=2.0)
        self.assertAlmostEqual((await here.get(failed.id)).finished_at, server, delta=2.0)
        [lost] = await here.executions(failed.id)
        self.assertAlmostEqual(lost.finished_at, server, delta=2.0)

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

    async def test_a_host_still_on_an_old_time_behind_stamps_leases_from_postgres_time(self):
        behind = await self.open()
        await self.wrong_by(behind, -120.0)
        job = await behind.enqueue(make_job(run_at=0.0))
        [claimed] = await behind.claim(
            worker_id="behind", queues=["default"], limit=1, lease_seconds=60.0
        )
        server = await server_time()
        self.assertGreater(claimed.lease_expires_at, server + 55.0)
        await behind.heartbeat([job.id], 60.0, worker_id="behind")
        renewed = await behind.get(job.id)
        self.assertGreater(renewed.lease_expires_at, server + 55.0)
        here = await self.open()
        self.assertEqual(await here.reap_expired(), 0)

    async def test_a_clock_that_must_be_read_again_is_read_before_a_job_is_built(self):
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)

        @app.task(name="built")
        def built() -> None:
            return None

        await app.ready()
        from tinyjobs.clock import elapsed

        started = elapsed()
        server = await server_time()
        app.clock.follow(server - 30.0, started, started, app.backend._server())
        with skewed(30.0):
            job = await built.aenqueue()
        self.assertAlmostEqual(job.created_at, await server_time(), delta=2.0)

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

        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
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
        server = await server_time()
        self.assertAlmostEqual(by_number.run_at, server + 60.0, delta=2.0)
        self.assertAlmostEqual(by_datetime.run_at, server + 120.0, delta=2.0)

    async def test_a_retry_at_a_local_time_waits_as_long_as_asked(self):
        from tinyjobs import Retry

        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
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
        self.assertEqual(stored.status, JobStatus.QUEUED)
        self.assertAlmostEqual(stored.run_at, await server_time() + 30.0, delta=3.0)

    async def test_a_host_fifteen_minutes_ahead_runs_every_job_once_beside_this_one(self):
        await sql(f'CREATE TABLE "{self.prefix}_ran" (value integer, host text)')
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)

        @app.task(name="mark")
        async def mark(value: int) -> None:
            await asyncio.sleep(1.0)
            await sql(
                f'INSERT INTO "{self.prefix}_ran" VALUES (%(value)s, %(host)s)',
                {"value": value, "host": "here"},
            )

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
                sys.executable, "-c", SKEWED_HOST, POSTGRES_URL, self.prefix, "900",
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
            marked = await sql(f'SELECT value, host FROM "{self.prefix}_ran"')
            self.assertEqual(sorted(value for value, _ in marked), list(range(24)))
            self.assertIn("skewed", {where for _, where in marked})
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
            await app.aclose()


FORK_KEEPS_PARENT = """
import gc, os, sys
from tinyjobs import TinyJobs
app = TinyJobs(sys.argv[1], prefix=sys.argv[2], min_connections=3).start()
app.send("t")
for ending in ("exit", "close", "collect"):
    child = os.fork()
    if child == 0:
        app.counts()
        if ending == "close":
            app.close()
        if ending == "collect":
            del app
            gc.collect()
        sys.exit(0)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0, ending
    for _ in range(5):
        app.send("t")
stats = app.backend._inner._pool.get_stats()
assert app.counts() == {"queued": 16}, app.counts()
assert stats["connections_num"] == 3 and "returns_bad" not in stats, stats
app.close()
"""


@requires_postgres
@unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
class PostgresForkTests(unittest.IsolatedAsyncioTestCase):
    async def test_forking_an_idle_started_app_raises_no_thread_warning(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        await asyncio.to_thread(fork_without_warning, self, POSTGRES_URL, f"prefix={prefix}")

    async def test_a_child_that_closes_its_copy_leaves_the_parent_sessions_alone(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        child = await asyncio.create_subprocess_exec(
            sys.executable, "-c", FORK_KEEPS_PARENT, POSTGRES_URL, prefix,
            stderr=asyncio.subprocess.PIPE,
        )
        _, errors_out = await asyncio.wait_for(child.communicate(), 60)
        self.assertEqual(child.returncode, 0, errors_out.decode())


@requires_postgres
class PostgresAppTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.prefix = new_prefix()
        self.app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
        await self.app.ready()

    async def asyncTearDown(self) -> None:
        await self.app.aclose()
        await drop(self.prefix)

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

    async def test_a_worker_runs_jobs_and_retries_against_postgres(self):
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

    async def test_the_worker_fires_cron_against_postgres(self):
        ran = []

        @self.app.task(cron="* * * * *")
        def tick():
            ran.append(1)

        await self.app.scheduling.register()
        state = (await self.app.aschedules())[0]
        await sql(
            f'UPDATE "{self.prefix}_schedules" SET next_run_at = %(at)s WHERE name = %(name)s',
            {"at": time.time() - 1, "name": state.name},
        )
        worker = self.app.worker(concurrency=2, poll_maximum=0.05)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 10
        while not ran and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual(ran, [1])

    async def test_check_and_stats_work_against_postgres(self):
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
        small = TinyJobs(POSTGRES_URL, prefix=self.prefix, max_connections=4)

        @small.task
        async def noop(value):
            return value

        try:
            jobs = await asyncio.gather(*(noop.aenqueue(n) for n in range(200)))
            self.assertEqual(len({job.id for job in jobs}), 200)
        finally:
            await small.aclose()

    async def test_a_connection_option_still_reaches_the_connection(self):
        named = TinyJobs(POSTGRES_URL, prefix=self.prefix, application_name="tinyjobs-probe")

        async def name() -> str:
            async with named.backend._connection() as connection:
                row = await (await connection.execute("SHOW application_name")).fetchone()
                return row["application_name"]

        try:
            await named.ready()
            self.assertEqual(await on_backend_loop(named, name()), "tinyjobs-probe")
        finally:
            await named.aclose()

    async def test_an_exhausted_pool_gives_up_after_its_timeout(self):
        small = TinyJobs(
            POSTGRES_URL, prefix=self.prefix, max_connections=1, pool_timeout=0.2
        )

        @small.task
        async def noop():
            return None

        await small.ready()

        async def hold() -> None:
            async with small.backend._pool.connection():
                await asyncio.sleep(1)

        holding = asyncio.create_task(on_backend_loop(small, hold()))
        try:
            await asyncio.sleep(0.1)
            with self.assertRaises(PoolTimeout):
                await noop.aenqueue()
        finally:
            await holding
            await small.aclose()

    async def test_two_prefixes_on_one_database_do_not_see_each_other(self):
        other_prefix = new_prefix()
        other = TinyJobs(POSTGRES_URL, prefix=other_prefix)
        await other.ready()
        try:
            await self.app.asend("a.task")
            self.assertEqual(len((await other.ajobs()).items), 0)
        finally:
            await other.aclose()
            await drop(other_prefix)

    async def idle_worker(self, app, **options):
        ran: list[float] = []

        @app.task(name="timed")
        def timed() -> None:
            ran.append(time.monotonic())

        worker = app.worker(concurrency=2, scheduler=False, **options)
        runner = asyncio.create_task(worker.start())
        self.addAsyncCleanup(self.stopped, worker, runner)
        await asyncio.sleep(0.5)
        return ran

    async def stopped(self, worker, runner) -> None:
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)

    async def latency(self, app, ran: list[float]) -> float:
        sent = time.monotonic()
        await app.asend("timed")
        deadline = sent + 10
        while not ran:
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.005)
        return ran.pop() - sent

    async def test_an_idle_worker_starts_a_new_job_long_before_its_next_poll(self):
        ran = await self.idle_worker(self.app, poll_minimum=5.0, poll_maximum=5.0)
        for _ in range(3):
            self.assertLess(await self.latency(self.app, ran), 0.3)
            await asyncio.sleep(0.2)

    async def test_an_idle_worker_without_notifications_still_polls(self):
        quiet = TinyJobs(POSTGRES_URL, prefix=self.prefix, notify=False)
        self.addAsyncCleanup(quiet.aclose)
        ran = await self.idle_worker(quiet, poll_minimum=0.05, poll_maximum=0.1)
        self.assertLess(await self.latency(quiet, ran), 1.0)

    async def test_a_worker_keeps_waking_after_its_listening_connection_drops(self):
        ran = await self.idle_worker(self.app, poll_minimum=5.0, poll_maximum=5.0)
        query = "SELECT pid FROM pg_stat_activity WHERE query LIKE 'LISTEN %%' AND query LIKE %(channel)s"
        channel = f"%{self.app.backend._tables.channel}%"
        [(pid,)] = await sql(query, {"channel": channel})
        await sql("SELECT pg_terminate_backend(%(pid)s)", {"pid": pid})
        await asyncio.sleep(2.5)
        self.assertNotEqual(await sql(query, {"channel": channel}), [(pid,)])
        self.assertLess(await self.latency(self.app, ran), 0.3)

    async def test_only_jobs_sent_after_the_last_claim_cut_the_wait_short(self):
        backend = self.app.backend._inner

        async def waited(heard_before: bool) -> float:
            if heard_before:
                backend._hear("default")
            await backend.claim(worker_id="w", queues=["default"], limit=1, lease_seconds=30)
            if not heard_before:
                backend._hear("default")
            started = time.monotonic()
            await backend.wait_for_jobs(worker_id="w", queues=["default"], timeout=0.4)
            return time.monotonic() - started

        await on_backend_loop(self.app, waited(True))
        self.assertGreater(await on_backend_loop(self.app, waited(True)), 0.3)
        self.assertLess(await on_backend_loop(self.app, waited(False)), 0.1)

    async def test_a_job_on_another_queue_does_not_wake_the_worker(self):
        backend = self.app.backend

        async def wait() -> float:
            started = time.monotonic()
            await backend.wait_for_jobs(worker_id="w", queues=["mail"], timeout=0.6)
            return time.monotonic() - started

        await wait()
        waiting = asyncio.create_task(wait())
        await asyncio.sleep(0.2)
        await self.app.asend("anything", queue="sms")
        self.assertGreater(await waiting, 0.5)

    async def test_closing_the_app_wakes_idle_workers_at_once(self):
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix)
        await app.ready()
        worker = app.worker(scheduler=False, poll_minimum=10.0, poll_maximum=10.0)
        runner = asyncio.create_task(worker.start())
        await asyncio.sleep(0.5)
        started = time.monotonic()
        await app.aclose()
        await asyncio.wait_for(runner, timeout=10)
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_closing_leaves_no_connection_behind(self):
        name = f"tinyjobs-{uuid.uuid4().hex[:8]}"
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix, application_name=name)
        await app.asend("anything")
        await app.aclose()
        query = "SELECT count(*) FROM pg_stat_activity WHERE application_name = %(name)s"
        deadline = time.monotonic() + 5
        while (await sql(query, {"name": name}))[0][0] and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        self.assertEqual(await sql(query, {"name": name}), [(0,)])


    async def connect(self, **options):
        return await psycopg.AsyncConnection.connect(POSTGRES_URL, **options)

    def noted_task(self):
        @self.app.task(name="noted", queue="notes")
        def noted(value=None) -> None:
            return None

        return noted

    async def test_a_job_sent_inside_a_transaction_appears_when_it_commits(self):
        noted = self.noted_task()
        async with await self.connect() as connection:
            async with connection.transaction():
                job = await noted.with_options(tx=connection, priority=4).aenqueue(7)
                self.assertIsNone(await self.app.aget(job.id))
            stored = await self.app.aget(job.id)
        self.assertEqual(
            (stored.status, stored.queue, stored.priority, self.app.decode(stored).args),
            (JobStatus.QUEUED, "notes", 4, (7,)),
        )

    async def test_a_job_sent_inside_a_rolled_back_transaction_is_never_stored(self):
        noted = self.noted_task()
        async with await self.connect() as connection:
            with self.assertRaises(ZeroDivisionError):
                async with connection.transaction():
                    job = await noted.with_options(tx=connection).aenqueue()
                    1 / 0
        self.assertIsNone(await self.app.aget(job.id))
        self.assertEqual((await self.app.ajobs()).items, [])

    async def test_send_and_a_cursor_join_the_transaction_too(self):
        async with await self.connect() as connection:
            async with connection.transaction():
                cursor = connection.cursor()
                job = await self.app.asend("x.task", args=[1], tx=cursor, queue="q")
                await cursor.execute("SELECT 1")
                self.assertEqual(await cursor.fetchall(), [(1,)])
            await connection.rollback()
        self.assertEqual((await self.app.aget(job.id)).queue, "q")

    async def test_a_bound_task_keeps_its_transaction_through_more_options(self):
        noted = self.noted_task()
        async with await self.connect() as connection:
            async with connection.transaction():
                bound = noted.with_options(tx=connection).with_options(queue="later")
                job = await bound.aenqueue()
                self.assertIsNone(await self.app.aget(job.id))
        self.assertEqual((await self.app.aget(job.id)).queue, "later")

    async def test_a_dedup_key_inside_a_transaction_finds_its_holder(self):
        noted = self.noted_task()
        held = await noted.with_options(key="k1").aenqueue()
        async with await self.connect() as connection:
            async with connection.transaction():
                keyed = noted.with_options(tx=connection, key="k1")
                self.assertEqual((await keyed.aenqueue()).id, held.id)
                first = await noted.with_options(tx=connection, key="k2").aenqueue()
                again = await noted.with_options(tx=connection, key="k2").aenqueue()
                self.assertEqual(again.id, first.id)
        self.assertEqual(len((await self.app.ajobs()).items), 2)

    async def test_workers_hear_of_a_job_only_when_its_transaction_commits(self):
        noted = self.noted_task()
        channel = self.app.backend._tables.channel
        async with await self.connect(autocommit=True) as listener:
            await listener.execute(
                psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel))
            )
            async with await self.connect() as connection:
                async with connection.transaction():
                    await noted.with_options(tx=connection).aenqueue()
                    self.assertEqual(await self.heard(listener, 0.3), [])
                self.assertEqual(await self.heard(listener, 5), ["notes"])

    async def test_a_quiet_app_sends_no_notification_from_a_transaction(self):
        quiet = TinyJobs(POSTGRES_URL, prefix=self.prefix, notify=False)
        self.addAsyncCleanup(quiet.aclose)
        channel = self.app.backend._tables.channel
        async with await self.connect(autocommit=True) as listener:
            await listener.execute(
                psycopg.sql.SQL("LISTEN {}").format(psycopg.sql.Identifier(channel))
            )
            async with await self.connect() as connection:
                async with connection.transaction():
                    job = await quiet.asend("x.task", tx=connection)
            self.assertEqual(await self.heard(listener, 0.5), [])
        self.assertIsNotNone(await quiet.aget(job.id))

    async def heard(self, listener, timeout: float) -> list[str]:
        return [
            notice.payload
            async for notice in listener.notifies(timeout=timeout, stop_after=1)
        ]

    async def test_an_idle_worker_runs_a_job_soon_after_its_transaction_commits(self):
        ran = await self.idle_worker(self.app, poll_minimum=5.0, poll_maximum=5.0)
        async with await self.connect() as connection:
            async with connection.transaction():
                await self.app.asend("timed", tx=connection)
                await asyncio.sleep(0.3)
                self.assertEqual(ran, [])
            committed = time.monotonic()
        deadline = committed + 10
        while not ran:
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.005)
        self.assertLess(ran.pop() - committed, 0.3)

    async def test_a_connection_that_cannot_see_the_tables_is_refused(self):
        async with await self.connect(options="-c search_path=nowhere") as connection:
            with self.assertRaisesRegex(ConfigurationError, "not in the database"):
                await self.app.asend("x.task", tx=connection)
            await connection.rollback()
            with self.assertRaisesRegex(ConfigurationError, "not in the database"):
                await self.app.asend("x.task", tx=connection)

    async def test_the_table_check_runs_once_per_connection(self):
        async with await self.connect() as connection:
            with mock.patch.object(
                psycopg.AsyncCursor, "execute", autospec=True, side_effect=psycopg.AsyncCursor.execute
            ) as executed:
                await self.app.asend("x.task", tx=connection)
                await self.app.asend("x.task", tx=connection)
            await connection.commit()
        queries = [str(call.args[1]) for call in executed.call_args_list]
        self.assertEqual(sum("to_regclass" in query for query in queries), 1)

    async def test_a_sync_connection_is_refused_by_the_async_calls(self):
        with psycopg.connect(POSTGRES_URL) as connection:
            with self.assertRaisesRegex(TypeError, "async connection"):
                await self.app.asend("x.task", tx=connection)

    async def test_a_failure_later_in_the_transaction_takes_the_job_with_it(self):
        async with await self.connect() as connection:
            with self.assertRaises(errors.DivisionByZero):
                async with connection.transaction():
                    job = await self.app.asend("x.task", tx=connection)
                    await connection.execute("SELECT 1 / 0")
        self.assertIsNone(await self.app.aget(job.id))


@requires_postgres
class PostgresSyncTransactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prefix = new_prefix()
        self.app = TinyJobs(POSTGRES_URL, prefix=self.prefix).start()
        self.addCleanup(asyncio.run, drop(self.prefix))
        self.addCleanup(self.app.close)

        @self.app.task(name="noted")
        def noted(value=None) -> None:
            return None

        self.noted = noted

    def test_a_sync_connection_joins_the_transaction(self):
        with psycopg.connect(POSTGRES_URL) as connection:
            with connection.transaction():
                job = self.noted.with_options(tx=connection).enqueue(3)
                self.assertIsNone(self.app.get(job.id))
            self.assertEqual(self.app.decode(self.app.get(job.id)).args, (3,))

    def test_a_rolled_back_sync_transaction_stores_nothing(self):
        with psycopg.connect(POSTGRES_URL) as connection:
            with connection.transaction(force_rollback=True):
                job = self.app.send("noted", tx=connection.cursor())
        self.assertIsNone(self.app.get(job.id))

    def test_a_sync_enqueue_runs_in_the_calling_thread(self):
        with psycopg.connect(POSTGRES_URL) as connection:
            with connection.transaction():
                job = self.app.send("noted", tx=connection, key="once")
                again = self.app.send("noted", tx=connection, key="once")
        self.assertEqual(again.id, job.id)
        self.assertEqual(len(self.app.jobs().items), 1)

    def test_an_async_connection_is_refused_by_the_sync_calls(self):
        async def connected():
            return await psycopg.AsyncConnection.connect(POSTGRES_URL)

        connection = asyncio.run(connected())
        try:
            with self.assertRaisesRegex(TypeError, "pass an async one"):
                self.app.send("noted", tx=connection)
        finally:
            connection.pgconn.finish()

    def test_eager_mode_refuses_a_transaction(self):
        eager = TinyJobs(POSTGRES_URL, prefix=self.prefix, eager=True).start()
        self.addCleanup(eager.close)
        with psycopg.connect(POSTGRES_URL) as connection:
            with self.assertRaisesRegex(UnsupportedCapabilityError, "eager"):
                eager.send("noted", tx=connection)


@requires_postgres
class PostgresEventLoopTests(LoopBoundAppCases, unittest.TestCase):
    backend_class = PostgresBackend

    def new_prefix(self) -> str:
        return new_prefix()

    def make_app(self) -> TinyJobs:
        return TinyJobs(POSTGRES_URL, prefix=self.prefix)

    async def forget(self, app: TinyJobs) -> None:
        await drop(self.prefix)

    def test_racing_start_and_close_leaves_every_connection_clean(self):
        with self.assertNoLogs("psycopg", "WARNING"):
            race_open_and_close(self, self.make_app())


class PostgresInstallTests(unittest.TestCase):
    def test_postgres_urls_are_known_without_importing_psycopg(self):
        from tinyjobs.backends import available_schemes

        self.assertIn("postgres", available_schemes())
        self.assertIn("postgresql", available_schemes())

    def test_a_missing_client_library_says_how_to_install_it(self):
        import builtins

        real_import = builtins.__import__

        def without_psycopg(name, globals=None, locals=None, fromlist=(), level=0):
            if level == 0 and name.split(".")[0] in ("psycopg", "psycopg_pool"):
                raise ImportError(f"No module named {name!r}")
            return real_import(name, globals, locals, fromlist, level)

        saved = {
            name: module
            for name, module in sys.modules.items()
            if "psycopg" in name or name.startswith("tinyjobs.backends.postgres")
        }
        for name in saved:
            del sys.modules[name]
        builtins.__import__ = without_psycopg
        try:
            with self.assertRaises(ConfigurationError) as caught:
                TinyJobs("postgresql://localhost:1/nothing").start()
        finally:
            builtins.__import__ = real_import
            sys.modules.update(saved)
        self.assertIn("pip install 'tinyjobs[postgres]'", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
