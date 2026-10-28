from __future__ import annotations

import asyncio
import itertools
import os
import re
import signal
import sys
import threading
import time
import unittest
import urllib.parse
import uuid
from contextlib import asynccontextmanager, nullcontext, suppress
from unittest import mock

import tinyjobs.clock
from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import ConfigurationError, UnsupportedCapabilityError
from tinyjobs.runtime import run_on
from tinyjobs.testing import BackendContract
from tinyjobs.models import ExecutionError
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
    from tinyjobs.backends.postgres import backend as postgres_backend
    from tinyjobs.backends.postgres.schema import Tables
except ImportError:
    PostgresBackend = None


def url_for(*, dbname: str | None = None, user: str | None = None, password: str | None = None) -> str:
    parts = urllib.parse.urlsplit(POSTGRES_URL)
    place = parts.hostname + (f":{parts.port}" if parts.port else "")
    who = user or parts.username or ""
    secret = password if user else parts.password
    signed = f"{who}:{secret}@" if secret else f"{who}@" if who else ""
    path = f"/{dbname}" if dbname else parts.path
    return parts._replace(netloc=signed + place, path=path).geturl()


@asynccontextmanager
async def stalling_proxy():
    frozen = asyncio.Event()
    links: list[asyncio.StreamWriter] = []
    target = urllib.parse.urlsplit(POSTGRES_URL)

    async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with suppress(OSError, asyncio.CancelledError):
            while data := await reader.read(65536):
                if not frozen.is_set():
                    writer.write(data)
                    await writer.drain()

    async def link(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            target.hostname, target.port or 5432
        )
        links.extend((writer, upstream_writer))
        await asyncio.gather(pipe(reader, upstream_writer), pipe(upstream_reader, writer))

    server = await asyncio.start_server(link, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    place = target.hostname + (f":{target.port}" if target.port else "")
    try:
        yield POSTGRES_URL.replace(place, f"127.0.0.1:{port}", 1), frozen
    finally:
        frozen.clear()
        for writer in links:
            writer.close()
        server.close()
        await server.wait_closed()


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
                f"UPDATE public.\"{self.prefix}_jobs\" SET status = 'success' WHERE id = %(id)s RETURNING id",
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

    async def test_creating_the_store_goes_ahead_past_a_lock_held_too_long(self):
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
                with self.assertLogs("tinyjobs.postgres", "WARNING") as logs:
                    await held.setup()
            self.assertLess(time.monotonic() - started, 3)
            self.assertIn("without it", "\n".join(logs.output))
            await holder.rollback()
        stored = await held.enqueue(make_job())
        self.assertEqual((await held.get(stored.id)).id, stored.id)

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
        for queues in (["a", "b", "default"], ["a"]):
            query, parameters = self.backend._claiming(
                self.clock.now(), queues, "explain", 10, 60.0
            )
            async with self.backend._connection() as connection:
                await connection.execute("SET plan_cache_mode = force_custom_plan")
                cursor = await connection.execute(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", parameters
                )
                [[plan]] = (await cursor.fetchone()).values()
            touched = plan["Plan"]["Shared Hit Blocks"] + plan["Plan"]["Shared Read Blocks"]
            self.assertLess(touched, 80)

    async def claim_plan(self, queues: list[str]) -> dict:
        query, parameters = self.backend._claiming(self.clock.now(), queues, "explain", 1, 60.0)
        async with self.backend._connection() as connection:
            async with connection.transaction(force_rollback=True):
                await connection.execute("SET LOCAL plan_cache_mode = force_custom_plan")
                cursor = await connection.execute(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}", parameters
                )
                [[plan]] = (await cursor.fetchone()).values()
        return plan["Plan"]

    async def generic_claim_plan(self, queues: list[str]) -> dict:
        query, parameters = self.backend._claiming(self.clock.now(), queues, "explain", 8, 60.0)
        names: list[str] = []

        def numbered(match: re.Match[str]) -> str:
            if match.group(1) not in names:
                names.append(match.group(1))
            return f"${names.index(match.group(1)) + 1}"

        prepared = re.sub(r"%\((\w+)\)[sb]", numbered, query).replace("%%", "%")
        async with self.backend._connection() as connection:
            async with connection.transaction(force_rollback=True):
                await connection.execute("SET LOCAL plan_cache_mode = force_generic_plan")
                await connection.execute(f"PREPARE generic_claim AS {prepared}")
                cursor = psycopg.AsyncClientCursor(connection)
                arguments = ", ".join(["%s"] * len(names))
                await cursor.execute(
                    f"EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE generic_claim({arguments})",
                    [parameters[name] for name in names],
                )
                [plan] = (await cursor.fetchone()).values()
                await connection.execute("DEALLOCATE generic_claim")
        return plan[0]["Plan"]

    async def claim_touches(self, queues: list[str]) -> int:
        plan = await self.claim_plan(queues)
        return plan["Shared Hit Blocks"] + plan["Shared Read Blocks"]

    async def claim_walk(self, queues: list[str]) -> tuple[int, int]:
        scans: list[dict] = []
        pending = [await self.claim_plan(queues)]
        while pending:
            node = pending.pop()
            pending.extend(node.get("Plans", []))
            if node.get("Index Name", "").endswith("_jobs_claim"):
                scans.append(node)
        widest = max(scans, key=lambda node: node["Actual Loops"])
        return widest["Actual Loops"], widest["Actual Rows"] * widest["Actual Loops"]

    async def test_due_jobs_are_claimed_past_many_delayed_higher_priorities(self):
        later = self.clock.now() + 3600
        delayed = [
            make_job(priority=level, queue=queue, run_at=later)
            for level in range(1, 5001)
            for queue in ("a", "b")
        ]
        await self.backend.enqueue_many(delayed)
        due = [await self.enqueue(priority=0, queue=q) for q in ("a", "b")]
        for queues in (["a"], ["a", "b"]):
            self.assertLess(await self.claim_touches(queues), 2000)
        claimed = await self.claim(limit=10, queues=("a", "b"))
        self.assertCountEqual([job.id for job in claimed], [job.id for job in due])

    async def test_delayed_levels_holding_a_few_jobs_each_are_skipped_in_spans(self):
        later = self.clock.now() + 3600
        levels = 0
        for copies in (3, 8):
            delayed = [
                make_job(priority=levels + level, run_at=later + copy)
                for level in range(1, 1501)
                for copy in range(copies)
            ]
            levels += 1500
            await self.backend.enqueue_many(delayed)
        due = await self.enqueue(priority=0)
        steps, _ = await self.claim_walk(["default"])
        self.assertLess(steps, 600)
        [claimed] = await self.claim(limit=10)
        self.assertEqual(claimed.id, due.id)

    async def test_delayed_levels_holding_many_jobs_each_cost_one_descent_each(self):
        later = self.clock.now() + 3600
        delayed = [
            make_job(priority=level, run_at=later + copy)
            for level in range(1, 301)
            for copy in range(100)
        ]
        await self.backend.enqueue_many(delayed)
        due = await self.enqueue(priority=0)
        _, entries = await self.claim_walk(["default"])
        self.assertLess(entries, 10000)
        [claimed] = await self.claim(limit=10)
        self.assertEqual(claimed.id, due.id)

    async def test_a_generic_plan_of_the_claim_never_scans_the_whole_table(self):
        queues = [f"q{number}" for number in range(50)]
        await self.backend.enqueue_many(
            [make_job(queue=queues[number % 50]) for number in range(20000)]
        )
        await sql(f'ANALYZE "{self.prefix}_jobs"')
        for wanted in (queues[:1], queues):
            pending = [await self.generic_claim_plan(wanted)]
            while pending:
                node = pending.pop()
                pending.extend(node.get("Plans", []))
                self.assertNotEqual(node.get("Node Type"), "Seq Scan", node.get("Relation Name"))

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

    async def test_bulk_enqueues_take_dedup_keys_in_one_order(self):
        keys = [f"key:{n:03d}" for n in range(200)]
        batches = []
        real = self.backend._insert

        async def recorded(connection, jobs, now, due):
            batches.append([job.dedup_key for job in jobs])
            return await real(connection, jobs, now, due)

        jobs = [make_job(dedup_key=key) for key in reversed(keys)]
        with mock.patch.object(self.backend, "_insert", recorded):
            stored = await self.backend.enqueue_many(jobs)
        self.assertEqual(batches, [keys])
        self.assertEqual([job.dedup_key for job in stored], list(reversed(keys)))
        self.assertEqual([job.id for job in stored], [job.id for job in jobs])

    async def test_one_call_with_a_key_twice_stores_one_job_even_if_it_finishes_at_once(self):
        def unreachable(*args):
            raise AssertionError("a key repeated inside one call needs no holder lookup")
            yield

        first, second, third = (make_job(dedup_key="shared") for _ in range(3))
        with mock.patch.object(self.backend, "_holders", unreachable):
            stored = await self.backend.enqueue_many([first, make_job(), second, third])
        self.assertEqual({stored[0].id, stored[2].id, stored[3].id}, {first.id})
        self.assertEqual((await self.backend.counts_by_status())["queued"], 2)

    async def test_bulk_enqueues_racing_on_the_same_keys_never_deadlock(self):
        racers = [await self.make_backend(self.clock) for _ in range(4)]
        for backend in racers:
            await backend.setup()
            self.addAsyncCleanup(backend.close)
        keys = [f"key:{n:03d}" for n in range(200)]

        async def burst(position: int) -> None:
            order = keys if position % 2 else list(reversed(keys))
            await racers[position].enqueue_many([make_job(dedup_key=key) for key in order])

        with self.assertNoLogs("tinyjobs.postgres", "WARNING"):
            for _ in range(15):
                await asyncio.gather(*(burst(position) for position in range(4)))
                await sql(f'UPDATE "{self.prefix}_jobs" SET status = \'cancelled\' WHERE status = \'queued\'')

    async def test_a_failure_message_with_a_nul_byte_is_stored(self):
        job = await self.claimed_one()
        error = ExecutionError(type="ValueError", message="bad frame \x00\x01", traceback="tb \x00")
        self.assertTrue(await self.backend.fail(job, error, None))
        stored = await self.stored(job)
        self.assertEqual(stored.status, JobStatus.FAILED)
        self.assertIn("bad frame \\x00", stored.last_error)
        [execution] = await self.backend.executions(job.id)
        self.assertEqual(execution.traceback, "tb \\x00")

    async def claim_in_flight(self, holder, queues, limit):
        cursor = await holder.execute(
            *self.backend._claiming(self.clock.now(), list(queues), "in-flight", limit, 60.0)
        )
        return [row[0] for row in await cursor.fetchall()]

    async def test_a_claim_in_flight_over_several_queues_leaves_the_next_urgent_job_to_others(self):
        first = await self.enqueue(queue="a", priority=10, run_at=1.0)
        urgent = await self.enqueue(queue="b", priority=10, run_at=2.0)
        await self.enqueue(queue="b", priority=0, run_at=0.5)
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as holder:
            taken = await self.claim_in_flight(holder, ("a", "b"), 1)
            claimed = await self.claim(limit=1, queues=("a", "b"), worker="worker-b")
            await holder.rollback()
        self.assertEqual(taken, [first.id])
        self.assertEqual([job.id for job in claimed], [urgent.id])

    async def test_a_claim_in_flight_over_several_queues_leaves_the_other_queues_to_others(self):
        await self.enqueue(queue="a", run_at=1.0)
        left = await self.enqueue(queue="b", run_at=2.0)
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as holder:
            await self.claim_in_flight(holder, ("a", "b"), 1)
            claimed = await self.claim(limit=1, queues=("a", "b"), worker="worker-b")
            await holder.rollback()
        self.assertEqual([job.id for job in claimed], [left.id])

    async def test_a_claim_in_flight_across_priorities_leaves_what_it_does_not_take(self):
        await self.enqueue(priority=5, run_at=1.0)
        await self.enqueue(run_at=2.0)
        left = await self.enqueue(run_at=3.0)
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as holder:
            taken = await self.claim_in_flight(holder, ("default",), 2)
            claimed = await self.claim(limit=1, worker="worker-b")
            await holder.rollback()
        self.assertEqual(len(taken), 2)
        self.assertEqual([job.id for job in claimed], [left.id])

    async def test_claims_racing_over_several_queues_take_every_due_job_once(self):
        racers = [await self.make_backend(self.clock) for _ in range(6)]
        for backend in racers:
            await backend.setup()
            self.addAsyncCleanup(backend.close)
        jobs = [
            make_job(queue=queue, priority=priority, run_at=float(position))
            for position in range(40)
            for queue, priority in (("a", 0), ("b", 3), ("c", 3))
        ]
        await self.backend.enqueue_many(jobs)
        taken = []

        async def drain(position: int) -> None:
            while True:
                claimed = await racers[position].claim(
                    worker_id=f"racer-{position}", queues=("a", "b", "c"), limit=2,
                    lease_seconds=60.0,
                )
                if not claimed:
                    return
                taken.extend(claimed)

        await asyncio.gather(*(drain(position) for position in range(6)))
        self.assertEqual(sorted(job.id for job in taken), sorted(job.id for job in jobs))
        self.assertEqual((await self.backend.counts_by_status())["running"], len(jobs))

    async def test_notices_on_queues_nobody_waits_for_are_not_kept(self):
        await self.claim(queues=("default",))
        for n in range(50):
            self.backend._hear(f"noise-{n}")
        self.backend._hear("default")
        self.assertEqual(set(self.backend._heard), {"default"})

    async def test_setup_can_be_tried_again_after_the_server_was_unreachable(self):
        backend = PostgresBackend.from_url(
            "postgresql://postgres@127.0.0.1:1/tinyjobs_test",
            prefix=self.prefix,
            clock=self.clock,
            pool_timeout=0.5,
        )
        self.addAsyncCleanup(backend.close)
        with self.assertRaises(PoolTimeout):
            await backend.setup()
        backend._conninfo = POSTGRES_URL
        await backend.setup()
        job = await backend.enqueue(make_job())
        self.assertEqual((await backend.get(job.id)).id, job.id)

    async def test_tables_split_across_schemas_by_the_search_path_are_refused(self):
        shadow = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE SCHEMA "{shadow}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA "{shadow}" CASCADE')
        await sql(f'CREATE TABLE "{shadow}"."{self.prefix}_jobs" (id text)')
        backend = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, clock=self.clock,
            options=f"-c search_path={shadow},public",
        )
        self.addAsyncCleanup(backend.close)
        with self.assertRaisesRegex(ConfigurationError, "more than one schema"):
            await backend.setup()

    async def role(self) -> str:
        role = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD 'tinyjobs'")
        self.addAsyncCleanup(sql, f'DROP ROLE "{role}"')
        self.addAsyncCleanup(sql, f'DROP OWNED BY "{role}"')
        self.addAsyncCleanup(sql, f'REASSIGN OWNED BY "{role}" TO CURRENT_USER')
        return role

    async def hand_tables_to(self, owner: str, user: str | None = None) -> None:
        for table in ("jobs", "executions", "schedules", "meta"):
            await sql(f'ALTER TABLE "{self.prefix}_{table}" OWNER TO "{owner}"')
            if user:
                await sql(f'GRANT ALL ON "{self.prefix}_{table}" TO "{user}"')
        if user:
            await sql(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{user}"')

    async def set_up_as(self, user: str | None, **options):
        url = POSTGRES_URL if user is None else url_for(user=user, password="tinyjobs")
        backend = PostgresBackend.from_url(url, prefix=self.prefix, clock=self.clock, **options)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        return backend

    async def test_tables_another_role_owns_are_refused_even_to_a_superuser(self):
        owner = await self.role()
        await self.hand_tables_to(owner)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await self.set_up_as(None)

    async def test_tables_owned_by_a_role_the_user_was_made_a_member_of_are_refused(self):
        owner, user = await self.role(), await self.role()
        await sql(f'GRANT "{owner}" TO "{user}"')
        await self.hand_tables_to(owner, user)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await self.set_up_as(user)

    async def test_tables_owned_by_a_role_that_holds_the_users_privileges_are_used(self):
        owner, user = await self.role(), await self.role()
        await sql(f'GRANT "{user}" TO "{owner}"')
        await self.hand_tables_to(owner, user)
        backend = await self.set_up_as(user)
        job = await backend.enqueue(make_job())
        self.assertEqual((await backend.get(job.id)).id, job.id)

    async def test_tables_whose_owner_passes_its_rights_to_a_role_that_cannot_become_the_user_are_refused(
        self,
    ):
        [(version,)] = await sql("SHOW server_version_num")
        if int(version) < 160000:
            self.skipTest("INHERIT without SET on role grants arrived in Postgres 16")
        owner, user, heir = await self.role(), await self.role(), await self.role()
        await sql(f'GRANT "{user}" TO "{owner}" WITH INHERIT FALSE, SET TRUE')
        await self.hand_tables_to(owner, user)
        await self.set_up_as(user)
        await sql(f'GRANT "{owner}" TO "{heir}" WITH INHERIT TRUE, SET FALSE')
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await self.set_up_as(user)
        async with await psycopg.AsyncConnection.connect(
            url_for(user=user, password="tinyjobs")
        ) as connection:
            with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                await self.backend.aenqueue_in(connection, [make_job()])
        await sql(f'REVOKE "{owner}" FROM "{heir}"')
        await sql(f'GRANT "{owner}" TO "{heir}" WITH INHERIT TRUE, SET TRUE')
        await self.set_up_as(user)

    async def test_a_schema_another_role_owns_is_refused(self):
        owner = await self.role()
        schema = f"tj_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"')
        self.addAsyncCleanup(drop, self.prefix, schema)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await self.set_up_as(None, schema=schema)

    async def test_tables_created_later_earlier_on_the_search_path_do_not_take_the_jobs(self):
        shadow = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        backend = await self.set_up_as(None, options=f"-c search_path={shadow},public")
        await sql(f'CREATE SCHEMA "{shadow}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA "{shadow}" CASCADE')
        for table in ("jobs", "executions", "schedules", "meta"):
            await sql(
                f'CREATE TABLE "{shadow}"."{self.prefix}_{table}" '
                f'(LIKE "public"."{self.prefix}_{table}" INCLUDING ALL)'
            )
        job = await backend.enqueue(make_job())
        self.assertEqual((await backend.get(job.id)).id, job.id)
        [(kept,)] = await sql(f'SELECT count(*) FROM "public"."{self.prefix}_jobs" WHERE id = %(id)s', {"id": job.id})
        [(diverted,)] = await sql(f'SELECT count(*) FROM "{shadow}"."{self.prefix}_jobs"')
        self.assertEqual((kept, diverted), (1, 0))

    async def test_a_store_missing_a_table_says_so(self):
        await sql(f'DROP TABLE "{self.prefix}_executions"')
        with self.assertRaisesRegex(ConfigurationError, "missing"):
            await self.set_up_as(None)

    async def test_a_schema_name_with_placeholder_characters_still_works(self):
        schema = f"tj%s{{}}%(now)s_{uuid.uuid4().hex[:8]}"
        quoted = '"' + schema + '"'
        await sql(f"CREATE SCHEMA {quoted}")
        self.addAsyncCleanup(sql, f"DROP SCHEMA {quoted} CASCADE")
        backend = await self.set_up_as(None, options=f"-c search_path={quoted}")
        job = await backend.enqueue(make_job(queue="mail"))
        page = await backend.list(queue="mail")
        self.assertEqual([listed.id for listed in page.items], [job.id])
        async with await psycopg.AsyncConnection.connect(
            POSTGRES_URL, options=f"-c search_path={quoted}"
        ) as connection:
            [joined] = await backend.aenqueue_in(connection, [make_job()])
            await connection.commit()
        self.assertEqual((await backend.get(joined.id)).id, joined.id)

    async def trap_schema(self) -> str:
        schema = f"tj_trap_{uuid.uuid4().hex[:10]}"
        await sql(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA "{schema}" CASCADE')
        await sql(f'CREATE TABLE "{schema}".calls (what text)')
        await sql(f'GRANT INSERT ON "{schema}".calls TO PUBLIC')
        await sql(f'GRANT USAGE ON SCHEMA "{schema}" TO PUBLIC')
        return schema

    async def calls(self, schema: str) -> list[str]:
        return [what for (what,) in await sql(f'SELECT what FROM "{schema}".calls')]

    async def test_functions_and_operators_planted_on_the_search_path_never_run(self):
        trap = await self.trap_schema()
        await sql(
            f'CREATE FUNCTION "{trap}".unnest(text[]) RETURNS SETOF text LANGUAGE sql AS '
            f"$$ INSERT INTO \"{trap}\".calls VALUES ('unnest'); SELECT pg_catalog.unnest($1) $$"
        )
        await sql(
            f'CREATE FUNCTION "{trap}".at_most(double precision, double precision) '
            f"RETURNS boolean LANGUAGE sql AS $$ INSERT INTO \"{trap}\".calls VALUES ('<='); "
            "SELECT $1 OPERATOR(pg_catalog.<=) $2 $$"
        )
        await sql(
            f'CREATE OPERATOR "{trap}".<= (LEFTARG = double precision, '
            f'RIGHTARG = double precision, FUNCTION = "{trap}".at_most)'
        )
        await sql(
            f'CREATE FUNCTION "{trap}".same(text, text) RETURNS boolean LANGUAGE sql AS '
            f"$$ INSERT INTO \"{trap}\".calls VALUES ('='); SELECT $1 OPERATOR(pg_catalog.=) $2 $$"
        )
        await sql(
            f'CREATE OPERATOR "{trap}".= (LEFTARG = text, RIGHTARG = text, '
            f'FUNCTION = "{trap}".same)'
        )
        await sql(
            f'CREATE FUNCTION "{trap}".noted(anyelement) RETURNS boolean LANGUAGE sql AS '
            f"$$ INSERT INTO \"{trap}\".calls VALUES ('domain'); SELECT true $$"
        )
        for kind in ("text", "int2", "int4", "int8", "float8", "bytea", "bool", "timestamptz"):
            await sql(
                f'CREATE DOMAIN "{trap}"."{kind}" AS pg_catalog."{kind}" '
                f'CHECK ("{trap}".noted(VALUE))'
            )
        path = f"-c search_path={trap},pg_catalog,public"
        backend = await self.set_up_as(None, options=path)
        job = await backend.enqueue(make_job(queue="a", dedup_key="k"))
        await backend.enqueue(make_job(queue="b", dedup_key="k"))
        [claimed] = await backend.claim(
            worker_id="w", queues=["a", "b"], limit=5, lease_seconds=30.0
        )
        self.assertTrue(await backend.complete(claimed, None))
        await backend.reap_expired()
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL, options=path) as connection:
            await backend.aenqueue_in(connection, [make_job(dedup_key="t"), make_job(dedup_key="t")])
            for _ in range(2):
                await backend.aenqueue_in(connection, [make_job(), make_job()])
            await connection.commit()
        with psycopg.connect(POSTGRES_URL, options=path) as connection:
            for _ in range(3):
                backend.enqueue_in(connection, [make_job(), make_job()])
            connection.commit()
        many = postgres_backend.ENQUEUE_BATCH + 1
        for piped in (False, True):
            with psycopg.connect(
                POSTGRES_URL, options=path, cursor_factory=psycopg.ClientCursor
            ) as connection:
                with connection.pipeline() if piped else nullcontext():
                    backend.enqueue_in(connection, [make_job()])
                    backend.enqueue_in(connection, [make_job() for _ in range(many)])
                connection.commit()
            async with await psycopg.AsyncConnection.connect(
                POSTGRES_URL, options=path, cursor_factory=psycopg.AsyncClientCursor
            ) as connection:
                async with connection.pipeline() if piped else nullcontext():
                    await backend.aenqueue_in(connection, [make_job()])
                    await backend.aenqueue_in(connection, [make_job() for _ in range(many)])
                await connection.commit()
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(await self.calls(trap), [])

    async def test_a_meta_table_another_role_planted_is_never_read(self):
        squatter = await self.role()
        schema = await self.trap_schema()
        await sql(f'GRANT CREATE ON SCHEMA "{schema}" TO "{squatter}"')
        prefix = new_prefix()
        async with await psycopg.AsyncConnection.connect(
            url_for(user=squatter, password="tinyjobs"), autocommit=True
        ) as connection:
            await connection.execute(
                f'CREATE FUNCTION "{schema}".leak() RETURNS TABLE (key text, value text) '
                f"LANGUAGE sql AS $$ INSERT INTO \"{schema}\".calls VALUES ('read'); "
                "SELECT 'schema_version'::text, '1'::text $$"
            )
            await connection.execute(
                f'CREATE VIEW "{schema}"."{prefix}_meta" AS SELECT * FROM "{schema}".leak()'
            )
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix, schema=schema)
        self.addAsyncCleanup(backend.close)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await backend.setup()
        self.assertEqual(await self.calls(schema), [])

    async def database_owned_by(self, owner: str) -> str:
        name = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE DATABASE "{name}" OWNER "{owner}"')
        self.addAsyncCleanup(sql, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        return name

    async def test_a_sql_ascii_database_takes_jobs_from_the_pool_and_through_a_utf8_tx(self):
        name = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(
            f"CREATE DATABASE \"{name}\" ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' "
            "TEMPLATE template0"
        )
        self.addAsyncCleanup(sql, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        backend = PostgresBackend.from_url(url_for(dbname=name), prefix=self.prefix)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        pooled = await backend.enqueue(make_job(task="café.task"))
        async with await psycopg.AsyncConnection.connect(url_for(dbname=name)) as connection:
            with self.assertRaisesRegex(ConfigurationError, "client_encoding=UTF8"):
                await backend.aenqueue_in(connection, [make_job()])
        async with await psycopg.AsyncConnection.connect(
            url_for(dbname=name), client_encoding="UTF8"
        ) as connection:
            [joined] = await backend.aenqueue_in(connection, [make_job(task="naïve.task")])
            await connection.commit()
        for job in (pooled, joined):
            self.assertEqual((await backend.get(job.id)).task, job.task)

    async def test_a_database_owner_without_the_users_privileges_is_not_trusted(self):
        owner = await self.role()
        name = await self.database_owned_by(owner)
        theirs = PostgresBackend.from_url(
            url_for(dbname=name, user=owner, password="tinyjobs"),
            prefix=self.prefix,
            schema="theirs",
        )
        self.addAsyncCleanup(theirs.close)
        await theirs.setup()
        mine = PostgresBackend.from_url(
            url_for(dbname=name), prefix=self.prefix, schema="theirs"
        )
        self.addAsyncCleanup(mine.close)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await mine.setup()

    async def test_a_role_that_may_not_read_the_cluster_id_still_sets_up_and_joins(self):
        owner = await self.role()
        name = await self.database_owned_by(owner)
        async with await psycopg.AsyncConnection.connect(url_for(dbname=name), autocommit=True) as admin:
            await admin.execute("REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC")
        theirs = url_for(dbname=name, user=owner, password="tinyjobs")
        backend = PostgresBackend.from_url(theirs, prefix=self.prefix)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        async with await psycopg.AsyncConnection.connect(theirs) as connection:
            [joined] = await backend.aenqueue_in(connection, [make_job()])
            await connection.commit()
        self.assertEqual((await backend.get(joined.id)).id, joined.id)

    async def test_an_empty_dedup_key_is_a_key_like_any_other(self):
        with mock.patch.object(
            PostgresBackend, "_holders", autospec=True, side_effect=PostgresBackend._holders
        ) as holders:
            first, second = await self.backend.enqueue_many(
                [make_job(id="first", dedup_key=""), make_job(id="second", dedup_key="")]
            )
        holders.assert_not_called()
        later = await self.backend.enqueue(make_job(id="third", dedup_key=""))
        self.assertEqual([first.id, second.id, later.id], ["first", "first", "first"])

    async def test_a_close_during_a_wake_up_leaves_no_wake_up_behind(self):
        backend = await self.open()
        original = backend._send

        async def slow(queues):
            await asyncio.sleep(0.05)
            await original(queues)

        await backend.enqueue(make_job(queue="a"))
        backend._send = slow
        enqueue = asyncio.ensure_future(
            backend.enqueue_many([make_job(queue="a"), make_job(queue="b")])
        )
        await asyncio.sleep(0.03)
        await backend.close()
        await enqueue
        self.assertIsNone(backend._waking)

    async def test_connections_the_server_closed_while_idle_are_never_handed_out(self):
        name = f"tinyjobs-{uuid.uuid4().hex[:8]}"
        backend = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, application_name=name, min_connections=8
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        await asyncio.gather(*(backend.get("missing") for _ in range(8)))
        live = "SELECT count(*) FROM pg_stat_activity WHERE application_name = %(name)s"
        await sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE application_name = %(name)s",
            {"name": name},
        )
        while (await sql(live, {"name": name}))[0][0]:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        started = time.monotonic()
        stored = [await backend.enqueue(make_job()) for _ in range(3)]
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(len({job.id for job in stored}), 3)

    async def test_a_read_cut_off_by_a_lost_connection_is_read_again(self):
        backend = await self.open()
        job = await backend.enqueue(make_job())
        original = backend._rows
        raised: list[Exception] = []

        async def losing(*args, **kwargs):
            if not raised:
                raised.append(failure)
                raise failure
            return await original(*args, **kwargs)

        backend._rows = losing
        for failure in (
            psycopg.errors.AdminShutdown("terminating connection"),
            psycopg.OperationalError("server closed the connection unexpectedly"),
        ):
            raised.clear()
            self.assertEqual((await backend.get(job.id)).id, job.id)
        raised.clear()
        failure = psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
        with self.assertRaises(psycopg.errors.QueryCanceled):
            await backend.get(job.id)

    async def test_a_read_waits_for_the_pool_only_once(self):
        backend = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, max_connections=1, pool_timeout=0.3
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        async with backend._connection():
            started = time.monotonic()
            with self.assertRaises(PoolTimeout):
                await backend.get("missing")
            self.assertLess(time.monotonic() - started, 0.55)

    async def test_a_misspelled_connection_option_is_refused_at_once(self):
        with self.assertRaisesRegex(ConfigurationError, "max_conections"):
            PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, max_conections=4)
        with self.assertRaisesRegex(ConfigurationError, "cursor_factory"):
            PostgresBackend.from_url(
                POSTGRES_URL, prefix=self.prefix, cursor_factory=psycopg.ClientCursor
            )
        with self.assertRaisesRegex(ConfigurationError, "max_conections"):
            TinyJobs(POSTGRES_URL, prefix=self.prefix, max_conections=4)
        with self.assertRaisesRegex(ConfigurationError, "max_connections"):
            TinyJobs(f"{POSTGRES_URL}?max_connections=4", prefix=self.prefix)
        for name in ("conninfo", "connection_options"):
            with self.assertRaisesRegex(ConfigurationError, name):
                TinyJobs(POSTGRES_URL, prefix=self.prefix, **{name: "x"})
        PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, connect_timeout=5, prepare_threshold=None
        )
        TinyJobs(POSTGRES_URL, prefix=self.prefix, max_connections=4, sslmode="disable")

    async def test_a_relation_squatting_an_index_name_stops_setup(self):
        prefix = new_prefix()
        self.addAsyncCleanup(sql, f'DROP TABLE IF EXISTS "{prefix}_jobs_dedup"')
        self.addAsyncCleanup(drop, prefix)
        await sql(f'CREATE TABLE "{prefix}_jobs_dedup" (id int)')
        squatted = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        self.addAsyncCleanup(squatted.close)
        with self.assertRaisesRegex(ConfigurationError, f"{prefix}_jobs_dedup"):
            await squatted.setup()
        await sql(f'DROP TABLE "{prefix}_jobs_dedup"')
        await squatted.setup()
        await self.assertUniqueKeysHold(squatted)

    async def test_setup_rebuilds_a_tinyjobs_index_that_was_dropped_or_left_invalid(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        await backend.enqueue(make_job(dedup_key="kept"))
        await sql(f'DROP INDEX "{prefix}_jobs_claim"')
        await sql(
            "UPDATE pg_index SET indisvalid = false "
            f"WHERE indexrelid = '\"{prefix}_jobs_dedup\"'::regclass"
        )
        for _ in range(2):
            again = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
            self.addAsyncCleanup(again.close)
            await again.setup()
        rows = await sql(
            "SELECT c.relname, i.indisvalid FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indexrelid "
            f"WHERE c.relname IN ('{prefix}_jobs_claim', '{prefix}_jobs_dedup')"
        )
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row[1] for row in rows))
        await self.assertUniqueKeysHold(again)

    async def assertUniqueKeysHold(self, backend: PostgresBackend) -> None:
        first = await backend.enqueue(make_job(dedup_key="held"))
        second = await backend.enqueue(make_job(dedup_key="held"))
        self.assertEqual(first.id, second.id)

    async def test_a_schema_the_role_owns_is_used_without_create_on_the_database(self):
        role = await self.role()
        schema = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        backend = PostgresBackend.from_url(
            url_for(user=role, password="tinyjobs"), prefix=self.prefix, schema=schema
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        stored = await backend.enqueue(make_job())
        self.assertEqual((await backend.get(stored.id)).id, stored.id)

    async def test_tx_never_writes_through_tables_the_callers_role_does_not_trust(self):
        owner = await self.role()
        name = await self.database_owned_by(owner)
        backend = PostgresBackend.from_url(
            url_for(dbname=name, user=owner, password="tinyjobs"),
            prefix=self.prefix,
            schema="theirs",
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        async with await psycopg.AsyncConnection.connect(url_for(dbname=name)) as connection:
            with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                await backend.aenqueue_in(connection, [make_job()])
        with psycopg.Connection.connect(url_for(dbname=name)) as connection:
            with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                backend.enqueue_in(connection, [make_job()])

    async def test_tx_follows_the_store_when_its_cluster_changes_under_a_running_app(self):
        instance, database, _ = self.backend._identity
        self.backend._identity = (instance, "stale", "stale")
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            [joined] = await self.backend.aenqueue_in(connection, [make_job()])
            await connection.commit()
        self.assertEqual(self.backend._identity[:2], (instance, database))
        self.backend._identity = (instance, "stale", "stale")
        self.backend._joined.clear()
        with psycopg.Connection.connect(POSTGRES_URL) as connection:
            [synced] = self.backend.enqueue_in(connection, [make_job()])
            connection.commit()
        self.assertEqual(self.backend._identity[:2], (instance, database))
        self.backend._identity = ("another", database, None)
        self.backend._joined.clear()
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            with self.assertRaisesRegex(ConfigurationError, "does not reach"):
                await self.backend.aenqueue_in(connection, [make_job()])
        for job in (joined, synced):
            self.assertIsNotNone(await self.backend.get(job.id))

    async def test_a_tx_refusal_does_not_wait_for_the_apps_own_pool(self):
        backend = PostgresBackend.from_url(
            POSTGRES_URL, prefix=self.prefix, max_connections=1, pool_timeout=2.0
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        instance, _, _ = backend._identity
        async with backend._connection():
            backend._identity = ("another", "stale", None)
            async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
                started = time.monotonic()
                with self.assertRaisesRegex(ConfigurationError, "does not reach"):
                    await backend.aenqueue_in(connection, [make_job()])
                self.assertLess(time.monotonic() - started, 1.5)

    async def test_tx_works_on_connections_whose_cursors_bind_raw(self):
        async with await psycopg.AsyncConnection.connect(
            POSTGRES_URL, cursor_factory=psycopg.AsyncRawCursor
        ) as connection:
            [joined] = await self.backend.aenqueue_in(connection, [make_job()])
            await connection.commit()
        with psycopg.Connection.connect(POSTGRES_URL, cursor_factory=psycopg.RawCursor) as connection:
            [synced] = self.backend.enqueue_in(connection, [make_job()])
            connection.commit()
        for job in (joined, synced):
            self.assertIsNotNone(await self.backend.get(job.id))

    async def test_a_session_the_server_closed_while_idle_is_written_again(self):
        await self.enqueue()
        original = self.backend._rows
        raised: list[Exception] = []

        async def closing(*args, **kwargs):
            if not raised:
                raised.append(failure)
                raise failure
            return await original(*args, **kwargs)

        failure = psycopg.errors.IdleSessionTimeout("terminating connection due to idle-session timeout")
        self.backend._rows = closing
        with self.assertLogs("tinyjobs.postgres", "WARNING"):
            [claimed] = await self.claim()
        self.assertEqual(claimed.status, JobStatus.RUNNING)

    async def test_a_meta_table_planted_while_setup_creates_the_store_is_never_written(self):
        role = await self.role()
        await sql(f'GRANT CREATE ON SCHEMA public TO "{role}"')
        self.addAsyncCleanup(sql, f'REVOKE CREATE ON SCHEMA public FROM "{role}"')
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        original = Tables.ddl

        def planting(tables, **options):
            with psycopg.Connection.connect(POSTGRES_URL, autocommit=True) as attacker:
                attacker.execute(f'SET ROLE "{role}"')
                attacker.execute(
                    f'CREATE TABLE public."{prefix}_meta" (key text PRIMARY KEY, value text NOT NULL)'
                )
                attacker.execute(f'GRANT ALL ON public."{prefix}_meta" TO PUBLIC')
            return original(tables, **options)

        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        self.addAsyncCleanup(backend.close)
        with mock.patch.object(Tables, "ddl", planting):
            with self.assertRaisesRegex(ConfigurationError, "does not hold"):
                await backend.setup()
        self.assertEqual(await sql(f'SELECT count(*) FROM public."{prefix}_meta"'), [(0,)])

    async def test_tx_checks_the_role_on_every_call(self):
        owner = await self.role()
        name = await self.database_owned_by(owner)
        backend = PostgresBackend.from_url(
            url_for(dbname=name, user=owner, password="tinyjobs"),
            prefix=self.prefix,
            schema="theirs",
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        async with await psycopg.AsyncConnection.connect(url_for(dbname=name)) as connection:
            await connection.execute(f'SET ROLE "{owner}"')
            await backend.aenqueue_in(connection, [make_job()])
            await connection.execute("RESET ROLE")
            with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                await backend.aenqueue_in(connection, [make_job()])

    async def store_with_owner_code(self) -> tuple[PostgresBackend, str, str]:
        owner = await self.role()
        name = await self.database_owned_by(owner)
        backend = PostgresBackend.from_url(
            url_for(dbname=name, user=owner, password="tinyjobs"),
            prefix=self.prefix,
            schema="theirs",
        )
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        jobs = f'theirs."{self.prefix}_jobs"'
        with psycopg.Connection.connect(
            url_for(dbname=name, user=owner, password="tinyjobs"), autocommit=True
        ) as planter:
            planter.execute(
                "CREATE FUNCTION theirs.fired() RETURNS trigger LANGUAGE plpgsql AS "
                "$$BEGIN RAISE WARNING 'owner code ran as %', current_user; RETURN NULL; END$$"
            )
            planter.execute(
                "CREATE FUNCTION theirs.planned() RETURNS integer LANGUAGE plpgsql IMMUTABLE AS "
                "$$BEGIN RAISE WARNING 'owner code ran as %', current_user; RETURN 1; END$$"
            )
            for timing in ("BEFORE", "AFTER"):
                planter.execute(
                    f"CREATE TRIGGER {timing.lower()}_statement {timing} INSERT ON {jobs} "
                    "FOR EACH STATEMENT EXECUTE FUNCTION theirs.fired()"
                )
            planter.execute(f"ALTER TABLE {jobs} ADD COLUMN planted integer DEFAULT theirs.planned()")
        return backend, name, owner

    async def test_a_role_change_never_runs_the_tables_owners_code(self):
        backend, name, owner = await self.store_with_owner_code()
        factories = (
            (psycopg.Cursor, psycopg.AsyncCursor),
            (psycopg.ClientCursor, psycopg.AsyncClientCursor),
            (psycopg.RawCursor, psycopg.AsyncRawCursor),
        )
        for (sync_factory, async_factory), autocommit, piped in itertools.product(
            factories, (False, True), (False, True)
        ):
            with self.subTest(sync_factory.__name__, autocommit=autocommit, piped=piped):
                ran: list[str] = []
                with psycopg.Connection.connect(
                    url_for(dbname=name), autocommit=autocommit, cursor_factory=sync_factory
                ) as connection, connection.pipeline() if piped else nullcontext():
                    connection.add_notice_handler(lambda notice: ran.append(notice.message_primary))
                    with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                        backend.enqueue_in(connection, [make_job()])
                    self.assertEqual(ran, [])
                    connection.rollback()
                    connection.execute(f'SET ROLE "{owner}"')
                    backend.enqueue_in(connection, [make_job()])
                    self.assertIn(f"owner code ran as {owner}", ran)
                    connection.execute("RESET ROLE")
                    ran.clear()
                    with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                        backend.enqueue_in(connection, [make_job()])
                    self.assertEqual(ran, [])
                async with await psycopg.AsyncConnection.connect(
                    url_for(dbname=name), autocommit=autocommit, cursor_factory=async_factory
                ) as connection:
                    connection.add_notice_handler(lambda notice: ran.append(notice.message_primary))
                    async with connection.pipeline() if piped else nullcontext():
                        with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                            await backend.aenqueue_in(connection, [make_job()])
                        self.assertEqual(ran, [])
                        await connection.rollback()
                        await connection.execute(f'SET ROLE "{owner}"')
                        await backend.aenqueue_in(connection, [make_job()])
                        self.assertIn(f"owner code ran as {owner}", ran)
                        await connection.execute("RESET ROLE")
                        ran.clear()
                        with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                            await backend.aenqueue_in(connection, [make_job()])
                        self.assertEqual(ran, [])

    async def test_a_role_reset_racing_an_enqueue_never_runs_the_owners_code_as_the_new_role(self):
        backend, name, owner = await self.store_with_owner_code()
        batch = postgres_backend.ENQUEUE_BATCH
        for autocommit, piped, joined, count in itertools.product(
            (False, True), (False, True), (False, True), (1, batch + 1)
        ):
            with self.subTest(autocommit=autocommit, piped=piped, joined=joined, count=count):
                ran: list[str] = []
                async with await psycopg.AsyncConnection.connect(
                    url_for(dbname=name), autocommit=autocommit
                ) as connection:
                    connection.add_notice_handler(lambda notice: ran.append(notice.message_primary))
                    await connection.execute(f'SET ROLE "{owner}"')
                    if joined:
                        await backend.aenqueue_in(connection, [make_job()])
                    async with connection.pipeline() if piped else nullcontext():
                        await asyncio.gather(
                            backend.aenqueue_in(connection, [make_job() for _ in range(count)]),
                            connection.execute("RESET ROLE"),
                            return_exceptions=True,
                        )
                self.assertEqual(
                    [line for line in ran if not line.endswith(f"ran as {owner}")], []
                )

    async def test_a_role_reset_from_another_thread_never_lands_between_the_check_and_the_insert(
        self,
    ):
        backend, name, owner = await self.store_with_owner_code()
        resets: list[threading.Thread] = []

        class Racing(psycopg.Connection):
            racing = False

            def wait(self, *args, **kwargs):
                result = super().wait(*args, **kwargs)
                if self.racing:
                    self.racing = False
                    reset = threading.Thread(target=self.execute, args=("RESET ROLE",))
                    resets.append(reset)
                    reset.start()
                    reset.join(0.2)
                return result

        batch = postgres_backend.ENQUEUE_BATCH
        for autocommit, piped, count in itertools.product((False, True), (False, True), (1, batch + 1)):
            with self.subTest(autocommit=autocommit, piped=piped, count=count):
                ran: list[str] = []
                with Racing.connect(url_for(dbname=name), autocommit=autocommit) as connection:
                    connection.add_notice_handler(lambda notice: ran.append(notice.message_primary))
                    connection.execute(f'SET ROLE "{owner}"')
                    backend.enqueue_in(connection, [make_job()])
                    with connection.pipeline() if piped else nullcontext():
                        connection.racing = True
                        with suppress(psycopg.Error, ConfigurationError):
                            backend.enqueue_in(connection, [make_job() for _ in range(count)])
                    for reset in resets:
                        reset.join()
                self.assertEqual(
                    [line for line in ran if not line.endswith(f"ran as {owner}")], []
                )

    async def test_code_another_role_owns_on_the_tables_is_refused_wherever_it_hides(self):
        planter = await self.role()
        await sql(f'CREATE SCHEMA "{planter}" AUTHORIZATION "{planter}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA IF EXISTS "{planter}" CASCADE')
        await sql(
            f'SET ROLE "{planter}"; '
            f'CREATE FUNCTION "{planter}".fine(text) RETURNS boolean LANGUAGE sql IMMUTABLE '
            "AS $$SELECT true$$; "
            f'CREATE FUNCTION "{planter}".same(text, text) RETURNS boolean LANGUAGE sql '
            "IMMUTABLE AS $$SELECT $1 = $2$$; "
            f'CREATE OPERATOR "{planter}".=== (leftarg = text, rightarg = text, '
            f'function = "{planter}".same); '
            f'CREATE DOMAIN "{planter}".word AS text CHECK ("{planter}".fine(VALUE)); '
            f'CREATE FUNCTION "{planter}".order_of(text, text) RETURNS integer LANGUAGE sql '
            "IMMUTABLE AS $$SELECT pg_catalog.bttextcmp($1, $2)$$"
        )
        await sql(
            f'CREATE OPERATOR CLASS "{self.prefix}_order" FOR TYPE text USING btree AS '
            f'FUNCTION 1 "{planter}".order_of(text, text)'
        )
        self.addAsyncCleanup(
            sql, f'DROP OPERATOR FAMILY IF EXISTS "{self.prefix}_order" USING btree CASCADE'
        )
        await sql(f'CREATE DOMAIN "{self.prefix}_word" AS text CHECK ("{planter}".fine(VALUE))')
        self.addAsyncCleanup(sql, f'DROP DOMAIN IF EXISTS "{self.prefix}_word" CASCADE')
        jobs = f'"{self.prefix}_jobs"'
        await self.backend.setup()
        for name, planted, removed in (
            (
                "check",
                f'ALTER TABLE {jobs} ADD CONSTRAINT planted CHECK ("{planter}".fine(id))',
                f"ALTER TABLE {jobs} DROP CONSTRAINT planted",
            ),
            (
                "operator",
                f'ALTER TABLE {jobs} ADD CONSTRAINT planted CHECK (id OPERATOR("{planter}".===) id)',
                f"ALTER TABLE {jobs} DROP CONSTRAINT planted",
            ),
            (
                "default",
                f'ALTER TABLE {jobs} ADD COLUMN planted boolean DEFAULT "{planter}".fine(\'x\')',
                f"ALTER TABLE {jobs} DROP COLUMN planted",
            ),
            (
                "domain",
                f'ALTER TABLE {jobs} ADD COLUMN planted "{planter}".word',
                f"ALTER TABLE {jobs} DROP COLUMN planted",
            ),
            (
                "trusted domain",
                f'ALTER TABLE {jobs} ADD COLUMN planted "{self.prefix}_word"',
                f"ALTER TABLE {jobs} DROP COLUMN planted",
            ),
            (
                "index",
                f'CREATE INDEX planted ON {jobs} ("{planter}".fine(task))',
                "DROP INDEX planted",
            ),
            (
                "operator class",
                f'CREATE INDEX planted ON {jobs} (task "{self.prefix}_order")',
                "DROP INDEX planted",
            ),
            (
                "policy",
                f'CREATE POLICY planted ON {jobs} USING ("{planter}".fine(id))',
                f"DROP POLICY planted ON {jobs}",
            ),
        ):
            with self.subTest(site=name):
                await sql(planted)
                try:
                    async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
                        with self.assertRaisesRegex(ConfigurationError, "never writes through"):
                            await self.backend.aenqueue_in(connection, [make_job()])
                    again = await self.make_backend(self.clock)
                    self.addAsyncCleanup(again.close)
                    with self.assertRaisesRegex(ConfigurationError, "drop that code"):
                        await again.setup()
                finally:
                    await sql(removed)
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            await self.backend.aenqueue_in(connection, [make_job()])
            await connection.commit()

    async def test_a_dedup_key_longer_than_an_index_row_still_dedups(self):
        key = os.urandom(3000).hex()
        first = await self.backend.enqueue(make_job(dedup_key=key))
        again = await self.backend.enqueue(make_job(dedup_key=key))
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            [inside] = await self.backend.aenqueue_in(connection, [make_job(dedup_key=key)])
            await connection.commit()
        self.assertEqual({again.id, inside.id}, {first.id})
        self.assertEqual((await self.backend.get(first.id)).dedup_key, key)

    async def test_queue_names_longer_than_an_index_row_are_stored_claimed_and_listed(self):
        long = os.urandom(3000).hex()
        twin = long[:-1] + ("0" if long[-1] != "0" else "1")
        first = await self.backend.enqueue(make_job(queue=long))
        await self.backend.enqueue(make_job(queue=twin))
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            [inside] = await self.backend.aenqueue_in(connection, [make_job(queue=long)])
            await connection.commit()
        listed = await self.backend.list(queue=long)
        self.assertEqual({job.id for job in listed.items}, {first.id, inside.id})
        claimed = await self.backend.claim(
            worker_id="w", queues=[long], limit=10, lease_seconds=30.0
        )
        self.assertEqual({job.id for job in claimed}, {first.id, inside.id})
        [other] = await self.backend.claim(
            worker_id="w", queues=[long, twin], limit=10, lease_seconds=30.0
        )
        self.assertEqual(other.queue, twin)

    async def test_large_payloads_are_sent_in_batches_bounded_by_bytes(self):
        jobs = [make_job(payload=os.urandom(300)) for _ in range(10)]
        with (
            mock.patch.object(postgres_backend, "ENQUEUE_BATCH_BYTES", 1000),
            mock.patch.object(
                psycopg.AsyncCursor,
                "_execute_gen",
                autospec=True,
                side_effect=psycopg.AsyncCursor._execute_gen,
            ) as executed,
        ):
            stored = await self.backend.enqueue_many(jobs)
        inserts = [call for call in executed.call_args_list if "INSERT INTO" in str(call.args[1])]
        self.assertEqual(len(inserts), 4)
        self.assertEqual([job.id for job in stored], [job.id for job in jobs])
        for job in jobs:
            self.assertEqual((await self.backend.get(job.id)).payload, job.payload)

    async def test_an_autocommit_tx_with_many_batches_stores_all_or_nothing(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        await sql(f'ALTER TABLE "{prefix}_jobs" ADD CONSTRAINT refused CHECK (task <> \'tests.boom\')')

        def jobs() -> list:
            return [make_job() for _ in range(2 * postgres_backend.ENQUEUE_BATCH + 500)] + [
                make_job(id="z" * 26, task="tests.boom")
            ]

        with psycopg.Connection.connect(POSTGRES_URL, autocommit=True) as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                backend.enqueue_in(connection, jobs())
            self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL, autocommit=True) as connection:
            with self.assertRaises(psycopg.errors.CheckViolation):
                await backend.aenqueue_in(connection, jobs())
        self.assertEqual(await backend.counts_by_status(), {})

    async def test_refusals_on_an_autocommit_connection_leave_later_calls_working(self):
        backend, name, owner = await self.store_with_owner_code()
        rounds = (True,) * 4 + (False,) * 2 + (True,) * 3
        with psycopg.Connection.connect(url_for(dbname=name), autocommit=True) as connection:
            for accepted in rounds:
                connection.execute(f'SET ROLE "{owner}"' if accepted else "RESET ROLE")
                if accepted:
                    backend.enqueue_in(connection, [make_job()])
                else:
                    with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                        backend.enqueue_in(connection, [make_job()])
        async with await psycopg.AsyncConnection.connect(
            url_for(dbname=name), autocommit=True
        ) as connection:
            for accepted in rounds:
                await connection.execute(f'SET ROLE "{owner}"' if accepted else "RESET ROLE")
                if accepted:
                    await backend.aenqueue_in(connection, [make_job()])
                else:
                    with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                        await backend.aenqueue_in(connection, [make_job()])
        self.assertEqual((await backend.counts_by_status())["queued"], 14)

    async def test_a_refusal_inside_a_caller_pipeline_recovers_like_any_failed_statement(self):
        backend, name, owner = await self.store_with_owner_code()
        for autocommit, accepted, _ in itertools.product((False, True), (False, True), range(10)):
            with psycopg.Connection.connect(url_for(dbname=name), autocommit=autocommit) as connection:
                with connection.pipeline() as pipeline:
                    if accepted:
                        connection.execute(f'SET ROLE "{owner}"')
                        backend.enqueue_in(connection, [make_job()])
                        connection.execute("RESET ROLE")
                    with (
                        mock.patch.object(
                            psycopg.Cursor,
                            "_execute_gen",
                            autospec=True,
                            side_effect=psycopg.Cursor._execute_gen,
                        ) as executed,
                        self.assertRaisesRegex(ConfigurationError, "never writes through them"),
                    ):
                        backend.enqueue_in(connection, [make_job()])
                    self.assertEqual(executed.call_count, 1)
                    with suppress(psycopg.Error):
                        pipeline.sync()
                    if not autocommit:
                        connection.rollback()
                    self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))
            async with await psycopg.AsyncConnection.connect(
                url_for(dbname=name), autocommit=autocommit
            ) as connection:
                async with connection.pipeline() as pipeline:
                    if accepted:
                        await connection.execute(f'SET ROLE "{owner}"')
                        await backend.aenqueue_in(connection, [make_job()])
                        await connection.execute("RESET ROLE")
                    with (
                        mock.patch.object(
                            psycopg.AsyncCursor,
                            "_execute_gen",
                            autospec=True,
                            side_effect=psycopg.AsyncCursor._execute_gen,
                        ) as executed,
                        self.assertRaisesRegex(ConfigurationError, "never writes through them"),
                    ):
                        await backend.aenqueue_in(connection, [make_job()])
                    self.assertEqual(executed.call_count, 1)
                    with suppress(psycopg.Error):
                        await pipeline.sync()
                    if not autocommit:
                        await connection.rollback()
                    cursor = await connection.execute("SELECT 1")
                    self.assertEqual(await cursor.fetchone(), (1,))

    async def test_tx_writes_behind_statements_the_caller_pipelined(self):
        table = f'"{self.prefix}_orders"'
        await sql(f"CREATE TABLE {table} (id integer PRIMARY KEY)")
        self.addAsyncCleanup(sql, f"DROP TABLE {table}")
        stored = []
        with psycopg.Connection.connect(POSTGRES_URL) as connection:
            with connection.pipeline():
                for order in (1, 2):
                    connection.execute(f"INSERT INTO {table} VALUES (%s)", (order,))
                    stored.extend(self.backend.enqueue_in(connection, [make_job()]))
            connection.commit()
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            async with connection.pipeline():
                for order in (3, 4):
                    await connection.execute(f"INSERT INTO {table} VALUES (%s)", (order,))
                    stored.extend(await self.backend.aenqueue_in(connection, [make_job()]))
            await connection.commit()
        self.assertEqual(await sql(f"SELECT id FROM {table} ORDER BY id"), [(1,), (2,), (3,), (4,)])
        for job in stored:
            self.assertIsNotNone(await self.backend.get(job.id))

    async def test_an_autocommit_pipeline_that_fails_later_keeps_neither_its_rows_nor_the_job(self):
        table = f'"{self.prefix}_orders"'
        await sql(f"CREATE TABLE {table} (id integer PRIMARY KEY)")
        self.addAsyncCleanup(sql, f"DROP TABLE {table}")
        jobs = []
        for joined in (False, True):
            with psycopg.Connection.connect(POSTGRES_URL, autocommit=True) as connection:
                if joined:
                    self.backend.enqueue_in(connection, [make_job()])
                with self.assertRaises(errors.UniqueViolation), connection.pipeline():
                    connection.execute(f"INSERT INTO {table} VALUES (1)")
                    jobs.extend(self.backend.enqueue_in(connection, [make_job()]))
                    connection.execute(f"INSERT INTO {table} VALUES (1)")
            async with await psycopg.AsyncConnection.connect(
                POSTGRES_URL, autocommit=True
            ) as connection:
                if joined:
                    await self.backend.aenqueue_in(connection, [make_job()])
                with self.assertRaises(errors.UniqueViolation):
                    async with connection.pipeline():
                        await connection.execute(f"INSERT INTO {table} VALUES (2)")
                        jobs.extend(await self.backend.aenqueue_in(connection, [make_job()]))
                        await connection.execute(f"INSERT INTO {table} VALUES (2)")
        self.assertEqual(await sql(f"SELECT count(*) FROM {table}"), [(0,)])
        for job in jobs:
            self.assertIsNone(await self.backend.get(job.id))

    async def test_a_tx_identity_check_gives_up_after_the_statement_timeout(self):
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, statement_timeout=1)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        meta = f'"{backend._queries.schema}"."{self.prefix}_meta"'
        backend._identity = ("another", "stale", None)
        caller = psycopg.Connection.connect(POSTGRES_URL)
        self.addCleanup(caller.close)
        caller.execute(f"SELECT FROM {meta}")
        locker = await psycopg.AsyncConnection.connect(POSTGRES_URL)
        self.addAsyncCleanup(locker.close)
        await locker.execute("SET lock_timeout = '4s'")
        locking = asyncio.ensure_future(
            locker.execute(f"LOCK TABLE {meta} IN ACCESS EXCLUSIVE MODE")
        )
        await asyncio.sleep(0.2)
        started = time.monotonic()
        try:
            with self.assertRaises((psycopg.errors.QueryCanceled, ConfigurationError)):
                await asyncio.to_thread(backend.enqueue_in, caller, [make_job()])
            self.assertLess(time.monotonic() - started, 3)
        finally:
            caller.rollback()
            with suppress(psycopg.Error):
                await locking
            await locker.rollback()

    async def test_payloads_keep_their_bytes_without_standard_conforming_strings(self):
        payload = b'\\x7b\x00\xff"quoted\'\\'
        options = "-c standard_conforming_strings=off"
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, options=options)
        self.addAsyncCleanup(backend.close)
        await backend.setup()
        pooled = await backend.enqueue(make_job(payload=payload))
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL, options=options) as connection:
            joined = [make_job(payload=payload) for _ in range(2)]
            for job in joined:
                await self.backend.aenqueue_in(connection, [job])
            await connection.commit()
        with psycopg.connect(POSTGRES_URL, options=options) as connection:
            synced = make_job(payload=payload)
            self.backend.enqueue_in(connection, [synced])
            connection.commit()
        for job in (pooled, *joined, synced):
            self.assertEqual((await self.backend.get(job.id)).payload, payload)

    async def test_text_in_a_client_encoding_other_than_utf8_round_trips_through_tx(self):
        given = make_job(task="ソ表.能", queue="キュー", dedup_key="予\\表")
        async with await psycopg.AsyncConnection.connect(
            POSTGRES_URL, options="-c client_encoding=SJIS"
        ) as connection:
            await self.backend.aenqueue_in(connection, [given])
            await connection.commit()
        stored = await self.backend.get(given.id)
        self.assertEqual(
            (stored.task, stored.queue, stored.dedup_key), (given.task, given.queue, given.dedup_key)
        )

    async def test_tx_is_refused_on_a_psycopg_whose_connections_cannot_be_held(self):
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            with mock.patch.object(postgres_backend, "HELD", False):
                with self.assertRaisesRegex(UnsupportedCapabilityError, "psycopg 3.2 or 3.3"):
                    await self.backend.aenqueue_in(connection, [make_job()])
        with psycopg.connect(POSTGRES_URL) as connection:
            with mock.patch.object(postgres_backend, "HELD", False):
                with self.assertRaisesRegex(UnsupportedCapabilityError, "psycopg 3.2 or 3.3"):
                    self.backend.enqueue_in(connection, [make_job()])

    async def test_tx_enqueues_running_at_once_on_one_connection_all_land(self):
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            await self.backend.aenqueue_in(connection, [make_job()])
            stored = await asyncio.gather(
                *(self.backend.aenqueue_in(connection, [make_job()]) for _ in range(10))
            )
            await connection.commit()
        with psycopg.Connection.connect(POSTGRES_URL) as connection:
            self.backend.enqueue_in(connection, [make_job()])
            threads = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        lambda: [self.backend.enqueue_in(connection, [make_job()]) for _ in range(10)]
                    )
                    for _ in range(4)
                )
            )
            connection.commit()
        jobs = [job for [job] in stored] + [job for run in threads for [job] in run]
        self.assertEqual(len(jobs), 50)
        for job in jobs:
            self.assertIsNotNone(await self.backend.get(job.id))

    async def test_tables_another_role_may_add_triggers_to_are_refused(self):
        outsider = await self.role()
        await sql(f'GRANT TRIGGER ON "{self.prefix}_jobs" TO "{outsider}"')
        backend = PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix)
        self.addAsyncCleanup(backend.close)
        with self.assertRaisesRegex(ConfigurationError, "TRIGGER grant"):
            await backend.setup()
        async with await psycopg.AsyncConnection.connect(POSTGRES_URL) as connection:
            with self.assertRaisesRegex(ConfigurationError, "never writes through them"):
                await self.backend.aenqueue_in(connection, [make_job()])
        await sql(f'REVOKE TRIGGER ON "{self.prefix}_jobs" FROM "{outsider}"')
        await sql(f'GRANT TRIGGER ON "{self.prefix}_jobs" TO PUBLIC')
        with self.assertRaisesRegex(ConfigurationError, "TRIGGER grant"):
            await backend.setup()
        await sql(f'REVOKE TRIGGER ON "{self.prefix}_jobs" FROM PUBLIC')
        await sql(
            f'CREATE FUNCTION public."{outsider}_hook"() RETURNS trigger LANGUAGE plpgsql '
            "AS $$ BEGIN RETURN NEW; END $$"
        )
        await sql(f'ALTER FUNCTION public."{outsider}_hook"() OWNER TO "{outsider}"')
        await sql(
            f'CREATE TRIGGER hook BEFORE INSERT ON "{self.prefix}_jobs" '
            f'FOR EACH ROW EXECUTE FUNCTION public."{outsider}_hook"()'
        )
        with self.assertRaisesRegex(ConfigurationError, "TRIGGER grant"):
            await backend.setup()
        await sql(f'DROP TRIGGER hook ON "{self.prefix}_jobs"')
        await sql(f'DROP FUNCTION public."{outsider}_hook"()')
        await backend.setup()

    async def test_an_owner_that_inherits_but_cannot_become_the_user_is_not_trusted(self):
        [(version,)] = await sql("SHOW server_version_num")
        if int(version) < 160000:
            self.skipTest("SET on role grants arrived in Postgres 16")
        owner = await self.role()
        user = await self.role()
        name = await self.database_owned_by(owner)
        await sql(f'GRANT "{user}" TO "{owner}" WITH INHERIT TRUE, SET FALSE')
        theirs = PostgresBackend.from_url(
            url_for(dbname=name, user=owner, password="tinyjobs"), prefix=self.prefix, schema="theirs"
        )
        self.addAsyncCleanup(theirs.close)
        await theirs.setup()
        mine = PostgresBackend.from_url(
            url_for(dbname=name, user=user, password="tinyjobs"), prefix=self.prefix, schema="theirs"
        )
        self.addAsyncCleanup(mine.close)
        with self.assertRaisesRegex(ConfigurationError, "does not hold"):
            await mine.setup()

    async def test_jobs_sharing_a_key_in_one_call_keep_the_first_one_given(self):
        given = [make_job(id="job-b", dedup_key="k"), make_job(id="job-a", dedup_key="k")]
        stored = await self.backend.enqueue_many(given)
        self.assertEqual([job.id for job in stored], ["job-b", "job-b"])

    async def test_an_enqueue_finishing_after_close_starts_no_wake_up(self):
        backend = await self.open()
        await backend.enqueue(make_job())
        slow = asyncio.ensure_future(backend.enqueue_many([make_job() for _ in range(3000)]))
        await asyncio.sleep(0.02)
        await backend.close()
        await slow
        self.assertIsNone(backend._waking)

    async def test_setup_tries_again_after_an_abandoned_first_attempt_failed(self):
        backend = PostgresBackend.from_url(
            "postgresql://postgres@127.0.0.1:1/tinyjobs_test",
            prefix=self.prefix,
            clock=self.clock,
            pool_timeout=0.5,
        )
        self.addAsyncCleanup(backend.close)
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(backend.setup(), 0.1)
        await asyncio.wait({backend._opening})
        backend._conninfo = POSTGRES_URL
        await backend.setup()
        job = await backend.enqueue(make_job())
        self.assertEqual((await backend.get(job.id)).id, job.id)

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
        await connection.execute(f'LISTEN "{backend._channel}"')
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
    def test_a_statement_timeout_that_would_switch_the_guard_off_is_refused(self):
        for timeout in (0.0004, 0.0, -1.0, float("nan"), float("inf"), 1e7):
            with self.subTest(timeout=timeout):
                with self.assertRaisesRegex(ConfigurationError, "statement_timeout"):
                    PostgresBackend.from_url(POSTGRES_URL, statement_timeout=timeout)

    def test_a_url_libpq_cannot_read_is_refused_without_its_password(self):
        for url in (
            "postgresql://app@db/shop?password=Hunter2%zzSecret",
            "postgresql://app:Hunter2%zzSecret@db/shop",
            "postgresql://app@db/shop?sslmode=disable&password=Hunter2%zzSecret",
            "postgresql://app:Hunter2Secret@[::1/shop",
            "postgresql://app:Hunter2Secret@[::1]x/shop",
        ):
            for build in (TinyJobs, PostgresBackend.from_url):
                with self.subTest(url=url, build=build):
                    with self.assertRaisesRegex(ConfigurationError, "not valid") as caught:
                        build(url)
                    self.assertNotIn("Hunter2", str(caught.exception))
                    self.assertNotIn("Secret", str(caught.exception))

    async def test_two_spellings_of_one_store_share_its_channel(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        plain = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        named = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix, schema="public")
        for backend in (plain, named):
            self.addAsyncCleanup(backend.close)
            await backend.setup()
        self.assertEqual(plain._channel, named._channel)
        self.assertLessEqual(len(plain._channel.encode()), 63)

    async def test_stores_behind_one_configuration_get_their_own_channels(self):
        prefix = new_prefix()
        schema = f"tj_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(drop, prefix)
        self.addAsyncCleanup(drop, prefix, schema)
        plain = PostgresBackend.from_url(POSTGRES_URL, prefix=prefix)
        diverted = PostgresBackend.from_url(
            POSTGRES_URL, prefix=prefix, options=f"-c search_path={schema}"
        )
        for backend in (plain, diverted):
            self.addAsyncCleanup(backend.close)
            await backend.setup()
        self.assertNotEqual(plain._channel, diverted._channel)

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
        self.assertNotEqual(inside._channel, outside._channel)

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
        ahead = "SELECT extract(epoch FROM clock_timestamp())::double precision + 120 AS now"
        with mock.patch("tinyjobs.backends.postgres.queries.READING", ahead):
            other = PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, clock=clock)
            self.opened.append(other)
            other._server = lambda: "postgres elsewhere:5432 db other"
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
        await listener.execute(f'LISTEN "{here._channel}"')
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
        channel = f"%{self.app.backend._channel}%"
        [(pid,)] = await sql(query, {"channel": channel})
        inner = self.app.backend._inner
        heard: list[str | None] = []
        hear = inner._hear
        inner._hear = lambda queue: (heard.append(queue), hear(queue))[1]
        await sql("SELECT pg_terminate_backend(%(pid)s)", {"pid": pid})
        await asyncio.sleep(2.5)
        self.assertNotEqual(await sql(query, {"channel": channel}), [(pid,)])
        self.assertEqual(heard.count(None), 1)
        self.assertLess(await self.latency(self.app, ran), 0.3)

    async def test_a_listener_reaching_a_server_that_never_answers_tries_again(self):
        accepted: list[asyncio.StreamWriter] = []
        server = await asyncio.start_server(
            lambda reader, writer: accepted.append(writer), "127.0.0.1", 0
        )
        port = server.sockets[0].getsockname()[1]
        backend = PostgresBackend.from_url(
            f"postgresql://nobody@127.0.0.1:{port}/nothing", pool_timeout=1.0
        )
        listening = asyncio.create_task(backend._listen())
        try:
            for _ in range(80):
                if len(accepted) >= 2:
                    break
                await asyncio.sleep(0.1)
            self.assertGreaterEqual(len(accepted), 2)
        finally:
            listening.cancel()
            with suppress(asyncio.CancelledError):
                await listening
            for writer in accepted:
                writer.close()
            server.close()
            await server.wait_closed()

    async def test_a_server_that_stops_answering_fails_a_pooled_call_instead_of_hanging(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        with mock.patch.object(postgres_backend, "ANSWER_MARGIN_SECONDS", 0.5):
            async with stalling_proxy() as (url, frozen):
                backend = PostgresBackend.from_url(url, prefix=prefix, statement_timeout=0.5)
                self.addAsyncCleanup(backend.close)
                await backend.setup()
                job = await self.claimed_on(backend)
                frozen.set()
                started = time.monotonic()
                with self.assertRaisesRegex(psycopg.OperationalError, "did not answer"):
                    await asyncio.wait_for(backend.complete(job, None), 20)
                self.assertLess(time.monotonic() - started, 5)
                frozen.clear()
                self.assertTrue(await backend.complete(job, None))

    @unittest.skipIf(
        PostgresBackend is not None and postgres_backend.WAITS_WITH_TIMEOUT,
        "psycopg bounds its own cancellation once wait() takes a timeout",
    )
    async def test_a_call_cancelled_while_the_server_stalls_gives_up_its_connection(self):
        prefix = new_prefix()
        self.addAsyncCleanup(drop, prefix)
        with mock.patch.object(postgres_backend, "CANCEL_SECONDS", 0.5):
            async with stalling_proxy() as (url, frozen):
                backend = PostgresBackend.from_url(url, prefix=prefix)
                self.addAsyncCleanup(backend.close)
                await backend.setup()
                job = await self.claimed_on(backend)
                frozen.set()
                completing = asyncio.create_task(backend.complete(job, None))
                await asyncio.sleep(0.3)
                completing.cancel()
                started = time.monotonic()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(completing, 20)
                self.assertLess(time.monotonic() - started, 5)
                frozen.clear()
                self.assertTrue(await backend.complete(job, None))

    async def claimed_on(self, backend: PostgresBackend):
        await backend.enqueue(make_job())
        [job] = await backend.claim(worker_id="w", queues=["default"], limit=1, lease_seconds=30.0)
        return job

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


    async def test_a_success_survives_the_server_dropping_every_connection(self):
        name = f"tinyjobs-{uuid.uuid4().hex[:8]}"
        app = TinyJobs(POSTGRES_URL, prefix=self.prefix, application_name=name)
        self.addAsyncCleanup(app.aclose)
        ran = []

        @app.task(name="dropping")
        def dropping() -> None:
            ran.append(1)

        job = await dropping.aenqueue()
        backend = app.backend
        original = backend.complete
        dropped = []

        async def after_a_drop(*args, **kwargs):
            if not dropped:
                dropped.append(1)
                await sql(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE application_name = %(name)s",
                    {"name": name},
                )
            return await original(*args, **kwargs)

        backend.complete = after_a_drop
        worker = app.worker(concurrency=1, scheduler=False, poll_maximum=0.05)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 15
        while (await job.arefresh()).status is not JobStatus.SUCCESS:
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.05)
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertEqual((ran, job.attempts), ([1], 1))

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
        channel = self.app.backend._channel
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
        channel = self.app.backend._channel
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

    async def other_database(self) -> str:
        name = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE DATABASE "{name}"')
        self.addAsyncCleanup(sql, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        return url_for(dbname=name)

    async def test_a_connection_to_a_copy_of_the_store_is_refused(self):
        name = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        original = await self.other_database()
        seed = TinyJobs(original, prefix=self.prefix)
        await seed.ready()
        await seed.aclose()
        await sql(f'CREATE DATABASE "{name}" TEMPLATE "{original.rsplit("/", 1)[1]}"')
        self.addAsyncCleanup(sql, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        app = TinyJobs(original, prefix=self.prefix)
        self.addAsyncCleanup(app.aclose)
        await app.ready()
        async with await psycopg.AsyncConnection.connect(url_for(dbname=name)) as connection:
            with self.assertRaisesRegex(ConfigurationError, "does not reach"):
                await app.asend("x.task", tx=connection)
            await connection.rollback()

    async def test_a_connection_to_another_database_is_refused(self):
        elsewhere = await self.other_database()
        async with await psycopg.AsyncConnection.connect(elsewhere) as connection:
            with self.assertRaisesRegex(ConfigurationError, "does not reach"):
                await self.app.asend("x.task", tx=connection)
            await connection.rollback()
        twin = TinyJobs(elsewhere, prefix=self.prefix)
        await twin.ready()
        await twin.aclose()
        async with await psycopg.AsyncConnection.connect(elsewhere) as connection:
            with self.assertRaisesRegex(ConfigurationError, "does not reach"):
                await self.app.asend("x.task", tx=connection)
            await connection.rollback()

    async def test_a_search_path_or_a_temporary_table_cannot_divert_the_job(self):
        shadow = f"tinyjobs_test_{uuid.uuid4().hex[:12]}"
        await sql(f'CREATE SCHEMA "{shadow}"')
        self.addAsyncCleanup(sql, f'DROP SCHEMA "{shadow}" CASCADE')
        await sql(f'CREATE TABLE "{shadow}"."{self.prefix}_jobs" (LIKE "{self.prefix}_jobs" INCLUDING ALL)')
        await sql(f'CREATE TABLE "{shadow}"."{self.prefix}_meta" (LIKE "{self.prefix}_meta" INCLUDING ALL)')
        await sql(
            f'INSERT INTO "{shadow}"."{self.prefix}_meta" SELECT * FROM "{self.prefix}_meta"'
        )
        sent = []
        async with await self.connect(options=f"-c search_path={shadow}") as connection:
            async with connection.transaction():
                sent.append(await self.app.asend("x.task", tx=connection))
        async with await self.connect(options="-c search_path=nowhere") as connection:
            async with connection.transaction():
                sent.append(await self.app.asend("x.task", tx=connection))
        async with await self.connect() as connection:
            async with connection.transaction():
                await connection.execute(
                    f'CREATE TEMPORARY TABLE "{self.prefix}_jobs" (LIKE "{self.prefix}_jobs" INCLUDING ALL)'
                )
                sent.append(await self.app.asend("x.task", tx=connection))
        for job in sent:
            self.assertIsNotNone(await self.app.aget(job.id))
        self.assertEqual(await sql(f'SELECT count(*) FROM "{shadow}"."{self.prefix}_jobs"'), [(0,)])

    async def test_the_longest_names_still_wake_workers_and_join_transactions(self):
        schema, prefix = "s" * 40, "p" * 40
        self.addAsyncCleanup(drop, prefix, schema)
        app = TinyJobs(POSTGRES_URL, prefix=prefix, schema=schema)
        self.addAsyncCleanup(app.aclose)
        await app.ready()
        ran = await self.idle_worker(app, poll_minimum=5.0, poll_maximum=5.0)
        self.assertLess(await self.latency(app, ran), 0.3)
        async with await self.connect() as connection:
            async with connection.transaction():
                job = await app.asend("timed", tx=connection)
        self.assertIsNotNone(await app.aget(job.id))

    async def test_a_very_long_queue_name_does_not_abort_the_callers_transaction(self):
        queue = "q" * 9000
        async with await self.connect() as connection:
            async with connection.transaction():
                job = await self.app.asend("x.task", tx=connection, queue=queue)
        self.assertEqual((await self.app.aget(job.id)).queue, queue)

    async def test_the_store_identity_is_read_once_per_connection(self):
        async with await self.connect() as connection:
            with mock.patch.object(
                psycopg.AsyncCursor,
                "_execute_gen",
                autospec=True,
                side_effect=psycopg.AsyncCursor._execute_gen,
            ) as executed:
                await self.app.asend("x.task", tx=connection)
                await self.app.asend("x.task", tx=connection)
            await connection.commit()
        queries = [str(call.args[1]) for call in executed.call_args_list]
        self.assertEqual(sum("'instance'" in query for query in queries), 1)
        self.assertEqual(sum("pg_class" in query for query in queries), 2)

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
