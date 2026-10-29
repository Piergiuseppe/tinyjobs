from __future__ import annotations

import asyncio
import contextvars
import gc
import inspect
import logging
import os
import queue
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import warnings
import weakref
from contextlib import ExitStack, contextmanager, suppress
from functools import partial
from pathlib import Path
from unittest import mock

from tinyjobs import runtime
from tinyjobs.backends import SQLiteBackend, load_backend
from tinyjobs.backends.sqlite import connection as connection_module
from tinyjobs.backends.sqlite.connection import ConnectionPool
from tinyjobs.status import ExecutionStatus
from tinyjobs.testing import BackendContract
from tinyjobs.testing.contract import ERROR, START

from tests.support import interrupt_repeatedly


class SQLiteContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    async def make_backend(self, clock):
        return SQLiteBackend(str(Path(self.directory.name) / "jobs.db"), clock=clock)

    async def test_a_claim_that_meets_its_own_old_row_starts_it_afresh(self):
        claimed = await self.claimed_one(max_attempts=2)
        await self.backend.fail(claimed, ERROR, None)
        with self.backend.transaction() as connection:
            connection.execute(
                "UPDATE jobs SET status = 'queued', claims = 0, finished_at = NULL "
                "WHERE id = ?",
                (claimed.id,),
            )
        again = (await self.claim(worker="worker-b"))[0]
        history = await self.backend.executions(claimed.id)
        self.assertEqual(again.claims, 1)
        self.assertEqual(
            [(e.status, e.worker_id, e.error_type) for e in history],
            [(ExecutionStatus.RUNNING, "worker-b", None)],
        )

    async def test_a_job_retried_before_any_claim_is_due_from_when_it_was_sent(self):
        job = await self.enqueue(run_at=START + 3600, created_at=START)
        self.assertTrue(await self.backend.cancel(job.id))
        self.clock.advance(10)
        self.assertTrue(await self.backend.requeue(job.id))
        self.assertEqual((await self.stored(job)).run_at, START)
        self.assertEqual(len(await self.claim()), 1)


class SQLiteWithoutReturningContractTests(SQLiteContractTests):
    def setUp(self) -> None:
        super().setUp()
        for module in ("backend", "claim"):
            patcher = mock.patch(
                f"tinyjobs.backends.sqlite.{module}.SUPPORTS_RETURNING", False
            )
            patcher.start()
            self.addCleanup(patcher.stop)


def timed_fork_hold() -> float:
    from tinyjobs import runtime
    from tinyjobs.backends.sqlite.connection import FORK_GATE

    ident = threading.get_ident()
    started = time.monotonic()
    runtime._fork_marks[ident] = os.getpid()
    try:
        FORK_GATE.wait_quiet(ident, os.getpid())
    finally:
        runtime._fork_marks.pop(ident, None)
    return time.monotonic() - started


class ForkGateCoverageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "jobs.db")

    async def test_a_fork_waits_for_a_backend_call_in_flight(self):
        backend = SQLiteBackend(self.path)
        await backend.setup()
        try:
            call = asyncio.ensure_future(backend._call(time.sleep, 0.5))
            await asyncio.sleep(0.1)
            took = await asyncio.to_thread(timed_fork_hold)
            await call
        finally:
            await backend.close()
        self.assertGreaterEqual(took, 0.3)

    async def test_a_fork_waits_for_a_connection_being_opened(self):
        pool = ConnectionPool(self.path)
        self.addCleanup(pool.close)
        opening = pool._open

        def slow_open():
            time.sleep(0.5)
            return opening()

        pool._open = slow_open
        opener = threading.Thread(target=lambda: pool.connection)
        opener.start()
        time.sleep(0.1)
        took = timed_fork_hold()
        opener.join(5)
        self.assertGreaterEqual(took, 0.3)

    def test_a_fork_waits_for_a_statement_outside_any_write(self):
        pool = ConnectionPool(self.path)
        self.addCleanup(pool.close)
        inside = threading.Event()

        def pause():
            inside.set()
            time.sleep(0.5)
            return 1

        def read() -> None:
            connection = pool.connection
            connection.create_function("pause", 0, pause)
            connection.execute("SELECT pause()").fetchone()

        reader = threading.Thread(target=read)
        reader.start()
        self.assertTrue(inside.wait(5))
        took = timed_fork_hold()
        reader.join(5)
        self.assertGreaterEqual(took, 0.3)

    def test_a_fork_from_inside_a_statement_still_waits_for_other_writes(self):
        pool = ConnectionPool(self.path)
        self.addCleanup(pool.close)
        opened, took = threading.Event(), []

        def hold() -> None:
            with pool.write() as connection:
                connection.execute("SELECT 1")
                opened.set()
                time.sleep(0.5)

        def fork_here():
            took.append(timed_fork_hold())
            return 1

        holder = threading.Thread(target=hold)
        holder.start()
        self.assertTrue(opened.wait(5))
        connection = pool.connection
        connection.create_function("fork_here", 0, fork_here)
        connection.execute("SELECT fork_here()").fetchone()
        holder.join(5)
        self.assertGreaterEqual(took[0], 0.3)

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_child_opens_its_own_connection_instead_of_the_inherited_one(self):
        pool = ConnectionPool(self.path)
        self.addCleanup(pool.close)
        inherited = pool.connection
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(5)
            code = 1
            try:
                fresh = pool.connection
                fresh.execute("SELECT 1")
                code = 0 if fresh is not inherited and inherited not in pool._all else 2
            finally:
                os._exit(code)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)


class ClosedSQLiteBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_closed_backend_says_it_is_closed(self):
        from tinyjobs.exceptions import AppClosedError

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await backend.setup()
        await backend.close()
        with self.assertRaises(AppClosedError):
            await backend.get("anything")

    async def test_a_backend_whose_executor_is_gone_still_returns_jobs(self):
        from tinyjobs.exceptions import AppClosedError
        from tinyjobs.status import JobStatus
        from tinyjobs.testing.contract import make_job

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await backend.setup()
        self.addCleanup(backend._close_sync)
        await backend.enqueue(make_job())
        (job,) = await backend.claim(
            worker_id="w", queues=("default",), limit=1, lease_seconds=60.0
        )
        backend._executor.shutdown(wait=True)
        with self.assertRaises(AppClosedError):
            await backend.claim(
                worker_id="w", queues=("default",), limit=1, lease_seconds=60.0
            )
        with self.assertRaises(AppClosedError):
            await backend.get(job.id)
        self.assertTrue(await backend.hand_back(job))
        self.assertEqual(backend._get_sync(job.id).status, JobStatus.QUEUED)

    async def test_a_call_held_by_a_fork_steps_aside_to_the_next_executor(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await backend.setup()
        self.addAsyncCleanup(backend.close)
        retiring = backend._executor
        runtime._fork_marks[-1] = os.getpid()
        self.addCleanup(runtime._fork_marks.pop, -1, None)
        call = asyncio.ensure_future(backend.get("anything"))
        await asyncio.sleep(0.1)
        self.assertFalse(retiring.idle)
        await asyncio.to_thread(backend.step_aside_for_fork)
        self.assertFalse(any(thread.is_alive() for thread in retiring._threads))
        runtime._fork_marks.pop(-1)
        self.assertIsNone(await asyncio.wait_for(call, 5))
        self.assertIsNot(backend._executor, retiring)

    async def test_a_fork_leaves_a_busy_executor_running(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await backend.setup()
        self.addAsyncCleanup(backend.close)
        release = threading.Event()
        executor = backend._executor
        busy = executor.submit(release.wait, 5)
        started = time.monotonic()
        backend.pause_for_fork()
        took = time.monotonic() - started
        release.set()
        busy.result(5)
        self.assertIs(backend._executor, executor)
        self.assertLess(took, 1.0)

    async def test_a_fork_does_not_bring_a_closed_backend_back(self):
        from tinyjobs.exceptions import AppClosedError

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await backend.setup()
        await backend.close()
        backend.pause_for_fork()
        with self.assertRaises(AppClosedError):
            await backend.get("anything")


class SQLiteCancelledCallTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.backend = SQLiteBackend(str(Path(directory.name) / "jobs.db"))
        await self.backend.setup()
        self.addAsyncCleanup(self.backend.close)
        self.gate = threading.Event()
        self.addCleanup(self.gate.set)

    async def cancelled(self, call: asyncio.Future, times: int = 1) -> float:
        started = time.monotonic()
        for _ in range(times):
            call.cancel()
            await asyncio.sleep(0.05)
        with self.assertRaises(asyncio.CancelledError):
            await call
        return time.monotonic() - started

    async def test_a_call_cancelled_while_queued_ends_at_once_and_never_runs(self):
        ran: list[str] = []
        real = self.backend._get_sync

        def counted(job_id):
            ran.append(job_id)
            return real(job_id)

        counted.__name__ = "_get_sync"
        self.backend._executor.submit(self.gate.wait, 5)
        with mock.patch.object(self.backend, "_get_sync", counted):
            call = asyncio.ensure_future(self.backend.get("cancelled"))
            await asyncio.sleep(0.05)
            took = await self.cancelled(call)
            self.gate.set()
            await self.backend.get("after")
        self.assertLess(took, 0.3)
        self.assertEqual(ran, ["after"])

    async def test_a_running_call_holds_its_cancel_back_half_a_second_at_most(self):
        from tinyjobs.backends.sqlite import backend as sqlite_backend

        failures: list[dict] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda loop, context: failures.append(context))
        self.addCleanup(loop.set_exception_handler, None)

        def locked(job_id):
            self.gate.wait(5)
            raise sqlite3.OperationalError("database is locked")

        locked.__name__ = "_get_sync"
        with mock.patch.object(self.backend, "_get_sync", locked):
            call = asyncio.ensure_future(self.backend.get("held"))
            await asyncio.sleep(0.05)
            took = await self.cancelled(call, times=2)
            self.gate.set()
        await self.backend.get("after")
        del call
        gc.collect()
        await asyncio.sleep(0)
        self.assertLess(took, sqlite_backend.CANCELLED_CALL_SECONDS + 0.3)
        self.assertEqual(failures, [])

    async def test_jobs_are_released_together_in_one_call(self):
        from tinyjobs.status import JobStatus
        from tinyjobs.testing.contract import make_job

        for _ in range(4):
            await self.backend.enqueue(make_job())
        jobs = await self.backend.claim(
            worker_id="w", queues=("default",), limit=4, lease_seconds=60.0
        )
        calls: list[str] = []
        real = self.backend._call

        async def counted(function, *args):
            calls.append(function.__name__)
            return await real(function, *args)

        with mock.patch.object(self.backend, "_call", counted):
            self.assertEqual(await self.backend.release_many([*jobs, jobs[0]]), 4)
        self.assertEqual(calls, ["_release_many_sync"])
        for job in jobs:
            self.assertIs((await self.backend.get(job.id)).status, JobStatus.QUEUED)
            [execution] = await self.backend.executions(job.id)
            self.assertIs(execution.status, ExecutionStatus.LOST)


class FreshDatabaseTests(unittest.TestCase):
    def test_a_write_rolls_back_a_transaction_left_open_on_its_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            try:
                pool.connection.execute("BEGIN")
                with pool.write() as connection:
                    connection.execute("CREATE TABLE written (id INTEGER)")
                self.assertFalse(pool.connection.in_transaction)
                tables = pool.connection.execute(
                    "SELECT name FROM sqlite_master WHERE name = 'written'"
                ).fetchall()
            finally:
                pool.close()
            self.assertEqual(len(tables), 1)

    def test_a_write_interrupted_on_its_way_out_does_not_swallow_the_next_one(self):
        import sys

        from tinyjobs.backends.sqlite.connection import EXIT_CODE, FORK_GATE

        def interrupt_exit(frame, event, arg):
            if event == "call" and frame.f_code is EXIT_CODE:
                sys.settrace(None)
                raise KeyboardInterrupt

        kept: list[BaseException] = []
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                try:
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        sys.settrace(interrupt_exit)
                except KeyboardInterrupt as error:
                    kept.append(error)
                finally:
                    sys.settrace(None)
                self.assertEqual(len(kept), 1)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                self.assertFalse(pool.connection.in_transaction)
                self.assertEqual(
                    [ref for ref in FORK_GATE._holders if ref() is not None], []
                )
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(marks, [(2,)])

    def test_a_nested_write_joins_the_outer_one(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(RuntimeError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        with pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                        connection.execute("INSERT INTO marks (n) VALUES (3)")
                        raise RuntimeError("roll back")
                self.assertEqual(
                    pool.connection.execute("SELECT n FROM marks").fetchall(), []
                )
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with self.assertRaises(RuntimeError):
                        with pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                            raise RuntimeError("roll back the inner write")
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                marks = pool.connection.execute(
                    "SELECT n FROM marks ORDER BY n"
                ).fetchall()
                self.assertFalse(pool.connection.in_transaction)
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (3,)])

    def test_a_write_taken_over_while_closing_refuses_to_commit(self):
        from tinyjobs.backends.sqlite.connection import CLOSING

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                closing = pool.write()
                closing.__enter__().execute("INSERT INTO marks (n) VALUES (1)")
                closing.state = CLOSING
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                with self.assertRaisesRegex(RuntimeError, "rolled it back"):
                    closing.__exit__(None, None, None)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                self.assertFalse(pool.connection.in_transaction)
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(2,)])

    def interrupt_on_return(self, code):
        import sys

        def tracer(frame, event, arg):
            if frame.f_code is code and event == "return":
                sys.settrace(None)
                raise KeyboardInterrupt
            return tracer

        sys.settrace(tracer)

    def test_a_kept_write_interrupted_on_entry_does_not_swallow_later_writes(self):
        import sys

        from tinyjobs.backends.sqlite.connection import ConnectionPool as Pool

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                kept = pool.write()
                try:
                    self.interrupt_on_return(Pool._begin.__code__)
                    with kept as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                except KeyboardInterrupt:
                    pass
                finally:
                    sys.settrace(None)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                self.assertFalse(pool.connection.in_transaction)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertIsNotNone(kept)
            self.assertEqual(marks, [(2,)])

    def test_an_inner_write_interrupted_on_entry_leaves_the_outer_one_whole(self):
        import sys

        from tinyjobs.backends.sqlite.connection import ConnectionPool as Pool

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    try:
                        self.interrupt_on_return(Pool._nest.__code__)
                        with pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                    except KeyboardInterrupt:
                        pass
                    finally:
                        sys.settrace(None)
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                self.assertFalse(pool.connection.in_transaction)
                marks = other.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(marks, [(1,), (3,)])

    def test_a_task_cannot_write_while_another_task_holds_the_connection(self):
        from tinyjobs.backends.sqlite.connection import HELD_ELSEWHERE_MESSAGE

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n TEXT)")
            outcomes: list[str] = []

            async def first(inside: asyncio.Event, done: asyncio.Event) -> None:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES ('first')")
                    inside.set()
                    await done.wait()

            async def second(inside: asyncio.Event, done: asyncio.Event) -> None:
                await inside.wait()
                try:
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES ('second')")
                except RuntimeError as error:
                    outcomes.append(str(error))
                finally:
                    done.set()

            async def both() -> None:
                inside, done = asyncio.Event(), asyncio.Event()
                await asyncio.gather(first(inside, done), second(inside, done))

            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                asyncio.run(both())
                self.assertFalse(pool.connection.in_transaction)
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(outcomes, [HELD_ELSEWHERE_MESSAGE])
            self.assertEqual(marks, [("first",)])

    def test_a_call_made_while_a_write_opens_is_refused(self):
        from tinyjobs import TinyJobs
        from tinyjobs.backends.sqlite import connection as module
        from tinyjobs.exceptions import InterruptedCallError

        with tempfile.TemporaryDirectory() as directory:
            app = TinyJobs(f"sqlite:///{Path(directory) / 'jobs.db'}").start()
            refused: list[BaseException] = []
            enter = module.FORK_GATE.enter

            def enter_then_call(*args, **kwargs):
                ref = enter(*args, **kwargs)
                if "alive" not in kwargs:
                    return ref
                try:
                    app.stats()
                except InterruptedCallError as error:
                    refused.append(error)
                return ref

            try:
                with app.backend.transaction() as connection:
                    connection.execute("CREATE TABLE marks (n INTEGER)")
                with mock.patch.object(module.FORK_GATE, "enter", enter_then_call):
                    with app.backend.transaction() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                with app.backend.transaction() as connection:
                    marks = connection.execute("SELECT n FROM marks").fetchall()
            finally:
                app.close()
            self.assertEqual(len(refused), 1)
            self.assertEqual([tuple(row) for row in marks], [(1,)])

    def test_a_write_left_on_one_thread_commits_from_another(self):
        from concurrent.futures import ThreadPoolExecutor
        from contextlib import contextmanager

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")

            @contextmanager
            def dependency():
                with pool.write() as connection:
                    yield connection

            manager = dependency()
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                with ThreadPoolExecutor(1) as first, ThreadPoolExecutor(1) as second:
                    connection = first.submit(manager.__enter__).result()
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    second.submit(manager.__exit__, None, None, None).result()
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(marks, [(1,)])

    def test_a_write_beside_a_suspended_one_on_the_same_thread_stays_apart(self):
        import contextvars
        from concurrent.futures import ThreadPoolExecutor
        from contextlib import contextmanager

        def apart(function):
            return contextvars.copy_context().run, function

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=5)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")

            @contextmanager
            def dependency():
                with pool.write() as connection:
                    yield connection

            def beside() -> None:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")

            manager = dependency()
            try:
                with ThreadPoolExecutor(1) as first, ThreadPoolExecutor(1) as second:
                    connection = first.submit(*apart(manager.__enter__)).result()
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    waiting = first.submit(*apart(beside))
                    time.sleep(0.1)
                    second.submit(manager.__exit__, None, None, None).result()
                    waiting.result(10)
                marks = pool.connection.execute(
                    "SELECT n FROM marks ORDER BY n"
                ).fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_a_loop_inside_a_write_keeps_it_live_at_every_step(self):
        import sys

        from tinyjobs.backends.sqlite import connection as module

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            seen: list[tuple[int, bool]] = []

            def body() -> None:
                with pool.write() as connection:
                    for n in range(3):
                        connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))
                    try:
                        raise ValueError
                    except ValueError:
                        connection.execute("INSERT INTO marks (n) VALUES (3)")
                    finally:
                        connection.execute("INSERT INTO marks (n) VALUES (4)")
                    count = 0
                    while count < 2:
                        count += 1

            def step(frame, event, arg):
                if event == "opcode":
                    scope = pool._writer(pool.connection)
                    if scope is not None and scope.state == module.OPEN:
                        seen.append((frame.f_lasti, module._live(scope)))
                return step

            def start(frame, event, arg):
                if frame.f_code is body.__code__:
                    frame.f_trace_opcodes = True
                    return step
                return None

            try:
                for _ in range(2):
                    sys.settrace(start)
                    body()
            finally:
                sys.settrace(None)
                pool.close()
            last = max(offset for offset, live in seen if live)
            leaving = {offset for offset, live in seen if not live}
            self.assertGreater(len(seen), 40)
            self.assertLessEqual(len(leaving), 5)
            self.assertTrue(all(offset > last for offset in leaving))

    def test_an_interrupt_leaving_a_nested_write_rolls_the_whole_write_back(self):
        import sys

        from tinyjobs.backends.sqlite.connection import EXIT_CODE

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            fired: list[bool] = []

            def tracer(frame, event, arg):
                if event == "call" and frame.f_code is EXIT_CODE and not fired:
                    fired.append(True)
                    raise KeyboardInterrupt
                return None

            try:
                with self.assertRaisesRegex(RuntimeError, "nested tinyjobs write"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        try:
                            sys.settrace(tracer)
                            with pool.write() as inner:
                                inner.execute("INSERT INTO marks (n) VALUES (2)")
                                raise ValueError
                        except (ValueError, KeyboardInterrupt):
                            sys.settrace(None)
                        connection.execute("INSERT INTO marks (n) VALUES (3)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                self.assertFalse(pool.connection.in_transaction)
            finally:
                sys.settrace(None)
                pool.close()
            self.assertEqual(fired, [True])
            self.assertEqual(marks, [])

    def test_a_write_interrupted_inside_an_exit_stack_does_not_hold_the_lock(self):
        import contextlib
        import sys

        from tinyjobs.backends.sqlite.connection import EXIT_CODE

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            fired: list[bool] = []

            def tracer(frame, event, arg):
                if event == "call" and frame.f_code is EXIT_CODE and not fired:
                    fired.append(True)
                    raise KeyboardInterrupt
                return None

            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                try:
                    with contextlib.ExitStack() as stack:
                        stack.enter_context(pool.write()).execute(
                            "INSERT INTO marks (n) VALUES (1)"
                        )
                        sys.settrace(tracer)
                except KeyboardInterrupt as error:
                    kept = error
                finally:
                    sys.settrace(None)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                self.assertFalse(pool.connection.in_transaction)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertIsNotNone(kept)
            self.assertEqual(marks, [(2,)])

    def test_the_fork_gate_holds_while_a_write_runs_and_while_it_exits(self):
        import sys

        from tinyjobs.backends.sqlite.connection import EXIT_CODE, FORK_GATE

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            inside, stop, at_exit, go = (threading.Event() for _ in range(4))

            def tracer(frame, event, arg):
                if event == "call" and frame.f_code is EXIT_CODE:
                    at_exit.set()
                    go.wait(10)
                return None

            def holder(fail: bool) -> None:
                sys.settrace(tracer)
                try:
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        inside.set()
                        count = 0
                        while not stop.is_set() and count < 50_000_000:
                            count += 1
                        if fail:
                            raise ValueError
                except ValueError:
                    pass
                finally:
                    sys.settrace(None)

            quiet: list[bool] = []
            try:
                for fail in (False, True):
                    for event in (inside, stop, at_exit, go):
                        event.clear()
                    thread = threading.Thread(target=holder, args=(fail,))
                    thread.start()
                    self.assertTrue(inside.wait(10))
                    for _ in range(5):
                        quiet.append(FORK_GATE._quiet(os.getpid()))
                        time.sleep(0.002)
                    stop.set()
                    self.assertTrue(at_exit.wait(10))
                    quiet.append(FORK_GATE._quiet(os.getpid()))
                    go.set()
                    thread.join(10)
            finally:
                stop.set()
                go.set()
                pool.close()
            self.assertEqual(quiet, [False] * 12)

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs interval timers")
    def test_a_signal_handler_call_during_a_loop_in_a_write_leaves_it_whole(self):
        from tinyjobs import TinyJobs

        with tempfile.TemporaryDirectory() as directory:
            app = TinyJobs(f"sqlite:///{Path(directory) / 'jobs.db'}").start()
            seen: list = []
            previous = signal.signal(signal.SIGALRM, lambda *_: seen or seen.append(app.counts()))
            try:
                with app.backend.transaction() as connection:
                    connection.execute("CREATE TABLE marks (n INTEGER)")
                with app.backend.transaction() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    signal.setitimer(signal.ITIMER_REAL, 0.01)
                    deadline = time.monotonic() + 5
                    while not seen and time.monotonic() < deadline:
                        try:
                            pass
                        except ValueError:
                            pass
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                with app.backend.transaction() as connection:
                    marks = connection.execute("SELECT n FROM marks").fetchall()
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous)
                app.close()
            self.assertEqual(len(seen), 1)
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_the_owner_thread_leaves_a_write_committing_elsewhere_alone(self):
        import inspect
        import sys
        from contextlib import contextmanager

        from tinyjobs.backends.sqlite import connection as module

        lines, first = inspect.getsourcelines(module.ConnectionPool._close_root)
        commit = first + next(i for i, line in enumerate(lines) if '"COMMIT"' in line)
        at_commit, resume = threading.Event(), threading.Event()

        def pause(frame, event, arg):
            closing = module.ConnectionPool._close_root.__code__
            if frame.f_code is closing and frame.f_lineno == commit:
                at_commit.set()
                resume.wait(5)
            return pause

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            outcomes: list[str] = []

            @contextmanager
            def dependency():
                with pool.write() as connection:
                    yield connection

            def finish() -> None:
                sys.settrace(pause)
                try:
                    manager.__exit__(None, None, None)
                    outcomes.append("committed")
                except Exception as error:
                    outcomes.append(repr(error))
                finally:
                    sys.settrace(None)

            try:
                for step in ("heal", "write"):
                    at_commit.clear(), resume.clear()
                    manager = dependency()
                    manager.__enter__().execute("INSERT INTO marks (n) VALUES (1)")
                    worker = threading.Thread(target=finish)
                    worker.start()
                    self.assertTrue(at_commit.wait(5))
                    if step == "heal":
                        pool.heal_here()
                    else:
                        with suppress(sqlite3.OperationalError):
                            with pool.write():
                                pass
                    resume.set()
                    worker.join(5)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                resume.set()
                pool.close()
            self.assertEqual(outcomes, ["committed", "committed"])
            self.assertEqual([tuple(row) for row in marks], [(1,), (1,)])

    def test_a_write_inside_a_suspended_generator_write_joins_it(self):
        import asyncio
        import contextlib

        def save(pool, n):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))

        class Unit:
            def __init__(self, pool):
                self.pool, self.lock = pool, threading.Lock()

            def __enter__(self):
                with self.lock:
                    self.scope = self.pool.write()
                    return self.scope.__enter__()

            def __exit__(self, *details):
                return self.scope.__exit__(*details)

        def fixture(pool):
            with pool.write() as connection:
                yield connection

        def through_next(pool):
            steps = fixture(pool)
            next(steps).execute("INSERT INTO marks (n) VALUES (1)")
            save(pool, 2)
            next(steps, None)

        def through_stack(pool):
            with contextlib.ExitStack() as stack:
                stack.enter_context(contextlib.contextmanager(fixture)(pool)).execute(
                    "INSERT INTO marks (n) VALUES (1)"
                )
                save(pool, 2)

        def through_moved_stack(pool):
            with contextlib.ExitStack() as stack:
                stack.enter_context(pool.write()).execute("INSERT INTO marks (n) VALUES (1)")
                kept = stack.pop_all()
            save(pool, 2)
            pool.heal_here()
            kept.close()

        def through_wrapper(pool):
            with Unit(pool) as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                save(pool, 2)
                pool.heal_here()

        def through_async(pool):
            @contextlib.asynccontextmanager
            async def transaction():
                with pool.write() as connection:
                    yield connection

            async def main():
                async with transaction() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    save(pool, 2)

            asyncio.run(main())

        for shape in (through_next, through_stack, through_moved_stack, through_wrapper, through_async):
            with self.subTest(shape.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.5)
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    began = time.monotonic()
                    shape(pool)
                    took = time.monotonic() - began
                    marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                finally:
                    pool.close()
                self.assertLess(took, 0.4)
                self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_an_interrupt_at_the_end_of_entry_does_not_keep_the_lock(self):
        import sys

        from tinyjobs.backends.sqlite.connection import _WriteScope

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                with self.assertLogs("tinyjobs.sqlite", "WARNING"):
                    try:
                        self.interrupt_on_return(_WriteScope.__enter__.__code__)
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                    except KeyboardInterrupt:
                        pass
                    finally:
                        sys.settrace(None)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                self.assertFalse(pool.connection.in_transaction)
            finally:
                other.close()
                pool.close()

    def test_a_large_function_writing_through_an_exit_stack_stays_fast(self):
        import contextlib

        lines = "\n".join(f"        n{i} = {i} + len(str({i}))" for i in range(400))
        source = (
            "def work(pool, contextlib):\n"
            "    with contextlib.ExitStack() as stack:\n"
            "        stack.enter_context(pool.write())\n"
            f"{lines}\n"
        )
        namespace: dict = {}
        exec(compile(source, "<large>", "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            try:
                began = time.monotonic()
                namespace["work"](pool, contextlib)
                took = time.monotonic() - began
            finally:
                pool.close()
        self.assertLess(took, 0.5)

    def test_a_collected_generator_write_keeps_its_work_inside_the_outer_one(self):
        class Batch:
            def __init__(self, pool):
                self.me = self
                self.steps = self.produce(pool)

            def produce(self, pool):
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                    yield

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            gc.disable()
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    batch = Batch(pool)
                    next(batch.steps)
                    del batch
                    gc.collect()
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                gc.enable()
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (3,)])

    def test_a_write_outlives_the_thread_that_entered_it(self):
        from contextlib import contextmanager

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")

            @contextmanager
            def dependency():
                with pool.write() as connection:
                    yield connection

            manager = dependency()
            held: list = []

            def enter() -> None:
                held.append(manager.__enter__())
                held[0].execute("INSERT INTO marks (n) VALUES (1)")

            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                entering = threading.Thread(target=enter)
                entering.start()
                entering.join(5)
                del entering
                gc.collect()
                time.sleep(0.2)
                manager.__exit__(None, None, None)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(marks, [(1,)])

    def test_a_lent_write_left_open_by_a_finished_thread_is_rolled_back(self):
        import contextvars
        import sys

        from tinyjobs.backends.sqlite.connection import ConnectionPool as Pool

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=5)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            kept: list = []

            def held():
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    yield

            steps = held()
            closing = threading.Timer(0.1, next, args=(steps, None))

            def worker() -> None:
                contextvars.copy_context().run(next, steps)
                closing.start()
                kept.append(pool.write())
                try:
                    self.interrupt_on_return(Pool._begin.__code__)
                    with kept[0] as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (2)")
                except KeyboardInterrupt:
                    pass
                finally:
                    sys.settrace(None)

            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                thread = threading.Thread(target=worker)
                thread.start()
                thread.join(5)
                closing.join(5)
                del thread
                gc.collect()
                time.sleep(0.2)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                other.close()
                pool.close()
            self.assertEqual(len(kept), 1)
            self.assertEqual(marks, [(1,)])

    def test_a_wrapper_entered_through_an_exit_stack_keeps_its_write_open(self):
        import contextlib

        class Unit:
            def __init__(self, pool):
                self.pool = pool

            def __enter__(self):
                self.scope = self.pool.write()
                return self.scope.__enter__()

            def __exit__(self, *details):
                return self.scope.__exit__(*details)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with contextlib.ExitStack() as stack:
                    stack.enter_context(Unit(pool)).execute("INSERT INTO marks (n) VALUES (1)")
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (2)")
                    pool.heal_here()
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_a_write_collected_on_another_thread_frees_the_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            opened, finish = threading.Event(), threading.Event()

            def holder() -> None:
                def held():
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        yield

                steps = held()
                next(steps)
                box: list = [steps]
                box.append(box)
                del steps, box
                opened.set()
                finish.wait(5)

            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            gc.disable()
            try:
                thread = threading.Thread(target=holder)
                thread.start()
                self.assertTrue(opened.wait(5))
                with self.assertLogs("tinyjobs.sqlite", "WARNING"):
                    gc.collect()
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                marks = other.execute("SELECT n FROM marks").fetchall()
            finally:
                gc.enable()
                finish.set()
                thread.join(5)
                other.close()
                pool.close()
            self.assertEqual(marks, [])

    def test_a_write_on_another_pool_does_not_break_joining(self):
        def fixture(pool):
            with pool.write() as connection:
                yield connection

        def save(pool, n):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))

        with tempfile.TemporaryDirectory() as directory:
            pools = [
                ConnectionPool(str(Path(directory) / f"{name}.db"), busy_timeout=0.2)
                for name in ("main", "other")
            ]
            for pool in pools:
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            main, other = pools
            try:
                steps = fixture(main)
                next(steps).execute("INSERT INTO marks (n) VALUES (1)")
                save(other, 9)
                save(main, 2)
                next(steps, None)
                marks = main.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                for pool in pools:
                    pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_a_dropped_write_frees_the_lock_without_a_collection(self):
        import contextlib

        def manual(pool):
            scope = pool.write()
            scope.__enter__().execute("INSERT INTO marks (n) VALUES (1)")

        def stacked(pool):
            stack = contextlib.ExitStack()
            stack.enter_context(pool.write()).execute("INSERT INTO marks (n) VALUES (1)")

        for shape in (manual, stacked):
            with self.subTest(shape.__name__), tempfile.TemporaryDirectory() as directory:
                path = str(Path(directory) / "jobs.db")
                pool = ConnectionPool(path, busy_timeout=0.2)
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
                gc.disable()
                try:
                    with self.assertLogs("tinyjobs.sqlite", "WARNING"):
                        shape(pool)
                    other.execute("BEGIN IMMEDIATE")
                    other.execute("ROLLBACK")
                    marks = other.execute("SELECT n FROM marks").fetchall()
                finally:
                    gc.enable()
                    other.close()
                    pool.close()
                self.assertEqual(marks, [])

    def test_a_generator_that_only_read_does_not_poison_its_outer_write(self):
        class Reader:
            def __init__(self, pool):
                self.me = self
                self.steps = self.read(pool)

            def read(self, pool):
                scope = pool.write()
                yield scope.__enter__().execute("SELECT count(*) FROM marks").fetchone()

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            gc.disable()
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    reader = Reader(pool)
                    next(reader.steps)
                    del reader
                    gc.collect()
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                gc.enable()
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_closing_a_nested_generator_write_keeps_the_outer_rows(self):
        def pending(pool):
            with pool.write() as connection:
                yield from (row[0] for row in connection.execute("SELECT n FROM queue ORDER BY n").fetchall())

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE queue (n INTEGER)")
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO queue (n) VALUES (?)", [(1,), (2,)])
            try:
                with pool.write() as connection:
                    rows = pending(pool)
                    connection.execute("INSERT INTO marks (n) VALUES (?)", (next(rows),))
                    rows.close()
                    connection.execute("INSERT INTO marks (n) VALUES (99)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (99,)])

    def test_a_reading_generator_kept_alive_by_an_error_keeps_the_outer_rows(self):
        def lookup(pool):
            with pool.write() as connection:
                yield connection.execute("SELECT count(*) FROM marks").fetchone()

        def handle(pool):
            try:
                steps = lookup(pool)
                next(steps)
                raise LookupError
            except LookupError as error:
                return error

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            gc.disable()
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    failure = handle(pool)
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                    del failure
                    gc.collect()
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                gc.enable()
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (3,)])

    def test_the_thread_that_collects_a_nested_write_does_not_change_the_outcome(self):
        def helper(pool):
            with pool.write() as connection:
                yield connection

        def collect_here():
            gc.collect()

        def collect_elsewhere():
            collector = threading.Thread(target=gc.collect)
            collector.start()
            collector.join(5)

        for n, collect in enumerate((collect_here, collect_elsewhere)):
            with self.subTest(collect.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                gc.disable()
                try:
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))
                        steps = helper(pool)
                        next(steps)
                        box: list = [steps]
                        box.append(box)
                        del steps, box
                        collect()
                    marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                finally:
                    gc.enable()
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], [(n,)])

    def test_a_write_after_closing_idle_connections_opens_a_new_one(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                pool.close_idle()
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,)])

    def test_only_a_few_lent_connections_are_kept_for_reuse(self):
        from tinyjobs.backends.sqlite.connection import LENT_KEPT

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            try:
                pool.connection
                lent = [pool._lend() for _ in range(LENT_KEPT + 3)]
                for connection in lent:
                    pool._give_back(connection)
                kept = len(pool._lent)
                still_open = sum(connection in pool._all for connection in lent)
            finally:
                pool.close()
            self.assertEqual(kept, LENT_KEPT)
            self.assertEqual(still_open, LENT_KEPT)

    def test_a_caught_nested_failure_rolls_back_only_the_nested_write(self):
        import contextlib

        def failing_insert(connection):
            connection.execute("INSERT INTO marks (n) VALUES (2)")
            connection.execute("CREATE TABLE half_done (n INTEGER)")
            connection.execute("PRAGMA user_version = 7")
            raise ValueError

        @contextlib.contextmanager
        def helper(pool):
            with pool.write() as connection:
                yield connection

        @contextlib.asynccontextmanager
        async def async_helper(pool):
            with pool.write() as connection:
                yield connection

        def plain(pool):
            with pool.write() as connection:
                failing_insert(connection)

        def through_helper(pool):
            with helper(pool) as connection:
                failing_insert(connection)

        def through_stack(pool):
            with contextlib.ExitStack() as stack:
                failing_insert(stack.enter_context(pool.write()))

        def through_coroutine(pool):
            async def inner():
                with pool.write() as connection:
                    await asyncio.sleep(0)
                    failing_insert(connection)

            async def main():
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with suppress(ValueError):
                        await inner()

            asyncio.run(main())

        def through_async_helper(pool):
            async def main():
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with suppress(ValueError):
                        async with async_helper(pool) as inner:
                            failing_insert(inner)

            asyncio.run(main())

        def outer(shape):
            def run(pool):
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with suppress(ValueError):
                        shape(pool)

            return run

        shapes = {
            "plain": outer(plain),
            "helper": outer(through_helper),
            "stack": outer(through_stack),
            "coroutine": through_coroutine,
            "async helper": through_async_helper,
        }
        for name, shape in shapes.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    shape(pool)
                    marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                    tables = pool.connection.execute(
                        "SELECT count(*) FROM sqlite_master WHERE name = 'half_done'"
                    ).fetchone()[0]
                    version = pool.connection.execute("PRAGMA user_version").fetchone()[0]
                finally:
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], [(1,)])
                self.assertEqual((tables, version), (0, 0))

    def test_closing_a_generator_with_a_wrapped_write_keeps_the_outer_rows(self):
        class Unit:
            def __init__(self, pool):
                self.pool = pool

            def __enter__(self):
                self.scope = self.pool.write()
                return self.scope.__enter__()

            def __exit__(self, *details):
                return self.scope.__exit__(*details)

        def reader(pool):
            with Unit(pool) as connection:
                yield from [row[0] for row in connection.execute("SELECT n FROM queue")]

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE queue (n INTEGER)")
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO queue (n) VALUES (?)", [(1,), (2,)])
            try:
                with pool.write() as connection:
                    rows = reader(pool)
                    connection.execute("INSERT INTO marks (n) VALUES (?)", (next(rows),))
                    rows.close()
                    connection.execute("INSERT INTO marks (n) VALUES (99)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (99,)])

    def test_a_write_whose_outer_ends_under_it_commits_nothing(self):
        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield connection

        def close(steps):
            steps.close()

        def throw(steps):
            with suppress(ValueError):
                steps.throw(ValueError)

        for end in (close, throw):
            with self.subTest(end.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    steps = dependency(pool)
                    next(steps)
                    with self.assertRaises(RuntimeError):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (2)")
                            end(steps)
                            connection.execute("INSERT INTO marks (n) VALUES (3)")
                    marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                    self.assertFalse(pool.connection.in_transaction)
                finally:
                    pool.close()
                self.assertEqual(marks, [])

    def test_an_outer_write_commits_past_a_suspended_nested_generator(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    steps = audit(pool)
                    next(steps)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                    steps.close()
                    connection.execute("INSERT INTO marks (n) VALUES (4)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (3,), (4,)])

    def test_work_done_inside_a_wrappers_enter_stays_in_its_write(self):
        class Unit:
            def __init__(self, pool):
                self.pool = pool

            def __enter__(self):
                self.scope = self.pool.write()
                connection = self.scope.__enter__()
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                with self.pool.write() as inner:
                    inner.execute("INSERT INTO marks (n) VALUES (2)")
                return connection

            def __exit__(self, *details):
                return self.scope.__exit__(*details)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(ValueError):
                    with Unit(pool) as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (3)")
                        raise ValueError
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_an_interrupt_inside_the_cleanup_of_a_dropped_write_still_frees_it(self):
        import sys

        from tinyjobs.backends.sqlite import connection as module

        def interrupt_cleanup(frame, event, arg):
            if event == "call" and frame.f_code is module._roll_back_abandoned.__code__:
                sys.settrace(None)
                raise KeyboardInterrupt
            return None

        def drop(pool):
            scope = pool.write()
            scope.__enter__().execute("INSERT INTO marks (n) VALUES (1)")
            sys.settrace(interrupt_cleanup)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            reported: list = []
            try:
                with mock.patch.object(sys, "unraisablehook", reported.append):
                    with self.assertLogs("tinyjobs.sqlite", "WARNING"):
                        drop(pool)
                sys.settrace(None)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                self.assertEqual(pool._writers, {})
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                sys.settrace(None)
                other.close()
                pool.close()
            self.assertEqual(len(reported), 1)
            self.assertEqual([tuple(row) for row in marks], [(2,)])

    def test_asyncio_run_inside_a_write_joins_it(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")

            async def inner():
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")

            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    asyncio.run(inner())
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_the_context_keeps_only_writes_still_open(self):
        from tinyjobs.backends.sqlite import connection as module

        with tempfile.TemporaryDirectory() as directory:
            pools = [ConnectionPool(str(Path(directory) / f"{n}.db")) for n in range(50)]
            try:
                for pool in pools:
                    with pool.write() as connection:
                        connection.execute("SELECT 1")
                held = len(module._writing.get())
            finally:
                for pool in pools:
                    pool.close()
            self.assertLessEqual(held, 1)

    def test_closing_idle_connections_spares_one_a_running_thread_uses(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            inside, release = threading.Event(), threading.Event()

            def pause():
                inside.set()
                release.wait(5)
                return 1

            def hold():
                connection = pool.connection
                connection.create_function("pause", 0, pause)
                connection.execute("SELECT pause()").fetchone()

            holder = threading.Thread(target=hold)
            holder.start()
            try:
                self.assertTrue(inside.wait(5))
                used = next(c for c, owner in pool._owners.items() if owner is holder)
                with mock.patch.object(threading.Thread, "is_alive", lambda self: False):
                    pool.close_idle()
                self.assertIn(used, pool._all)
            finally:
                release.set()
                holder.join(5)
                pool.close()

    def test_an_outer_write_waiting_on_a_paused_joined_write_stays_open(self):
        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield connection

        def endpoint(pool, steps):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                next(steps, None)
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        async def in_tasks(pool):
            async def dependency():
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    yield connection

            async def endpoint():
                steps = dependency()
                await steps.__anext__()
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                    with suppress(StopAsyncIteration):
                        await steps.__anext__()
                    await asyncio.sleep(0.05)
                    connection.execute("INSERT INTO marks (n) VALUES (3)")

            async def other():
                await asyncio.sleep(0.01)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (4)")

            return await asyncio.gather(endpoint(), other(), return_exceptions=True)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                steps = dependency(pool)
                next(steps)
                paused = endpoint(pool, steps)
                next(paused)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (4)")
                next(paused, None)
                joined = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                pool.connection.execute("DELETE FROM marks")
                outcomes = asyncio.run(in_tasks(pool))
                apart = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in joined], [(1,), (2,), (3,), (4,)])
            self.assertIsNone(outcomes[0])
            self.assertIsInstance(outcomes[1], RuntimeError)
            self.assertEqual([tuple(row) for row in apart], [(1,), (2,), (3,)])

    def test_a_suspended_write_resumed_after_its_outer_ended_cannot_write(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                raise ValueError

        def alone(steps, pool):
            next(steps, None)

        def inside_a_later_write(steps, pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (4)")
                next(steps, None)

        for resume in (alone, inside_a_later_write):
            with self.subTest(resume.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        steps = audit(pool)
                        next(steps)
                    with self.assertRaises(sqlite3.ProgrammingError):
                        resume(steps, pool)
                    marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                finally:
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_a_nested_write_failing_after_its_parent_ended_poisons_the_outer(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                raise ValueError

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(RuntimeError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        with pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                            steps = audit(pool)
                            next(steps)
                        with suppress(ValueError):
                            next(steps, None)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_an_interrupt_while_a_waiting_outer_closes_commits_nothing(self):
        import sys

        from tinyjobs.backends.sqlite import connection as module

        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield connection

        def interrupt(frame, event, arg):
            if event == "call" and frame.f_code is module.ConnectionPool._undo_savepoint.__code__:
                sys.settrace(None)
                raise KeyboardInterrupt
            return None

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                steps = dependency(pool)
                next(steps)
                with self.assertRaises(KeyboardInterrupt):
                    try:
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (2)")
                            next(steps, None)
                            sys.settrace(interrupt)
                            raise ValueError
                    finally:
                        sys.settrace(None)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                self.assertFalse(pool.connection.in_transaction)
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_a_lost_nested_write_that_changed_the_schema_poisons_the_outer(self):
        class Careless:
            def __init__(self, pool):
                self.pool = pool

            def __enter__(self):
                self.scope = self.pool.write()
                return self.scope.__enter__()

            def __exit__(self, kind, error, traceback):
                if kind is None:
                    return self.scope.__exit__(None, None, None)
                return False

        for statement in ("CREATE TABLE extra (n INTEGER)", "PRAGMA user_version = 7"):
            with self.subTest(statement), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    with self.assertRaises(RuntimeError):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            with suppress(ValueError):
                                with Careless(pool) as inner:
                                    inner.execute(statement)
                                    raise ValueError
                    tables = pool.connection.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]
                    version = pool.connection.execute("PRAGMA user_version").fetchone()[0]
                finally:
                    pool.close()
                self.assertEqual((tables, version), (1, 0))

    def test_a_helper_delegating_with_yield_from_rolls_back_only_its_write(self):
        import contextlib

        def unit(pool):
            with pool.write() as connection:
                yield connection

        @contextlib.contextmanager
        def helper(pool):
            yield from unit(pool)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with suppress(ValueError), helper(pool) as inner:
                        inner.execute("INSERT INTO marks (n) VALUES (2)")
                        raise ValueError
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,)])

    def test_asyncio_run_beside_a_suspended_generator_write_joins_it(self):
        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield connection

        async def endpoint(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                steps = dependency(pool)
                next(steps)
                asyncio.run(endpoint(pool))
                next(steps, None)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_an_outer_write_ending_under_a_write_running_elsewhere_rolls_back(self):
        running, finish = threading.Event(), threading.Event()

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                running.set()
                finish.wait(5)
                yield

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            worker = None
            try:
                with self.assertRaisesRegex(RuntimeError, "still running on another thread"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        steps = audit(pool)
                        next(steps)
                        worker = threading.Thread(target=next, args=(steps,))
                        worker.start()
                        self.assertTrue(running.wait(5))
                finish.set()
                worker.join(5)
                self.assertFalse(pool.connection.in_transaction)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                finish.set()
                pool.close()
            self.assertEqual(marks, [])

    def test_a_joined_write_left_running_elsewhere_cannot_write_after_the_rollback(self):
        running, finish = threading.Event(), threading.Event()
        errors = []

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                running.set()
                finish.wait(5)
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        def resume(steps):
            try:
                next(steps, None)
            except sqlite3.ProgrammingError as error:
                errors.append(error)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            worker = None
            try:
                with self.assertRaisesRegex(RuntimeError, "still running on another thread"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        steps = audit(pool)
                        next(steps)
                        worker = threading.Thread(target=resume, args=(steps,))
                        worker.start()
                        self.assertTrue(running.wait(5))
                finish.set()
                worker.join(5)
                self.assertFalse(pool.connection.in_transaction)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                finish.set()
                pool.close()
            self.assertEqual(len(errors), 1)
            self.assertEqual(marks, [])

    def test_a_suspended_write_resumed_after_its_outer_ended_on_another_thread_cannot_write(self):
        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        calls, outcomes = queue.Queue(), queue.Queue()

        def serve():
            while (call := calls.get()) is not None:
                try:
                    outcomes.put((True, call()))
                except BaseException as error:
                    outcomes.put((False, error))

        def on_other_thread(call):
            calls.put(call)
            succeeded, value = outcomes.get(timeout=5)
            if not succeeded:
                raise value
            return value

        def later(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (4)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            server = threading.Thread(target=serve)
            server.start()
            try:
                outer, steps = dependency(pool), audit(pool)
                on_other_thread(lambda: next(outer))
                on_other_thread(lambda: next(steps))
                next(outer, None)
                with self.assertRaises(sqlite3.ProgrammingError):
                    on_other_thread(lambda: next(steps, None))
                on_other_thread(lambda: later(pool))
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                calls.put(None)
                server.join(5)
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (4,)])

    def test_a_grandchild_resumed_after_the_root_committed_cannot_write(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (4)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    with pool.write() as inner:
                        inner.execute("INSERT INTO marks (n) VALUES (2)")
                        steps = audit(pool)
                        next(steps)
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps, None)
                self.assertFalse(pool.connection.in_transaction)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (3,)])

    def test_a_grandchild_outliving_a_failed_write_rolls_back_the_root(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (4)")
                yield

        def transfer(pool, steps):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                next(steps)
                raise ValueError

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaisesRegex(RuntimeError, "rolled back instead of committing"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        steps = audit(pool)
                        with suppress(ValueError):
                            transfer(pool, steps)
                        next(steps)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_a_grandchild_outliving_its_parent_joins_the_root(self):
        def audit(pool, fail):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (4)")
                if fail:
                    raise ValueError

        def run(pool, fail):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                with pool.write() as inner:
                    inner.execute("INSERT INTO marks (n) VALUES (2)")
                    steps = audit(pool, fail)
                    next(steps)
                with suppress(ValueError):
                    next(steps, None)

        for fail, expected in ((False, [(1,), (2,), (3,), (4,)]), (True, [])):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, "rolled back instead of committing"):
                            run(pool, fail)
                    else:
                        run(pool, fail)
                    marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                finally:
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], expected)

    def test_a_failed_write_whose_child_runs_elsewhere_poisons_the_outer(self):
        running, finish = threading.Event(), threading.Event()

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                running.set()
                finish.wait(5)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            worker = None
            try:
                with self.assertRaisesRegex(RuntimeError, "rolled back instead of committing"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        with suppress(ValueError), pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                            steps = audit(pool)
                            next(steps)
                            worker = threading.Thread(target=next, args=(steps, None))
                            worker.start()
                            self.assertTrue(running.wait(5))
                            raise ValueError
                        finish.set()
                        worker.join(5)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                finish.set()
                pool.close()
            self.assertEqual(marks, [])

    def test_a_write_entered_by_hand_and_left_open_cannot_write_after_its_outer_ended(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaisesRegex(RuntimeError, "entered by hand is still open"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        inner = pool.write()
                        inner.__enter__().execute("INSERT INTO marks (n) VALUES (2)")
                with self.assertRaises(sqlite3.ProgrammingError):
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                with suppress(RuntimeError):
                    inner.__exit__(None, None, None)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (4)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(4,)])

    def test_every_handle_of_a_write_cut_loose_by_its_outer_refuses_to_write(self):
        @contextmanager
        def helper(pool):
            with pool.write() as connection:
                yield connection

        def batches(pool):
            with helper(pool) as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield connection, connection.cursor()
                yield

        def attempt(statement):
            try:
                statement()
            except sqlite3.ProgrammingError:
                return "refused"
            return "written"

        def from_a_thread(statement):
            outcome = []
            thread = threading.Thread(target=lambda: outcome.append(attempt(statement)))
            thread.start()
            thread.join(5)
            return outcome[0]

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    steps = batches(pool)
                    handle, cursor = next(steps)
                insert = "INSERT INTO marks (n) VALUES (3)"
                outcomes = [
                    attempt(lambda: handle.execute(insert)),
                    attempt(lambda: cursor.execute(insert)),
                    attempt(lambda: handle.cursor()),
                    from_a_thread(lambda: handle.execute(insert)),
                ]
                steps.close()
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (4)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual(outcomes, ["refused"] * 4)
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (4,)])

    def test_a_write_entered_by_hand_below_a_suspended_write_cannot_write_after_the_rollback(self):
        def audit(pool):
            with pool.write() as connection:
                held = pool.write()
                held.__enter__()
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaisesRegex(RuntimeError, "entered by hand is still open"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        steps = audit(pool)
                        next(steps)
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_a_statement_running_elsewhere_when_its_outer_rolls_back_commits_nothing(self):
        started = threading.Event()

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                started.set()
                connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(200000)])

        def slowly(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                started.set()

                def rows():
                    for n in range(30):
                        time.sleep(0.005)
                        yield (n,)

                connection.executemany("INSERT INTO marks (n) VALUES (?)", rows())

        def resume(steps):
            with suppress(sqlite3.Error):
                next(steps, None)

        for writer in (audit, slowly):
            with self.subTest(writer.__name__), tempfile.TemporaryDirectory() as directory:
                started.clear()
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                worker = None
                try:
                    with self.assertRaisesRegex(RuntimeError, "still running on another thread"):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            steps = writer(pool)
                            next(steps)
                            worker = threading.Thread(target=resume, args=(steps,))
                            worker.start()
                            self.assertTrue(started.wait(5))
                            time.sleep(0.02)
                    worker.join(10)
                    marks = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                finally:
                    pool.close()
                self.assertEqual(marks, 0)

    def test_a_thread_ending_never_closes_a_connection_another_thread_is_using(self):
        started = threading.Event()
        outcome = []

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                started.set()

                def rows():
                    for n in range(30):
                        time.sleep(0.005)
                        yield (n,)

                try:
                    connection.executemany("INSERT INTO marks (n) VALUES (?)", rows())
                except sqlite3.Error as error:
                    outcome.append(type(error).__name__)

        def owner(pool, holder):
            with suppress(RuntimeError), pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                steps = audit(pool)
                next(steps)
                holder.append(threading.Thread(target=next, args=(steps, None)))
                holder[0].start()
                started.wait(5)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            holder = []
            try:
                thread = threading.Thread(target=owner, args=(pool, holder))
                thread.start()
                thread.join(5)
                del thread
                gc.collect()
                holder[0].join(10)
                marks = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
            finally:
                pool.close()
            self.assertEqual(marks, 0)

    def test_a_statement_that_waited_out_a_fork_still_sees_its_write_cut_loose(self):
        go, errors = threading.Event(), []

        def child(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield connection

        def late(connection):
            go.wait(5)
            try:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
            except sqlite3.ProgrammingError as error:
                errors.append(error)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            worker = None
            self.addCleanup(runtime._fork_marks.pop, -1, None)
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    steps = child(pool)
                    handle = next(steps)
                    worker = threading.Thread(target=late, args=(handle,))
                    worker.start()
                    runtime._fork_marks[-1] = os.getpid()
                    go.set()
                    time.sleep(0.05)
                    threading.Timer(0.2, runtime._fork_marks.pop, (-1, None)).start()
                worker.join(5)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual(len(errors), 1)
            self.assertIn("outlived the write it joined", str(errors[0]))
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,)])

    def test_a_write_cut_loose_cannot_open_a_fresh_write_or_use_the_pool(self):
        def batch(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                with pool.write() as inner:
                    inner.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                pool.connection.execute("INSERT INTO marks (n) VALUES (4)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                    steps = batch(pool)
                    next(steps)
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (5)")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (5,)])

    def test_a_write_ending_while_another_thread_runs_its_statement_commits_none_of_it(self):
        started = threading.Event()

        def rows():
            started.set()
            for n in range(40):
                time.sleep(0.005)
                yield (n,)

        def insert(connection):
            with suppress(sqlite3.Error):
                connection.executemany("INSERT INTO marks (n) VALUES (?)", rows())

        def root(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (-1)")
                threading.Thread(target=insert, args=(connection,)).start()
                started.wait(5)
                raise ValueError

        def nested(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (-1)")
                with suppress(ValueError), pool.write() as inner:
                    threading.Thread(target=insert, args=(inner,)).start()
                    started.wait(5)
                    raise ValueError

        for run, failure in ((root, ValueError), (nested, RuntimeError)):
            with self.subTest(run.__name__), tempfile.TemporaryDirectory() as directory:
                started.clear()
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                try:
                    with self.assertRaises(failure):
                        run(pool)
                    time.sleep(0.3)
                    count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                finally:
                    pool.close()
                self.assertEqual(count, 0)

    def test_an_outer_waiting_on_a_running_write_commits_without_a_write_handed_to_it(self):
        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (3)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (4)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                stack = ExitStack()
                stack.enter_context(pool.write()).execute("INSERT INTO marks (n) VALUES (1)")
                with pool.write() as inner:
                    inner.execute("INSERT INTO marks (n) VALUES (2)")
                    steps = audit(pool)
                    next(steps)
                    stack.close()
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (5)")
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(1,), (2,), (3,), (5,)])

    def test_an_interrupt_while_an_outer_cuts_a_write_loose_still_retires_it(self):
        from tinyjobs.backends.sqlite import connection as module

        armed = []
        real = module._running_below

        def landing(scope):
            result = real(scope)
            if armed:
                armed.clear()
                raise KeyboardInterrupt
            return result

        def audit(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                steps = audit(pool)
                with mock.patch.object(module, "_running_below", landing):
                    with self.assertRaises(KeyboardInterrupt):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            next(steps)
                            armed.append(True)
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (4)")
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(4,)])

    def test_a_thread_writing_between_calls_through_a_write_that_ended_is_refused(self):
        def import_rows(connection, stop, outcome):
            try:
                for n in range(50):
                    time.sleep(0.01)
                    connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))
            except sqlite3.ProgrammingError as error:
                outcome.append(error)

        def root(pool, worker):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (-1)")
                worker(connection)
                time.sleep(0.05)
                raise TimeoutError

        def nested(pool, worker):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (-1)")
                with suppress(TimeoutError), pool.write() as inner:
                    worker(inner)
                    time.sleep(0.05)
                    raise TimeoutError

        for run, failure in ((root, TimeoutError), (nested, RuntimeError)):
            with self.subTest(run.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER)")
                outcome, threads = [], []

                def worker(connection):
                    threads.append(threading.Thread(target=import_rows, args=(connection, None, outcome)))
                    threads[0].start()

                try:
                    with self.assertRaises(failure):
                        run(pool, worker)
                    threads[0].join(5)
                    count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                finally:
                    pool.close()
                self.assertEqual(len(outcome), 1)
                self.assertEqual(count, 0)

    def test_a_generator_write_finishing_inside_a_later_write_commits(self):
        def produce(pool):
            with pool.write() as connection:
                for n in range(3):
                    connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))
                    yield n

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (-1)")
                    rows = produce(pool)
                    next(rows)
                    with pool.write() as inner:
                        for n in rows:
                            inner.execute("INSERT INTO marks (n) VALUES (?)", (10 + n,))
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                pool.close()
            self.assertEqual([row[0] for row in marks], [-1, 0, 1, 2, 11, 12])

    def test_a_failing_write_cut_off_under_an_open_cursor_leaves_the_database_unlocked(self):
        started = threading.Event()

        def rows():
            started.set()
            for n in range(3):
                yield (100 + n,)
            time.sleep(0.2)

        def insert(connection):
            with suppress(sqlite3.Error):
                connection.executemany("INSERT INTO marks (n) VALUES (?)", rows())

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.5)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                with self.assertRaises(TimeoutError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        cursor = connection.execute("SELECT n FROM marks")
                        cursor.fetchone()
                        threading.Thread(target=insert, args=(connection,)).start()
                        self.assertTrue(started.wait(5))
                        time.sleep(0.05)
                        raise TimeoutError
                other = sqlite3.connect(path, timeout=0.5, isolation_level=None)
                try:
                    other.execute("BEGIN IMMEDIATE")
                    other.execute("ROLLBACK")
                finally:
                    other.close()
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (7)")
                count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                del cursor
            finally:
                pool.close()
            self.assertEqual(count, 11)

    def test_a_dropped_lent_write_with_a_suspended_child_retires_its_connection(self):
        def holder(pool):
            with pool.write():
                yield

        def child(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")
                yield
                connection.execute("INSERT INTO marks (n) VALUES (3)")

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                held = holder(pool)
                contextvars.copy_context().run(next, held)
                threading.Timer(0.2, held.close).start()
                stack = ExitStack()
                lent = stack.enter_context(pool.write())
                self.assertIsNot(lent, pool.connection)
                lent.execute("INSERT INTO marks (n) VALUES (1)")
                steps = child(pool)
                next(steps)
                del stack
                gc.collect()
                pool.heal_here()
                with self.assertRaises(sqlite3.ProgrammingError):
                    next(steps)
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_a_finished_write_keeps_none_of_its_frames_alive(self):
        class Payload:
            pass

        class Service:
            def save(self, pool):
                body = Payload()
                self.write = pool.write()
                with self.write as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (1)")
                return weakref.ref(body)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                service = Service()
                body = service.save(pool)
                gc.collect()
            finally:
                pool.close()
            self.assertIsNone(body())

    def test_a_write_interrupted_under_open_statements_leaves_nothing_locked(self):
        def locked(path):
            other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
            try:
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                return False
            except sqlite3.OperationalError:
                return True
            finally:
                other.close()

        def with_cursor(connection):
            return connection.execute("SELECT n FROM marks")

        def with_blob(connection):
            return connection.blobopen("marks", "n", 1, readonly=True)

        for opened, frozen in ((with_cursor, False), (with_blob, False), (with_cursor, True)):
            with self.subTest(opened.__name__, frozen=frozen), tempfile.TemporaryDirectory() as directory:
                path = str(Path(directory) / "jobs.db")
                pool = ConnectionPool(path, busy_timeout=0.5)
                pool.connection.execute("CREATE TABLE marks (n BLOB)")
                pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(b"x",) for _ in range(10)])
                try:
                    with self.assertRaises(sqlite3.OperationalError):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (x'00')")
                            statement = opened(connection)
                            if isinstance(statement, sqlite3.Cursor):
                                statement.fetchone()
                            if frozen:
                                gc.freeze()
                            connection.interrupt()
                            connection.execute("UPDATE marks SET n = x'01' WHERE rowid = 1")
                    if frozen:
                        gc.unfreeze()
                    self.assertFalse(pool.connection.in_transaction)
                    self.assertFalse(locked(path))
                    count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                    del statement
                finally:
                    gc.unfreeze()
                    pool.close()
                self.assertEqual(count, 10)

    def test_a_thread_inside_a_statement_when_a_write_begins_still_counts_as_a_visitor(self):
        began, opened, done = threading.Event(), threading.Event(), threading.Event()

        def rows():
            yield (100,)
            began.wait(5)
            yield (101,)
            opened.wait(5)
            yield (102,)

        def foreign(connection):
            with suppress(sqlite3.Error):
                connection.executemany("INSERT INTO marks (n) VALUES (?)", rows())
            done.set()

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            worker = threading.Thread(target=foreign, args=(pool.connection,))
            try:
                worker.start()
                time.sleep(0.05)
                with self.assertRaisesRegex(RuntimeError, "rolled back instead of committing"):
                    with pool.write() as connection:
                        began.set()
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        with suppress(ValueError), pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                            opened.set()
                            self.assertTrue(done.wait(5))
                            raise ValueError
                worker.join(5)
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
            finally:
                began.set()
                opened.set()
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(100,)])

    def test_an_outer_closed_from_inside_a_failing_write_reports_its_poisoning(self):
        def dependency(pool):
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                yield connection

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                steps = dependency(pool)
                handle = next(steps)
                reader = threading.Thread(target=lambda: handle.execute("SELECT count(*) FROM marks").fetchone())
                reader.start()
                reader.join(5)
                with self.assertRaisesRegex(RuntimeError, "rolled back instead of committing"):
                    with pool.write() as inner:
                        inner.execute("INSERT INTO marks (n) VALUES (2)")
                        next(steps, None)
                        raise ValueError
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])

    def test_a_ctrl_c_while_closing_open_statements_still_rolls_back(self):
        scan = gc.get_referrers
        pressed = []

        def interrupted(*objects):
            found = scan(*objects)
            if not pressed:
                pressed.append(True)
                raise KeyboardInterrupt
            return found

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.5)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                with self.assertRaises(KeyboardInterrupt), mock.patch.object(gc, "get_referrers", interrupted):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        cursor = connection.execute("SELECT n FROM marks")
                        cursor.fetchone()
                        connection.interrupt()
                        raise TimeoutError
                other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
                try:
                    other.execute("BEGIN IMMEDIATE")
                    other.execute("ROLLBACK")
                finally:
                    other.close()
                count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
            finally:
                pool.close()
            self.assertEqual(pressed, [True])
            self.assertEqual(count, 10)

    def test_a_failed_rollback_leaves_the_frozen_heap_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                gc.freeze()
                frozen = gc.get_freeze_count()
                young = [[] for _ in range(1000)]
                with self.assertRaises(TimeoutError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        cursor = connection.execute("SELECT n FROM marks")
                        cursor.fetchone()
                        connection.interrupt()
                        raise TimeoutError
                after = gc.get_freeze_count()
                in_transaction = pool.connection.in_transaction
                del young, cursor
            finally:
                gc.unfreeze()
                pool.close()
            self.assertFalse(in_transaction)
            self.assertLess(after, frozen + 1000)

    def test_ending_a_tinyjobs_transaction_by_hand_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(ValueError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        for end in (
                            connection.commit,
                            connection.rollback,
                            partial(connection.executescript, "INSERT INTO marks (n) VALUES (2);"),
                            partial(connection.cursor().executescript, "INSERT INTO marks (n) VALUES (3);"),
                        ):
                            with self.assertRaisesRegex(sqlite3.ProgrammingError, "would end its transaction"):
                                end()
                        self.assertTrue(connection.in_transaction)
                        raise ValueError
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                pool.connection.executescript("INSERT INTO marks (n) VALUES (4);")
                pool.connection.commit()
                pool.connection.rollback()
                after = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])
            self.assertEqual([tuple(row) for row in after], [(4,)])

    def test_a_transaction_sqlite_ended_inside_its_block_is_reported(self):
        for failing in (False, True):
            with self.subTest(failing=failing), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER UNIQUE)")
                pool.connection.execute("INSERT INTO marks (n) VALUES (0)")
                try:
                    with self.assertRaisesRegex(RuntimeError, "committed on its own") as caught:
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            with suppress(sqlite3.IntegrityError):
                                connection.execute("INSERT OR ROLLBACK INTO marks (n) VALUES (0)")
                            connection.execute("INSERT INTO marks (n) VALUES (2)")
                            if failing:
                                raise ValueError
                    marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                    pool.connection.execute("INSERT INTO marks (n) VALUES (3)")
                finally:
                    pool.close()
                self.assertEqual(isinstance(caught.exception.__context__, ValueError), failing)
                self.assertEqual([tuple(row) for row in marks], [(0,), (2,)])

    def test_the_error_that_ended_a_transaction_leaves_its_block_unchanged(self):
        def interrupted(connection):
            connection.execute("SELECT count(*) FROM marks").fetchone()
            reading = connection.execute("SELECT n FROM marks")
            reading.fetchone()
            connection.interrupt()
            connection.execute("UPDATE marks SET n = n + 1")

        def conflicting(connection):
            connection.execute("INSERT OR ROLLBACK INTO marks (n) VALUES (0)")

        for statement, error in ((interrupted, sqlite3.OperationalError), (conflicting, sqlite3.IntegrityError)):
            with self.subTest(statement.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER UNIQUE)")
                pool.connection.execute("INSERT INTO marks (n) VALUES (0)")
                try:
                    with self.assertRaises(error):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            statement(connection)
                    marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                finally:
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], [(0,)])

    def test_a_ctrl_c_before_the_rollback_of_a_failed_write_leaves_nothing_locked(self):
        rolled = connection_module._roll_back
        armed, pressed = [], []

        def interrupted(connection):
            if armed and not pressed:
                pressed.append(True)
                raise KeyboardInterrupt
            return rolled(connection)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(KeyboardInterrupt), mock.patch.object(connection_module, "_roll_back", interrupted):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        armed.append(True)
                        raise ValueError
                self.assertFalse(any(connection.in_transaction for connection in list(pool._all)))
                other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
                try:
                    other.execute("BEGIN IMMEDIATE")
                    other.execute("ROLLBACK")
                finally:
                    other.close()
                count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
            finally:
                pool.close()
            self.assertEqual(pressed, [True])
            self.assertEqual(count, 0)

    def test_ending_a_tinyjobs_transaction_through_the_connection_itself_is_refused(self):
        def nested_context(connection):
            with connection:
                connection.execute("INSERT INTO marks (n) VALUES (2)")

        def manual(connection):
            connection.isolation_level = "DEFERRED"

        def automatic(connection):
            connection.autocommit = True

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with self.assertRaises(ValueError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        for end in (nested_context, manual, automatic):
                            with self.assertRaisesRegex(sqlite3.ProgrammingError, "would end its"):
                                end(connection)
                        self.assertTrue(connection.in_transaction)
                        self.assertIsNone(connection.isolation_level)
                        raise ValueError
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
                with pool.connection as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (3)")
                after = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(marks, [])
            self.assertEqual([tuple(row) for row in after], [(3,)])

    def test_a_ctrl_c_while_the_heap_is_unfrozen_refreezes_it(self):
        unfreeze = gc.unfreeze

        def interrupted():
            unfreeze()
            raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                with self.assertRaises(KeyboardInterrupt), mock.patch.object(gc, "unfreeze", interrupted):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        cursor = connection.execute("SELECT n FROM marks")
                        cursor.fetchone()
                        gc.freeze()
                        connection.interrupt()
                        raise TimeoutError
                frozen = gc.get_freeze_count()
                in_transaction = pool.connection.in_transaction
                del cursor
            finally:
                gc.unfreeze()
                pool.close()
            self.assertGreater(frozen, 0)
            self.assertFalse(in_transaction)

    def test_refreezing_after_a_frozen_cursor_lets_pending_garbage_go(self):
        class Node:
            pass

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                with self.assertRaises(TimeoutError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        cursor = connection.execute("SELECT n FROM marks")
                        cursor.fetchone()
                        gc.freeze()
                        cycle = Node()
                        cycle.self = cycle
                        gone = weakref.ref(cycle)
                        del cycle
                        connection.interrupt()
                        raise TimeoutError
                gc.collect()
                collected = gone() is None
                in_transaction = pool.connection.in_transaction
                del cursor
            finally:
                gc.unfreeze()
                pool.close()
            self.assertFalse(in_transaction)
            self.assertTrue(collected)

    def test_a_later_error_does_not_hide_that_the_transaction_ended_early(self):
        def conflicting(connection):
            with suppress(sqlite3.IntegrityError):
                connection.execute("INSERT OR ROLLBACK INTO marks (n) VALUES (0)")

        def committing(connection):
            connection.execute("COMMIT")

        for end, kept in ((conflicting, [(0,), (2,)]), (committing, [(0,), (1,), (2,)])):
            with self.subTest(end.__name__), tempfile.TemporaryDirectory() as directory:
                pool = ConnectionPool(str(Path(directory) / "jobs.db"))
                pool.connection.execute("CREATE TABLE marks (n INTEGER UNIQUE)")
                pool.connection.execute("INSERT INTO marks (n) VALUES (0)")
                try:
                    with self.assertRaisesRegex(RuntimeError, "committed on its own"):
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                            end(connection)
                            connection.execute("INSERT INTO marks (n) VALUES (2)")
                            connection.execute("INSERT INTO marks (n) VALUES (2)")
                    marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                finally:
                    pool.close()
                self.assertEqual([tuple(row) for row in marks], kept)

    def test_a_ctrl_c_while_a_write_exits_does_not_keep_the_lock_with_the_exception(self):
        visited = connection_module._visited_besides
        pressed = []

        def interrupted(*args):
            if not pressed:
                pressed.append(True)
                raise KeyboardInterrupt
            return visited(*args)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with mock.patch.object(connection_module, "_visited_besides", interrupted):
                    try:
                        with pool.write() as connection:
                            connection.execute("INSERT INTO marks (n) VALUES (1)")
                    except KeyboardInterrupt as error:
                        kept = error
                other = sqlite3.connect(path, timeout=0.2, isolation_level=None)
                try:
                    other.execute("BEGIN IMMEDIATE")
                    other.execute("ROLLBACK")
                finally:
                    other.close()
                count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                del kept
            finally:
                pool.close()
            self.assertEqual(pressed, [True])
            self.assertEqual(count, 0)

    def test_a_write_whose_abandon_callback_was_interrupted_is_still_rolled_back(self):
        def interrupted(ref):
            raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                scope = pool.write()
                connection = scope.__enter__()
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                settle = pool._writers[connection].settle
                replaced = connection_module._WriterRef(scope, interrupted)
                replaced.settle = settle
                replaced.claims = {}
                pool._writers[connection] = replaced
                with mock.patch.object(sys, "unraisablehook", lambda unraisable: None):
                    del scope
                    gc.collect()
                with pool.write() as again:
                    again.execute("INSERT INTO marks (n) VALUES (2)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(2,)])

    def test_a_ctrl_c_releasing_an_abandon_claim_does_not_block_later_writes(self):
        lines, start = inspect.getsourcelines(connection_module._abandoned)
        target = start + next(index for index, line in enumerate(lines) if "claims.pop" in line)
        pressed = []

        def trace(frame, event, arg):
            if frame.f_code is not connection_module._abandoned.__code__:
                return None
            if event == "line" and frame.f_lineno == target and not pressed:
                pressed.append(True)
                raise KeyboardInterrupt
            return trace

        def drop(pool, n):
            scope = pool.write()
            connection = scope.__enter__()
            connection.execute("INSERT INTO marks (n) VALUES (?)", (n,))
            del scope, connection

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with mock.patch.object(sys, "unraisablehook", lambda unraisable: None):
                    sys.settrace(trace)
                    try:
                        drop(pool, 1)
                        gc.collect()
                    finally:
                        sys.settrace(None)
                    drop(pool, 2)
                    gc.collect()
                with pool.write() as again:
                    again.execute("INSERT INTO marks (n) VALUES (3)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(pressed, [True])
            self.assertEqual([tuple(row) for row in marks], [(3,)])

    def test_the_context_of_a_finished_write_does_not_reach_its_connection(self):
        def write(path):
            pool = ConnectionPool(path)
            try:
                with pool.write() as connection:
                    connection.execute("CREATE TABLE marks (n INTEGER)")
            finally:
                pool.close()
            return connection, connection_module._writing.get()

        def reaches(start, target):
            seen, layer = set(), [start]
            for _ in range(6):
                following = []
                for item in layer:
                    if item is target:
                        return True
                    if id(item) not in seen:
                        seen.add(id(item))
                        following.extend(gc.get_referents(item))
                layer = following
            return False

        with tempfile.TemporaryDirectory() as directory:
            connection, refs = contextvars.copy_context().run(write, str(Path(directory) / "jobs.db"))
            self.assertTrue(refs)
            self.assertFalse(reaches(refs, connection))

    def test_a_stray_connection_left_in_a_transaction_is_closed_when_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            try:
                stray = pool.connection
                stray.execute("CREATE TABLE marks (n INTEGER)")
                stray.execute("BEGIN IMMEDIATE")
                stray.execute("INSERT INTO marks (n) VALUES (1)")
                pool._all.discard(stray)
                replacement = pool.connection
                with pool.write() as connection:
                    connection.execute("INSERT INTO marks (n) VALUES (2)")
                marks = replacement.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertIsNot(replacement, stray)
            self.assertEqual([tuple(row) for row in marks], [(2,)])

    def test_two_ctrl_cs_inside_an_abandon_callback_do_not_lock_the_database_for_good(self):
        pressed = []
        getpid = os.getpid

        def interrupted_getpid():
            if sys._getframe(1).f_code is connection_module._abandoned.__code__ and "pid" not in pressed:
                pressed.append("pid")
                raise KeyboardInterrupt
            return getpid()

        class Claims(dict):
            def pop(self, *args):
                if "pop" not in pressed:
                    pressed.append("pop")
                    raise KeyboardInterrupt
                return super().pop(*args)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                scope = pool.write()
                connection = scope.__enter__()
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                pool._writers[connection].claims = Claims(pool._writers[connection].claims)
                del connection
                with mock.patch.object(sys, "unraisablehook", lambda unraisable: None):
                    with mock.patch.object(os, "getpid", interrupted_getpid):
                        del scope
                        gc.collect()
                    gc.collect()
                with pool.write() as again:
                    again.execute("INSERT INTO marks (n) VALUES (2)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual(pressed, ["pid", "pop"])
            self.assertEqual([tuple(row) for row in marks], [(2,)])

    def test_two_threads_settling_the_same_dropped_write_still_roll_it_back(self):
        inserting, inserted = threading.Barrier(2, timeout=5), threading.Barrier(2, timeout=5)
        popping = threading.Barrier(2, timeout=1)

        class Interleaved(dict):
            def __setitem__(self, key, value):
                inserting.wait()
                super().__setitem__(key, value)
                inserted.wait()

            def pop(self, *args):
                with suppress(threading.BrokenBarrierError):
                    popping.wait()
                return super().pop(*args)

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                scope = pool.write()
                connection = scope.__enter__()
                connection.execute("INSERT INTO marks (n) VALUES (1)")
                ref = pool._writers[connection]
                ref.claims = Interleaved()
                held = [connection]

                def visitor():
                    while ref() is not None:
                        time.sleep(0.0005)
                    pool._writer(held[0])

                other = threading.Thread(target=visitor)
                other.start()
                del scope
                other.join(5)
                outsider = sqlite3.connect(path, timeout=0.2, isolation_level=None)
                try:
                    outsider.execute("BEGIN IMMEDIATE")
                    outsider.execute("ROLLBACK")
                finally:
                    outsider.close()
                registered = connection in pool._writers
                count = pool.connection.execute("SELECT count(*) FROM marks").fetchone()[0]
                del connection
            finally:
                pool.close()
            self.assertFalse(registered)
            self.assertEqual(count, 0)

    def test_a_late_settle_leaves_a_new_write_on_the_same_connection_alone(self):
        main = threading.current_thread()
        dead, passed, checked = threading.Event(), threading.Event(), threading.Event()
        looked, opened = threading.Event(), threading.Event()
        visits = []

        class Choreographed(dict):
            def __setitem__(self, key, value):
                if threading.current_thread() is main:
                    dead.set()
                    passed.wait(5)
                else:
                    passed.set()
                    checked.wait(5)
                super().__setitem__(key, value)

            def __iter__(self):
                if threading.current_thread() is main:
                    snapshot = list(dict.keys(self))
                    checked.set()
                    return iter(snapshot)
                visits.append(True)
                if len(visits) == 2:
                    looked.set()
                    opened.wait(5)
                return iter(list(dict.keys(self)))

        rolled = connection_module._roll_back_abandoned
        waited = []

        def roll_back(connection, handed):
            if threading.current_thread() is main and not waited:
                waited.append(True)
                looked.wait(5)
            return rolled(connection, handed)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.2)
            pool.connection.execute("CREATE TABLE marks (n TEXT)")
            try:
                with mock.patch.object(connection_module, "_roll_back_abandoned", roll_back):
                    scope = pool.write()
                    connection = scope.__enter__()
                    connection.execute("INSERT INTO marks (n) VALUES ('dropped')")
                    pool._writers[connection].claims = Choreographed()
                    held = [connection]

                    def visitor():
                        dead.wait(5)
                        pool._writer(held[0])

                    other = threading.Thread(target=visitor)
                    other.start()
                    del scope
                    with pool.write() as again:
                        again.execute("INSERT INTO marks (n) VALUES ('a')")
                        opened.set()
                        other.join(5)
                        again.execute("INSERT INTO marks (n) VALUES ('b')")
                marks = pool.connection.execute("SELECT n FROM marks ORDER BY n").fetchall()
                del connection
            finally:
                opened.set()
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [("a",), ("b",)])

    def test_a_failed_rollback_spares_a_cursor_with_nothing_running(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            pool.connection.executemany("INSERT INTO marks (n) VALUES (?)", [(n,) for n in range(10)])
            try:
                idle = pool.connection.cursor()
                with self.assertRaises(TimeoutError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (-1)")
                        reading = connection.execute("SELECT n FROM marks")
                        reading.fetchone()
                        connection.interrupt()
                        raise TimeoutError
                count = idle.execute("SELECT count(*) FROM marks").fetchone()[0]
                del reading
            finally:
                pool.close()
            self.assertEqual(count, 10)

    def test_a_write_joining_a_transaction_sqlite_ended_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER UNIQUE)")
            pool.connection.execute("INSERT INTO marks (n) VALUES (0)")
            try:
                with self.assertRaisesRegex(RuntimeError, "already ended by SQLite"):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        with suppress(sqlite3.IntegrityError):
                            connection.execute("INSERT OR ROLLBACK INTO marks (n) VALUES (0)")
                        with pool.write() as inner:
                            inner.execute("INSERT INTO marks (n) VALUES (2)")
                marks = pool.connection.execute("SELECT n FROM marks").fetchall()
            finally:
                pool.close()
            self.assertEqual([tuple(row) for row in marks], [(0,)])

    def test_closing_idle_connections_spares_open_and_orphaned_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            try:
                scope = pool.write()
                held = scope.__enter__()
                orphan = pool._lend()
                pool._owners.pop(orphan, None)
                pool._orphans.add(orphan)
                pool.close_idle()
                self.assertIn(held, pool._all)
                self.assertIn(orphan, pool._all)
                scope.__exit__(None, None, None)
            finally:
                pool.close()

    def test_a_retiring_thread_hands_its_connection_to_the_next_one(self):
        from tinyjobs.backends.sqlite import connection

        closed: list = []
        close_one = connection._close_one
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))

            def tracked(*args) -> None:
                if args[1] is pool._all:
                    closed.append(args)
                else:
                    close_one(*args)

            with mock.patch.object(connection, "_close_one", tracked):
                try:
                    seen: list = []

                    def retire() -> None:
                        seen.append(pool.connection)
                        pool.hand_over()

                    retiring = threading.Thread(target=retire)
                    retiring.start()
                    retiring.join(5)
                    gc.collect()
                    time.sleep(0.1)
                    seen.append(pool.connection)
                    self.assertIs(seen[1], seen[0])
                    self.assertEqual(seen[1].execute("SELECT 1").fetchone()[0], 1)
                    self.assertEqual(closed, [])
                finally:
                    pool.close()

    def test_the_pools_left_open_are_closed_when_the_process_exits(self):
        from tinyjobs.backends.sqlite import connection

        self.assertIn(connection._close_pools, runtime._exit_steps)
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            opened = pool.connection
            with mock.patch.object(connection, "_pools", {weakref.ref(pool): None}):
                connection._close_pools()
            with self.assertRaises(sqlite3.ProgrammingError):
                opened.execute("SELECT 1")

    def test_opening_waits_for_another_process_holding_a_fresh_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            holder = sqlite3.connect(
                path, isolation_level=None, check_same_thread=False
            )
            holder.execute("CREATE TABLE claimed (id INTEGER)")
            holder.execute("BEGIN IMMEDIATE")
            release = threading.Timer(0.3, holder.execute, ("COMMIT",))
            release.start()
            pool = ConnectionPool(path, busy_timeout=5.0)
            try:
                mode = pool.connection.execute("PRAGMA journal_mode").fetchone()[0]
            finally:
                release.join()
                pool.close()
                holder.close()
            self.assertEqual(mode, "wal")

    def test_a_connection_that_cannot_be_configured_is_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"), busy_timeout=0.1)
            opened: list[sqlite3.Connection] = []
            connect = sqlite3.connect

            def tracked(*args, **kwargs):
                opened.append(connect(*args, **kwargs))
                return opened[-1]

            refused = sqlite3.OperationalError("disk I/O error")
            with (
                mock.patch("sqlite3.connect", tracked),
                mock.patch.object(ConnectionPool, "_enable_wal", side_effect=refused),
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    pool.connection
            pool.close()
            with self.assertRaises(sqlite3.ProgrammingError):
                opened[0].execute("SELECT 1")

    def test_a_dropped_pool_closes_its_connections(self):
        opened: list[sqlite3.Connection] = []
        connect = sqlite3.connect

        def tracked(*args, **kwargs):
            opened.append(connect(*args, **kwargs))
            return opened[-1]

        from tinyjobs.backends.sqlite import connection as module

        closed = threading.Event()
        close_all = module._close_all

        def close_and_note(*args) -> None:
            close_all(*args)
            closed.set()

        with (
            mock.patch("sqlite3.connect", tracked),
            mock.patch.object(module, "_close_all", close_and_note),
        ):
            pool = ConnectionPool("file:dropped?mode=memory&cache=shared", uri=True)
            pool.connection
        del pool
        gc.collect()
        self.assertTrue(closed.wait(5))
        self.assertEqual(len(opened), 2)
        for connection in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")

    @unittest.skipUnless(hasattr(signal, "setitimer"), "needs signal.setitimer")
    def test_interrupts_during_transactions_leave_nothing_open(self):
        from tinyjobs.backends.sqlite.connection import FORK_GATE

        def write() -> None:
            with pool.write() as connection:
                connection.execute("INSERT INTO marks (n) VALUES (1)")

        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=1.0)
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            quiet = mock.patch.object(logging.getLogger("tinyjobs.sqlite"), "warning")
            try:
                with quiet:
                    self.assertGreater(interrupt_repeatedly(write), 0)
                pool.heal_here()
                self.assertFalse(pool.connection.in_transaction)
                gc.collect()
                with FORK_GATE.using():
                    pass
                self.assertEqual(FORK_GATE._holders, {})
                other = sqlite3.connect(path, timeout=1.0, isolation_level=None)
                other.execute("BEGIN IMMEDIATE")
                other.execute("ROLLBACK")
                other.close()
                with self.assertRaises(RuntimeError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (2)")
                        raise RuntimeError("roll back")
                self.assertEqual(
                    pool.connection.execute(
                        "SELECT COUNT(*) FROM marks WHERE n = 2"
                    ).fetchone()[0],
                    0,
                )
            finally:
                pool.close()

    def test_a_write_after_a_write_is_atomic_again(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            pool.connection.execute("CREATE TABLE marks (n INTEGER)")
            try:
                with pool.write():
                    pass
                with self.assertRaises(RuntimeError):
                    with pool.write() as connection:
                        connection.execute("INSERT INTO marks (n) VALUES (1)")
                        raise RuntimeError("roll back")
                count = pool.connection.execute("SELECT COUNT(*) FROM marks").fetchone()
                self.assertEqual(count[0], 0)
            finally:
                pool.close()

    def test_a_commit_that_fails_rolls_the_write_back(self):
        class FailingCommit:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self._connection = connection

            def execute(self, statement: str, *args):
                if statement == "COMMIT":
                    raise sqlite3.OperationalError("disk I/O error")
                return self._connection.execute(statement, *args)

            def __getattr__(self, name: str):
                return getattr(self._connection, name)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            real = pool.connection
            try:
                with mock.patch.object(
                    ConnectionPool,
                    "connection",
                    new_callable=mock.PropertyMock,
                    return_value=FailingCommit(real),
                ):
                    with self.assertRaises(sqlite3.OperationalError):
                        with pool.write():
                            pass
                self.assertFalse(real.in_transaction)
            finally:
                pool.close()

    def test_a_write_that_cannot_begin_raises_why(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            pool = ConnectionPool(path, busy_timeout=0.05)
            pool.connection
            holder = sqlite3.connect(path, isolation_level=None)
            holder.execute("BEGIN IMMEDIATE")
            try:
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    with pool.write():
                        pass
            finally:
                holder.close()
                pool.close()

    def test_an_interrupt_right_after_begin_rolls_the_write_back(self):
        class InterruptedAfterBegin:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self._connection = connection

            def execute(self, statement: str, *args):
                result = self._connection.execute(statement, *args)
                if statement == "BEGIN IMMEDIATE":
                    raise KeyboardInterrupt
                return result

            def __getattr__(self, name: str):
                return getattr(self._connection, name)

        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            real = pool.connection
            try:
                with mock.patch.object(
                    ConnectionPool,
                    "connection",
                    new_callable=mock.PropertyMock,
                    return_value=InterruptedAfterBegin(real),
                ):
                    with self.assertRaises(KeyboardInterrupt):
                        with pool.write():
                            pass
                self.assertFalse(real.in_transaction)
                with pool.write() as connection:
                    connection.execute("SELECT 1")
            finally:
                pool.close()

    def test_a_thread_that_ends_closes_its_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            pool = ConnectionPool(str(Path(directory) / "jobs.db"))
            opened: list[sqlite3.Connection] = []

            def write():
                with pool.write() as connection:
                    opened.append(connection)

            for _ in range(20):
                thread = threading.Thread(target=write)
                thread.start()
                thread.join()
            gc.collect()
            deadline = time.monotonic() + 5
            while (pool._all or runtime._closing) and time.monotonic() < deadline:
                time.sleep(0.005)
            try:
                for connection in opened:
                    with self.assertRaises(sqlite3.ProgrammingError):
                        connection.execute("SELECT 1")
            finally:
                pool.close()


class MemoryContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    independent_clients = False

    async def make_backend(self, clock):
        return load_backend("memory://", clock=clock)


if __name__ == "__main__":
    unittest.main()
