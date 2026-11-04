from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.status import ExecutionStatus, JobStatus
from tests import isolated
from tests.test_worker import SETTLE_TIMEOUT, WorkerTestCase


def request_for(task, encoded: bytes) -> tuple:
    return (
        task.registered_in,
        task.name,
        task.function.__module__,
        task.function.__qualname__,
        encoded,
        "json",
        task.store_result,
        None,
    )


class IsolatedTestCase(WorkerTestCase):
    def setUp(self) -> None:
        super().setUp()
        for task in isolated.TASKS:
            self.app.registry.register(task)

    async def send(self, task, *args, **options):
        return await self.app.asend(task.name, list(args), **options)

    async def executions_of(self, job):
        return await self.app.aexecutions(job.id)

    async def result_of(self, job):
        return (await self.app.aget(job.id)).result()

    def children_of(self, worker):
        pool = worker._executors._process._pool
        return [*pool._busy, *pool._idle]

    async def finished(self, job, timeout: float = SETTLE_TIMEOUT) -> None:
        deadline = time.monotonic() + timeout
        while not (await self.app.aget(job.id)).status.is_terminal:
            self.assertLess(time.monotonic(), deadline, f"job {job.id} never finished")
            await asyncio.sleep(0.02)

    async def first_child_pid(self, worker, timeout: float = SETTLE_TIMEOUT) -> int:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            children = self.children_of(worker)
            if children:
                return children[0].pid
            await asyncio.sleep(0.02)
        raise AssertionError("no job process was started")


class RunsElsewhereTests(IsolatedTestCase):
    async def test_an_isolated_task_runs_in_another_process(self):
        job = await self.send(isolated.pid)
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertNotEqual(await self.result_of(job), os.getpid())

    async def test_arguments_and_results_cross_through_the_codec(self):
        value = {"a": [1, 2.5, "x"], "b": None}
        job = await self.send(isolated.echo, value)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(await self.result_of(job), value)

    async def test_a_result_the_codec_refuses_fails_only_when_it_would_be_stored(self):
        stored = await self.send(isolated.unencodable_result)
        dropped = await self.send(isolated.unencodable_result_dropped)
        await self.settle(self.make_worker(), [stored, dropped])
        self.assertIs(await self.status_of(stored), JobStatus.FAILED)
        self.assertEqual((await self.executions_of(stored))[0].error_type, "SerializationError")
        self.assertIs(await self.status_of(dropped), JobStatus.SUCCESS)

    async def test_the_process_is_reused_across_jobs(self):
        jobs = [await self.send(isolated.pid) for _ in range(3)]
        await self.settle(self.make_worker(processes=1), jobs)
        pids = {await self.result_of(job) for job in jobs}
        self.assertEqual(len(pids), 1)

    async def test_jobs_per_process_replaces_the_process(self):
        jobs = [await self.send(isolated.pid) for _ in range(3)]
        await self.settle(self.make_worker(processes=1, jobs_per_process=2), jobs)
        pids = [await self.result_of(job) for job in jobs]
        self.assertEqual(len(set(pids)), 2)
        self.assertEqual(sorted(pids.count(pid) for pid in set(pids)), [1, 2])

    async def test_the_processes_cap_runs_isolated_jobs_in_turn(self):
        jobs = [await self.send(isolated.sleep_then_pid, 0.2) for _ in range(2)]
        began = time.monotonic()
        await self.settle(self.make_worker(processes=1), jobs)
        self.assertGreaterEqual(time.monotonic() - began, 0.4)
        self.assertEqual(len({await self.result_of(job) for job in jobs}), 1)


class FailureTests(IsolatedTestCase):
    async def test_a_process_that_exits_fails_the_job_and_is_replaced(self):
        dying = await self.send(isolated.exit_with, 3)
        after = await self.send(isolated.pid)
        await self.settle(self.make_worker(processes=1), [dying, after])
        self.assertIs(await self.status_of(dying), JobStatus.FAILED)
        execution = (await self.executions_of(dying))[0]
        self.assertEqual(execution.error_type, "ProcessDied")
        self.assertIn("exit status 3", (await self.app.aget(dying.id)).last_error)
        self.assertIs(await self.status_of(after), JobStatus.SUCCESS)

    async def test_a_signal_that_kills_the_process_is_named(self):
        job = await self.send(isolated.segfault)
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIn("SIGSEGV", (await self.app.aget(job.id)).last_error)

    async def test_a_crash_is_retried_like_any_failure(self):
        job = await self.send(isolated.exit_with, 1, max_attempts=2)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(len(await self.executions_of(job)), 2)

    async def test_a_timeout_kills_the_process(self):
        worker = self.make_worker(processes=1)
        before = await self.send(isolated.pid)
        await self.settle(worker, [before])
        first = await self.result_of(before)
        hanging = await self.send(isolated.hang)
        after = await self.send(isolated.pid)
        began = time.monotonic()
        await self.settle(worker, [hanging, after])
        self.assertLess(time.monotonic() - began, 10)
        self.assertIs(await self.status_of(hanging), JobStatus.FAILED)
        self.assertIs((await self.executions_of(hanging))[0].status, ExecutionStatus.TIMEOUT)
        self.assertIn("killed", (await self.app.aget(hanging.id)).last_error)
        self.assertNotEqual(await self.result_of(after), first)
        with self.assertRaises(ProcessLookupError):
            os.kill(first, 0)

    async def test_retry_and_abandon_raised_in_the_process_are_honoured(self):
        retried = await self.send(isolated.retry_now, max_attempts=2)
        abandoned = await self.send(isolated.abandon, max_attempts=3)
        await self.settle(self.make_worker(), [retried, abandoned])
        self.assertEqual(len(await self.executions_of(retried)), 2)
        self.assertIs(await self.status_of(abandoned), JobStatus.FAILED)
        self.assertEqual(len(await self.executions_of(abandoned)), 1)
        self.assertIn("not worth it", (await self.app.aget(abandoned.id)).last_error)

    async def test_the_exception_type_survives_the_process_boundary(self):
        job = await self.send(isolated.key_error, max_attempts=2)
        await self.settle(self.make_worker(), [job])
        executions = await self.executions_of(job)
        self.assertEqual(len(executions), 2)
        self.assertEqual(executions[0].error_type, "KeyError")
        self.assertIn("raise KeyError", executions[0].traceback)
        self.assertIn("isolated.py", executions[0].traceback)

    async def test_an_exception_the_worker_cannot_rebuild_keeps_its_name(self):
        job = await self.send(isolated.picky)
        await self.settle(self.make_worker(), [job])
        self.assertEqual((await self.executions_of(job))[0].error_type, "Picky")
        self.assertIn("7: bad", (await self.app.aget(job.id)).last_error)

    async def test_exceptions_with_their_own_constructor_still_match_retry_on(self):
        picky = await self.send(isolated.picky_retried, max_attempts=2)
        unicode = await self.send(isolated.unicode_error, max_attempts=2)
        await self.settle(self.make_worker(), [picky, unicode])
        self.assertEqual(len(await self.executions_of(picky)), 2)
        executions = await self.executions_of(unicode)
        self.assertEqual(len(executions), 2)
        self.assertEqual(executions[0].error_type, "UnicodeDecodeError")

    async def test_a_process_that_died_while_idle_is_not_blamed_on_the_next_job(self):
        worker = self.make_worker(processes=1)
        first = await self.send(isolated.pid)
        runner = asyncio.create_task(worker.start())
        try:
            await self.finished(first)
            pid = await self.result_of(first)
            os.kill(pid, signal.SIGKILL)
            await asyncio.sleep(0.2)
            second = await self.send(isolated.pid)
            await self.finished(second)
        finally:
            worker.stop()
            await asyncio.wait_for(runner, 10)
        self.assertIs(await self.status_of(second), JobStatus.SUCCESS)
        self.assertEqual(len(await self.executions_of(second)), 1)
        self.assertNotEqual(await self.result_of(second), pid)

    async def test_the_timeout_starts_when_the_task_starts_not_when_the_process_does(self):
        from tests import isolated_slow

        self.app.registry.register(isolated_slow.quick)
        job = await self.send(isolated_slow.quick)
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual(await self.result_of(job), "quick")

    async def test_a_payload_larger_than_the_pipe_buffer_gets_through(self):
        value = "x" * (2 * 1024 * 1024)
        job = await self.send(isolated.echo, value)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(await self.result_of(job), value)

    async def test_a_result_larger_than_the_pipe_buffer_gets_through(self):
        job = await self.send(isolated.echo, ["y" * 100_000] * 30)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(len(await self.result_of(job)), 30)

    async def test_a_death_before_the_task_started_cancels_the_outcome(self):
        from tinyjobs.executors.process import _Processes, ProcessDied

        pool = _Processes(1, None)
        child = await pool.acquire()
        request = request_for(isolated.pid, b'{"args": [], "kwargs": {}}')
        booted, started, reply = child.ask(asyncio.get_running_loop(), "pid", request)
        await asyncio.wait_for(booted, 30)
        os.kill(child.pid, signal.SIGKILL)
        with self.assertRaises(ProcessDied):
            await asyncio.wait_for(started, 10)
        self.assertTrue(reply.cancelled())
        pool.release(child, gone=True)
        pool.shutdown()

    def test_an_isolated_task_error_survives_pickling(self):
        import pickle

        from tinyjobs.exceptions import IsolatedTaskError

        error = pickle.loads(pickle.dumps(IsolatedTaskError("gone", "Custom")))
        self.assertEqual((str(error), error.type_name), ("gone", "Custom"))
        self.assertIsNone(IsolatedTaskError("plain").type_name)

    async def test_attributes_of_ordinary_exceptions_do_not_change_what_is_recorded(self):
        await self.app.ready()
        error = ValueError("bad")
        error.type_name = "Other"
        error.remote_traceback = "elsewhere"
        described = self.app.lifecycle._describe(error)
        self.assertEqual(described.type, "ValueError")
        self.assertIn("ValueError: bad", described.traceback)


class ProcessLifeTests(IsolatedTestCase):
    async def test_a_retired_process_is_reaped_while_the_worker_runs(self):
        from tinyjobs.executors import process

        worker = self.make_worker(processes=1, jobs_per_process=1)
        runner = asyncio.create_task(worker.start())
        try:
            jobs = [await self.send(isolated.pid) for _ in range(4)]
            for job in jobs:
                await self.finished(job)
            pool = worker._executors._process._pool
            deadline = time.monotonic() + process.JOIN_SECONDS + SETTLE_TIMEOUT
            while pool._closing and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            self.assertEqual(pool._closing, [])
            self.assertEqual(self.children_of(worker), [])
            pids = {await self.result_of(job) for job in jobs}
            self.assertEqual(len(pids), 4)
            self.assertFalse({child.pid for child in process._children} & pids)
        finally:
            worker.stop()
            await asyncio.wait_for(runner, 10)

    async def test_a_subprocess_started_by_an_isolated_task_can_be_terminated(self):
        job = await self.send(isolated.terminate_subprocess)
        await self.settle(self.make_worker(), [job])
        self.assertEqual(await self.result_of(job), -signal.SIGTERM)

    async def test_process_setup_is_not_bound_by_the_startup_limit(self):
        from unittest import mock

        from tinyjobs.executors import process

        job = await self.send(isolated.was_prepared)
        with mock.patch.object(process, "STARTUP_SECONDS", 1.0):
            await self.settle(
                self.make_worker(processes=1, process_setup=isolated.prepare_slowly), [job]
            )
        self.assertIs(await self.result_of(job), True)

    async def test_a_setup_that_hangs_is_killed_after_its_own_limit(self):
        from unittest import mock

        from tinyjobs.executors import process

        job = await self.send(isolated.pid)
        with mock.patch.object(process, "SETUP_SECONDS", 0.3):
            await self.settle(
                self.make_worker(processes=1, process_setup=isolated.prepare_forever), [job]
            )
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIn("process_setup", (await self.app.aget(job.id)).last_error)

    def test_process_setup_is_validated_on_the_app(self):
        from functools import partial

        for setup in (partial(print), lambda: None, 3, isolated.holder.method):
            with self.assertRaisesRegex(ConfigurationError, "module-level"):
                TinyJobs("memory://", process_setup=setup)
        with self.assertRaisesRegex(ConfigurationError, "module-level"):
            self.make_worker(process_setup=lambda: None)
        for setup in (isolated.Holder.classy, isolated.Holder.static):
            TinyJobs("memory://", process_setup=setup).close()

    async def test_a_classmethod_can_be_the_process_setup(self):
        job = await self.send(isolated.was_prepared)
        await self.settle(self.make_worker(processes=1, process_setup=isolated.Holder.classy), [job])
        self.assertIs(await self.result_of(job), True)

    async def test_a_task_registered_in_functional_style_is_found(self):
        jobs = [
            await self.send(isolated.registered_pid),
            await self.send(isolated.imported_pid),
            await self.send(isolated.via_helper),
        ]
        await self.settle(self.make_worker(), jobs)
        for job in jobs:
            self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
            result = await self.result_of(job)
            self.assertIsInstance(result, int)
            self.assertNotEqual(result, os.getpid())

    async def test_two_apps_over_one_function_with_one_name_are_refused(self):
        from tinyjobs.executors.process import _resolve
        from tinyjobs.exceptions import IsolatedTaskError

        self.assertIs(
            _resolve("tests.isolated", "registered_pid", "tests.isolated", "plain_pid"),
            isolated.registered_pid,
        )
        twin = TinyJobs("memory://").task(isolated=True, name="registered_pid")(isolated.plain_pid)
        isolated.twin = twin
        try:
            with self.assertRaisesRegex(IsolatedTaskError, "more than one"):
                _resolve("tests.isolated", "registered_pid", "tests.isolated", "plain_pid")
        finally:
            del isolated.twin

    async def test_a_process_reaped_by_someone_else_is_let_go(self):
        from tinyjobs.executors import process
        from tinyjobs.executors.process import _Processes, ProcessDied

        pool = _Processes(1, None)
        child = await pool.acquire()
        request = request_for(isolated.exit_with, b'{"args": [4], "kwargs": {}}')
        booted, started, reply = child.ask(asyncio.get_running_loop(), "exit_with", request)
        await asyncio.wait_for(started, 30)
        self.assertFalse(reply.done())
        pid = child.pid
        os.waitpid(pid, 0)
        with self.assertRaisesRegex(ProcessDied, "collected by another part"):
            await asyncio.wait_for(reply, 10)
        pool.release(child, gone=True)
        self.assertFalse(child.alive)
        self.assertTrue(child.reap())
        self.assertNotIn(child, process._children)
        pool.shutdown()
        self.assertEqual(pool._closing, [])

    async def test_a_timeout_kills_what_the_task_started_too(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "grandchild"
            job = await self.send(isolated.hang_with_subprocess, str(path))
            await self.settle(self.make_worker(processes=1), [job])
            self.assertIs((await self.executions_of(job))[0].status, ExecutionStatus.TIMEOUT)
            grandchild = int(path.read_text())
        self.assertTrue(gone(grandchild, 5.0), f"grandchild {grandchild} survived the timeout")

    async def test_a_process_that_dies_before_it_comes_up_is_replaced_once(self):
        from unittest import mock

        from tinyjobs.executors.process import ProcessExecutor, _Child
        from tinyjobs.models import JobPayload

        original = _Child.ask
        killed = []

        def ask_then_die(child, loop, name, request):
            if not killed:
                killed.append(child.pid)
                os.kill(child.pid, signal.SIGKILL)
            return original(child, loop, name, request)

        executor = ProcessExecutor(1)
        try:
            with mock.patch.object(_Child, "ask", ask_then_die):
                result = await executor.execute(isolated.pid, JobPayload(args=(), kwargs={}))
        finally:
            executor.shutdown()
        self.assertEqual(len(killed), 1)
        self.assertNotIn(result, (os.getpid(), killed[0]))

    async def test_a_process_that_does_not_come_up_fails_the_job(self):
        from unittest import mock

        from tinyjobs.executors import process

        job = await self.send(isolated.pid)
        with mock.patch.object(process, "STARTUP_SECONDS", 0.001):
            await self.settle(self.make_worker(processes=1), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIn("did not come up", (await self.app.aget(job.id)).last_error)

    def test_the_app_level_process_setup_serves_workers_drain_and_eager_mode(self):
        app = TinyJobs("memory://", process_setup=isolated.prepare)
        app.registry.register(isolated.was_prepared)
        self.assertIs(app.worker(concurrency=1)._process_setup, isolated.prepare)
        job = app.send(isolated.was_prepared.name, [])
        app.drain()
        self.assertIs(app.get(job.id).result(), True)
        app.close()
        eager = TinyJobs("memory://", eager=True, process_setup=isolated.prepare)
        eager.registry.register(isolated.was_prepared)
        job = eager.send(isolated.was_prepared.name, [])
        self.assertIs(eager.get(job.id).result(), True)
        eager.close()

    async def test_an_isolated_task_can_start_processes_of_its_own(self):
        job = await self.send(isolated.grandchild_pid)
        await self.settle(self.make_worker(), [job])
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertNotEqual(await self.result_of(job), os.getpid())

    async def test_process_setup_runs_once_in_each_process_before_its_first_job(self):
        jobs = [await self.send(isolated.was_prepared) for _ in range(2)]
        await self.settle(self.make_worker(processes=1, process_setup=isolated.prepare), jobs)
        self.assertEqual([await self.result_of(job) for job in jobs], [True, True])
        bare = await self.send(isolated.was_prepared)
        await self.settle(self.make_worker(processes=1), [bare])
        self.assertIs(await self.result_of(bare), False)

    async def test_a_failing_process_setup_fails_the_job_with_the_reason(self):
        job = await self.send(isolated.pid)
        await self.settle(self.make_worker(process_setup=isolated.fail_setup), [job])
        self.assertIs(await self.status_of(job), JobStatus.FAILED)
        self.assertIn("process_setup", (await self.app.aget(job.id)).last_error)
        self.assertIn("no settings", (await self.app.aget(job.id)).last_error)

    def test_process_setup_must_be_a_module_level_function(self):
        def local() -> None:
            return None

        with self.assertRaisesRegex(ConfigurationError, "module-level"):
            self.make_worker(process_setup=local)


class SignalTests(IsolatedTestCase):
    async def test_a_job_process_ignores_sigint(self):
        job = await self.send(isolated.sleep_then_pid, 0.6)
        worker = self.make_worker(processes=1)
        runner = asyncio.create_task(worker.start())
        try:
            pid = await self.first_child_pid(worker)
            os.kill(pid, signal.SIGINT)
            os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + SETTLE_TIMEOUT
            while not (await self.app.aget(job.id)).status.is_terminal:
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.02)
        finally:
            worker.stop()
            await asyncio.wait_for(runner, 10)
        self.assertIs(await self.status_of(job), JobStatus.SUCCESS)
        self.assertEqual(await self.result_of(job), pid)

    async def test_stopping_the_worker_kills_the_process_and_hands_the_job_back(self):
        job = await self.send(isolated.sleep_then_pid, 30)
        worker = self.make_worker(processes=1, shutdown_timeout=1.5)
        runner = asyncio.create_task(worker.start())
        pid = await self.first_child_pid(worker)
        began = time.monotonic()
        worker.stop()
        await asyncio.wait_for(runner, 10)
        self.assertLess(time.monotonic() - began, 5)
        self.assertIs(await self.status_of(job), JobStatus.QUEUED)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


class DeclarationTests(WorkerTestCase):
    def test_an_async_task_cannot_be_isolated(self):
        with self.assertRaisesRegex(ConfigurationError, "async def"):

            @self.app.task(isolated=True)
            async def handler():
                return None

    def test_a_main_a_job_process_cannot_import_is_recognised(self):
        import sys
        from types import SimpleNamespace
        from unittest import mock

        from tinyjobs.executors.process import main_spawn_problem

        cases = [
            (SimpleNamespace(__spec__=SimpleNamespace(name="pkg.__main__")), "python -m pkg"),
            (SimpleNamespace(__spec__=None), "python -c or an interactive session"),
            (SimpleNamespace(__spec__=None, __file__="script.py"), None),
            (SimpleNamespace(__spec__=SimpleNamespace(name="pkg.mod")), None),
        ]
        for main, expected in cases:
            with mock.patch.dict(sys.modules, {"__main__": main}):
                self.assertEqual(main_spawn_problem(), expected)

    def test_worker_options_are_validated(self):
        with self.assertRaisesRegex(ConfigurationError, "processes must be at least 1"):
            self.make_worker(processes=0)
        with self.assertRaisesRegex(ConfigurationError, "jobs_per_process must be at least 1"):
            self.make_worker(jobs_per_process=0)
        with self.assertRaisesRegex(ConfigurationError, "at least loops"):
            self.make_worker(loops=2, processes=1)

    def test_processes_are_shared_out_among_the_loops(self):
        worker = self.make_worker(loops=2, concurrency=4, processes=3, jobs_per_process=5)
        self.assertEqual(worker._processes, 2)
        self.assertEqual(
            [(options["processes"], options["jobs_per_process"]) for options in worker._lane_options],
            [(1, 5)],
        )

    def test_processes_default_to_the_smaller_of_concurrency_and_the_cpu_count(self):
        worker = self.make_worker(concurrency=1)
        self.assertEqual(worker._processes, 1)
        worker = self.make_worker(concurrency=1000)
        self.assertEqual(worker._processes, os.cpu_count() or 1)
        cpus = os.cpu_count() or 1
        if cpus >= 4:
            worker = self.make_worker(loops=2, concurrency=4)
            self.assertEqual(
                worker._processes + sum(o["processes"] for o in worker._lane_options), 4
            )


SCRIPT = """
import os
import threading

from tinyjobs import TinyJobs

app = TinyJobs("sqlite:///{database}")


@app.task(isolated=True, store_result=True)
def pid() -> int:
    return os.getpid()


if __name__ == "__main__":
    job = pid.enqueue()
    worker = app.worker(concurrency=1, handle_signals=False, scheduler=False)
    threading.Thread(target=lambda: (job.wait(timeout=30), worker.stop()), daemon=True).start()
    worker.run()
    print("child" if job.refresh().result() != os.getpid() else "same", job.status.value)
    app.close()
"""


ORPHAN_SCRIPT = """
import asyncio
import os

from tinyjobs.executors.process import _Processes


REQUEST = ("tests.isolated", "tests.isolated.sleep_then_pid", "tests.isolated", "sleep_then_pid", b'{"args": [30], "kwargs": {}}', "json", True, None)


async def main() -> None:
    pool = _Processes(1, None)
    child = await pool.acquire()
    print(child.pid, flush=True)
    booted, started, reply = child.ask(asyncio.get_running_loop(), "sleep", REQUEST)
    await started
    assert not reply.done(), reply.result()
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(main())
"""

EXIT_SCRIPT = """
import asyncio

from tinyjobs.executors.process import _Processes

REQUEST = ("tests.isolated", "tests.isolated.sleep_then_pid", "tests.isolated", "sleep_then_pid", b'{"args": [30], "kwargs": {}}', "json", True, None)


async def main() -> None:
    pool = _Processes(2, None)
    first = await pool.acquire()
    second = await pool.acquire()
    pool.release(first, gone=False)
    booted, started, reply = second.ask(asyncio.get_running_loop(), "sleep", REQUEST)
    await started
    assert not reply.done(), reply.result()
    print("leaving", first.pid, second.pid, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
"""

FORK_SCRIPT = """
import asyncio
import os
import sys

from tinyjobs.executors.process import ProcessExecutor
from tinyjobs.models import JobPayload
from tests import isolated


async def run(executor: ProcessExecutor) -> int:
    return await executor.execute(isolated.pid, JobPayload(args=(), kwargs={}))


if __name__ == "__main__":
    executor = ProcessExecutor(1)
    first = asyncio.run(run(executor))
    forked = os.fork()
    if forked == 0:
        sys.exit(0)
    _, status = os.waitpid(forked, 0)
    second = asyncio.run(run(executor))
    executor.shutdown()
    print(status, "same" if first == second else "replaced", flush=True)
"""


MAIN_SOURCE = """
from tinyjobs import TinyJobs

app = TinyJobs("memory://")


def handler() -> None:
    return None


app.task(isolated=True)(handler)
"""


REPO = Path(__file__).resolve().parent.parent
REPO_ON_PATH = {**os.environ, "PYTHONPATH": str(REPO)}


def run_script(source: str, timeout: float = 60) -> subprocess.CompletedProcess:
    with tempfile.TemporaryDirectory() as directory:
        script = Path(directory) / "jobs_script.py"
        script.write_text(source)
        return subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO,
            env=REPO_ON_PATH,
        )


def gone(pid: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


class ScriptTests(unittest.TestCase):
    def test_a_task_declared_in_a_script_runs_in_a_job_process(self):
        with tempfile.TemporaryDirectory() as directory:
            completed = run_script(SCRIPT.format(database=Path(directory) / "jobs.db"))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "child success")

    def test_a_job_process_exits_when_its_worker_dies_mid_job(self):
        completed = run_script(ORPHAN_SCRIPT, timeout=30)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        pid = int(completed.stdout.split()[0])
        self.assertTrue(gone(pid, 5.0), f"job process {pid} outlived its worker")

    def test_the_interpreter_exits_with_job_processes_still_alive(self):
        began = time.monotonic()
        completed = run_script(EXIT_SCRIPT, timeout=30)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertLess(time.monotonic() - began, 15)
        word, idle, busy = completed.stdout.split()
        self.assertEqual(word, "leaving")
        self.assertTrue(gone(int(idle), 5.0), "the idle job process outlived the interpreter")
        self.assertTrue(gone(int(busy), 5.0), "the busy job process outlived the interpreter")

    def test_a_task_registered_from_python_c_is_refused(self):
        completed = subprocess.run(
            [sys.executable, "-c", MAIN_SOURCE],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=REPO,
            env=REPO_ON_PATH,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("python -c", completed.stderr)

    def test_a_task_registered_in_a_package_main_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "pkg"
            package.mkdir()
            (package / "__init__.py").write_text("")
            (package / "__main__.py").write_text(MAIN_SOURCE)
            completed = subprocess.run(
                [sys.executable, "-m", "pkg"],
                capture_output=True,
                text=True,
                timeout=60,
                cwd=directory,
                env=REPO_ON_PATH,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("python -m pkg", completed.stderr)

    def test_a_fork_of_the_worker_leaves_the_job_processes_alone(self):
        completed = run_script(FORK_SCRIPT, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.split(), ["0", "same"])
        self.assertEqual(completed.stderr, "")
