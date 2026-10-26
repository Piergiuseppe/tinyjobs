from __future__ import annotations

import asyncio
import inspect
import signal
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from tinyjobs import ConfigurationError, JobStatus, TinyJobs, WaitTimeout, runtime
from tinyjobs.backends import register_builtin
from tinyjobs.exceptions import (
    AppClosedError,
    BackendNotReadyError,
    EventLoopStoppedError,
)
from tinyjobs.runtime import LoopRunner, run_on, submit
from tests.test_async_backend import RamBackend

SETTLE_TIMEOUT = 10.0


class LoopBoundBackend(RamBackend):
    capabilities = replace(RamBackend.capabilities, loop_bound=True)

    async def setup(self) -> None:
        self._home = asyncio.get_running_loop()

    async def enqueue(self, job):
        if asyncio.get_running_loop() is not self._home:
            raise RuntimeError("got Future attached to a different loop")
        return await super().enqueue(job)


class LoopStoppingBackend(LoopBoundBackend):
    async def enqueue(self, job):
        asyncio.get_running_loop().stop()
        await asyncio.Event().wait()


register_builtin("loopbound", LoopBoundBackend.from_url)
register_builtin("stopping", LoopStoppingBackend.from_url)


class ServeTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    async def wait_for_status(self, job, *statuses: JobStatus) -> JobStatus:
        deadline = time.monotonic() + SETTLE_TIMEOUT
        while time.monotonic() < deadline:
            status = (await self.app.aget(job.id)).status
            if status in statuses:
                return status
            await asyncio.sleep(0.02)
        return status


class ServeTests(ServeTestCase):
    async def test_serve_connects_for_the_block_and_closes_after(self):
        async with self.app.serve() as app:
            self.assertIs(app, self.app)
            self.assertEqual(await app.acounts(), {})

        with self.assertRaises(BackendNotReadyError):
            self.app.backend

    async def test_serve_without_a_worker_leaves_jobs_queued(self):
        @self.app.task
        async def noop():
            return None

        async with self.app.serve():
            job = await noop.aenqueue()
            await asyncio.sleep(0.3)
            self.assertIs((await self.app.aget(job.id)).status, JobStatus.QUEUED)

    async def test_the_embedded_worker_runs_jobs_enqueued_inside_the_block(self):
        @self.app.task(store_result=True)
        async def double(value):
            return value * 2

        async with self.app.serve(worker=True, concurrency=2, poll_maximum=0.05):
            job = await double.aenqueue(21)
            status = await self.wait_for_status(job, JobStatus.SUCCESS)
            self.assertIs(status, JobStatus.SUCCESS)
            self.assertEqual((await job.arefresh()).result(), 42)

    async def test_leaving_the_block_lets_a_running_job_finish(self):
        @self.app.task
        async def slow():
            await asyncio.sleep(0.4)

        async with self.app.serve(worker=True, poll_maximum=0.05):
            job = await slow.aenqueue()
            await self.wait_for_status(job, JobStatus.RUNNING)

        async with self.app.serve():
            self.assertIs((await self.app.aget(job.id)).status, JobStatus.SUCCESS)

    async def test_the_embedded_worker_leaves_the_host_signal_handlers_alone(self):
        def host_handler(number, frame):
            return None

        previous = signal.signal(signal.SIGTERM, host_handler)
        try:
            async with self.app.serve(worker=True, poll_maximum=0.05):
                await asyncio.sleep(0.2)
                self.assertIs(signal.getsignal(signal.SIGTERM), host_handler)
            self.assertIs(signal.getsignal(signal.SIGTERM), host_handler)
        finally:
            signal.signal(signal.SIGTERM, previous)

    async def test_a_sync_task_in_the_embedded_worker_can_enqueue_another(self):
        @self.app.task
        async def child():
            return None

        @self.app.task
        def parent():
            return child.enqueue().id

        async with self.app.serve(worker=True, poll_maximum=0.05):
            job = await parent.aenqueue()
            self.assertIs(
                await self.wait_for_status(job, JobStatus.SUCCESS, JobStatus.FAILED),
                JobStatus.SUCCESS,
            )
            spawned = [
                item for item in (await self.app.ajobs()).items if item.task.endswith("child")
            ]
            self.assertEqual(len(spawned), 1)

    async def test_a_wait_timeout_from_a_thread_reaches_the_caller(self):
        @self.app.task
        async def noop():
            return None

        async with self.app.serve():
            job = await noop.aenqueue()
            with self.assertRaises(WaitTimeout):
                await asyncio.wait_for(
                    asyncio.to_thread(job.wait, timeout=0.2), timeout=5
                )

    async def test_an_inner_block_ending_leaves_the_outer_one_connected(self):
        @self.app.task
        async def noop():
            return None

        async with self.app.serve():
            async with self.app.serve():
                pass
            job = await noop.aenqueue()
            self.assertIs((await self.app.aget(job.id)).status, JobStatus.QUEUED)

    async def test_a_call_after_the_block_ends_does_not_reconnect(self):
        @self.app.task
        async def noop():
            return None

        async with self.app.serve():
            pass
        with self.assertRaises(AppClosedError):
            await asyncio.to_thread(noop.enqueue)

    async def test_worker_options_without_a_worker_are_rejected(self):
        with self.assertRaises(ConfigurationError) as caught:
            async with self.app.serve(concurrency=4):
                pass
        self.assertIn("concurrency", str(caught.exception))


    async def test_an_embedded_worker_takes_handle_signals_false_and_refuses_true(self):
        async with self.app.serve(worker=True, handle_signals=False):
            pass
        with self.assertRaises(ConfigurationError) as caught:
            async with self.app.serve(worker=True, handle_signals=True):
                pass
        self.assertIn("signal handlers", str(caught.exception))

class LifespanTests(ServeTestCase):
    async def test_lifespan_takes_the_asgi_app_and_serves(self):
        @self.app.task
        async def noop():
            return None

        lifespan = self.app.lifespan(worker=True, poll_maximum=0.05)
        async with lifespan(object()):
            job = await noop.aenqueue()
            self.assertIs(
                await self.wait_for_status(job, JobStatus.SUCCESS), JobStatus.SUCCESS
            )

    def test_lifespan_rejects_worker_options_at_declaration(self):
        with self.assertRaises(ConfigurationError):
            self.app.lifespan(concurrency=4)


class SyncFacadeFromThreadsTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_calls_from_a_thread_run_on_the_loop_that_owns_the_backend(self):
        app = TinyJobs("loopbound://")

        @app.task
        async def noop():
            return None

        async with app.serve():
            job = await asyncio.to_thread(noop.enqueue)
            self.assertIs((await app.aget(job.id)).status, JobStatus.QUEUED)


class InterruptedCallTests(unittest.TestCase):
    def test_a_call_routed_to_the_home_loop_from_a_lock_holder_is_refused(self):
        from tinyjobs.exceptions import InterruptedCallError

        app = TinyJobs("loopbound://")
        self.addCleanup(app.close)
        app.counts()

        async def call() -> None:
            with app._lock_guard:
                await app.acounts()

        with self.assertRaises(InterruptedCallError):
            asyncio.run(call())
        self.assertEqual(app.counts(), {})


class StoppedLoopTests(unittest.TestCase):
    def test_a_sync_call_raises_when_the_backend_loop_stops_under_it(self):
        app = TinyJobs("stopping://").start()

        @app.task
        async def noop():
            return None

        try:
            with self.assertRaises(EventLoopStoppedError):
                noop.enqueue()
        finally:
            app.close()


class Closable:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


async def noop() -> None:
    return None


class LoopRunnerTests(unittest.TestCase):
    def test_a_call_handed_a_loop_closed_under_it_says_the_app_closed(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        runner.close()
        made: list = []
        with mock.patch.object(runner, "_ensure_loop", return_value=loop):
            with self.assertRaises(AppClosedError):
                runner.run(lambda: made.append(noop()))
        self.assertEqual(made, [])

    def test_an_interrupted_sync_call_cancels_its_work(self):
        ran: list[int] = []

        async def later():
            await asyncio.sleep(0.3)
            ran.append(1)

        runner = LoopRunner()
        try:
            with mock.patch.object(runtime, "_wait", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    runner.run(lambda: later())
            time.sleep(0.5)
        finally:
            runner.close()
        self.assertEqual(ran, [])

    def test_a_call_cancelled_before_it_starts_never_runs(self):
        ran: list[int] = []

        async def record():
            ran.append(1)

        loop = asyncio.new_event_loop()
        serving = threading.Thread(target=loop.run_forever)
        serving.start()
        try:
            loop.call_soon_threadsafe(time.sleep, 0.3)
            pending = record()
            submit(loop, pending, "record").cancel()
            time.sleep(0.5)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            serving.join()
            loop.close()
        self.assertEqual(ran, [])
        self.assertEqual(inspect.getcoroutinestate(pending), inspect.CORO_CLOSED)

    def test_every_resident_of_a_loop_is_closed_when_it_stops(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        first, second = Closable(), Closable()
        runner.keep(loop, first)
        runner.keep(loop, second)
        runner.close()
        self.assertEqual((first.closed, second.closed), (True, True))

    def test_a_call_that_never_started_is_discarded_when_its_loop_stops(self):
        loop = asyncio.new_event_loop()
        serving = threading.Thread(target=loop.run_forever)
        serving.start()
        busy = threading.Event()

        def stall() -> None:
            busy.set()
            time.sleep(0.3)
            loop.stop()

        loop.call_soon_threadsafe(stall)
        busy.wait(5)
        pending = noop()
        try:
            with self.assertRaises(EventLoopStoppedError):
                asyncio.run(run_on(loop, pending, "noop"))
        finally:
            serving.join()
            loop.close()
        self.assertEqual(inspect.getcoroutinestate(pending), inspect.CORO_CLOSED)


if __name__ == "__main__":
    unittest.main()
