from __future__ import annotations

import asyncio
import contextlib
import dis
import gc
import os
import random
import signal
import sys
import tempfile
import threading
import time
import types
import unittest
from contextlib import contextmanager, suppress
from functools import partial
from pathlib import Path
from unittest import mock

import tinyjobs
from tinyjobs import JobStatus, TinyJobs
from tinyjobs.exceptions import AppClosedError

PACKAGE = str(Path(tinyjobs.__file__).parent)
CALL_OPCODES = {"CALL", "CALL_FUNCTION_EX", "CALL_KW", "PRECALL"}
SWEEP_LIMIT = 20000
LATE_ALARM_SECONDS = 0.01


class Injected(BaseException):
    pass


def flush_control(seconds: float = 5) -> None:
    from tinyjobs import runtime

    flushed = threading.Event()
    runtime._inbox.put((lambda: flushed.set() and False,))
    runtime._ensure_control()
    if not flushed.wait(seconds):
        raise AssertionError("the control thread never reached the end of its inbox")


def inside_tinyjobs(frame) -> bool:
    while frame is not None:
        if frame.f_code.co_filename.startswith(PACKAGE):
            return True
        frame = frame.f_back
    return False


def interrupt_at(point: int, action) -> bool:
    seen = [0]
    previous: dict[int, str] = {}
    pid = os.getpid()

    def reached(frame) -> bool:
        if os.getpid() != pid or not inside_tinyjobs(frame):
            return False
        seen[0] += 1
        return seen[0] == point

    def tracer(frame, event, arg):
        frame.f_trace_opcodes = True
        if event == "call":
            if reached(frame):
                raise Injected
        elif event == "opcode":
            name = dis.opname[frame.f_code.co_code[frame.f_lasti]]
            before = previous.get(id(frame))
            previous[id(frame)] = name
            if (name == "JUMP_BACKWARD" or before in CALL_OPCODES) and reached(frame):
                raise Injected
        return tracer

    sys.settrace(tracer)
    try:
        action()
    except Injected:
        pass
    finally:
        sys.settrace(None)
    return seen[0] >= point


def sweep_interrupts(case: unittest.TestCase, action, check) -> int:
    point = 1
    while interrupt_at(point, action):
        check(point)
        point += 1
        if point > SWEEP_LIMIT:
            case.fail(f"the sweep passed {SWEEP_LIMIT} points")
    check(point)
    return point


def interrupt_repeatedly(action, seconds: float = 1.5, *, only_tinyjobs: bool = False) -> int:
    armed = [False]
    busy = [False]

    def interrupt(signum, frame) -> None:
        if busy[0]:
            return
        busy[0] = True
        try:
            hit = armed[0] and (not only_tinyjobs or inside_tinyjobs(frame))
        finally:
            busy[0] = False
        if hit:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGALRM, interrupt)
    interrupts = 0
    deadline = time.monotonic() + seconds
    try:
        while time.monotonic() < deadline:
            try:
                armed[0] = True
                delay = random.uniform(0.00002, 0.0015)
                signal.setitimer(signal.ITIMER_REAL, delay, delay)
                repeat_until(deadline, action)
                armed[0] = False
            except KeyboardInterrupt:
                armed[0] = False
                interrupts += 1
    finally:
        disarm_alarm(previous)
    return interrupts


def disarm_alarm(previous) -> None:
    signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, signal.SIG_IGN)
    time.sleep(LATE_ALARM_SECONDS)
    signal.signal(signal.SIGALRM, previous)


def repeat_until(deadline: float, action) -> None:
    while time.monotonic() < deadline:
        action()


class BackendTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.db"
        self.app = TinyJobs(f"sqlite:///{self.database}").start()
        self.backend = self.app.backend

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def drive(self, coroutine):
        return self.app.run_sync(lambda: coroutine, name="test")


def assert_fork_hooks_settled(case: unittest.TestCase, app: TinyJobs) -> None:
    from tinyjobs import runtime
    from tinyjobs.backends.sqlite.connection import FORK_GATE

    deadline = time.monotonic() + 5
    while runtime._pausing or app._runner._paused is not None:
        case.assertLess(time.monotonic(), deadline, "the fork never resumed")
        time.sleep(0.01)
    case.assertFalse(runtime.holds_tinyjobs())
    case.assertEqual(runtime._fork_marks, {})
    case.assertEqual(runtime._after_fork_actions, {})
    case.assertEqual(runtime._kept_at_fork, {})
    case.assertEqual(runtime._paused_for_fork, [])
    while True:
        gc.collect()
        held = [ref for ref in list(FORK_GATE._holders) if ref() is not None]
        if not held or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    case.assertEqual(held, [])


def race_open_and_close(case: unittest.TestCase, app: TinyJobs, seconds: float = 1.5) -> None:
    handler = app.task(name="handler")(lambda: None)
    unexpected: list[BaseException] = []
    deadline = time.monotonic() + seconds

    def sync_caller(step: int) -> None:
        actions = [handler.enqueue, handler.enqueue, app.start, app.close]
        while time.monotonic() < deadline:
            step += 1
            try:
                actions[step % len(actions)]()
            except AppClosedError:
                pass
            except BaseException as error:
                unexpected.append(error)

    def async_caller(step: int) -> None:
        async def main() -> None:
            nonlocal step
            actions = [handler.aenqueue, handler.aenqueue, app.ready, app.aclose]
            while time.monotonic() < deadline:
                step += 1
                try:
                    await actions[step % len(actions)]()
                except AppClosedError:
                    pass
                except BaseException as error:
                    unexpected.append(error)
                await asyncio.sleep(0)

        asyncio.run(main())

    callers = [threading.Thread(target=sync_caller, args=(n,)) for n in range(4)]
    callers += [threading.Thread(target=async_caller, args=(n,)) for n in range(3)]
    with case.assertNoLogs("asyncio", "ERROR"):
        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join(max(0.0, deadline + 30 - time.monotonic()))
        stuck = [caller.name for caller in callers if caller.is_alive()]
        app.close()
        gc.collect()
    case.assertEqual(stuck, [])
    case.assertEqual(unexpected, [])


FORK_WITHOUT_WARNING = """
import gc, os, sys, time, warnings
from tinyjobs import TinyJobs
app = TinyJobs(sys.argv[1], **dict(arg.split("=", 1) for arg in sys.argv[2:])).start()
app.send("t")
before = app.counts().get("queued", 0)
descriptors = len(os.listdir("/dev/fd"))


def fork_once():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        child = os.fork()
        if child == 0:
            app.counts()
            gc.collect()
            os._exit(0 if app.counts() else 1)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0, status
    deadline = time.monotonic() + 5
    while not (app._runner._loop and app._runner._loop.is_running()):
        assert time.monotonic() < deadline, "the loop did not resume"
        time.sleep(0.01)
    return [str(warning.message) for warning in caught]


for _ in range(3):
    warned = fork_once()
    assert warned == [], warned
    app.send("t")
assert app.counts()["queued"] == before + 3, app.counts()
deadline = time.monotonic() + 5
while len(os.listdir("/dev/fd")) > descriptors + 1 and time.monotonic() < deadline:
    gc.collect()
    time.sleep(0.01)
assert len(os.listdir("/dev/fd")) <= descriptors + 1, (descriptors, os.listdir("/dev/fd"))
app.close()
"""


def fork_without_warning(case: unittest.TestCase, url: str, *options: str) -> None:
    import subprocess
    import sys

    finished = subprocess.run(
        [sys.executable, "-c", FORK_WITHOUT_WARNING, url, *options],
        capture_output=True,
        text=True,
        timeout=60,
    )
    case.assertEqual(finished.returncode, 0, finished.stderr)


@contextmanager
def skewed(seconds: float):
    import tinyjobs.clock

    real = time.time
    fake = types.SimpleNamespace(time=lambda: real() + seconds, monotonic=time.monotonic)
    with mock.patch.object(tinyjobs.clock, "time", fake):
        yield


@contextmanager
def later_by(seconds: float, *modules):
    import tinyjobs.clock

    real = tinyjobs.clock.elapsed

    def shifted() -> float:
        return real() + seconds

    with contextlib.ExitStack() as stack:
        for module in (tinyjobs.clock, *modules):
            stack.enter_context(mock.patch.object(module, "elapsed", shifted))
        stack.enter_context(skewed(seconds))
        yield


class Untouched:
    def now(self) -> float:
        return time.time() + 900.0

    def monotonic(self) -> float:
        return time.monotonic()


class LoopBoundAppCases:
    def setUp(self) -> None:
        self.prefix = self.new_prefix()
        self.app = self.make_app()

        @self.app.task
        def handler(value):
            return value

        self.handler = handler

    def tearDown(self) -> None:
        self.app.start()
        self.app.run_sync(partial(self.forget, self.app), name="forget")
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
        dropped = self.make_app()
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

        setup = self.backend_class.setup

        async def slowly(backend):
            await asyncio.sleep(seconds)
            await setup(backend)

        patcher = mock.patch.object(self.backend_class, "setup", slowly)
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
        close = self.backend_class.close

        async def record(backend):
            closed.append(backend)
            await close(backend)

        patcher = mock.patch.object(self.backend_class, "close", record)
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
        dropped = self.make_app()
        handler = dropped.task(name="handler")(lambda value: value)
        handler.enqueue(1)
        del dropped, handler
        gc.collect()
        deadline = time.monotonic() + 5
        while not closed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(closed), 1)

    def test_sync_and_async_callers_racing_start_and_close_leave_nothing_behind(self):
        race_open_and_close(self, self.make_app())

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

    def test_a_connection_that_never_reached_its_loop_lets_the_next_caller_connect(self):
        import tinyjobs.app as app_module
        from tinyjobs.exceptions import EventLoopStoppedError

        real = app_module.run_on
        refused: list[str] = []

        async def refusing(loop, coroutine, name):
            if name == "connect" and not refused:
                refused.append(name)
                coroutine.close()
                raise EventLoopStoppedError("the loop stopped")
            return await real(loop, coroutine, name)

        async def connect_twice() -> None:
            with self.assertRaises(EventLoopStoppedError):
                await self.app.ready()
            try:
                async with asyncio.timeout(5):
                    await self.handler.aenqueue(1)
            except TimeoutError:
                self.app._connecting = None
                raise

        with mock.patch.object(app_module, "run_on", refusing):
            asyncio.run(connect_twice())
        self.assertEqual(refused, ["connect"])
        self.assertEqual(self.app.counts()["queued"], 1)

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
