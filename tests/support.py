from __future__ import annotations

import asyncio
import dis
import gc
import os
import random
import signal
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import tinyjobs
from tinyjobs import TinyJobs
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
            caller.join(30)
        app.close()
        gc.collect()
    case.assertEqual(unexpected, [])


FORK_WITHOUT_WARNING = """
import gc, os, sys, time, warnings
from tinyjobs import TinyJobs
app = TinyJobs(sys.argv[1]).start()
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


def fork_without_warning(case: unittest.TestCase, url: str) -> None:
    import subprocess
    import sys

    finished = subprocess.run(
        [sys.executable, "-c", FORK_WITHOUT_WARNING, url],
        capture_output=True,
        text=True,
        timeout=60,
    )
    case.assertEqual(finished.returncode, 0, finished.stderr)
