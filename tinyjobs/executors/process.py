from __future__ import annotations

import asyncio
import atexit
import importlib
import itertools
import logging
import multiprocessing
import multiprocessing.process
import multiprocessing.resource_tracker
import multiprocessing.util
import os
import pickle
import socket
import struct
import sys
import threading
import time
import traceback
from collections import deque
from contextlib import suppress
from typing import Any, Callable, TYPE_CHECKING

from ..exceptions import (
    REMOTE_TRACEBACK,
    Abandon,
    ConfigurationError,
    ExecutorClosedError,
    IsolatedTaskError,
    ProcessDied,
    Retry,
    error_message,
)
from ..exiting import (
    ignore_stop_signals,
    kill_group,
    kill_own_group,
    own_group,
    signal_name,
    stop_signals_blocked,
)
from ..models import Job, JobPayload
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task

logger = logging.getLogger("tinyjobs.worker")

EXECUTOR_CLOSED_MESSAGE = (
    "the job processes were shut down, so this job was handed back without running"
)
TIMEOUT_MESSAGE = "{name} ran past its timeout of {seconds:g}s, so its process was killed"
STARTUP_MESSAGE = "the job process for {name} did not come up within {seconds:g}s, so it was killed"
SETUP_MESSAGE = (
    "the job process did not get to {name} within {seconds:g}s of starting, in process_setup, "
    "django.setup() or the import of its module, so it was killed"
)
DIED_MESSAGE = "the process running {name} died {how} before it answered"
IMPORT_MESSAGE = (
    "the job process could not find the task {name} in {module}; an isolated task must be kept "
    "at module level of the module that registered it, one the job process can import"
)
AMBIGUOUS_MESSAGE = (
    "the job process found more than one isolated task named {name} over the same function in "
    "{module}, so it cannot tell which app's to run"
)
DIED_LOST_MESSAGE = (
    "the process running {name} died before it answered, and its exit status was collected by "
    "another part of the worker"
)
SETUP_FAILED_MESSAGE = "the job process could not call process_setup {module}:{qualname}"
PROCESS_SETUP_MESSAGE = (
    "process_setup must be a module-level function, since each job process imports it by name"
)
PROCESS_SETUP_MAIN_MESSAGE = (
    "process_setup cannot live in __main__ when the worker runs from {how}, since a job process "
    "cannot import it from there"
)
MAIN_MODULE = "__main__"
SPAWNED_MAIN_MODULE = "__mp_main__"
START_METHOD = "spawn"
STARTUP_SECONDS = 120.0
SETUP_SECONDS = 600.0
JOIN_SECONDS = 1.0
KILL_SECONDS = 2.0
EXIT_GRACE_SECONDS = 0.1
WAIT_POLL_SECONDS = 0.01
ALIVE_POLL_SECONDS = 0.5
PARENT_POLL_SECONDS = 0.5
READ_SIZE = 1 << 16
DJANGO_SETTINGS = "DJANGO_SETTINGS_MODULE"

_children: "set[_Child]" = set()


def assert_process_setup(setup: Any) -> None:
    qualname = getattr(setup, "__qualname__", None)
    module = getattr(setup, "__module__", None)
    if (
        not callable(setup)
        or not isinstance(qualname, str)
        or not isinstance(module, str)
        or "<locals>" in qualname
        or "<lambda>" in qualname
    ):
        raise ConfigurationError(PROCESS_SETUP_MESSAGE)
    try:
        found = _attribute(module, qualname, main=MAIN_MODULE)
    except Exception:
        raise ConfigurationError(PROCESS_SETUP_MESSAGE) from None
    if found != setup:
        raise ConfigurationError(PROCESS_SETUP_MESSAGE)
    if module == MAIN_MODULE:
        how = main_spawn_problem()
        if how is not None:
            raise ConfigurationError(PROCESS_SETUP_MAIN_MESSAGE.format(how=how))


def main_spawn_problem() -> str | None:
    main = sys.modules.get(MAIN_MODULE)
    spec = getattr(main, "__spec__", None)
    if spec is None:
        if getattr(main, "__file__", None):
            return None
        return "python -c or an interactive session"
    if spec.name == MAIN_MODULE or spec.name.endswith(".__main__"):
        return f"python -m {spec.name.removesuffix('.__main__')}"
    return None


class ProcessExecutor(Executor):
    def __init__(
        self,
        processes: int,
        jobs_per_process: int | None = None,
        setup: Callable[[], Any] | None = None,
    ) -> None:
        self._pool = _Processes(processes, jobs_per_process)
        self._setup = None if setup is None else (setup.__module__, setup.__qualname__)

    async def execute(self, task: "Task", payload: JobPayload, *, job: Job | None = None) -> Any:
        loop = asyncio.get_running_loop()
        serialization = task.app.serialization
        if job is None:
            encoded, codec = serialization.encode_payload(payload, task.options.codec)
        else:
            encoded, codec = job.payload, job.codec
        request = (
            task.registered_in,
            task.name,
            task.function.__module__,
            task.function.__qualname__,
            encoded,
            codec,
            task.store_result,
            self._setup,
        )
        for last in (False, True):
            child = await self._pool.acquire()
            gone = True
            try:
                booted, started, reply = child.ask(loop, task.name, request)
                try:
                    await asyncio.wait_for(booted, STARTUP_SECONDS)
                except TimeoutError:
                    child.kill()
                    raise TimeoutError(
                        STARTUP_MESSAGE.format(name=task.name, seconds=STARTUP_SECONDS)
                    ) from None
                except ProcessDied:
                    if last:
                        raise
                    continue
                try:
                    await asyncio.wait_for(started, SETUP_SECONDS)
                except TimeoutError:
                    child.kill()
                    raise TimeoutError(
                        SETUP_MESSAGE.format(name=task.name, seconds=SETUP_SECONDS)
                    ) from None
                except ProcessDied:
                    if last:
                        raise
                    continue
                try:
                    if task.timeout is None:
                        answer = await reply
                    else:
                        answer = await asyncio.wait_for(reply, task.timeout)
                except TimeoutError:
                    child.kill()
                    raise TimeoutError(
                        TIMEOUT_MESSAGE.format(name=task.name, seconds=task.timeout)
                    ) from None
                gone = False
            except asyncio.CancelledError:
                child.kill()
                for future in (booted, started, reply):
                    if future.done() and not future.cancelled():
                        future.exception()
                raise
            finally:
                self._pool.release(child, gone=gone)
            return _outcome(answer, serialization, codec)
        raise AssertionError("unreachable")

    def shutdown(self) -> None:
        self._pool.shutdown()


class _Processes:
    def __init__(self, size: int, limit: int | None) -> None:
        self._size = size
        self._limit = limit
        self._context = multiprocessing.get_context(START_METHOD)
        self._idle: deque[_Child] = deque()
        self._busy: set[_Child] = set()
        self._closing: list[_Child] = []
        self._waiting: deque[asyncio.Future] = deque()
        self._closed = False
        self._names = itertools.count()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._collector: asyncio.TimerHandle | None = None

    async def acquire(self) -> "_Child":
        self._loop = asyncio.get_running_loop()
        while True:
            if self._closed:
                raise ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE)
            if self._idle:
                child = self._idle.pop()
                if not child.alive:
                    self._drop(child)
                    continue
            elif len(self._busy) < self._size:
                child = _Child(self._context, f"tinyjobs-job-{next(self._names)}")
            else:
                waiter = self._loop.create_future()
                self._waiting.append(waiter)
                try:
                    await waiter
                except asyncio.CancelledError:
                    if waiter.done() and not waiter.cancelled():
                        self._wake()
                    raise
                finally:
                    with suppress(ValueError):
                        self._waiting.remove(waiter)
                continue
            self._busy.add(child)
            return child

    def release(self, child: "_Child", *, gone: bool) -> None:
        if child not in self._busy:
            return
        self._busy.discard(child)
        if gone:
            self._drop(child)
        elif self._closed or (self._limit is not None and child.jobs >= self._limit):
            self._retire(child)
        else:
            self._idle.append(child)
        self._wake()

    def _wake(self) -> None:
        while self._waiting:
            waiter = self._waiting.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def _retire(self, child: "_Child") -> None:
        child.stop()
        self._closing.append(child)
        self._collect_later()

    def _drop(self, child: "_Child") -> None:
        child.discard()
        self._closing.append(child)
        self._collect_later()

    def _collect_later(self) -> None:
        if self._collector is None and self._loop is not None and not self._loop.is_closed():
            self._collector = self._loop.call_later(JOIN_SECONDS, self._collect)

    def _collect(self) -> None:
        self._collector = None
        self._reap(0.0)
        now = time.monotonic()
        for child in self._closing:
            if now - child.stopped_at >= JOIN_SECONDS:
                child._kill_process()
        if self._closing:
            self._collect_later()

    def shutdown(self) -> None:
        self._closed = True
        if self._collector is not None:
            self._collector.cancel()
            self._collector = None
        while self._waiting:
            waiter = self._waiting.popleft()
            if not waiter.done():
                waiter.set_exception(ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE))
        idle, busy = tuple(self._idle), tuple(self._busy)
        self._idle.clear()
        self._busy.clear()
        self._closing.extend(idle)
        self._closing.extend(busy)
        for child in idle:
            child.stop()
        for child in busy:
            child.kill()
        self._reap(JOIN_SECONDS)
        for child in self._closing:
            child.kill()
        self._reap(KILL_SECONDS)

    def _reap(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        for child in tuple(self._closing):
            if child.join_until(deadline):
                self._closing.remove(child)


class _Child:
    def __init__(self, context: Any, name: str) -> None:
        self._conn, remote = context.Pipe()
        self._process = context.Process(target=serve, args=(remote, os.getpid()), name=name)
        try:
            with stop_signals_blocked():
                self._process.start()
        except BaseException:
            self._conn.close()
            raise
        finally:
            remote.close()
        _known_children().discard(self._process)
        self._fd = self._conn.fileno()
        self._buffer = bytearray()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: deque[asyncio.Future] = deque()
        self._watching = False
        self._checker: asyncio.TimerHandle | None = None
        self._sending: asyncio.Future | None = None
        self._close_after_send = False
        self._booted = False
        self._reaped = False
        self._lost = False
        self._name = ""
        self.jobs = 0
        self.stopped_at = 0.0
        _children.add(self)
        logger.debug("job process %d started", self._process.pid)

    @property
    def pid(self) -> int | None:
        return self._process.pid

    @property
    def alive(self) -> bool:
        return not self._reaped and self._poll() is None

    def _poll(self) -> int | None:
        popen = self._process._popen
        if popen is not None and popen.returncode is None:
            try:
                pid, status = os.waitpid(popen.pid, os.WNOHANG)
            except ChildProcessError:
                popen.returncode = 0
                self._lost = True
            else:
                if pid == popen.pid:
                    popen.returncode = os.waitstatus_to_exitcode(status)
        return None if popen is None else popen.returncode

    def _wait_until(self, deadline: float) -> int | None:
        while (code := self._poll()) is None and time.monotonic() < deadline:
            time.sleep(WAIT_POLL_SECONDS)
        return code

    def ask(
        self, loop: asyncio.AbstractEventLoop, name: str, request: tuple
    ) -> tuple[asyncio.Future, asyncio.Future, asyncio.Future]:
        booted, started, reply = (loop.create_future() for _ in range(3))
        self._loop, self._name = loop, name
        if self._booted:
            booted.set_result(None)
        else:
            self._pending.append(booted)
        self._pending.extend((started, reply))
        self._watch()
        self._sending = loop.run_in_executor(None, self._conn.send, request)
        self._sending.add_done_callback(self._sent)
        return booted, started, reply

    def _sent(self, sending: asyncio.Future) -> None:
        if self._sending is not sending:
            return
        self._sending = None
        if self._close_after_send:
            self._close()
        elif not sending.cancelled() and sending.exception() is not None:
            self._fail(self._died())

    def _watch(self) -> None:
        if self._loop is None or self._watching:
            return
        self._watching = True
        self._loop.add_reader(self._fd, self._readable)
        self._checker = self._loop.call_later(ALIVE_POLL_SECONDS, self._check)

    def _unwatch(self) -> None:
        if self._watching and self._loop is not None:
            self._watching = False
            with suppress(Exception):
                self._loop.remove_reader(self._fd)
        if self._checker is not None:
            self._checker.cancel()
            self._checker = None

    def _check(self) -> None:
        self._checker = None
        if not self._pending or self._loop is None:
            return
        if self.alive:
            self._checker = self._loop.call_later(ALIVE_POLL_SECONDS, self._check)
            return
        with suppress(OSError):
            while self._pending and self._conn.poll(0):
                if not self._read():
                    break
        if self._pending:
            self._fail(self._died())

    def _readable(self) -> None:
        try:
            if not self._read():
                self._fail(self._died())
        except OSError:
            self._fail(self._died())

    def _read(self) -> bool:
        data = os.read(self._fd, READ_SIZE)
        if not data:
            return False
        self._buffer += data
        while self._pending:
            frame = self._frame()
            if frame is None:
                break
            offset, size = frame
            view = memoryview(self._buffer)[offset : offset + size]
            try:
                answer = pickle.loads(view)
            except Exception:
                return False
            finally:
                view.release()
            del self._buffer[: offset + size]
            self._handle(answer)
        return True

    def _frame(self) -> tuple[int, int] | None:
        buffer = self._buffer
        if len(buffer) < 4:
            return None
        (size,) = struct.unpack("!i", buffer[:4])
        offset = 4
        if size == -1:
            if len(buffer) < 12:
                return None
            (size,) = struct.unpack("!Q", buffer[4:12])
            offset = 12
        if len(buffer) < offset + size:
            return None
        return offset, size

    def _handle(self, answer: tuple) -> None:
        kind = answer[0]
        if kind == "booted":
            self._booted = True
            self._resolve_next(None)
        elif kind == "started":
            self._resolve_next(None)
        else:
            self.jobs += 1
            while self._pending:
                self._resolve_next(answer)
        if not self._pending:
            self._unwatch()

    def _resolve_next(self, value: Any) -> None:
        if self._pending:
            future = self._pending.popleft()
            if not future.done():
                future.set_result(value)

    def _fail(self, error: BaseException) -> None:
        self._unwatch()
        abandoned = any(future.cancelled() for future in self._pending)
        pending = [future for future in self._pending if not future.done()]
        self._pending.clear()
        for index, future in enumerate(pending):
            if index == 0 and not abandoned:
                future.set_exception(error)
            else:
                future.cancel()

    def _died(self) -> ProcessDied:
        code = self._wait_until(time.monotonic() + EXIT_GRACE_SECONDS)
        self._close()
        if self._lost:
            return ProcessDied(DIED_LOST_MESSAGE.format(name=self._name))
        if code is None:
            self._kill_process()
            how = "without answering"
        elif code < 0:
            how = f"from {signal_name(-code)}"
        else:
            how = f"with exit status {code}"
        return ProcessDied(DIED_MESSAGE.format(name=self._name, how=how))

    def _close(self) -> None:
        if self._sending is not None and not self._sending.done():
            self._close_after_send = True
            self._shut()
            return
        with suppress(OSError):
            self._conn.close()

    def _shut(self) -> None:
        with suppress(OSError):
            held = socket.socket(fileno=self._fd)
            try:
                held.shutdown(socket.SHUT_RDWR)
            finally:
                held.detach()

    def _kill_process(self) -> None:
        if self._reaped or self._poll() is not None:
            return
        kill_group(self._process.pid)
        with suppress(OSError, ValueError):
            self._process.kill()

    def kill(self) -> None:
        self._kill_process()
        self._unwatch()
        while self._pending:
            future = self._pending.popleft()
            if not future.get_loop().is_closed():
                future.cancel()
        self._close()

    def stop(self) -> None:
        self._unwatch()
        self.stopped_at = time.monotonic()
        with suppress(OSError, ValueError):
            self._conn.send(None)
        self._close()

    def discard(self) -> None:
        if self._reaped:
            return
        self._unwatch()
        self.stopped_at = time.monotonic()
        if self.alive:
            self._kill_process()
        self._close()

    def join_until(self, deadline: float) -> bool:
        if not self._reaped:
            self._wait_until(deadline)
        return self.reap()

    def reap(self) -> bool:
        if self._reaped:
            return True
        if self._poll() is None:
            return False
        self._process.close()
        self._reaped = True
        _children.discard(self)
        return True


def _kill_remaining() -> None:
    for child in list(_children):
        with suppress(Exception):
            child._kill_process()


def _known_children() -> set:
    return getattr(multiprocessing.process, "_children", set())


def _forget_after_fork() -> None:
    _children.clear()
    tracker = getattr(multiprocessing.resource_tracker, "_resource_tracker", None)
    if tracker is not None and not hasattr(tracker, "_after_fork_in_child"):
        tracker._pid = None
        with suppress(AttributeError):
            tracker._lock._at_fork_reinit()
        if not _forked_by_multiprocessing():
            fd = getattr(tracker, "_fd", None)
            tracker._fd = None
            if fd is not None:
                with suppress(OSError):
                    os.close(fd)


def _forked_by_multiprocessing() -> bool:
    frame = sys._getframe(2)
    code = frame.f_code
    return code.co_name == "_launch" and code.co_filename.endswith("popen_fork.py")


atexit.register(_kill_remaining)
os.register_at_fork(after_in_child=_forget_after_fork)


def _outcome(answer: tuple, serialization: Any, codec: str) -> Any:
    kind, *details = answer
    if kind == "ok":
        raw = details[0]
        return None if raw is None else serialization.decode_result(raw, codec)
    if kind == "retry":
        raise Retry(in_=details[0], at=details[1])
    if kind == "abandon":
        raise Abandon(details[0])
    raise _rebuilt(*details)


class RemoteTraceback(Exception):
    pass


def _attach(error: BaseException, remote_traceback: str) -> None:
    with suppress(Exception):
        object.__setattr__(error, REMOTE_TRACEBACK, remote_traceback)
    BaseException.__cause__.__set__(error, RemoteTraceback(remote_traceback))


def _rebuilt(
    module: str, qualname: str, message: str, remote_traceback: str, pickled: bytes | None
) -> BaseException:
    error: Any = None
    if pickled is not None:
        with suppress(Exception):
            error = pickle.loads(pickled)
    if not isinstance(error, Exception):
        cls = _exception_class(module, qualname)
        if cls is not None:
            with suppress(Exception):
                error = cls(message)
            if not isinstance(error, Exception):
                with suppress(Exception):
                    candidate = cls.__new__(cls)
                    BaseException.__init__(candidate, message)
                    str(candidate)
                    error = candidate
    if not isinstance(error, Exception):
        error = IsolatedTaskError(message, qualname.rsplit(".", 1)[-1])
    _attach(error, remote_traceback)
    return error


def _exception_class(module: str, qualname: str) -> type[Exception] | None:
    holder: Any = sys.modules.get(module)
    for part in qualname.split("."):
        holder = getattr(holder, part, None)
    if isinstance(holder, type) and issubclass(holder, Exception):
        return holder
    return None


def serve(conn: Any, parent: int) -> None:
    own_group()
    ignore_stop_signals()
    _watch_parent(parent)
    conn.send(("booted",))
    state = _State()
    while True:
        try:
            request = conn.recv()
        except EOFError:
            return
        if request is None:
            return
        _run(conn, request, state)


class _State:
    __slots__ = ("tasks", "prepared", "setups")

    def __init__(self) -> None:
        self.tasks: dict[tuple[str, str], Any] = {}
        self.prepared = False
        self.setups: set[tuple[str, str]] = set()


def _watch_parent(parent: int) -> None:
    def watch() -> None:
        while os.getppid() == parent:
            time.sleep(PARENT_POLL_SECONDS)
        kill_own_group()
        os._exit(1)

    threading.Thread(target=watch, name="tinyjobs-parent-watch", daemon=True).start()


def _run(conn: Any, request: tuple, state: _State) -> None:
    module, name, function_module, qualname, encoded, codec, store_result, setup = request
    try:
        if not state.prepared:
            _prepare()
            state.prepared = True
        if setup is not None and setup not in state.setups:
            _call_setup(*setup)
            state.setups.add(setup)
        task = state.tasks.get((module, name))
        if task is None:
            task = _resolve(module, name, function_module, qualname)
            state.tasks[(module, name)] = task
        serialization = task.app.serialization
        raw = serialization.codecs.decode(encoded, codec)
    except Exception as error:
        _reply(conn, _failure(error))
        return
    conn.send(("started",))
    try:
        result = task.function(*raw["args"], **raw["kwargs"])
    except Retry as error:
        answer = ("retry", error.in_, error.at)
    except Abandon as error:
        answer = ("abandon", error.reason)
    except Exception as error:
        answer = _failure(error)
    else:
        try:
            answer = ("ok", serialization.encode_result(result, codec))
        except Exception as error:
            answer = _failure(error) if store_result else ("ok", None)
    _reply(conn, answer)


def _reply(conn: Any, answer: tuple) -> None:
    try:
        conn.send(answer)
    except Exception as error:
        conn.send(_failure(error))


def _prepare() -> None:
    if os.environ.get(DJANGO_SETTINGS):
        with suppress(ImportError):
            import django

            django.setup()


def _failure(error: Exception) -> tuple:
    cls = type(error)
    pickled: bytes | None = None
    with suppress(Exception):
        pickled = pickle.dumps(error)
    return (
        "error",
        cls.__module__,
        cls.__qualname__,
        error_message(error),
        "".join(traceback.format_exception(error)),
        pickled,
    )


def _call_setup(module: str, qualname: str) -> None:
    try:
        _attribute(module, qualname)()
    except Exception as error:
        raise IsolatedTaskError(
            f"{SETUP_FAILED_MESSAGE.format(module=module, qualname=qualname)}: {error!r}"
        ) from error


def _resolve(module: str, name: str, function_module: str, qualname: str) -> Any:
    from ..task import Task

    wanted = {name}
    if module == MAIN_MODULE and name.startswith(f"{MAIN_MODULE}."):
        wanted.add(SPAWNED_MAIN_MODULE + name[len(MAIN_MODULE) :])
    function_module = _spawned(function_module)
    problem = ""
    found: dict[int, Any] = {}
    for candidate_module in dict.fromkeys((module, function_module)):
        try:
            namespace = vars(importlib.import_module(_spawned(candidate_module)))
        except ImportError as error:
            problem = f": {error}"
            continue
        for value in list(namespace.values()):
            if isinstance(value, Task) and value.isolated and value.name in wanted:
                found.setdefault(id(value), value)
    exact = [
        task
        for task in found.values()
        if task.function.__module__ == function_module
        and task.function.__qualname__ == qualname
    ]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1 or (not exact and len(found) > 1):
        raise IsolatedTaskError(AMBIGUOUS_MESSAGE.format(name=name, module=module))
    if found:
        return next(iter(found.values()))
    raise IsolatedTaskError(IMPORT_MESSAGE.format(module=module, name=name) + problem)


def _spawned(module: str) -> str:
    return SPAWNED_MAIN_MODULE if module == MAIN_MODULE else module


def _attribute(module: str, qualname: str, main: str = SPAWNED_MAIN_MODULE) -> Any:
    holder: Any = importlib.import_module(main if module == MAIN_MODULE else module)
    for part in qualname.split("."):
        holder = getattr(holder, part)
    return holder
