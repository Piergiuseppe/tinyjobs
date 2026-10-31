from __future__ import annotations

import _thread
import asyncio
import atexit
import concurrent.futures
import contextvars
import inspect
import itertools
import logging
import operator
import os
import threading
import time
import weakref
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from functools import partial
from queue import SimpleQueue
from typing import Any, Callable, Coroutine, Iterator, TypeVar

from .deadlines import current as current_deadline
from .exceptions import (
    AppClosedError,
    EventLoopStoppedError,
    InterruptedCallError,
    SynchronousCallInsideEventLoopError,
)

T = TypeVar("T")

INSIDE_LOOP_MESSAGE = (
    "{name}() is synchronous and was called from inside a running event loop, "
    "which would block it. Use the awaitable form instead: await {async_name}()"
)

STOPPED_LOOP_MESSAGE = (
    "{name}() was waiting on tinyjobs' event loop, and that loop stopped before "
    "the call finished"
)

CLOSED_MESSAGE = (
    "this app was closed, so it will not reconnect on its own; call app.start() "
    "or await app.ready() to connect it again"
)

INTERRUPTED_MESSAGE = (
    "{name}() was called on a thread that tinyjobs had to interrupt: it holds one "
    "of tinyjobs' locks or is in the middle of a fork, which is where a signal "
    "handler lands. The call would wait for work that needs this thread to go on, "
    "so it is refused instead of hanging; set a flag in the handler and make the "
    "call once the handler has returned"
)

LOOP_POLL_SECONDS = 0.1
SETTLE_POLL_SECONDS = 0.001
EXITING_MESSAGE = (
    "the interpreter is exiting, so tinyjobs starts no new event loop for this call"
)

STOP_SECONDS = 5.0
INSIST_SECONDS = 0.5
RECHECK_SECONDS = 0.05
FORK_PAUSE_SECONDS = 0.1
FORK_ACK_SECONDS = 1.0
LOOP_THREAD_PREFIX = "tinyjobs-loop"
CONTROL_THREAD_NAME = "tinyjobs-control"
CLOSE_THREAD_NAME = "tinyjobs-close"
OWNED_THREAD_PREFIXES = (LOOP_THREAD_PREFIX, "tinyjobs-sqlite", CONTROL_THREAD_NAME)
OS_THREADS = "/proc/self/task"
THREAD_HANDLES = ("_os_thread_handle", "_handle")
THREAD_EXIT_SECONDS = 1.0
FORK_WAIT_TRIES = 3
THREAD_EXIT_POLL_SECONDS = 0.001

_retired: "weakref.WeakSet[asyncio.AbstractEventLoop]" = weakref.WeakSet()
_ending: "weakref.WeakSet[asyncio.AbstractEventLoop]" = weakref.WeakSet()
_runners: "weakref.WeakSet[LoopRunner]" = weakref.WeakSet()
_fork_pausers: "weakref.WeakSet[Any]" = weakref.WeakSet()
_paused_for_fork: list["LoopRunner"] = []
_inherited_loops: list[asyncio.AbstractEventLoop] = []
_fork_gates: list[Any] = []
_resting: set[asyncio.AbstractEventLoop] = set()
_tracked: set[weakref.ref] = set()
_child_steps: list[Any] = []
_exit_steps: list[Any] = []
logger = logging.getLogger("tinyjobs.runtime")

_call_steps: list[Any] = []
_inside_checks: list[Callable[[], bool]] = []
_exiting = threading.Event()
_watched: dict[weakref.ref, list[Any]] = {}
_closing: set[threading.Thread] = set()
_kept: "weakref.WeakSet[asyncio.Task]" = weakref.WeakSet()
_keeping: contextvars.ContextVar[bool] = contextvars.ContextVar("tinyjobs_keeping", default=False)
_sparing: contextvars.ContextVar[bool] = contextvars.ContextVar("tinyjobs_sparing", default=False)
_inbox: SimpleQueue = SimpleQueue()
_fork_marks: dict[int, int] = {}
_after_fork_actions: dict[int, Any] = {}
_kept_at_fork: dict[int, list[weakref.ref]] = {}
_settling: dict[int, int] = {}
_settled_pid = os.getpid()
_control_lock = threading.Lock()
_control_thread: threading.Thread | None = None
_control_ident: int | None = None
_control_pid = 0
_pausing = False


class _ForkState:
    __slots__ = ("generation",)


fork_state = _ForkState()
fork_state.generation = 0


def tracked_lock() -> Any:
    lock = threading.RLock()
    _tracked.add(weakref.ref(lock, _tracked.discard))
    return lock


def _holds_a_lock() -> bool:
    return any(map(_owned_lock, _tracked.copy()))


def holds_tinyjobs() -> bool:
    ident = _thread.get_ident()
    if _fork_marks.get(ident) == os.getpid():
        return True
    if ident != threading.main_thread().ident:
        return False
    return _holds_a_lock() or any(check() for check in _inside_checks)


def refuse_inside(check: Callable[[], bool]) -> None:
    _inside_checks.append(check)


def refuse_interrupted(name: str) -> None:
    if holds_tinyjobs():
        raise InterruptedCallError(INTERRUPTED_MESSAGE.format(name=name))


def forking_elsewhere(ident: int, pid: int) -> bool:
    return any(
        owner == pid and marked != ident
        for marked, owner in list(_fork_marks.items())
    )


def forking_threads(pid: int) -> set[int]:
    return {marked for marked, owner in list(_fork_marks.items()) if owner == pid}


class CountingPool(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._pending: set[object] = set()
        self.retiring = False

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> concurrent.futures.Future:
        token = object()
        self._pending.add(token)
        try:
            future = super().submit(fn, *args, **kwargs)
        except BaseException:
            self._pending.discard(token)
            raise
        future.add_done_callback(lambda _: self._pending.discard(token))
        return future

    @property
    def idle(self) -> bool:
        return not self._pending

    @property
    def started(self) -> bool:
        return bool(self._threads)

    def join(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        for thread in list(self._threads):
            thread.join(max(deadline - time.monotonic(), 0))


def pause_threads_at_fork(owner: Any) -> None:
    _fork_pausers.add(owner)


def hold_at_fork(gate: Any) -> None:
    _fork_gates.append(gate)


def after_fork_in_child(step: Any) -> None:
    _child_steps.append(step)


def before_each_call(step: Any) -> None:
    _call_steps.append(step)


def heal_this_thread() -> None:
    for step in _call_steps:
        with suppress(Exception):
            step()


def at_exit(step: Any) -> None:
    _exit_steps.append(step)


def when_collected(
    target: Any, function: Callable[..., Any], *args: Any
) -> Callable[[], None]:
    holder: list[weakref.ref] = []
    spawns = [
        partial(
            _thread.start_new_thread, _collected, (holder, os.getpid(), function, args)
        )
    ]
    mine = map({os.getpid(): spawns}.get, itertools.islice(iter(os.getpid, None), 1), [()])
    ref = weakref.ref(
        target, partial(next, map(operator.call, itertools.chain.from_iterable(mine)))
    )
    holder.append(ref)
    _watched[ref] = spawns
    if _exiting.is_set():
        spawns.clear()
    return partial(_forget_collected, ref, spawns)


def _forget_collected(ref: weakref.ref, spawns: list[Any]) -> None:
    spawns.clear()
    _watched.pop(ref, None)


def _collected(
    holder: list[weakref.ref], pid: int, function: Callable[..., Any], args: Any
) -> None:
    if pid != os.getpid() or _exiting.is_set():
        return
    with suppress(Exception):
        _watched.pop(holder[0], None)
        _start_closing(_close_collected, function, args)


def _close_collected(function: Callable[..., Any], args: Any) -> None:
    try:
        with suppress(Exception):
            function(*args)
    finally:
        _closing.discard(threading.current_thread())


def _start_closing(target: Callable[..., Any], *args: Any) -> None:
    thread = threading.Thread(target=target, args=args, name=CLOSE_THREAD_NAME, daemon=True)
    _closing.add(thread)
    try:
        thread.start()
    except BaseException:
        _closing.discard(thread)
        raise


def inside_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def refuse_inside_loop(name: str, async_name: str | None = None) -> None:
    if inside_running_loop():
        raise SynchronousCallInsideEventLoopError(
            INSIDE_LOOP_MESSAGE.format(name=name, async_name=async_name or f"a{name}")
        )


async def settled() -> None:
    pid = os.getpid()
    if _settled_pid == pid:
        return
    _ensure_control()
    while _settled_pid != pid:
        await asyncio.sleep(SETTLE_POLL_SECONDS)


def submit(
    loop: asyncio.AbstractEventLoop,
    coroutine: Coroutine[Any, Any, T],
    name: str,
    finished: Any = None,
    outcome: concurrent.futures.Future | None = None,
    taken: list[bool] | None = None,
    kept: bool = False,
    returning: bool = False,
) -> concurrent.futures.Future:
    if outcome is None:
        outcome = concurrent.futures.Future()
    if finished is not None:
        outcome.add_done_callback(lambda _: finished.release())

    def begin() -> None:
        if taken is not None:
            taken.append(True)
        if outcome.cancelled() or (
            not returning and (loop in _retired or loop in _ending)
        ):
            _discard(coroutine)
            _settle(outcome, error=_stopped(loop, name))
            return
        if kept or returning:
            context = contextvars.copy_context()
            context.run(_keeping.set, True)
            task = loop.create_task(coroutine, context=context)
            _kept.add(task)
        else:
            task = loop.create_task(coroutine)
        task.add_done_callback(partial(_deliver, outcome))
        outcome.add_done_callback(partial(_abandon, loop, task))

    try:
        loop.call_soon_threadsafe(begin)
    except RuntimeError:
        _discard(coroutine)
        raise _stopped(loop, name) from None
    return outcome


async def run_on(
    loop: asyncio.AbstractEventLoop, coroutine: Coroutine[Any, Any, T], name: str
) -> T:
    if loop is asyncio.get_running_loop():
        return await coroutine
    if _settled_pid != os.getpid():
        try:
            await settled()
        except BaseException:
            _discard(coroutine)
            raise
    if _gone(loop) or loop in _retired or loop in _ending:
        _discard(coroutine)
        raise _stopped(loop, name)
    if holds_tinyjobs():
        _discard(coroutine)
        raise InterruptedCallError(INTERRUPTED_MESSAGE.format(name=name))
    outcome: concurrent.futures.Future | None = None
    future: asyncio.Future | None = None
    taken: list[bool] = []
    try:
        outcome = concurrent.futures.Future()
        submit(loop, coroutine, name, outcome=outcome, taken=taken)
        future = asyncio.wrap_future(outcome)
        while True:
            done, _ = await asyncio.wait({future}, timeout=LOOP_POLL_SECONDS)
            if done and not future.cancelled():
                return future.result()
            if done or _gone(loop):
                raise _stopped(loop, name)
            _ensure_control()
    except BaseException:
        if future is not None:
            future.cancel()
        if outcome is not None:
            outcome.cancel()
        if not taken:
            _discard(coroutine)
        raise


async def run_kept(
    loop: asyncio.AbstractEventLoop,
    coroutine: Coroutine[Any, Any, T],
    name: str,
    outcome: concurrent.futures.Future,
) -> T:
    try:
        if _settled_pid != os.getpid():
            try:
                await settled()
            except BaseException:
                _discard(coroutine)
                raise
        if _gone(loop) or loop in _retired or loop in _ending:
            _discard(coroutine)
            raise _stopped(loop, name)
        if loop is not asyncio.get_running_loop() and holds_tinyjobs():
            _discard(coroutine)
            raise InterruptedCallError(INTERRUPTED_MESSAGE.format(name=name))
        submit(loop, coroutine, name, outcome=outcome, kept=True)
    except BaseException:
        outcome.cancel()
        raise
    return await outcome_of(loop, outcome, name)


async def outcome_of(
    loop: asyncio.AbstractEventLoop, outcome: concurrent.futures.Future, name: str
) -> Any:
    here = asyncio.get_running_loop()
    waiting = here.create_future()
    outcome.add_done_callback(partial(wake, here, waiting))
    while not outcome.done():
        await asyncio.wait({waiting}, timeout=LOOP_POLL_SECONDS)
        if not outcome.done() and _gone(loop):
            raise _stopped(loop, name)
        _ensure_control()
    return outcome.result()


def wake(
    loop: asyncio.AbstractEventLoop, waiting: asyncio.Future, _: concurrent.futures.Future
) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(_resolve, waiting)


def _resolve(waiting: asyncio.Future) -> None:
    if not waiting.done():
        waiting.set_result(None)


class _Request:
    __slots__ = (
        "runner",
        "factory",
        "name",
        "pid",
        "done",
        "claims",
        "outcome",
        "loop",
        "task",
        "cancelled",
        "run_message",
        "cancel_message",
        "drive_message",
    )

    def __init__(self, runner: Any, factory: Callable[[], Any], name: str) -> None:
        self.runner = runner
        self.factory = factory
        self.name = name
        self.pid = os.getpid()
        self.done = _thread.allocate_lock()
        self.done.acquire()
        self.claims = itertools.count()
        self.outcome: tuple[BaseException | None, Any] = (None, None)
        self.loop: asyncio.AbstractEventLoop | None = None
        self.task: asyncio.Task | None = None
        self.cancelled = False
        self.run_message = (_start, self)
        self.cancel_message = (_cancel, self)
        self.drive_message = (_drive_off, self)


def drive(function: Callable[[], T], *, name: str) -> T:
    request = _Request(None, function, name)
    _inbox.put(request.drive_message)
    _ensure_control()
    return _wait(request)


def _wait(request: _Request) -> Any:
    while not request.done.acquire(timeout=LOOP_POLL_SECONDS):
        if request.pid != os.getpid():
            raise _stopped(None, request.name)
        _ensure_control()
    error, result = request.outcome
    if error is not None:
        raise error
    return result


def _ensure_control() -> None:
    thread = _control_thread
    if (
        thread is not None
        and _control_pid == os.getpid()
        and threading._active.get(_control_ident) is thread
    ):
        return
    _thread.start_new_thread(_control_bootstrap, (False,))


def _control_bootstrap(resuming: bool) -> None:
    global _control_thread, _control_ident, _control_pid, _pausing
    pid = os.getpid()
    _settle_here(pid)
    with _control_lock:
        if _pausing:
            if not resuming and forking_threads(pid):
                return
            _wait_until_done(_control_thread)
            _pausing = False
            _resume_paused()
        current = _control_thread
        if current is not None and _control_pid == pid and current.is_alive():
            return
        thread = threading.Thread(
            target=_control_main, name=CONTROL_THREAD_NAME, daemon=True
        )
        thread.start()
        _control_thread, _control_ident, _control_pid = thread, thread.ident, pid


def _control_main() -> None:
    while not _handled(_inbox.get()):
        pass


def _handled(message: tuple[Any, ...]) -> bool:
    with suppress(Exception):
        return bool(message[0](*message[1:]))
    return False


def _start(request: _Request) -> bool:
    if request.pid != os.getpid():
        return False
    runner = request.runner
    try:
        loop = runner._ensure_loop()
        request.loop = loop
        runner._awaiting.setdefault(loop, set()).add(request)
        loop.call_soon_threadsafe(_begin, request)
    except RuntimeError:
        _settle_request(request, error=_stopped(request.loop, request.name))
    except BaseException as error:
        _settle_request(request, error=error)
    return False


def _begin(request: _Request) -> None:
    loop = request.loop
    if request.cancelled or loop in _retired or loop in _ending:
        _settle_request(request, error=_stopped(loop, request.name))
        return
    try:
        task = loop.create_task(request.factory())
    except BaseException as error:
        _settle_request(request, error=error)
        return
    request.task = task
    task.add_done_callback(partial(_finish, request))


def _finish(request: _Request, task: asyncio.Task) -> None:
    if task.cancelled():
        _settle_request(request, error=_stopped(request.loop, request.name))
        return
    error = task.exception()
    if error is None:
        _settle_request(request, result=task.result())
    else:
        _settle_request(request, error=error)


def _cancel(request: _Request) -> bool:
    request.cancelled = True
    loop = request.loop
    if loop is not None and request.pid == os.getpid():
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(_cancel_task, request)
    return False


def _cancel_task(request: _Request) -> None:
    if request.task is not None:
        _cancel_once(request.task)


def _cancel_once(task: asyncio.Task) -> None:
    if not task.cancelling():
        task.cancel()


def _settle_request(
    request: _Request, *, result: Any = None, error: BaseException | None = None
) -> None:
    if next(request.claims):
        return
    request.outcome = (error, result)
    if request.loop is not None and request.runner is not None:
        request.runner._awaiting.get(request.loop, set()).discard(request)
    request.done.release()


def _drive_off(request: _Request) -> bool:
    if request.pid != os.getpid():
        return False
    try:
        _start_closing(_drive_through, request)
    except BaseException as error:
        _settle_request(request, error=error)
    return False


def _drive_through(request: _Request) -> None:
    try:
        result = request.factory()
    except BaseException as error:
        _settle_request(request, error=error)
    else:
        _settle_request(request, result=result)
    finally:
        _closing.discard(threading.current_thread())


class LoopRunner:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._residents: dict[asyncio.AbstractEventLoop, list[Any]] = {}
        self._awaiting: dict[asyncio.AbstractEventLoop, set[_Request]] = {}
        self._guard = tracked_lock()
        self._paused: asyncio.AbstractEventLoop | None = None
        self._keep_serving = False
        self._pool: CountingPool | None = None
        self._ready = threading.Event()
        _runners.add(self)

    def run(
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
        *,
        name: str = "this call",
        async_name: str | None = None,
    ) -> T:
        refuse_inside_loop(name, async_name)
        refuse_interrupted(name)
        heal_this_thread()
        request = _Request(self, factory, name)
        try:
            _inbox.put(request.run_message)
            _ensure_control()
            return _wait(request)
        except BaseException:
            _inbox.put(request.cancel_message)
            _ensure_control()
            raise

    def own_loop(self) -> asyncio.AbstractEventLoop:
        return self._ensure_loop()

    def serves(self, loop: asyncio.AbstractEventLoop) -> bool:
        return self._loop is loop and (loop.is_running() or loop in _resting)

    def on_own_loop(self) -> bool:
        return self._loop is not None and self._loop is asyncio.get_running_loop()

    def keep(self, loop: asyncio.AbstractEventLoop, resident: Any) -> None:
        with self._guard:
            self._residents.setdefault(loop, []).append(resident)

    def release(self, resident: Any) -> None:
        with self._guard:
            for kept in self._residents.values():
                if resident in kept:
                    kept.remove(resident)

    def stop_own(self) -> None:
        self._take_own()

    def close(self) -> None:
        drive(self.close_here, name="close")

    def close_here(self) -> None:
        loop, thread = self._take_own()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=STOP_SECONDS + INSIST_SECONDS)

    def _take_own(self) -> tuple[asyncio.AbstractEventLoop | None, Any]:
        with self._guard:
            loop, thread = self._loop, self._thread
            self._loop, self._thread = None, None
            if loop is None:
                return None, None
            _stop(loop)
            if self._paused is loop:
                self._clear_paused()
                if thread is None:
                    thread = _loop_thread(self._serve, loop, threading.Event())
                    thread.start()
        return loop, thread

    def pause(self) -> bool:
        if not self._guard.acquire(blocking=False):
            return False
        try:
            loop, thread, pool = self._loop, self._thread, self._pool
            if loop is None or thread is None or not loop.is_running():
                return False
            if thread is threading.current_thread() or self._paused is not None:
                return False
            if pool is not None and not pool.idle:
                return False
            self._paused, self._keep_serving = loop, False
            _resting.add(loop)
        finally:
            self._guard.release()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=FORK_PAUSE_SECONDS)
        if thread.is_alive():
            with self._guard:
                self._keep_serving = True
            return True
        if pool is not None and not pool.idle:
            return True
        self._pool = _loop_executor()
        loop.set_default_executor(self._pool)
        if pool is not None:
            pool.shutdown(wait=True)
        return True

    def resume(self) -> None:
        with self._guard:
            if self._paused is None or self._thread is not None:
                return
            self._restart_paused()

    def forget_inherited_loop(self) -> None:
        _inherited_loops.extend(
            loop for loop in (self._loop, self._paused) if loop is not None
        )
        self._loop, self._thread = None, None
        self._residents = {}
        self._awaiting = {}
        self._paused, self._keep_serving = None, False
        self._pool = None

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._guard:
            if _exiting.is_set() and not self._serving():
                raise AppClosedError(EXITING_MESSAGE)
            if self._paused is not None:
                if self._thread is None:
                    self._restart_paused()
                else:
                    self._keep_serving = True
            elif not self._serving():
                self._loop = asyncio.new_event_loop()
                self._pool = _loop_executor()
                self._loop.set_default_executor(self._pool)
                self._start(self._loop)
            return self._loop

    def _serving(self) -> bool:
        if self._loop is None or self._thread is None or not self._thread.is_alive():
            return False
        return self._loop.is_running() or not self._ready.is_set()

    def _restart_paused(self) -> None:
        loop = self._paused
        self._clear_paused()
        self._loop = loop
        self._start(loop)

    def _start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._ready = threading.Event()
        self._thread = _loop_thread(self._serve, loop, self._ready)
        _resting.add(loop)
        try:
            self._thread.start()
        except BaseException:
            _resting.discard(loop)
            self._loop, self._thread = None, None
            raise

    def _serve(self, loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        asyncio.set_event_loop(loop)
        loop.call_soon(self._awake, loop, ready)
        try:
            if self._serve_until_stopped(loop):
                self._wind_down(loop)
        except BaseException:
            self._settle_left(loop)
            raise

    def _serve_until_stopped(self, loop: asyncio.AbstractEventLoop) -> bool:
        while True:
            loop.run_forever()
            with self._guard:
                if self._paused is not loop:
                    return True
                if not self._keep_serving:
                    self._thread = None
                    return False
                loop.call_soon(self._served, loop)

    def _wind_down(self, loop: asyncio.AbstractEventLoop) -> None:
        with self._guard:
            if self._loop is loop:
                self._loop, self._thread = None, None
            residents = self._residents.pop(loop, [])
        _ending.add(loop)
        _resting.discard(loop)
        deadline = time.monotonic() + STOP_SECONDS
        finishing = _Finishing()
        _finish_tasks(loop, deadline, spare=True, finishing=finishing)
        for resident in residents:
            with suppress(Exception):
                loop.run_until_complete(resident.close())
            _finish_tasks(loop, deadline, spare=True, finishing=finishing)
        _finish_tasks(loop, deadline, finishing=finishing)
        loop.close()
        self._settle_left(loop)

    def _settle_left(self, loop: asyncio.AbstractEventLoop) -> None:
        for request in list(self._awaiting.pop(loop, ())):
            task = request.task
            if task is not None and task.done():
                _finish(request, task)
            else:
                _settle_request(request, error=_stopped(loop, request.name))

    def _awake(self, loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        with self._guard:
            if self._paused is not loop:
                _resting.discard(loop)
        ready.set()

    def _served(self, loop: asyncio.AbstractEventLoop) -> None:
        with self._guard:
            if self._paused is loop:
                self._clear_paused()

    def _clear_paused(self) -> None:
        _resting.discard(self._paused)
        self._paused, self._keep_serving = None, False


@contextmanager
def spared_until_close() -> Iterator[None]:
    token = _sparing.set(True)
    try:
        yield
    finally:
        _sparing.reset(token)


@dataclass
class _Finishing:
    asked: set[asyncio.Task] = field(default_factory=set)
    insisted: set[asyncio.Task] = field(default_factory=set)


def _finish_tasks(
    loop: asyncio.AbstractEventLoop,
    deadline: float,
    *,
    spare: bool = False,
    finishing: _Finishing | None = None,
) -> None:
    state = _Finishing() if finishing is None else finishing
    asked = state.asked
    loop.run_until_complete(asyncio.sleep(0))
    while pending := {
        task
        for task in asyncio.all_tasks(loop)
        if not (spare and task.get_context().get(_sparing, False))
    }:
        left = deadline - time.monotonic()
        fresh = [
            task
            for task in pending
            if task not in asked and (left <= 0 or not _is_kept(task))
        ]
        stubborn = pending - state.insisted
        if not fresh and left <= 0 and stubborn:
            state.insisted |= stubborn
            for task in stubborn:
                task.cancel()
            loop.run_until_complete(asyncio.wait(stubborn, timeout=INSIST_SECONDS))
            continue
        if not fresh and left <= 0:
            logger.warning(
                "%d task(s) ignored cancellation; closing the loop without them: %s",
                len(pending),
                ", ".join(sorted(task.get_name() for task in pending)),
            )
            return
        for task in fresh:
            _cancel_once(task)
            asked.add(task)
        for task in pending & asked:
            if not task.done() and not task.cancelling():
                task.cancel()
        loop.run_until_complete(
            asyncio.wait(pending, timeout=min(max(left, 0), RECHECK_SECONDS))
        )
        loop.run_until_complete(asyncio.sleep(0))
    loop.run_until_complete(asyncio.sleep(0))


def _is_kept(task: asyncio.Task) -> bool:
    context = task.get_context()
    if not (task in _kept or context.get(_keeping, False)):
        return False
    deadline = context.get(current_deadline)
    return deadline is None or deadline.left() > 0


def _gone(loop: asyncio.AbstractEventLoop) -> bool:
    return not loop.is_running() and loop not in _resting


def _stopped(loop: asyncio.AbstractEventLoop | None, name: str) -> Exception:
    if loop is not None and loop in _retired:
        return AppClosedError(CLOSED_MESSAGE)
    return EventLoopStoppedError(STOPPED_LOOP_MESSAGE.format(name=name))


def _discard(coroutine: Coroutine[Any, Any, Any]) -> None:
    if inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED:
        with suppress(RuntimeError, ValueError):
            coroutine.close()


def _deliver(outcome: concurrent.futures.Future, task: asyncio.Task) -> None:
    if task.cancelled():
        outcome.cancel()
        return
    error = task.exception()
    if error is None:
        _settle(outcome, result=task.result())
    else:
        _settle(outcome, error=error)


def _settle(
    outcome: concurrent.futures.Future,
    *,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    with suppress(concurrent.futures.InvalidStateError):
        if error is None:
            outcome.set_result(result)
        else:
            outcome.set_exception(error)


def _abandon(
    loop: asyncio.AbstractEventLoop,
    task: asyncio.Task,
    outcome: concurrent.futures.Future,
) -> None:
    if outcome.cancelled():
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(_cancel_once, task)


def _stop(loop: asyncio.AbstractEventLoop) -> None:
    _retired.add(loop)
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(loop.stop)


def _loop_thread(
    serve: Any, loop: asyncio.AbstractEventLoop, ready: threading.Event
) -> threading.Thread:
    return threading.Thread(
        target=serve, args=(loop, ready), name="tinyjobs-loop", daemon=True
    )


def _loop_executor() -> CountingPool:
    return CountingPool(thread_name_prefix=f"{LOOP_THREAD_PREFIX}-pool")


def _owned(thread: threading.Thread) -> bool:
    return thread.name.startswith(OWNED_THREAD_PREFIXES)


def _loop_threads_alive() -> bool:
    threads = [*threading._active.values(), *threading._limbo.values()]
    return any(thread.name.startswith(LOOP_THREAD_PREFIX) for thread in threads)


def _pause_wanted(ident: int) -> bool:
    threads = list(threading._active.items())
    current = threading._active.get(ident)
    if current is not None and _owned(current):
        return False
    others = [thread for key, thread in threads if key != ident]
    others.extend(threading._limbo.values())
    if not others or not all(map(_owned, others)):
        return False
    os_threads = _os_thread_count()
    return os_threads is None or os_threads <= len(threads)


def _pause(ident: int, pid: int, acked: Any, resume: Any) -> bool:
    global _pausing
    try:
        if pid != os.getpid():
            return False
        with _control_lock:
            if _after_fork_actions.get(ident) is not resume:
                return False
            _pausing = True
        for runner in list(_runners):
            with suppress(Exception):
                if runner.pause():
                    _paused_for_fork.append(runner)
        if not _loop_threads_alive():
            for owner in list(_fork_pausers):
                with suppress(Exception):
                    owner.pause_for_fork()
        with _control_lock:
            if _after_fork_actions.get(ident) is resume:
                return True
            _pausing = False
            _resume_paused()
        return False
    finally:
        acked.release()


def _resume_paused() -> None:
    while _paused_for_fork:
        with suppress(Exception):
            _paused_for_fork.pop().resume()


def _wait_for_pause(acked: Any) -> None:
    if not acked.acquire(timeout=FORK_ACK_SECONDS):
        return
    acked.release()
    _wait_until_done(_control_thread)
    if _os_thread_count() is not None:
        _wait_for_os_threads(len(threading._active))


def _wait_until_done(thread: threading.Thread | None) -> None:
    deadline = time.monotonic() + THREAD_EXIT_SECONDS
    while not _finished(thread) and time.monotonic() < deadline:
        time.sleep(THREAD_EXIT_POLL_SECONDS)


def _finished(thread: threading.Thread | None) -> bool:
    if thread is None:
        return True
    for name in THREAD_HANDLES:
        handle = getattr(thread, name, None)
        if handle is not None:
            return handle.is_done()
    if hasattr(thread, "_tstate_lock"):
        lock = thread._tstate_lock
        return lock is None or not lock.locked()
    return not thread.is_alive()


def _os_thread_count() -> int | None:
    try:
        return len(os.listdir(OS_THREADS))
    except OSError:
        return None


def _wait_for_os_threads(expected: int) -> None:
    deadline = time.monotonic() + THREAD_EXIT_SECONDS
    while time.monotonic() < deadline:
        count = _os_thread_count()
        if count is None or count <= expected:
            return
        time.sleep(THREAD_EXIT_POLL_SECONDS)


def _before_fork() -> None:
    ident = _thread.get_ident()
    pid = os.getpid()
    _fork_marks[ident] = pid
    _kept_at_fork[ident] = [ref for ref in _tracked.copy() if _owned_lock(ref)]
    failures: list[BaseException] = []
    pausing = _pause_wanted(ident)
    if pausing:
        acked = _thread.allocate_lock()
        acked.acquire()
        resume = partial(_RESUME)
        _after_fork_actions[ident] = resume
        _inbox.put((_pause, ident, pid, acked, resume))
        _ensure_control()
        _through(failures, _wait_for_pause, acked)
    for gate in _fork_gates:
        _through(failures, gate.wait_quiet, ident, pid)
    if pausing:
        _through(failures, _pause_idle_owners)
    if failures:
        raise failures[0]


def _pause_idle_owners() -> None:
    if _loop_threads_alive():
        return
    for owner in list(_fork_pausers):
        with suppress(Exception):
            owner.step_aside_for_fork()
    if _os_thread_count() is not None:
        _wait_for_os_threads(len(threading._active))


def _through(failures: list[BaseException], wait: Callable[..., None], *args: Any) -> None:
    for _ in range(FORK_WAIT_TRIES):
        if _waited(failures, wait, args):
            return


def _waited(failures: list[BaseException], wait: Callable[..., None], args: Any) -> bool:
    try:
        wait(*args)
    except BaseException as error:
        failures[:] = failures or [error]
        return False
    return True


def _settle_here(pid: int) -> None:
    if _settled_pid == pid:
        return
    me = _thread.get_ident()
    if _settling.setdefault(pid, me) != me:
        while _settled_pid != pid:
            time.sleep(THREAD_EXIT_POLL_SECONDS)
        return
    _settle_child(pid)


def _settle_child(pid: int) -> None:
    global _control_lock, _control_thread, _control_ident, _pausing, _settled_pid
    _control_lock = threading.Lock()
    _control_thread, _control_ident, _pausing = None, None, False
    _paused_for_fork.clear()
    _resting.clear()
    _closing.clear()
    kept = _kept_at_fork.pop(threading.main_thread().ident, [])
    _kept_at_fork.clear()
    for ref in _tracked.copy():
        if ref not in kept:
            _free_if_inherited(ref)
    for runner in list(_runners):
        runner.forget_inherited_loop()
    for step in _child_steps:
        with suppress(Exception):
            step()
    _settled_pid = pid


def _owned_lock(ref: weakref.ref) -> bool:
    lock = ref()
    return lock is not None and lock._is_owned()


def _free_if_inherited(ref: weakref.ref) -> None:
    lock = ref()
    if lock is not None:
        lock._at_fork_reinit()


def _at_exit() -> None:
    _exiting.set()
    for spawns in list(_watched.values()):
        spawns.clear()
    _watched.clear()
    for thread in list(_closing):
        _join_closing(thread)
    for runner in list(_runners):
        with suppress(Exception):
            runner.close_here()
    for step in _exit_steps:
        with suppress(Exception):
            step()


def _join_closing(thread: threading.Thread) -> None:
    deadline = time.monotonic() + STOP_SECONDS
    while not thread.is_alive() and thread in _closing and time.monotonic() < deadline:
        time.sleep(THREAD_EXIT_POLL_SECONDS)
    with suppress(RuntimeError):
        thread.join(max(deadline - time.monotonic(), 0))


_RESUME = partial(_thread.start_new_thread, _control_bootstrap, (True,))
_POP_MARK = partial(
    next, map(_fork_marks.pop, iter(_thread.get_ident, None), itertools.repeat(None))
)
_RUN_ACTION = partial(
    next,
    map(
        operator.call,
        map(_after_fork_actions.pop, iter(_thread.get_ident, None), itertools.repeat(int)),
    ),
)
_POP_KEPT = partial(
    next, map(_kept_at_fork.pop, iter(_thread.get_ident, None), itertools.repeat(None))
)
_NEXT_GENERATION = partial(
    next, map(partial(setattr, fork_state, "generation"), itertools.count(1))
)
_DROP_ACTION = partial(
    next,
    map(_after_fork_actions.pop, iter(_thread.get_ident, None), itertools.repeat(None)),
)

atexit.register(_at_exit)

if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_before_fork, after_in_parent=_RUN_ACTION, after_in_child=_DROP_ACTION
    )
    os.register_at_fork(after_in_parent=_POP_MARK, after_in_child=_POP_MARK)
    os.register_at_fork(after_in_parent=_POP_KEPT, after_in_child=_NEXT_GENERATION)
