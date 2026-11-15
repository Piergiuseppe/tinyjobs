from __future__ import annotations

import _thread
import asyncio
import contextvars
import ctypes
import logging
import os
import selectors
import signal
import socket
import sys
import threading
import time
import weakref
from contextlib import contextmanager, suppress
from functools import cache, partial
from typing import Any, Callable, Iterable, Iterator

from ..exceptions import ForcedInterrupt
from ..exiting import STOP_SIGNALS
from ..exiting import exit_cleanly as _exit_cleanly


SIGACTION_BYTES = 512

ANSWER_SECONDS = 0.2

UNANSWERED_EXIT_SECONDS = 1.0

UNANSWERED_EXIT_STATUS = 130

POKE_SECONDS = 0.05

NUDGE_GRACE_SECONDS = 0.05

NUDGE_IDLE_SECONDS = 1.0

NUDGE_PEEK_BYTES = 65536

NUDGE_SIGNAL = getattr(signal, "SIGURG", None)

NUDGING = NUDGE_SIGNAL is not None and hasattr(signal, "pthread_kill")

NUDGE_THREAD = "tinyjobs-nudge"

logger = logging.getLogger("tinyjobs.signals")

_owning = False


class _Presses:
    __slots__ = (
        "unanswered", "escalating", "escalated_at", "closed", "poked", "_timers", "_lock"
    )

    ends_process = False

    def __init__(self) -> None:
        self.unanswered = 0
        self.escalating = False
        self.escalated_at = 0.0
        self.closed = False
        self.poked = False
        self._timers: set[threading.Timer] = set()
        self._lock = threading.RLock()

    def pressed(self, forcing: bool) -> None:
        self.poked = False
        if self.closed:
            return
        self.unanswered += 1
        if self.unanswered > 1 or self.escalating:
            self.unanswered = 0
            if not self.escalating:
                self.escalating = True
                self.escalated_at = time.monotonic()
            self.marked(True)
            self.escalated()
            self._watch()
            raise ForcedInterrupt
        if forcing:
            self.marked(False)
            self._watch()

    def answered(self) -> None:
        self.unanswered = 0
        self.escalating = False

    def escalated(self) -> None:
        pass

    def marked(self, every: bool) -> None:
        pass

    def running(self) -> bool:
        return True

    def close(self) -> None:
        self.unanswered = 0
        self.escalating = False
        with self._lock:
            self.closed = True
            for timer in self._timers:
                timer.cancel()
            self._timers.clear()
        end = time.monotonic() + POKE_SECONDS
        while self.poked and time.monotonic() < end:
            time.sleep(0.001)
        self.poked = False

    def _watch(self) -> None:
        with self._lock:
            if self.closed:
                return
            number = self.poked_with()
            timer = threading.Timer(ANSWER_SECONDS, partial(contextvars.Context().run, self._unanswered))
            timer.args = (timer, number, None if number is None else signal.getsignal(number))
            timer.daemon = True
            self._timers.add(timer)
        timer.start()

    def poked_with(self) -> int | None:
        return signal.SIGINT

    def _unanswered(self, timer: threading.Timer, number: int | None, handler: Any) -> None:
        with self._lock:
            self._timers.discard(timer)
            if self.closed or not (self.unanswered or self.escalating):
                return
            if number is not None and signal.getsignal(number) is not handler:
                return
            if not self.running():
                rearm = True
            else:
                rearm = False
                if (
                    self.escalating
                    and self.ends_process
                    and time.monotonic() - self.escalated_at >= UNANSWERED_EXIT_SECONDS
                ):
                    _end_process()
                if number is not None:
                    self.poked = True
                    if hasattr(signal, "pthread_kill"):
                        signal.pthread_kill(threading.main_thread().ident, number)
                    else:
                        _thread.interrupt_main(number)
        if rearm:
            self._watch()


class _Nudges:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._watched: dict[asyncio.AbstractEventLoop, tuple[set[Any], threading.Event]] = {}
        self._previous: tuple[Any, bytes | None] | None = None
        self._installed: bytes | None = None
        self._woken: tuple[asyncio.AbstractEventLoop, int] | None = None

    def watch(self, loop: asyncio.AbstractEventLoop, owner: Any, waking: bool = False) -> None:
        reader = getattr(loop, "_ssock", None)
        if not NUDGING or reader is None:
            return
        if threading.current_thread() is not threading.main_thread():
            return
        with self._lock:
            for closed in [known for known in self._watched if known.is_closed()]:
                self.forget(closed)
            watched = self._watched.get(loop)
            if watched is None:
                if not self._take():
                    return
                watched = (set(), threading.Event())
                try:
                    threading.Thread(
                        target=_nudge,
                        args=(reader, threading.main_thread().ident, watched[1]),
                        name=NUDGE_THREAD,
                        daemon=True,
                    ).start()
                except RuntimeError:
                    if not self._watched:
                        self._give_back()
                    return
                self._watched[loop] = watched
            watched[0].add(owner)
            if waking and self._woken is None:
                self._wake(loop)

    def unwatch(self, loop: asyncio.AbstractEventLoop, owner: Any) -> None:
        with self._lock:
            watched = self._watched.get(loop)
            if watched is None:
                return
            watched[0].discard(owner)
            if not watched[0]:
                self.forget(loop)

    def forget(self, loop: asyncio.AbstractEventLoop) -> None:
        with self._lock:
            watched = self._watched.pop(loop, None)
            if watched is not None:
                watched[1].set()
            if self._woken is not None and self._woken[0] is loop:
                self._sleep()
            if not self._watched:
                self._give_back()

    def forked(self) -> None:
        self._lock = threading.RLock()
        self._watched = {}
        self._sleep()
        self._give_back()

    def woken_again(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._woken is not None and self._woken[0] is loop:
            signal.set_wakeup_fd(self._woken[1], warn_on_full_buffer=False)

    def _wake(self, loop: asyncio.AbstractEventLoop) -> None:
        writer = getattr(loop, "_csock", None)
        if writer is None:
            return
        before = signal.set_wakeup_fd(writer.fileno(), warn_on_full_buffer=False)
        if before == -1:
            self._woken = loop, writer.fileno()
        else:
            _wake_as_before(before)

    def _sleep(self) -> None:
        if self._woken is None or threading.current_thread() is not threading.main_thread():
            return
        (_, woken), self._woken = self._woken, None
        before = signal.set_wakeup_fd(-1)
        if before not in (-1, woken):
            _wake_as_before(before)

    def _take(self) -> bool:
        if self._previous is not None:
            return True
        handler, raw = _snapshot(NUDGE_SIGNAL)
        if handler not in (signal.SIG_DFL, signal.SIG_IGN) or not _unhandled(raw):
            return False
        self._previous = handler, raw
        signal.signal(NUDGE_SIGNAL, _nudged)
        self._installed = _pointer(_raw_action(NUDGE_SIGNAL))
        return True

    def _give_back(self) -> None:
        if self._previous is None or threading.current_thread() is not threading.main_thread():
            return
        (handler, raw), self._previous = self._previous, None
        ours = _pointer(_raw_action(NUDGE_SIGNAL)) == self._installed
        if ours and signal.getsignal(NUDGE_SIGNAL) is _nudged:
            signal.signal(NUDGE_SIGNAL, handler)
            if raw is not None:
                _set_raw_action(NUDGE_SIGNAL, raw)


def _wake_as_before(before: int) -> None:
    with suppress(OSError, ValueError):
        signal.set_wakeup_fd(before)


def _nudge(reader: socket.socket, main: int, ended: threading.Event) -> None:
    with suppress(OSError, ValueError), selectors.DefaultSelector() as waiting:
        waiting.register(reader, selectors.EVENT_READ)
        _nudge_while(waiting, reader, main, ended)


def _nudge_while(
    waiting: selectors.BaseSelector, reader: socket.socket, main: int, ended: threading.Event
) -> None:
    stops = bytes(STOP_SIGNALS)
    seen = sent = 0
    while True:
        readable = waiting.select(NUDGE_IDLE_SECONDS)
        if ended.is_set() or reader.fileno() < 0:
            return
        if readable and ended.wait(NUDGE_GRACE_SECONDS):
            return
        try:
            unread = reader.recv(NUDGE_PEEK_BYTES, socket.MSG_PEEK) if readable else b""
        except BlockingIOError:
            unread = b""
        unhandled = sum(unread.count(stop) for stop in stops)
        if unhandled and unhandled == seen != sent and signal.getsignal(NUDGE_SIGNAL) is _nudged:
            signal.pthread_kill(main, NUDGE_SIGNAL)
            sent = unhandled
        if unhandled != seen:
            sent = 0
        seen = unhandled


def _nudged(number: int, frame: Any) -> None:
    return None


def _unhandled(raw: bytes | None) -> bool:
    return raw is None or int.from_bytes(_pointer(raw) or b"", sys.byteorder) in (0, 1)


_nudges = _Nudges()

if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=lambda: _nudges.forked())


def loop_ended(loop: asyncio.AbstractEventLoop) -> None:
    _nudges.forget(loop)


class RunInterrupts(_Presses):
    __slots__ = (
        "count",
        "ends_process",
        "_force",
        "_forcing",
        "_handled",
        "_loop",
        "_stop",
        "_stopped",
    )

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        stop: Callable[[], None],
        force: Callable[[], None],
        forcing: Callable[[], None],
        stopped: Callable[[], bool] = lambda: False,
        ends_process: bool = False,
    ) -> None:
        super().__init__()
        self.count = 0
        self.ends_process = ends_process
        self._stopped = stopped
        self._handled: dict[int, tuple[Any, bytes | None]] = {}
        self._loop = loop
        self._stop = stop
        self._force = force
        self._forcing = forcing
        if threading.current_thread() is not threading.main_thread():
            return
        defaults = {signal.SIGINT: signal.default_int_handler}
        if ends_process:
            defaults[signal.SIGTERM] = signal.SIG_DFL
        for number, default in defaults.items():
            if signal.getsignal(number) is default:
                self._handled[number] = _snapshot(number)
                signal.signal(number, self._interrupted)
        if self._handled:
            _nudges.watch(loop, self, waking=True)

    def _interrupted(self, number: int, frame: Any) -> None:
        self.count += 1
        forcing = self.count > 1 or self._stopped()
        self.pressed(forcing)
        try:
            self._loop.call_soon_threadsafe(self._answer_force if forcing else self._answer)
        except RuntimeError:
            pass

    def _answer(self) -> None:
        self.answered()
        self._stop()

    def _answer_force(self) -> None:
        self.answered()
        self._force()

    def escalated(self) -> None:
        try:
            self._loop.call_soon_threadsafe(self._answer_escalation)
        except RuntimeError:
            pass

    def marked(self, every: bool) -> None:
        self._forcing()

    def poked_with(self) -> int | None:
        for number in self._handled:
            if signal.getsignal(number) == self._interrupted:
                return number
        return None

    def _answer_escalation(self) -> None:
        self.answered()
        self._stop()
        self._force()

    def running(self) -> bool:
        return self._loop.is_running()

    def restore(self, exiting: bool = False) -> None:
        self.close()
        _nudges.unwatch(self._loop, self)
        for number, (handler, raw) in self._handled.items():
            if signal.getsignal(number) != self._interrupted:
                continue
            if exiting and _owning:
                signal.signal(number, _exit_cleanly)
                continue
            signal.signal(number, handler)
            if raw is not None:
                _set_raw_action(number, raw)
        self._handled = {}


class _Stack(_Presses):
    __slots__ = ("installed", "number", "previous", "trampoline", "watchers")

    ends_process = True

    def __init__(self) -> None:
        super().__init__()
        self.installed: Any = None
        self.number: int | None = None
        self.previous: tuple[Any, bytes | None] = (None, None)
        self.trampoline: bytes | None = None
        self.watchers: list[ShutdownWatcher] = []

    def mark(self, number: int) -> None:
        self.number = number
        self.installed = signal.getsignal(number)
        self.trampoline = _raw_action(number)

    def displaced(self, number: int) -> bool:
        return signal.getsignal(number) != self.installed or _pointer(
            _raw_action(number)
        ) != _pointer(self.trampoline)

    def press(self, siblings: Iterable[_Stack]) -> None:
        for sibling in siblings:
            if sibling is not self and sibling.unanswered:
                self.unanswered += sibling.unanswered
                sibling.unanswered = 0
        self.pressed(any(watcher._triggered for watcher in self.watchers))

    def poked_with(self) -> int | None:
        return self.number

    def escalated(self) -> None:
        for watcher in list(self.watchers):
            watcher._schedule(watcher._escalate)

    def marked(self, every: bool) -> None:
        for watcher in list(self.watchers):
            if every or watcher._triggered:
                watcher._on_forcing()

    def running(self) -> bool:
        return any(
            watcher._loop is not None and watcher._loop.is_running()
            for watcher in self.watchers
        )

    def restore(self, number: int, snapshot: tuple[Any, bytes | None]) -> None:
        handler, raw = snapshot
        if handler is not None and handler != self.installed:
            signal.signal(number, handler)
        if raw is not None and _pointer(raw) != _pointer(self.trampoline):
            _set_raw_action(number, raw)


class _LoopStack(_Stack):
    __slots__ = ("dispatch", "handle", "loop")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.dispatch = self._dispatch
        self.handle: Any = None
        self.loop = loop

    def _dispatch(self) -> None:
        for watcher in list(self.watchers):
            watcher._handle()

    def _escalate(self, number: int, frame: Any) -> None:
        self.press(_loop_stacks.get(self.loop, {}).values())

    def take(self, number: int) -> None:
        handle = self.loop._signal_handlers.get(number)
        self.handle = (
            None if handle is not None and handle._callback == self.dispatch else handle
        )
        self.previous = _snapshot(number)
        self.loop.add_signal_handler(number, self.dispatch)
        _nudges.watch(self.loop, self)
        signal.signal(number, self._escalate)
        self.mark(number)

    def owns(self, number: int) -> bool:
        handle = self.loop._signal_handlers.get(number)
        return handle is not None and handle._callback == self.dispatch

    def leave(self, number: int) -> None:
        self.close()
        _nudges.unwatch(self.loop, self)
        stacks = _loop_stacks.get(self.loop, {})
        if stacks.get(number) is not self:
            return
        stacks.pop(number)
        if not self.owns(number):
            return
        displaced = self.displaced(number)
        if self.handle is not None and not displaced:
            self.loop.add_signal_handler(
                number, self.handle._callback, *self.handle._args
            )
            return
        if not displaced and self.previous[0] in (None, self.installed):
            signal.signal(
                number,
                signal.default_int_handler if number == signal.SIGINT else signal.SIG_DFL,
            )
        self.restore(number, _snapshot(number) if displaced else self.previous)
        self.loop._signal_handlers.pop(number, None)
        if not self.loop._signal_handlers:
            signal.set_wakeup_fd(-1)
            _nudges.woken_again(self.loop)


class _SignalStack(_Stack):
    __slots__ = ("dispatch",)

    def __init__(self) -> None:
        super().__init__()
        self.dispatch = self._dispatch

    def _dispatch(self, number: int, frame: Any) -> None:
        self.press(_signal_stacks.values())
        for watcher in list(self.watchers):
            watcher._schedule()

    def take(self, number: int) -> None:
        self.previous = _snapshot(number)
        signal.signal(number, self.dispatch)
        self.mark(number)

    def leave(self, number: int) -> None:
        self.close()
        if _signal_stacks.get(number) is not self:
            return
        _signal_stacks.pop(number)
        if not self.displaced(number):
            self.restore(number, self.previous)


_loop_stacks: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[int, _LoopStack]]" = weakref.WeakKeyDictionary()
_signal_stacks: dict[int, _SignalStack] = {}


class ShutdownWatcher:
    def __init__(
        self,
        on_stop: Callable[[], None],
        on_force: Callable[[], None],
        on_forcing: Callable[[], None] = lambda: None,
    ):
        self._on_stop = on_stop
        self._on_force = on_force
        self._on_forcing = on_forcing
        self._triggered = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._installed: list[tuple[int, _LoopStack | _SignalStack]] = []

    def install(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            logger.info("not on the main thread, so no signal handlers are installed")
            return
        loop = asyncio.get_running_loop()
        self._loop = loop
        for number in STOP_SIGNALS:
            stack = (
                self._join_loop(loop, number)
                if hasattr(loop, "_signal_handlers")
                else None
            )
            if stack is None:
                stack = self._join_signal(number)
                _nudges.watch(loop, self, waking=True)
            stack.watchers.append(self)
            self._installed.append((number, stack))

    def uninstall(self) -> None:
        if self._loop is not None:
            _nudges.unwatch(self._loop, self)
        while self._installed:
            number, stack = self._installed.pop()
            if self in stack.watchers:
                stack.watchers.remove(self)
            if not stack.watchers:
                stack.leave(number)

    def _join_loop(
        self, loop: asyncio.AbstractEventLoop, number: int
    ) -> _LoopStack | None:
        stacks = _loop_stacks.setdefault(loop, {})
        stack = stacks.get(number)
        if stack is None or not stack.watchers:
            stack = _LoopStack(loop)
        if not stack.watchers or not stack.owns(number) or stack.displaced(number):
            try:
                stack.take(number)
            except NotImplementedError:
                return None
        stacks[number] = stack
        return stack

    def _join_signal(self, number: int) -> _SignalStack:
        stack = _signal_stacks.get(number)
        if stack is None or not stack.watchers:
            stack = _SignalStack()
        if not stack.watchers or stack.displaced(number):
            stack.take(number)
        _signal_stacks[number] = stack
        return stack

    def _schedule(self, callback: Callable[[], None] | None = None) -> None:
        if self._loop is None:
            return
        try:
            self._loop.call_soon_threadsafe(callback or self._handle)
        except RuntimeError:
            return

    def _escalate(self) -> None:
        for _, stack in self._installed:
            stack.answered()
        if not self._triggered:
            self._triggered = True
            self._on_stop()
        self._on_force()

    def _handle(self) -> None:
        for _, stack in self._installed:
            stack.answered()
        if self._triggered:
            self._on_force()
            return
        self._triggered = True
        self._on_stop()


@contextmanager
def owning_process() -> Iterator[None]:
    global _owning
    if threading.current_thread() is threading.main_thread():
        for number in STOP_SIGNALS:
            if signal.getsignal(number) is _exit_cleanly:
                signal.signal(
                    number,
                    signal.default_int_handler if number == signal.SIGINT else signal.SIG_DFL,
                )
    _owning = True
    try:
        yield
    finally:
        _owning = False


def _end_process() -> None:
    os.write(
        2,
        (
            "tinyjobs: the event loop did not answer the forced stop within "
            f"{UNANSWERED_EXIT_SECONDS:g}s; ending the process, and the reaper takes its "
            "jobs back once their leases expire\n"
        ).encode(),
    )
    os._exit(UNANSWERED_EXIT_STATUS)


def _pointer(raw: bytes | None) -> bytes | None:
    return None if raw is None else raw[: ctypes.sizeof(ctypes.c_void_p)]


def _snapshot(number: int) -> tuple[Any, bytes | None]:
    return signal.getsignal(number), _raw_action(number)


@cache
def _libc() -> Any:
    if os.name != "posix":
        return None
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        action = libc.sigaction
    except (OSError, AttributeError):
        return None
    action.restype = ctypes.c_int
    action.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p]
    return libc


def _raw_action(number: int) -> bytes | None:
    libc = _libc()
    if libc is None:
        return None
    buffer = ctypes.create_string_buffer(SIGACTION_BYTES)
    if libc.sigaction(number, None, buffer) != 0:
        return None
    return buffer.raw


def _set_raw_action(number: int, raw: bytes) -> None:
    libc = _libc()
    if libc is not None:
        libc.sigaction(number, ctypes.create_string_buffer(raw, len(raw)), None)
