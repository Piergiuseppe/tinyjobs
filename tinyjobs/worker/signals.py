from __future__ import annotations

import asyncio
import ctypes
import logging
import os
import signal
import threading
import weakref
from functools import cache
from typing import Any, Callable

STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)

SIGACTION_BYTES = 512

logger = logging.getLogger("tinyjobs.signals")


class _Stack:
    __slots__ = ("installed", "previous", "trampoline", "watchers")

    def __init__(self) -> None:
        self.installed: Any = None
        self.previous: tuple[Any, bytes | None] = (None, None)
        self.trampoline: bytes | None = None
        self.watchers: list[ShutdownWatcher] = []

    def mark(self, number: int) -> None:
        self.installed = signal.getsignal(number)
        self.trampoline = _raw_action(number)

    def displaced(self, number: int) -> bool:
        return signal.getsignal(number) != self.installed or _pointer(
            _raw_action(number)
        ) != _pointer(self.trampoline)

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

    def take(self, number: int) -> None:
        handle = self.loop._signal_handlers.get(number)
        self.handle = (
            None if handle is not None and handle._callback == self.dispatch else handle
        )
        self.previous = _snapshot(number)
        self.loop.add_signal_handler(number, self.dispatch)
        self.mark(number)

    def owns(self, number: int) -> bool:
        handle = self.loop._signal_handlers.get(number)
        return handle is not None and handle._callback == self.dispatch

    def leave(self, number: int) -> None:
        stacks = _loop_stacks.get(self.loop, {})
        if stacks.get(number) is not self:
            return
        stacks.pop(number)
        if not self.owns(number):
            return
        if self.displaced(number):
            replaced = _snapshot(number)
            self.loop.remove_signal_handler(number)
            self.restore(number, replaced)
            return
        self.loop.remove_signal_handler(number)
        if self.handle is not None:
            self.loop.add_signal_handler(
                number, self.handle._callback, *self.handle._args
            )
            return
        self.restore(number, self.previous)


class _SignalStack(_Stack):
    __slots__ = ("dispatch",)

    def __init__(self) -> None:
        super().__init__()
        self.dispatch = self._dispatch

    def _dispatch(self, number: int, frame: Any) -> None:
        for watcher in list(self.watchers):
            watcher._schedule()

    def take(self, number: int) -> None:
        self.previous = _snapshot(number)
        signal.signal(number, self.dispatch)
        self.mark(number)

    def leave(self, number: int) -> None:
        if _signal_stacks.get(number) is not self:
            return
        _signal_stacks.pop(number)
        if not self.displaced(number):
            self.restore(number, self.previous)


_loop_stacks: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[int, _LoopStack]]" = weakref.WeakKeyDictionary()
_signal_stacks: dict[int, _SignalStack] = {}


class ShutdownWatcher:
    def __init__(self, on_stop: Callable[[], None], on_force: Callable[[], None]):
        self._on_stop = on_stop
        self._on_force = on_force
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
            stack.watchers.append(self)
            self._installed.append((number, stack))

    def uninstall(self) -> None:
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

    def _schedule(self) -> None:
        if self._loop is None:
            return
        try:
            self._loop.call_soon_threadsafe(self._handle)
        except RuntimeError:
            return

    def _handle(self) -> None:
        if self._triggered:
            self._on_force()
            return
        self._triggered = True
        self._on_stop()


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
