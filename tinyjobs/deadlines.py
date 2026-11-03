from __future__ import annotations

import asyncio
import contextvars
import math
import time
import weakref
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Iterator

GRACE_SECONDS = 0.5
WATCH_SLACK_SECONDS = 0.001


class Deadline:
    __slots__ = ("at", "outer")

    def __init__(self, at: float = math.inf, outer: Deadline | None = None) -> None:
        self.at = at
        self.outer = outer

    def left(self) -> float:
        at, outer = self.at, self.outer
        while outer is not None:
            at, outer = min(at, outer.at), outer.outer
        return at - time.monotonic()

    def end(self, at: float) -> None:
        self.at = min(self.at, at)


current: contextvars.ContextVar[Deadline | None] = contextvars.ContextVar(
    "tinyjobs_deadline", default=None
)
final: contextvars.ContextVar[Deadline | None] = contextvars.ContextVar(
    "tinyjobs_final_deadline", default=None
)


def end_of_caller() -> Deadline | None:
    return final.get() or current.get()


def final_left() -> float:
    end = final.get()
    return math.inf if end is None else end.left()


def left() -> float:
    deadline = current.get()
    return math.inf if deadline is None else deadline.left()


@contextmanager
def until(at: float) -> Iterator[Deadline]:
    deadline = Deadline(at, current.get())
    token = current.set(deadline)
    try:
        yield deadline
    finally:
        current.reset(token)


@contextmanager
def under(deadline: Deadline | None) -> Iterator[Deadline | None]:
    token = current.set(deadline)
    try:
        yield deadline
    finally:
        current.reset(token)


@contextmanager
def finishing(deadline: Deadline | None) -> Iterator[Deadline | None]:
    token = final.set(deadline)
    try:
        yield deadline
    finally:
        final.reset(token)


class bounded:
    __slots__ = ("_at", "_token", "_task", "_cut", "_cancelling", "_expired", "_watching")

    def __init__(self, at: float) -> None:
        self._at = at

    async def __aenter__(self) -> Deadline:
        return self.__enter__()

    async def __aexit__(self, kind: Any, error: Any, trace: Any) -> bool | None:
        return self.__exit__(kind, error, trace)

    def __enter__(self) -> Deadline:
        outer = current.get()
        deadline = Deadline(self._at, outer)
        self._token = current.set(deadline)
        self._task = None
        self._expired = False
        at = self._at
        while outer is not None:
            if outer.at < at:
                at = outer.at
            outer = outer.outer
        last = math.inf
        hard = final.get()
        while hard is not None:
            if hard.at < last:
                last = hard.at
            hard = hard.outer
        if at == math.inf and last == math.inf:
            return deadline
        now = time.monotonic()
        cut = min(max(0.0, at - now) + GRACE_SECONDS, max(0.0, last - now))
        try:
            task = asyncio.current_task()
        except RuntimeError:
            current.reset(self._token)
            raise
        if task is None:
            current.reset(self._token)
            raise RuntimeError("Timeout should be used inside a task")
        self._task = task
        self._cancelling = task.cancelling()
        self._cut = now + cut
        watching = self._watching = _watch()
        watching.add(self)
        return deadline

    def __exit__(self, kind: Any, error: Any, trace: Any) -> bool | None:
        try:
            task = self._task
            if task is None:
                return None
            if not self._expired:
                self._watching.active.pop(self, None)
                return None
            if (
                task.uncancel() <= self._cancelling
                and kind is not None
                and issubclass(kind, asyncio.CancelledError)
            ):
                raise TimeoutError from error
            return None
        finally:
            current.reset(self._token)

    def _expire(self) -> None:
        self._expired = True
        self._task.cancel()


class _Watch:
    __slots__ = ("active", "handle", "at")

    def __init__(self) -> None:
        self.active: dict[bounded, None] = {}
        self.handle: weakref.ref[asyncio.TimerHandle] | None = None
        self.at = math.inf

    def add(self, bound: bounded) -> None:
        self.active[bound] = None
        if bound._cut < self.at:
            self._arm(bound._cut)

    def _arm(self, at: float) -> None:
        armed = None if self.handle is None else self.handle()
        if armed is not None:
            armed.cancel()
        self.at = at
        self.handle = weakref.ref(
            asyncio.get_running_loop().call_later(max(0.0, at - time.monotonic()), self._fire)
        )

    def _fire(self) -> None:
        self.handle = None
        self.at = math.inf
        now = time.monotonic() + WATCH_SLACK_SECONDS
        due = [bound for bound in self.active if bound._cut <= now]
        for bound in due:
            del self.active[bound]
            bound._expire()
        if self.active:
            self._arm(min(bound._cut for bound in self.active))


_watches: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _Watch]" = (
    weakref.WeakKeyDictionary()
)


_last: tuple[Any, Any] = (None, None)


def forget_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _last
    _watches.pop(loop, None)
    seen, _ = _last
    if seen is not None and seen() is loop:
        _last = (None, None)


def _watch() -> _Watch:
    global _last
    loop = asyncio.get_running_loop()
    seen, watch = _last
    if seen is not None and seen() is loop:
        return watch
    watch = _watches.get(loop)
    if watch is None:
        watch = _watches.setdefault(loop, _Watch())
    _last = (weakref.ref(loop), watch)
    return watch


@asynccontextmanager
async def lastly(seconds: float) -> AsyncIterator[Deadline]:
    hard = final.get()
    deadline = Deadline(time.monotonic() + seconds, current.get() if hard is None else hard)
    cut = max(0.0, deadline.left()) + GRACE_SECONDS
    if hard is not None:
        cut = min(cut, max(0.0, hard.left()))
    with under(deadline):
        async with asyncio.timeout(cut):
            yield deadline
