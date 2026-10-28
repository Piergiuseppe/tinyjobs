from __future__ import annotations

import asyncio
import contextvars
import math
import time
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

GRACE_SECONDS = 0.5


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
def under(deadline: Deadline) -> Iterator[Deadline]:
    token = current.set(deadline)
    try:
        yield deadline
    finally:
        current.reset(token)


@asynccontextmanager
async def bounded(at: float) -> AsyncIterator[Deadline]:
    with until(at) as deadline:
        remaining = deadline.left()
        async with asyncio.timeout(
            None if remaining == math.inf else max(0.0, remaining) + GRACE_SECONDS
        ):
            yield deadline
