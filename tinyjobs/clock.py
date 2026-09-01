from __future__ import annotations

import time


class Clock:
    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()


class FrozenClock(Clock):
    def __init__(self, at: float) -> None:
        self._at = at

    def now(self) -> float:
        return self._at

    def monotonic(self) -> float:
        return self._at

    def advance(self, seconds: float) -> None:
        self._at += seconds
