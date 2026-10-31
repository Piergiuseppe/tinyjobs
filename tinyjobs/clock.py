from __future__ import annotations

import time
from datetime import datetime
from functools import partial
from typing import Callable

from .exceptions import ConfigurationError

FOLLOW_SECONDS = 30.0
JUMP_SECONDS = 0.5
SLOWEST_SAMPLE_SECONDS = 0.5
RETRY_SECONDS = 1.0
AGREEMENT_SECONDS = 1.0


def _elapsed_source() -> Callable[[], float]:
    for name in ("CLOCK_BOOTTIME", "CLOCK_MONOTONIC"):
        identifier = getattr(time, name, None)
        if identifier is None:
            continue
        try:
            time.clock_gettime(identifier)
        except OSError:
            continue
        return partial(time.clock_gettime, identifier)
    return time.monotonic


elapsed = _elapsed_source()


def epoch(instant: float | datetime) -> float:
    if isinstance(instant, datetime):
        return instant.timestamp()
    return float(instant)


def confirms(
    sample: tuple[float, float, float, float], reference: float, middle: float, spread: float
) -> bool:
    expected = sample[0] + (middle - sample[1])
    return abs(reference - expected) <= (spread + SLOWEST_SAMPLE_SECONDS) / 2


def from_wall(clock: object, instant: float) -> float:
    translate = getattr(clock, "from_wall", None)
    return instant if translate is None else translate(instant)


class Clock:
    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    def from_wall(self, instant: float) -> float:
        return instant


class FrozenClock(Clock):
    def __init__(self, at: float) -> None:
        self._at = at

    def now(self) -> float:
        return self._at

    def monotonic(self) -> float:
        return self._at

    def advance(self, seconds: float) -> None:
        self._at += seconds


class FollowingClock(Clock):
    def __init__(self) -> None:
        self._sample: tuple[float, float, float, float] | None = None
        self._stale = False
        self._jumped = False
        self._source: str | None = None
        self._next_try = 0.0
        self._checked = 0.0
        self._forgotten = float("-inf")

    def now(self) -> float:
        sample = self._sample
        if sample is None:
            return time.time()
        reference, at, wall, _ = sample
        passed = elapsed() - at
        if abs(time.time() - wall - passed) >= JUMP_SECONDS:
            self._stale = self._jumped = True
        return reference + passed

    def from_wall(self, instant: float) -> float:
        sample = self._sample
        if sample is None:
            return instant
        reference, at, wall, _ = sample
        if abs(time.time() - wall - (elapsed() - at)) < JUMP_SECONDS:
            return instant + (reference - wall)
        return instant + (self.now() - time.time())

    @property
    def following(self) -> bool:
        return self._sample is not None

    @property
    def trusted(self) -> bool:
        sample = self._sample
        if sample is None or sample[3] <= self._forgotten:
            return False
        self.now()
        return not self._jumped

    def overdue(self) -> bool:
        return self._sample is None or elapsed() - self._checked >= 2 * FOLLOW_SECONDS

    def due(self) -> bool:
        sample = self._sample
        if sample is None:
            return True
        if elapsed() < self._next_try:
            return False
        self.now()
        return self._stale or elapsed() - self._checked >= FOLLOW_SECONDS

    def forget(self) -> None:
        self._forgotten = elapsed()
        self._next_try = float("-inf")
        self._stale = True

    def follow(
        self, reference: float, started: float, finished: float, source: str
    ) -> None:
        if self._source is None:
            self._source = source
        elif source != self._source:
            self._compare(source, reference, (started + finished) / 2, finished - started)
        current = self._sample
        if started <= self._forgotten or (current is not None and started < current[3]):
            return
        middle, spread = (started + finished) / 2, finished - started
        slow = spread > SLOWEST_SAMPLE_SECONDS
        if slow:
            self._next_try = finished + RETRY_SECONDS
        if slow and current is not None and confirms(current, reference, middle, spread):
            kept, at, _, began = current
            self._sample = (kept, at, time.time() - (elapsed() - at), max(began, started))
            self._checked = middle
            self._stale = self._jumped = False
            return
        self._sample = (reference, middle, time.time() - (elapsed() - middle), started)
        self._checked = middle
        self._stale = slow
        self._jumped = False

    def _compare(
        self, source: str, reference: float, middle: float, spread: float
    ) -> None:
        sample = self._sample
        if sample is None:
            return
        expected = sample[0] + (middle - sample[1])
        if abs(reference - expected) > AGREEMENT_SECONDS + spread / 2:
            raise ConfigurationError(
                f"this clock follows {self._source}, whose time is "
                f"{expected - reference:+.1f}s from {source}; give each app its own clock"
            )
