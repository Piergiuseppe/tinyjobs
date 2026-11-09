from __future__ import annotations

import re
from dataclasses import dataclass

from .exceptions import ConfigurationError

CONCURRENCY_MESSAGE = (
    "concurrency on {name} is the most jobs of the task running at once across every worker, "
    "a whole number of at least one, not {value!r}"
)
RATE_MESSAGE = (
    'rate on {name} is the most starts of the task per period, written like "10/m", "100/h" '
    'or "1/30s", not {value!r}'
)

SECONDS_OF = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}

RATE = re.compile(r"^\s*([0-9]{1,9})\s*/\s*([0-9]{0,9})\s*([a-z]+)\s*$", re.IGNORECASE)
MOST_CONCURRENCY = 2**31 - 1


@dataclass(frozen=True, slots=True)
class Limit:
    concurrency: int | None = None
    interval: float | None = None


def declare_limit(name: str, concurrency: object, rate: object) -> Limit | None:
    if concurrency is None and rate is None:
        return None
    if concurrency is not None and (
        isinstance(concurrency, bool)
        or not isinstance(concurrency, int)
        or not 1 <= concurrency <= MOST_CONCURRENCY
    ):
        raise ConfigurationError(CONCURRENCY_MESSAGE.format(name=name, value=concurrency))
    interval = None if rate is None else interval_of(name, rate)
    return Limit(concurrency=concurrency, interval=interval)


def interval_of(name: str, rate: object) -> float:
    match = RATE.match(rate) if isinstance(rate, str) else None
    if match is None:
        raise ConfigurationError(RATE_MESSAGE.format(name=name, value=rate))
    starts, span, unit = match.groups()
    seconds = SECONDS_OF.get(unit.lower())
    if seconds is None or int(starts) < 1 or (span and int(span) < 1):
        raise ConfigurationError(RATE_MESSAGE.format(name=name, value=rate))
    return (int(span) if span else 1) * seconds / int(starts)
