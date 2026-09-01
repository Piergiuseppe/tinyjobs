from __future__ import annotations

import random


class AdaptiveInterval:
    def __init__(
        self,
        *,
        minimum: float = 0.005,
        maximum: float = 0.5,
        factor: float = 2.0,
        jitter: float = 0.2,
    ) -> None:
        self._minimum = minimum
        self._maximum = maximum
        self._factor = factor
        self._jitter = jitter
        self._current = minimum

    def reset(self) -> None:
        self._current = self._minimum

    def next(self) -> float:
        delay = self._current
        self._current = min(self._current * self._factor, self._maximum)
        spread = delay * self._jitter
        return max(delay + random.uniform(-spread, spread), 0.0)
