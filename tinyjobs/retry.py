from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable

from .exceptions import Abandon, Retry

BackoffStrategy = str | Callable[[int], float]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    backoff: BackoffStrategy = "exponential"
    base: float = 1.0
    maximum: float = 600.0
    jitter: bool = True

    def should_retry(
        self, attempts: int, max_attempts: int, error: BaseException
    ) -> bool:
        if isinstance(error, Abandon):
            return False
        if attempts >= max_attempts:
            return False
        if isinstance(error, Retry):
            return True
        return isinstance(error, self.retry_on)

    def delay_for(self, attempt: int, error: BaseException | None = None) -> float:
        if isinstance(error, Retry) and error.in_ is not None:
            return max(error.in_, 0.0)
        return self._apply_jitter(self._raw_delay(attempt))

    def _raw_delay(self, attempt: int) -> float:
        if callable(self.backoff):
            return min(float(self.backoff(attempt)), self.maximum)
        if self.backoff == "fixed":
            return min(self.base, self.maximum)
        return min(self.base * 2 ** max(attempt - 1, 0), self.maximum)

    def _apply_jitter(self, delay: float) -> float:
        if not self.jitter:
            return delay
        return delay * random.uniform(0.5, 1.0)


DEFAULT_RETRY_POLICY = RetryPolicy()
