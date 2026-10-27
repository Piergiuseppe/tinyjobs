from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from .clock import Clock, epoch, from_wall


@dataclass(frozen=True, slots=True)
class EnqueueOptions:
    queue: str | None = None
    priority: int | None = None
    delay: float | None = None
    run_at: float | datetime | None = None
    max_attempts: int | None = None
    key: str | None = None
    codec: str | None = None

    def merged_with(self, **overrides: object) -> "EnqueueOptions":
        provided = {k: v for k, v in overrides.items() if v is not None}
        return replace(self, **provided)

    def scheduled_for(self, clock: Clock, now: float) -> float:
        if self.run_at is not None:
            return from_wall(clock, epoch(self.run_at))
        if self.delay is not None:
            return now + self.delay
        return now
