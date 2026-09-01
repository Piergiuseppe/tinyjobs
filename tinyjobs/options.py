from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class EnqueueOptions:
    queue: str | None = None
    priority: int | None = None
    delay: float | None = None
    run_at: float | None = None
    max_attempts: int | None = None
    key: str | None = None
    codec: str | None = None

    def merged_with(self, **overrides: object) -> "EnqueueOptions":
        provided = {k: v for k, v in overrides.items() if v is not None}
        return replace(self, **provided)

    def scheduled_for(self, now: float) -> float:
        if self.run_at is not None:
            return self.run_at
        if self.delay is not None:
            return now + self.delay
        return now
