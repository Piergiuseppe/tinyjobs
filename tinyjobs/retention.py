from __future__ import annotations

from dataclasses import dataclass

from .duration import format_duration
from .status import JobStatus

DAY = 86400.0


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    success: float = 7 * DAY
    cancelled: float = 7 * DAY
    failed: float = 30 * DAY
    interval: float = 3600.0
    batch: int = 500
    maximum_per_sweep: int = 50_000

    @property
    def enabled(self) -> bool:
        return any(keep > 0 for keep in (self.success, self.cancelled, self.failed))

    @property
    def check_interval(self) -> float:
        return max(min(self.interval / 4.0, 300.0), 1.0)

    def horizons(self, now: float) -> dict[JobStatus, float]:
        keeps = (
            (JobStatus.SUCCESS, self.success),
            (JobStatus.CANCELLED, self.cancelled),
            (JobStatus.FAILED, self.failed),
        )
        return {status: now - keep for status, keep in keeps if keep > 0}

    def describe(self) -> str:
        if not self.enabled:
            return "retention disabled, nothing is pruned"
        parts = [
            f"{name} {format_duration(keep)}"
            for name, keep in (
                ("success", self.success),
                ("cancelled", self.cancelled),
                ("failed", self.failed),
            )
            if keep > 0
        ]
        return (
            f"keeping {', '.join(parts)}, "
            f"sweeping every {format_duration(self.interval)}"
        )


KEEP_EVERYTHING = RetentionPolicy(success=0, cancelled=0, failed=0)
