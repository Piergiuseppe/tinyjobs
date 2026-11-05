from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .duration import format_duration, is_finite_seconds
from .exceptions import ConfigurationError, described
from .status import JobStatus

DAY = 86400.0
SWEEP_SIZE_MESSAGE = "retention {name} must be a whole number of at least 1, got {value}"
SECONDS_MESSAGE = (
    "retention {name} must be a finite number of seconds, zero or more, got {value}"
)
NOT_A_POLICY_MESSAGE = (
    "retention must be a RetentionPolicy, got {type}; build one with "
    "RetentionPolicy(success=..., failed=..., interval=...)"
)



@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    success: float = 7 * DAY
    cancelled: float = 7 * DAY
    failed: float = 30 * DAY
    interval: float = 3600.0
    batch: int = 500
    maximum_per_sweep: int = 50_000

    def __post_init__(self) -> None:
        for name in ("success", "cancelled", "failed", "interval"):
            value = getattr(self, name)
            if not is_finite_seconds(value) or value < 0:
                raise ConfigurationError(
                    SECONDS_MESSAGE.format(name=name, value=described(value))
                )
        for name in ("batch", "maximum_per_sweep"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ConfigurationError(
                    SWEEP_SIZE_MESSAGE.format(name=name, value=described(value))
                )

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


def assert_retention_policy(value: Any) -> None:
    if value is not None and not isinstance(value, RetentionPolicy):
        raise ConfigurationError(NOT_A_POLICY_MESSAGE.format(type=type(value).__name__))
