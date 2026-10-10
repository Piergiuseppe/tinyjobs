from __future__ import annotations

from dataclasses import dataclass
from datetime import tzinfo
from enum import StrEnum

from .cron import CronExpression, load_zone
from .exceptions import ConfigurationError

DEDUP_PREFIX = "tinyjobs:schedule:"


class MisfirePolicy(StrEnum):
    SKIP = "skip"
    RUN_ONCE = "run_once"
    RUN_ALL = "run_all"


@dataclass(frozen=True, slots=True)
class Schedule:
    task: str
    cron: CronExpression
    timezone: str
    zone: tzinfo
    misfire: MisfirePolicy
    overlap: bool

    @classmethod
    def declare(
        cls,
        task: str,
        cron: str,
        *,
        timezone: str = "UTC",
        misfire: str = "skip",
        overlap: bool = False,
    ) -> "Schedule":
        try:
            policy = MisfirePolicy(misfire)
        except ValueError:
            known = ", ".join(str(choice) for choice in MisfirePolicy)
            raise ConfigurationError(
                f"misfire={misfire!r} is not a policy; use one of {known}"
            ) from None
        if policy is MisfirePolicy.RUN_ALL and not overlap:
            raise ConfigurationError(
                f"{task}: misfire='run_all' creates several jobs at once, which "
                f"overlap=False would collapse into one; pass overlap=True"
            )
        return cls(
            task=task,
            cron=CronExpression.parse(cron),
            timezone=timezone,
            zone=load_zone(timezone),
            misfire=policy,
            overlap=overlap,
        )

    @property
    def dedup_key(self) -> str | None:
        return None if self.overlap else f"{DEDUP_PREFIX}{self.task}"

    def next_after(self, instant: float) -> float:
        return self.cron.next_after(instant, self.zone)
