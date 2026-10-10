from __future__ import annotations

from dataclasses import dataclass

from ..backends.base import Backend
from ..clock import Clock
from ..duration import format_age
from ..models import Stats
from ..registry import TaskRegistry

OVERDUE_TOLERANCE = 60.0


@dataclass(frozen=True, slots=True)
class Finding:
    ok: bool
    message: str


class HealthService:
    def __init__(self, backend: Backend, registry: TaskRegistry, clock: Clock) -> None:
        self._backend = backend
        self._registry = registry
        self._clock = clock

    async def stats(self) -> Stats:
        return await self._backend.stats(self._clock.now())

    async def check(self, *, max_wait: float | None = None) -> list[Finding]:
        now = self._clock.now()
        stats = await self._backend.stats(now)
        return [
            self._tasks(),
            *self._unknown_tasks(stats),
            self._leases(stats),
            *self._waiting(stats, now, max_wait),
            *await self._schedules(now),
        ]

    def _tasks(self) -> Finding:
        if not self._registry:
            return Finding(
                False,
                "no tasks registered; import the module that defines them, "
                "or pass --import",
            )
        cron = sum(1 for task in self._registry if task.schedule)
        return Finding(True, f"{len(self._registry)} task(s) registered, {cron} on cron")

    def _unknown_tasks(self, stats: Stats) -> list[Finding]:
        return [
            Finding(
                False,
                f"{count} queued job(s) for {task}, which this app does not "
                f"register; workers started like this leave them queued",
            )
            for task, count in sorted(stats.queued_tasks.items())
            if task not in self._registry
        ]

    def _leases(self, stats: Stats) -> Finding:
        if stats.expired_leases:
            return Finding(
                False,
                f"{stats.expired_leases} running job(s) past their lease: the "
                f"workers holding them stopped reporting",
            )
        return Finding(True, "no running job is past its lease")

    def _waiting(
        self, stats: Stats, now: float, max_wait: float | None
    ) -> list[Finding]:
        findings = []
        for queue in stats.queues:
            if queue.oldest_due_at is None:
                continue
            waited = now - queue.oldest_due_at
            late = max_wait is not None and waited > max_wait
            findings.append(
                Finding(
                    not late,
                    f"queue {queue.queue}: oldest due job has waited "
                    f"{format_age(waited)}"
                    + (f", over the {format_age(max_wait)} allowed" if late else ""),
                )
            )
        return findings

    async def _schedules(self, now: float) -> list[Finding]:
        if not self._backend.capabilities.schedules:
            return []
        declared = {task.name for task in self._registry if task.schedule}
        return [
            Finding(
                False,
                f"schedule {state.name} is {format_age(now - state.next_run_at)} "
                f"overdue: no worker or scheduler is firing it",
            )
            for state in await self._backend.schedules()
            if state.name in declared
            and now - state.next_run_at > OVERDUE_TOLERANCE
        ]
