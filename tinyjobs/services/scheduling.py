from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, TYPE_CHECKING

from ..backends.base import Backend
from ..clock import Clock
from ..models import Job, JobPayload, ScheduleState
from ..options import EnqueueOptions
from ..registry import TaskRegistry
from ..schedules import MisfirePolicy, Schedule
from .dispatch import DispatchService

if TYPE_CHECKING:
    from ..task import Task

MAXIMUM_CATCH_UP = 100


@dataclass(frozen=True, slots=True)
class Firing:
    run_at: tuple[float, ...]
    next_run_at: float


@dataclass(slots=True)
class Tick:
    created: list[Job] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    missed: list[str] = field(default_factory=list)
    rewound: list[str] = field(default_factory=list)
    wake_at: float | None = None


class SchedulingService:
    def __init__(
        self,
        backend: Backend,
        dispatch: DispatchService,
        registry: TaskRegistry,
        clock: Clock,
    ) -> None:
        self._backend = backend
        self._dispatch = dispatch
        self._registry = registry
        self._clock = clock

    def declared(self) -> list["Task"]:
        return [task for task in self._registry if task.schedule is not None]

    async def register(self, tasks: Iterable["Task"] | None = None) -> None:
        now = self._clock.now()
        await self._backend.register_schedules(
            [
                ScheduleState(
                    name=task.name,
                    cron=task.schedule.cron.source,
                    timezone=task.schedule.timezone,
                    next_run_at=task.schedule.next_after(now),
                )
                for task in (self.declared() if tasks is None else tasks)
            ]
        )

    async def fire_due(self, grace: float) -> Tick:
        tasks = self.declared()
        states = {state.name: state for state in await self._backend.schedules()}
        missing = [task for task in tasks if task.name not in states]
        if missing:
            await self.register(missing)
            states.update(
                (state.name, state) for state in await self._backend.schedules()
            )
        tick = Tick()
        for task in tasks:
            next_run_at = await self._fire(task, states[task.name], grace, tick)
            if tick.wake_at is None or next_run_at < tick.wake_at:
                tick.wake_at = next_run_at
        return tick

    def plan(
        self, schedule: Schedule, state: ScheduleState, now: float, grace: float
    ) -> Firing:
        if state.next_run_at > schedule.next_after(now + grace):
            return Firing((), schedule.next_after(now))
        if state.next_run_at > now:
            return Firing((), state.next_run_at)
        if schedule.misfire is MisfirePolicy.RUN_ALL:
            due: list[float] = []
            moment = state.next_run_at
            while moment <= now and len(due) < MAXIMUM_CATCH_UP:
                due.append(moment)
                moment = schedule.next_after(moment)
            return Firing(tuple(due), moment)
        following = schedule.next_after(now)
        if now - state.next_run_at <= grace:
            return Firing((state.next_run_at,), following)
        recent = schedule.next_after(now - grace)
        if recent <= now:
            return Firing((recent,), following)
        if schedule.misfire is MisfirePolicy.RUN_ONCE:
            return Firing((now,), following)
        return Firing((), following)

    async def _fire(
        self, task: "Task", state: ScheduleState, grace: float, tick: Tick
    ) -> float:
        schedule = task.schedule
        now = self._clock.now()
        firing = self.plan(schedule, state, now, grace)
        if firing.next_run_at == state.next_run_at:
            return state.next_run_at
        jobs = [
            self._dispatch.prepare(
                task.name,
                JobPayload(),
                EnqueueOptions(run_at=run_at, key=schedule.dedup_key),
            )
            for run_at in firing.run_at
        ]
        stored = await self._backend.fire_schedule(
            task.name,
            expected=state.next_run_at,
            next_run_at=firing.next_run_at,
            jobs=jobs,
        )
        if stored is None:
            return firing.next_run_at
        if state.next_run_at > now:
            tick.rewound.append(task.name)
        elif firing.run_at[:1] != (state.next_run_at,):
            tick.missed.append(task.name)
        for built, kept in zip(jobs, stored):
            if kept.id == built.id:
                tick.created.append(kept)
            else:
                tick.skipped.append(task.name)
        return firing.next_run_at
