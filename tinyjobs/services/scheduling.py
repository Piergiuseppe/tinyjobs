from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, TYPE_CHECKING

from ..backends.base import Backend
from ..clock import Clock
from ..exceptions import UnknownTaskError, UnsupportedCapabilityError
from ..models import NEVER, Job, JobPayload, ScheduleState
from ..options import EnqueueOptions
from ..registry import TaskRegistry
from ..schedules import MisfirePolicy, Schedule
from .dispatch import DispatchService

if TYPE_CHECKING:
    from ..task import Task

MAXIMUM_CATCH_UP = 100
NO_SCHEDULE_MESSAGE = (
    "no schedule named {name!r}; tinyjobs schedules lists the ones this app "
    "declares and the store holds"
)
NO_SCHEDULES_MESSAGE = "this backend cannot store schedules, so there is nothing to {action}"
NOT_DECLARED_MESSAGE = (
    "this app does not declare a cron task named {name!r}, so it cannot fire it; "
    "import the module that defines it, or pass --import"
)


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
        states = await self._states(tasks)
        tick = Tick()
        for task in tasks:
            state = states[task.name]
            if state.paused:
                continue
            next_run_at = await self._fire(task, state, grace, tick)
            if tick.wake_at is None or next_run_at < tick.wake_at:
                tick.wake_at = next_run_at
        return tick

    async def pause(self, name: str) -> bool:
        return await self._settle(name, paused=True, target=lambda state: NEVER)

    async def resume(self, name: str) -> bool:
        return await self._settle(
            name,
            paused=False,
            target=lambda state: self._schedule_of(state).next_after(self._clock.now()),
        )

    async def _settle(
        self, name: str, *, paused: bool, target: Callable[[ScheduleState], float]
    ) -> bool:
        if not self._backend.capabilities.schedules:
            raise UnsupportedCapabilityError(
                NO_SCHEDULES_MESSAGE.format(action="pause" if paused else "resume")
            )
        while True:
            state = await self._state(name)
            if state.paused == paused:
                return False
            if await self._move(state, target(state)):
                return True

    def _schedule_of(self, state: ScheduleState) -> Schedule:
        task = self._registry.get(state.name) if state.name in self._registry else None
        if task is not None and task.schedule is not None:
            return task.schedule
        return Schedule.declare(state.name, state.cron, timezone=state.timezone)

    def run_key(self, name: str) -> str | None:
        task = self._registry.get(name) if name in self._registry else None
        if task is None or task.schedule is None:
            raise UnknownTaskError(NOT_DECLARED_MESSAGE.format(name=name))
        return task.schedule.dedup_key

    async def _states(self, tasks: list["Task"]) -> dict[str, ScheduleState]:
        states = {state.name: state for state in await self._backend.schedules()}
        missing = [task for task in tasks if task.name not in states]
        if missing:
            await self.register(missing)
            states.update(
                (state.name, state) for state in await self._backend.schedules()
            )
        return states

    async def _state(self, name: str) -> ScheduleState:
        declared = [task for task in self.declared() if task.name == name]
        states = await self._states(declared)
        if name not in states:
            raise UnknownTaskError(NO_SCHEDULE_MESSAGE.format(name=name))
        return states[name]

    async def _move(self, state: ScheduleState, next_run_at: float) -> bool:
        stored = await self._backend.fire_schedule(
            state.name, expected=state.next_run_at, next_run_at=next_run_at, jobs=()
        )
        return stored is not None

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
                EnqueueOptions(key=schedule.dedup_key),
                due_at=run_at,
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
