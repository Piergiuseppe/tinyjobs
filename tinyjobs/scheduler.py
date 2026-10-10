from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .services import Tick
from .worker.signals import ShutdownWatcher

if TYPE_CHECKING:
    from .app import TinyJobs

logger = logging.getLogger("tinyjobs.scheduler")


class Scheduler:
    def __init__(
        self,
        app: "TinyJobs",
        *,
        misfire_grace: float = 60.0,
        maximum_sleep: float = 30.0,
        error_backoff: float = 5.0,
    ) -> None:
        self._app = app
        self._grace = misfire_grace
        self._maximum_sleep = maximum_sleep
        self._error_backoff = error_backoff
        self._stopping = asyncio.Event()

    def run(self) -> None:
        asyncio.run(self._serve())

    def stop(self) -> None:
        self._stopping.set()

    async def start(self) -> None:
        await self._app.ready()
        declared = self._app.scheduling.declared()
        if not declared:
            logger.debug("no cron tasks registered, scheduler idle")
            return
        if not self._app.backend.capabilities.schedules:
            logger.warning("this backend cannot store schedules; cron is disabled")
            return
        logger.info("scheduler watching %d schedule(s)", len(declared))
        for task in declared:
            logger.debug(
                "schedule %s: %s %s", task.name, task.schedule.cron.source,
                task.schedule.timezone,
            )
        await self._register()
        while not self._stopping.is_set():
            try:
                tick = await self.tick()
            except Exception:
                logger.exception("scheduler tick failed; backing off")
                await self._pause(self._error_backoff)
                continue
            await self._pause(self._sleep_for(tick))

    async def tick(self) -> Tick:
        await self._app.ready()
        tick = await self._app.scheduling.fire_due(self._grace)
        for job in tick.created:
            logger.info("schedule %s created job %s", job.task, job.id)
        for name in tick.skipped:
            logger.warning(
                "schedule %s skipped a run: the previous one is still queued or running",
                name,
            )
        for name in tick.missed:
            logger.warning(
                "schedule %s missed runs by more than %.0fs, while nothing could "
                "fire it; applying its misfire policy",
                name,
                self._grace,
            )
        for name in tick.rewound:
            logger.warning(
                "schedule %s was due further ahead than its cron allows; the clock "
                "moved back, so its next run was recomputed",
                name,
            )
        return tick

    async def _register(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._app.scheduling.register()
            except Exception:
                logger.exception("registering schedules failed; backing off")
                await self._pause(self._error_backoff)
                continue
            return

    def _sleep_for(self, tick: Tick) -> float:
        if tick.wake_at is None:
            return self._maximum_sleep
        remaining = tick.wake_at - self._app.clock.now()
        return min(max(remaining, 0.0), self._maximum_sleep)

    async def _serve(self) -> None:
        watcher = ShutdownWatcher(self.stop, self.stop)
        watcher.install()
        try:
            await self.start()
        finally:
            watcher.uninstall()
            await self._app.aclose()

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return
