from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from ..exceptions import UnknownTaskError
from ..executors import ExecutorResolver
from ..models import Job

if TYPE_CHECKING:
    from ..app import TinyJobs

DRAIN_WORKER_ID = "drain"
DRAIN_LEASE_SECONDS = 3600.0


class Drain:
    def __init__(self, app: "TinyJobs", queues: Sequence[str] | None = None) -> None:
        self._app = app
        self._queues = tuple(queues) if queues else None
        self._executors = ExecutorResolver(concurrency=1)

    async def run(self) -> int:
        await self._app.ready()
        ran = 0
        try:
            while True:
                queues = self._queues or await self._known_queues()
                drained = 0
                while job := await self._claim(queues):
                    await self._execute(job)
                    drained += 1
                ran += drained
                if not drained or self._queues:
                    return ran
        finally:
            self._executors.shutdown()

    async def _claim(self, queues: Sequence[str]) -> Job | None:
        jobs = await self._app.backend.claim(
            worker_id=DRAIN_WORKER_ID,
            queues=queues,
            limit=1,
            lease_seconds=DRAIN_LEASE_SECONDS,
        )
        return jobs[0] if jobs else None

    async def _known_queues(self) -> tuple[str, ...]:
        stats = await self._app.astats()
        return tuple(queue.queue for queue in stats.queues)

    async def _execute(self, job: Job) -> None:
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._app.backend.release(job)
            raise
        await self._app.execution.execute(self._app.bind(job), task, self._executors)
