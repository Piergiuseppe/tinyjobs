from __future__ import annotations

import asyncio
import logging

from .worker import Worker

logger = logging.getLogger("tinyjobs.worker")


class EmbeddedWorker:
    def __init__(self, worker: Worker) -> None:
        self._worker = worker
        self._task: asyncio.Task | None = None

    @property
    def worker(self) -> Worker:
        return self._worker

    async def __aenter__(self) -> Worker:
        self._task = asyncio.create_task(
            self._worker.start(), name="tinyjobs-embedded-worker"
        )
        self._task.add_done_callback(self._report)
        return self._worker

    async def __aexit__(self, *exc_info: object) -> None:
        self._worker.stop()
        await asyncio.gather(self._task, return_exceptions=True)

    def _report(self, task: asyncio.Task) -> None:
        if task.cancelled() or task.exception() is None:
            return
        logger.error(
            "embedded worker %s stopped running jobs",
            self._worker.id,
            exc_info=task.exception(),
        )
