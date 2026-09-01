from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

from ..models import JobPayload
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task


class CoroutineExecutor(Executor):
    def __init__(self, concurrency: int) -> None:
        self._slots = asyncio.Semaphore(concurrency)

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        async with self._slots:
            if task.timeout is None:
                return await task.function(*payload.args, **payload.kwargs)
            async with asyncio.timeout(task.timeout):
                return await task.function(*payload.args, **payload.kwargs)
