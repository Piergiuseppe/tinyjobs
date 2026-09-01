from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, TYPE_CHECKING

from ..models import JobPayload
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task


class ThreadExecutor(Executor):
    def __init__(self, concurrency: int) -> None:
        self._pool = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="tinyjobs"
        )

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        call = partial(task.function, *payload.args, **payload.kwargs)
        future = asyncio.get_running_loop().run_in_executor(self._pool, call)
        if task.timeout is None:
            return await future
        return await asyncio.wait_for(asyncio.shield(future), task.timeout)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
