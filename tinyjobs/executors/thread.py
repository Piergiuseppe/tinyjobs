from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from functools import partial
from typing import Any, TYPE_CHECKING

from ..exceptions import ExecutorClosedError
from ..models import JobPayload
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task

EXECUTOR_CLOSED_MESSAGE = (
    "the job threads were shut down, so this job was handed back without running"
)


class ThreadExecutor(Executor):
    def __init__(self, concurrency: int) -> None:
        self._pool = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="tinyjobs"
        )

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        call = partial(
            copy_context().run, task.function, *payload.args, **payload.kwargs
        )
        try:
            future = asyncio.get_running_loop().run_in_executor(self._pool, call)
        except RuntimeError as error:
            raise ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE) from error
        if task.timeout is None:
            return await future
        return await asyncio.wait_for(asyncio.shield(future), task.timeout)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
