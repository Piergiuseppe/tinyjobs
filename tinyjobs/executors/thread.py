from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import copy_context
from functools import partial
from typing import Any, TYPE_CHECKING

from ..exceptions import ExecutorClosedError
from ..models import JobPayload
from ..sheltered import sheltered
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
        self._threaded: dict[asyncio.Task | None, Future] = {}

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        call = partial(
            copy_context().run, task.function, *payload.args, **payload.kwargs
        )
        try:
            running = self._pool.submit(call)
        except RuntimeError as error:
            raise ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE) from error
        owner = asyncio.current_task()
        self._threaded[owner] = running
        try:
            future = asyncio.wrap_future(running)
            if task.timeout is None:
                return await future
            return await asyncio.wait_for(sheltered(future), task.timeout)
        finally:
            if self._threaded.get(owner) is running:
                del self._threaded[owner]

    def keeps(self, owner: asyncio.Task) -> Future | None:
        running = self._threaded.get(owner)
        if running is None or running.cancel():
            return None
        return running

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
