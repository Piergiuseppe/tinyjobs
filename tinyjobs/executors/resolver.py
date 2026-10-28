from __future__ import annotations

import asyncio
from concurrent.futures import Future
from typing import TYPE_CHECKING

from .base import Executor
from .coroutine import CoroutineExecutor
from .thread import ThreadExecutor

if TYPE_CHECKING:
    from ..task import Task


class ExecutorResolver:
    def __init__(self, *, concurrency: int, async_concurrency: int | None = None):
        self._coroutine = CoroutineExecutor(async_concurrency or concurrency)
        self._thread = ThreadExecutor(concurrency)

    def resolve(self, task: "Task") -> Executor:
        return self._coroutine if task.is_coroutine else self._thread

    def thread_of(self, owner: asyncio.Task) -> Future | None:
        return self._thread.thread_of(owner)

    def shutdown(self) -> None:
        self._coroutine.shutdown()
        self._thread.shutdown()
