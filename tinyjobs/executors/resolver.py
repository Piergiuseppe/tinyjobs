from __future__ import annotations

import asyncio
from concurrent.futures import Future
from typing import TYPE_CHECKING, Callable

from .base import Executor
from .coroutine import CoroutineExecutor
from .thread import ThreadExecutor

if TYPE_CHECKING:
    from ..task import Task


class ExecutorResolver:
    def __init__(
        self,
        *,
        concurrency: int,
        async_concurrency: int | None = None,
        freed: Callable[[], None] | None = None,
    ):
        self._coroutine = CoroutineExecutor(async_concurrency or concurrency)
        self._thread = ThreadExecutor(concurrency, freed)

    @property
    def strays(self) -> int:
        return self._thread.strays

    def resolve(self, task: "Task") -> Executor:
        return self._coroutine if task.is_coroutine else self._thread

    def keeps(self, owner: asyncio.Task) -> Future | None:
        return self._thread.keeps(owner)

    def shutdown(self) -> None:
        self._coroutine.shutdown()
        self._thread.shutdown()
