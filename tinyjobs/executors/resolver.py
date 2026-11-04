from __future__ import annotations

import asyncio
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Callable

from .base import Executor
from .coroutine import CoroutineExecutor
from .process import ProcessExecutor
from .thread import ThreadExecutor

if TYPE_CHECKING:
    from ..task import Task


class ExecutorResolver:
    def __init__(
        self,
        *,
        concurrency: int,
        async_concurrency: int | None = None,
        processes: int | None = None,
        jobs_per_process: int | None = None,
        process_setup: Callable[[], Any] | None = None,
        freed: Callable[[], None] | None = None,
        widens: bool = False,
    ):
        self._coroutine = CoroutineExecutor(async_concurrency or concurrency)
        self._thread = ThreadExecutor(concurrency, freed, widens=widens)
        self._process = ProcessExecutor(
            processes or concurrency, jobs_per_process, process_setup
        )

    @property
    def strays(self) -> int:
        return self._thread.strays

    def stray_names(self) -> list[str]:
        return self._thread.stray_names()

    def resolve(self, task: "Task") -> Executor:
        if task.is_coroutine:
            return self._coroutine
        if task.isolated:
            return self._process
        return self._thread

    def keeps(self, owner: asyncio.Task) -> Future | None:
        return self._thread.keeps(owner)

    def shutdown(self) -> None:
        self._coroutine.shutdown()
        self._thread.shutdown()
        self._process.shutdown()
