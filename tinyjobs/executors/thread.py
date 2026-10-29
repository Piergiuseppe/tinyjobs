from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from contextvars import copy_context
from functools import partial
from typing import Any, Callable, TYPE_CHECKING

from ..exceptions import ExecutorClosedError
from ..models import JobPayload
from ..sheltered import sheltered
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task

logger = logging.getLogger("tinyjobs.worker")

EXECUTOR_CLOSED_MESSAGE = (
    "the job threads were shut down, so this job was handed back without running"
)


class ThreadExecutor(Executor):
    def __init__(self, concurrency: int, freed: Callable[[], None] | None = None) -> None:
        self._pool = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="tinyjobs"
        )
        self._threaded: dict[asyncio.Task | None, Future] = {}
        self._strays: set[Future] = set()
        self._freed = freed

    @property
    def strays(self) -> int:
        return len(self._strays)

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        loop = asyncio.get_running_loop()
        started: asyncio.Future[float] = loop.create_future()
        call = partial(
            _started,
            loop,
            started,
            partial(copy_context().run, task.function, *payload.args, **payload.kwargs),
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
            try:
                await asyncio.wait({future, started}, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                running.cancel()
                raise
            if future.done():
                return future.result()
            left = started.result() + task.timeout - time.monotonic()
            return await asyncio.wait_for(sheltered(future), max(0.0, left))
        finally:
            started.cancel()
            if self._threaded.get(owner) is running:
                del self._threaded[owner]
            if not running.cancel() and not running.done():
                self._stray(loop, running, task)

    def _stray(self, loop: asyncio.AbstractEventLoop, running: Future, task: "Task") -> None:
        self._strays.add(running)
        logger.warning(
            "%s ended while its thread still runs; %d job thread(s) are taken this way, "
            "so the worker runs that many fewer jobs until they return",
            task.name,
            len(self._strays),
        )
        running.add_done_callback(partial(_returned, loop, partial(self._back, running)))

    def _back(self, running: Future) -> None:
        self._strays.discard(running)
        if self._freed is not None:
            self._freed()

    def keeps(self, owner: asyncio.Task) -> Future | None:
        running = self._threaded.get(owner)
        if running is None or running.cancel():
            return None
        return running

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)


def _started(loop: asyncio.AbstractEventLoop, started: asyncio.Future, call: Any) -> Any:
    began = time.monotonic()
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(_began, started, began)
    return call()


def _returned(loop: asyncio.AbstractEventLoop, back: Any, _: object) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(back)


def _began(started: asyncio.Future, began: float) -> None:
    if not started.done():
        started.set_result(began)
