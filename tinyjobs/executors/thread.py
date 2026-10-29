from __future__ import annotations

import asyncio
import itertools
import logging
import queue
import threading
import time
from concurrent.futures import Future
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
    def __init__(
        self,
        concurrency: int,
        freed: Callable[[], None] | None = None,
        *,
        replacing: bool = False,
    ) -> None:
        self._pool = _Threads(concurrency)
        self._threaded: dict[asyncio.Task | None, Future] = {}
        self._strays: dict[Future, str] = {}
        self._freed = freed
        self._replacing = replacing

    @property
    def strays(self) -> int:
        return len(self._strays)

    def stray_names(self) -> list[str]:
        return sorted(self._strays.values())

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
        self._strays[running] = task.name
        if self._replacing:
            self._pool.widen(1)
        logger.warning(
            "%s ended while its thread still runs; %d job thread(s) are taken this way, "
            "so the worker runs that many fewer jobs until they return",
            task.name,
            len(self._strays),
        )
        running.add_done_callback(partial(_returned, loop, partial(self._back, running)))

    def _back(self, running: Future) -> None:
        if self._strays.pop(running, None) is not None and self._replacing:
            self._pool.widen(-1)
        if self._freed is not None:
            self._freed()

    def keeps(self, owner: asyncio.Task) -> Future | None:
        running = self._threaded.get(owner)
        if running is None or running.cancel():
            return None
        return running

    def shutdown(self) -> None:
        self._pool.shutdown()


class _Threads:
    def __init__(self, size: int) -> None:
        self._size = size
        self._work: queue.SimpleQueue = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._threads: set[threading.Thread] = set()
        self._idle = 0
        self._waiting = 0
        self._closed = False
        self._names = itertools.count()

    def submit(self, call: Callable[[], Any]) -> Future:
        future: Future = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self._waiting += 1
            self._work.put((future, call))
            self._grow()
        return future

    def widen(self, change: int) -> None:
        with self._lock:
            self._size += change
            if not self._closed:
                self._grow()

    def _grow(self) -> None:
        while self._waiting > self._idle and len(self._threads) < self._size:
            thread = threading.Thread(
                target=self._serve, name=f"tinyjobs_{next(self._names)}", daemon=True
            )
            self._threads.add(thread)
            self._idle += 1
            thread.start()

    def _serve(self) -> None:
        while True:
            item = self._work.get()
            with self._lock:
                if item is None:
                    self._idle -= 1
                    self._threads.discard(threading.current_thread())
                    return
                self._idle -= 1
                self._waiting -= 1
            future, call = item
            del item
            if future.set_running_or_notify_cancel():
                try:
                    result = call()
                except BaseException as error:
                    future.set_exception(error)
                else:
                    future.set_result(result)
            del future, call
            with self._lock:
                if self._closed or len(self._threads) > self._size:
                    self._threads.discard(threading.current_thread())
                    return
                self._idle += 1

    def shutdown(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending = []
            with suppress(queue.Empty):
                while True:
                    pending.append(self._work.get_nowait())
            self._waiting = 0
            for _ in self._threads:
                self._work.put(None)
        for item in pending:
            if item is not None:
                item[0].cancel()


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
