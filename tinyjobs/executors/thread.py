from __future__ import annotations

import asyncio
import concurrent.futures
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
from ..logs import job_fields
from ..models import Job, JobPayload
from ..runtime import fresh_thread
from ..sheltered import landing, sheltered
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
        widens: bool = False,
    ) -> None:
        self._pool = _Threads(concurrency)
        self._threaded: dict[asyncio.Task | None, Future] = {}
        self._strays: dict[Future, str] = {}
        self._replaced: set[Future] = set()
        self._freed = freed
        self._widens = widens

    @property
    def strays(self) -> int:
        return len(self._strays) - len(self._replaced)

    def stray_names(self) -> list[str]:
        return sorted(self._strays.values())

    async def execute(self, task: "Task", payload: JobPayload, *, job: Job | None = None) -> Any:
        loop = asyncio.get_running_loop()
        owner = asyncio.current_task()
        call = partial(copy_context().run, task.function, *payload.args, **payload.kwargs)
        started: asyncio.Future[float] | None = None
        if task.timeout is not None:
            started = loop.create_future()
            call = partial(_started, loop, started, call)
        try:
            running = self._pool.submit(call)
        except RuntimeError as error:
            if not self._pool.closed:
                raise
            raise ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE) from error
        self._threaded[owner] = running
        try:
            if started is None:
                return await landing(running)
            future = asyncio.wrap_future(running)
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
            if started is not None:
                started.cancel()
            if self._threaded.get(owner) is running:
                del self._threaded[owner]
            if not running.done() and not running.cancel():
                self._stray(loop, running, task, job)

    def _stray(
        self,
        loop: asyncio.AbstractEventLoop,
        running: Future,
        task: "Task",
        job: Job | None,
    ) -> None:
        self._strays[running] = task.name
        fields = {"task": task.name} if job is None else job_fields(job)
        if self._widens:
            self._replaced.add(running)
            self._pool.widen(1)
            logger.warning(
                "%s ended while its thread still runs; a new thread takes its place until it returns",
                task.name,
                extra=fields,
            )
        else:
            logger.warning(
                "%s ended while its thread still runs; %d job thread(s) are taken this way, "
                "so that many fewer jobs run at once until they return",
                task.name,
                self.strays,
                extra={**fields, "strays": self.strays},
            )
        running.add_done_callback(partial(_returned, loop, partial(self._back, running)))

    def _back(self, running: Future) -> None:
        if self._strays.pop(running, None) is not None and running in self._replaced:
            self._replaced.discard(running)
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


class DaemonThreadPool(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, size: int) -> None:
        super().__init__(max_workers=size)
        self._daemons = _Threads(size)

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        return self._daemons.submit(partial(fn, *args, **kwargs))

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self._daemons.shutdown()


class _Threads:
    def __init__(self, size: int) -> None:
        self._size = size
        self._work: queue.SimpleQueue = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._threads: set[threading.Thread] = set()
        self._idle = threading.Semaphore(0)
        self._short = False
        self._closed = False
        self._names = itertools.count()

    @property
    def closed(self) -> bool:
        return self._closed

    def submit(self, call: Callable[[], Any]) -> Future:
        future: Future = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self._work.put((future, call))
            if not self._short and self._idle.acquire(blocking=False):
                return future
            try:
                self._grow()
            except RuntimeError:
                self._short = True
                if self._threads:
                    return future
                future.cancel()
                raise
        return future

    def widen(self, change: int) -> None:
        with self._lock:
            self._size += change
            if change > 0 and not self._closed:
                try:
                    self._grow()
                except RuntimeError:
                    self._short = True

    def _grow(self) -> None:
        if len(self._threads) < self._size:
            thread = fresh_thread(self._serve, name=f"tinyjobs_{next(self._names)}")
            thread.start()
            self._threads.add(thread)
        if len(self._threads) >= self._size:
            self._short = False

    def _serve(self) -> None:
        while True:
            item = self._work.get()
            if item is None:
                with self._lock:
                    self._threads.discard(threading.current_thread())
                return
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
            if self._closed or len(self._threads) > self._size:
                with self._lock:
                    if self._closed or len(self._threads) > self._size:
                        self._threads.discard(threading.current_thread())
                        return
            self._idle.release()

    def shutdown(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending = []
            with suppress(queue.Empty):
                while True:
                    pending.append(self._work.get_nowait())
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
