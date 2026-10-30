from __future__ import annotations

import asyncio
import itertools
import logging
import queue
import threading
import time
from concurrent.futures import Future
from contextlib import suppress
from contextvars import ContextVar, copy_context
from functools import partial
from typing import Any, Callable, Coroutine, TYPE_CHECKING, TypeVar

from ..exceptions import ExecutorClosedError
from ..models import JobPayload
from ..sheltered import sheltered
from .base import Executor

if TYPE_CHECKING:
    from ..task import Task

logger = logging.getLogger("tinyjobs.worker")

T = TypeVar("T")

EXECUTOR_CLOSED_MESSAGE = (
    "the job threads were shut down, so this job was handed back without running"
)


class Spares:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._taken = 0
        self._strayed = 0
        self._lock = threading.Lock()
        self._waiting: set[tuple[asyncio.AbstractEventLoop, asyncio.Future]] = set()

    @property
    def taken(self) -> int:
        return self._taken

    @property
    def strayed(self) -> int:
        return self._strayed

    @property
    def exhausted(self) -> bool:
        return self._strayed >= self.limit

    async def reserve(self, *, wait: bool = True) -> "Spare | None":
        loop = asyncio.get_running_loop()
        while True:
            with self._lock:
                if self._taken < self.limit:
                    self._taken += 1
                    return Spare(self)
                if self._strayed >= self.limit or not wait:
                    return None
                waiter = (loop, loop.create_future())
                self._waiting.add(waiter)
            try:
                await waiter[1]
            finally:
                with self._lock:
                    self._waiting.discard(waiter)

    def borrow(self) -> "Spare | None":
        with self._lock:
            if self._strayed >= self.limit:
                return None
        return Spare(self, borrowed=True)

    def _stray(self, taking: bool) -> None:
        with self._lock:
            self._taken += taking
            self._strayed += 1
            waiting = list(self._waiting)
        wake(waiting)

    def _give(self, strayed: bool) -> None:
        with self._lock:
            self._taken -= 1
            self._strayed -= strayed
            waiting = list(self._waiting)
        wake(waiting)


class Spare:
    def __init__(self, spares: Spares, *, borrowed: bool = False) -> None:
        self.spares = spares
        self._borrowed = borrowed
        self._kept = False
        self._ended = False

    def keep(self, running: Future) -> None:
        if not self._kept and not self._ended:
            self._kept = True
            self.spares._stray(self._borrowed)
            running.add_done_callback(self._returned)

    def end(self) -> None:
        if not self._kept and not self._ended:
            self._ended = True
            if not self._borrowed:
                self.spares._give(False)

    def _returned(self, _: object) -> None:
        self.spares._give(True)


def wake(waiting: list[tuple[asyncio.AbstractEventLoop, asyncio.Future]]) -> None:
    for loop, future in waiting:
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(_resolve, future)


def _resolve(future: asyncio.Future) -> None:
    if not future.done():
        future.set_result(None)


class ThreadExecutor(Executor):
    def __init__(
        self,
        concurrency: int,
        freed: Callable[[], None] | None = None,
    ) -> None:
        self._pool = _Threads(concurrency)
        self._threaded: dict[asyncio.Task | None, Future] = {}
        self._strays: dict[Future, str] = {}
        self._replaced: set[Future] = set()
        self._freed = freed
        self._lent: dict[asyncio.Task, Spare] = {}

    @property
    def strays(self) -> int:
        return len(self._strays) - len(self._replaced)

    def stray_names(self) -> list[str]:
        return sorted(self._strays.values())

    def lend(self, owner: asyncio.Task, spare: Spare | None) -> None:
        if spare is None:
            self._lent.pop(owner, None)
        else:
            self._lent[owner] = spare

    async def execute(self, task: "Task", payload: JobPayload) -> Any:
        loop = asyncio.get_running_loop()
        started: asyncio.Future[float] = loop.create_future()
        owner = asyncio.current_task()
        context = copy_context()
        lent = self._lent.get(owner)
        if lent is not None:
            context.run(_spared.set, _spared.get() | {lent.spares})
        call = partial(
            _started,
            loop,
            started,
            partial(context.run, task.function, *payload.args, **payload.kwargs),
        )
        try:
            running = self._pool.submit(call)
        except RuntimeError as error:
            if not self._pool.closed:
                raise
            raise ExecutorClosedError(EXECUTOR_CLOSED_MESSAGE) from error
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
                self._stray(loop, running, task, self._lent.get(owner))

    def _stray(
        self,
        loop: asyncio.AbstractEventLoop,
        running: Future,
        task: "Task",
        spare: Spare | None,
    ) -> None:
        self._strays[running] = task.name
        if spare is not None:
            self._replaced.add(running)
            self._pool.widen(1)
            spare.keep(running)
            logger.warning(
                "%s ended while its thread still runs; a new thread takes its place, "
                "%d of the %d such thread(s) allowed are in use until the old ones return",
                task.name,
                spare.spares.strayed,
                spare.spares.limit,
            )
        else:
            logger.warning(
                "%s ended while its thread still runs; %d job thread(s) are taken this way, "
                "so that many fewer jobs run at once until they return",
                task.name,
                self.strays,
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

    @property
    def closed(self) -> bool:
        return self._closed

    def submit(self, call: Callable[[], Any]) -> Future:
        future: Future = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self._waiting += 1
            self._work.put((future, call))
            try:
                self._grow()
            except RuntimeError:
                if self._threads:
                    return future
                future.cancel()
                raise
        return future

    def widen(self, change: int) -> None:
        with self._lock:
            self._size += change
            if not self._closed:
                with suppress(RuntimeError):
                    self._grow()

    def _grow(self) -> None:
        while self._waiting > self._idle and len(self._threads) < self._size:
            thread = threading.Thread(
                target=self._serve, name=f"tinyjobs_{next(self._names)}", daemon=True
            )
            thread.start()
            self._threads.add(thread)
            self._idle += 1

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


_spared: ContextVar[frozenset[Spares]] = ContextVar("tinyjobs_spared", default=frozenset())


def held_spares() -> frozenset[Spares]:
    return _spared.get()


async def spared(held: frozenset[Spares], coroutine: Coroutine[Any, Any, T]) -> T:
    _spared.set(held)
    return await coroutine


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
