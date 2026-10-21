from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from typing import Any, Coroutine, TypeVar

from .exceptions import EventLoopStoppedError, SynchronousCallInsideEventLoopError

T = TypeVar("T")

INSIDE_LOOP_MESSAGE = (
    "{name}() is synchronous and was called from inside a running event loop, "
    "which would block it. Use the awaitable form instead: await {async_name}()"
)

STOPPED_LOOP_MESSAGE = (
    "{name}() was waiting on the event loop that connected the backend, and that "
    "loop stopped before the call finished"
)

HOME_LOOP_POLL_SECONDS = 0.1


def inside_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


class LoopRunner:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._home: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._guard = threading.Lock()

    def run(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        name: str = "this call",
        async_name: str | None = None,
    ) -> T:
        if inside_running_loop():
            coroutine.close()
            raise SynchronousCallInsideEventLoopError(
                INSIDE_LOOP_MESSAGE.format(
                    name=name, async_name=async_name or f"a{name}"
                )
            )
        loop = self._target()
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        if loop is self._loop:
            return future.result()
        return self._await_home(future, loop, name)

    def attach(self, loop: asyncio.AbstractEventLoop) -> None:
        self._home = loop

    def detach(self) -> None:
        self._home = None

    def close(self) -> None:
        with self._guard:
            loop, thread = self._loop, self._thread
            self._loop, self._thread = None, None
        if loop is None:
            return
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5)
        loop.close()

    def _await_home(
        self,
        future: concurrent.futures.Future,
        loop: asyncio.AbstractEventLoop,
        name: str,
    ) -> Any:
        while True:
            try:
                return future.result(timeout=HOME_LOOP_POLL_SECONDS)
            except TimeoutError:
                if future.done():
                    return future.result()
            if not loop.is_running():
                future.cancel()
                raise EventLoopStoppedError(STOPPED_LOOP_MESSAGE.format(name=name))

    def _target(self) -> asyncio.AbstractEventLoop:
        home = self._home
        if home is not None and home.is_running():
            return home
        return self._ensure_loop()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._guard:
            if self._loop is not None:
                return self._loop
            ready = threading.Event()
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._serve,
                args=(self._loop, ready),
                name="tinyjobs-loop",
                daemon=True,
            )
            self._thread.start()
            ready.wait(timeout=5)
            return self._loop

    def _serve(self, loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()
