from __future__ import annotations

import asyncio
import threading
from typing import Any, Coroutine, TypeVar

from .exceptions import SynchronousCallInsideEventLoopError

T = TypeVar("T")

INSIDE_LOOP_MESSAGE = (
    "{name}() is synchronous and was called from inside a running event loop, "
    "which would block it. Use the awaitable form instead: await {async_name}()"
)


def inside_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


class LoopRunner:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
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
        return asyncio.run_coroutine_threadsafe(coroutine, self._ensure_loop()).result()

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
