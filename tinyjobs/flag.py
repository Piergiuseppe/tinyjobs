from __future__ import annotations

import asyncio
from contextlib import suppress


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def _raise(future: asyncio.Future) -> None:
    if not future.done():
        future.set_result(None)


class Flag:
    __slots__ = ("_set", "_future")

    def __init__(self) -> None:
        self._set = False
        self._future: asyncio.Future | None = None

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True
        future = self._future
        if future is None or future.done():
            return
        loop = future.get_loop()
        if loop is _running_loop():
            future.set_result(None)
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(_raise, future)

    def clear(self) -> None:
        self._set = False
        if self._future is not None and self._future.done():
            self._future = None

    def raised(self) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        if self._future is None or self._future.get_loop() is not loop:
            self._future = loop.create_future()
            if self._set:
                self._future.set_result(None)
        return self._future
