from __future__ import annotations

import asyncio


class Flag:
    __slots__ = ("_set", "_future")

    def __init__(self) -> None:
        self._set = False
        self._future: asyncio.Future | None = None

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True
        if self._future is not None and not self._future.done():
            self._future.set_result(None)

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
