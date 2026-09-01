from __future__ import annotations

import asyncio
import signal
from typing import Callable

STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class ShutdownWatcher:
    def __init__(self, on_stop: Callable[[], None], on_force: Callable[[], None]):
        self._on_stop = on_stop
        self._on_force = on_force
        self._triggered = False
        self._installed: list[tuple[int, object]] = []

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        for number in STOP_SIGNALS:
            try:
                loop.add_signal_handler(number, self._handle)
                self._installed.append((number, None))
            except NotImplementedError:
                previous = signal.getsignal(number)
                signal.signal(number, lambda *_: self._handle())
                self._installed.append((number, previous))

    def uninstall(self) -> None:
        loop = asyncio.get_running_loop()
        for number, previous in self._installed:
            if previous is None:
                loop.remove_signal_handler(number)
            else:
                signal.signal(number, previous)
        self._installed.clear()

    def _handle(self) -> None:
        if self._triggered:
            self._on_force()
            return
        self._triggered = True
        self._on_stop()
