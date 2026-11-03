from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Callable

from ..exceptions import AppClosedError
from ..runtime import fresh_thread

if TYPE_CHECKING:
    from .worker import Worker

logger = logging.getLogger("tinyjobs.worker")

THREAD_PREFIX = "tinyjobs-worker"


def shares(total: int, parts: int) -> list[int]:
    base, extra = divmod(total, parts)
    return [base + (1 if index < extra else 0) for index in range(parts)]


class Lanes:
    def __init__(self, lanes: list["Worker"], failed: Callable[[], None]) -> None:
        self._lanes = lanes
        self._failed = failed
        self._done: list[concurrent.futures.Future] = []
        self._errors: list[BaseException] = []
        self._stopped = False

    @property
    def error(self) -> BaseException | None:
        return self._errors[0] if self._errors else None

    def start(self) -> None:
        for index, worker in enumerate(self._lanes, start=1):
            done: concurrent.futures.Future = concurrent.futures.Future()
            thread = fresh_thread(self._run, worker, done, name=f"{THREAD_PREFIX}-{index}")
            thread.start()
            self._done.append(done)

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        for worker in self._lanes:
            worker.stop()

    def force(self) -> None:
        for worker in self._lanes:
            worker.force()

    def mark_forcing(self) -> None:
        for worker in self._lanes:
            worker.mark_forcing()

    async def finish(self) -> None:
        self.stop()
        if self._done:
            await asyncio.wait([asyncio.wrap_future(done) for done in self._done])

    def _run(self, worker: "Worker", done: concurrent.futures.Future) -> None:
        try:
            worker.run()
        except AppClosedError:
            self._failed()
        except BaseException as error:
            self._errors.append(error)
            logger.error("worker %s stopped on an error: %r", worker.id, error)
            self._failed()
        finally:
            with suppress(concurrent.futures.InvalidStateError):
                done.set_result(None)

