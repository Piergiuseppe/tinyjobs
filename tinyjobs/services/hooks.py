from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, TypeVar

from ..models import Job

logger = logging.getLogger("tinyjobs.hooks")

START = "start"
SUCCESS = "success"
FAILURE = "failure"

Listener = TypeVar("Listener", bound=Callable[..., Any])


class Hooks:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Callable[..., Any]]] = {
            START: [],
            SUCCESS: [],
            FAILURE: [],
        }

    def add(self, event: str, listener: Listener) -> Listener:
        self._listeners[event].append(listener)
        return listener

    async def started(self, job: Job) -> None:
        await self._emit(START, job)

    async def succeeded(self, job: Job, result: Any, duration: float) -> None:
        await self._emit(SUCCESS, job, result, duration)

    async def failed(self, job: Job, error: BaseException, will_retry: bool) -> None:
        await self._emit(FAILURE, job, error, will_retry)

    async def _emit(self, event: str, *args: Any) -> None:
        for listener in self._listeners[event]:
            try:
                outcome = listener(*args)
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception:
                logger.exception(
                    "on_job_%s hook %s raised; the job is unaffected",
                    event,
                    getattr(listener, "__qualname__", listener),
                )
