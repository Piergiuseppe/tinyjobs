from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, TypeVar

from ..logs import job_fields
from ..metrics import Metrics
from ..models import Job

logger = logging.getLogger("tinyjobs.hooks")

START = "start"
SUCCESS = "success"
FAILURE = "failure"
END = "end"
LISTENER_NAMES = {
    START: "on_job_start hook",
    SUCCESS: "on_job_success hook",
    FAILURE: "on_job_failure hook",
    END: "job-end listener",
}

Listener = TypeVar("Listener", bound=Callable[..., Any])


class Hooks:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Callable[..., Any]]] = {
            START: [],
            SUCCESS: [],
            FAILURE: [],
            END: [],
        }
        self._metrics = Metrics()
        self.add(START, self._metrics.started)
        self.add(SUCCESS, self._metrics.succeeded)
        self.add(FAILURE, self._metrics.failed)
        self.add(END, self._metrics.ended)

    def add(self, event: str, listener: Listener) -> Listener:
        self._listeners[event].append(listener)
        return listener

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    async def started(self, job: Job) -> None:
        await self._emit(START, job)

    async def succeeded(self, job: Job, result: Any, duration: float) -> None:
        await self._emit(SUCCESS, job, result, duration)

    async def failed(self, job: Job, error: BaseException, will_retry: bool) -> None:
        await self._emit(FAILURE, job, error, will_retry)

    def ended(self, job: Job) -> None:
        for listener in self._listeners[END]:
            try:
                listener(job)
            except Exception:
                self._report(END, listener, job)

    async def _emit(self, event: str, *args: Any) -> None:
        for listener in self._listeners[event]:
            try:
                outcome = listener(*args)
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception:
                self._report(event, listener, args[0])

    def _report(self, event: str, listener: Callable[..., Any], job: Job) -> None:
        logger.exception(
            "%s %s raised; the job is unaffected",
            LISTENER_NAMES[event],
            getattr(listener, "__qualname__", listener),
            extra=job_fields(job),
        )
