from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field, fields
from functools import partial
from typing import Any, TYPE_CHECKING

from .exceptions import JobNotFoundError, TinyJobsError, WaitTimeout
from .polling import AdaptiveInterval
from .status import ExecutionStatus, JobStatus

if TYPE_CHECKING:
    from .app import TinyJobs

UNBOUND_MESSAGE = (
    "this Job was not handed out by a TinyJobs app, so it has nothing to talk "
    "to; jobs returned by enqueue(), app.get() and app.jobs() are bound"
)
IMMUTABLE_FIELDS = frozenset({"id", "task", "payload", "codec", "created_at", "_app"})


@dataclass(slots=True)
class JobPayload:
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Job:
    id: str
    task: str
    payload: bytes
    codec: str
    queue: str
    status: JobStatus
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 1
    run_at: float = 0.0
    created_at: float = 0.0
    finished_at: float | None = None
    lease_expires_at: float | None = None
    worker_id: str | None = None
    dedup_key: str | None = None
    result_payload: bytes | None = None
    last_error: str | None = None
    hand_backs: int = 0
    claims: int = 0
    _app: "TinyJobs | None" = field(default=None, compare=False, repr=False)

    @property
    def attempts_remaining(self) -> int:
        return max(self.max_attempts - self.attempts, 0)

    def bound_to(self, app: "TinyJobs") -> "Job":
        self._app = app
        return self

    def refresh(self) -> "Job":
        return self._app_or_fail().run_sync(self.arefresh, name="refresh")

    async def arefresh(self) -> "Job":
        fresh = await self._app_or_fail().aget(self.id)
        if fresh is None:
            raise JobNotFoundError(
                f"job {self.id} is no longer in the store; it may have been purged"
            )
        return self._absorb(fresh)

    def wait(
        self,
        timeout: float | None = None,
        *,
        poll_minimum: float = 0.02,
        poll_maximum: float = 1.0,
    ) -> "Job":
        return self._app_or_fail().run_sync(
            partial(
                self.awaited,
                timeout,
                poll_minimum=poll_minimum,
                poll_maximum=poll_maximum,
            ),
            name="wait",
            async_name="awaited",
        )

    async def awaited(
        self,
        timeout: float | None = None,
        *,
        poll_minimum: float = 0.02,
        poll_maximum: float = 1.0,
    ) -> "Job":
        interval = AdaptiveInterval(minimum=poll_minimum, maximum=poll_maximum)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            await self.arefresh()
            if self.status.is_terminal:
                return self
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise WaitTimeout(
                    f"job {self.id} was still {self.status} after {timeout}s"
                )
            pause = interval.next()
            await asyncio.sleep(pause if remaining is None else min(pause, remaining))

    def result(self) -> Any:
        if self.result_payload is None:
            return None
        app = self._app_or_fail()
        return app.serialization.decode_result(self.result_payload, self.codec)

    def _absorb(self, other: "Job") -> "Job":
        for entry in fields(self):
            if entry.name not in IMMUTABLE_FIELDS:
                setattr(self, entry.name, getattr(other, entry.name))
        return self

    def _app_or_fail(self) -> "TinyJobs":
        if self._app is None:
            raise TinyJobsError(UNBOUND_MESSAGE)
        return self._app


@dataclass(slots=True)
class Execution:
    job_id: str
    attempt: int
    status: ExecutionStatus
    worker_id: str
    started_at: float
    id: int | None = None
    finished_at: float | None = None
    error_type: str | None = None
    traceback: str | None = None
    claim: int | None = None

    @property
    def duration(self) -> float | None:
        if self.finished_at is None:
            return None
        return self.finished_at - self.started_at


@dataclass(frozen=True, slots=True)
class ExecutionError:
    type: str
    message: str
    traceback: str

    @property
    def summary(self) -> str:
        return f"{self.type}: {self.message}" if self.message else self.type


@dataclass(slots=True)
class ScheduleState:
    name: str
    cron: str
    timezone: str
    next_run_at: float
    last_run_at: float | None = None
    last_job_id: str | None = None


@dataclass(frozen=True, slots=True)
class QueueStats:
    queue: str
    counts: dict[str, int]
    oldest_due_at: float | None = None


@dataclass(frozen=True, slots=True)
class Stats:
    queues: list[QueueStats]
    expired_leases: int = 0
    queued_tasks: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Page:
    items: list[Job]
    cursor: str | None = None
