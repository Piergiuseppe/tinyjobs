from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol, Sequence, runtime_checkable

from ..models import Execution, ExecutionError, Job, Page, ScheduleState, Stats
from ..retention import RetentionPolicy
from ..status import ExecutionStatus, JobStatus


LEASE_EXPIRED_MESSAGE = (
    "lease expired: the worker holding this job stopped reporting"
)


@dataclass(frozen=True, slots=True)
class Capabilities:
    priority: bool = True
    delayed: bool = True
    dedup: bool = False
    transactional_enqueue: bool = False
    retention: bool = False
    schedules: bool = False
    max_payload_bytes: int | None = None
    loop_bound: bool = False


@runtime_checkable
class Backend(Protocol):
    capabilities: Capabilities

    async def setup(self) -> None: ...

    async def close(self) -> None: ...

    async def enqueue(self, job: Job) -> Job: ...

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]: ...

    async def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]: ...

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> None: ...

    async def complete(self, job: Job, result: bytes | None) -> bool: ...

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool: ...

    async def release(self, job: Job, delay: float = 0.0) -> bool: ...

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool: ...

    async def cancel(self, job_id: str) -> bool: ...

    async def requeue(self, job_id: str) -> bool: ...

    async def get(self, job_id: str) -> Job | None: ...

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page: ...

    async def executions(self, job_id: str) -> list[Execution]: ...

    async def reap_expired(self) -> int: ...

    async def counts_by_status(self) -> dict[str, int]: ...

    async def stats(self, now: float) -> Stats: ...

    async def purge(
        self, policy: RetentionPolicy, now: float
    ) -> int | None: ...

    async def schedules(self) -> list[ScheduleState]: ...

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None: ...

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None: ...


class BaseBackend(ABC):
    capabilities: Capabilities = Capabilities()

    async def setup(self) -> None:
        return None

    async def close(self) -> None:
        return None

    @abstractmethod
    async def enqueue(self, job: Job) -> Job: ...

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        return [await self.enqueue(job) for job in jobs]

    async def hand_back(self, job: Job, delay: float = 0.0) -> bool:
        return await self.release(job, delay)

    async def purge(self, policy: RetentionPolicy, now: float) -> int | None:
        return None

    async def schedules(self) -> list[ScheduleState]:
        return []

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None:
        return None

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        return None

    async def __aenter__(self) -> "BaseBackend":
        await self.setup()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
