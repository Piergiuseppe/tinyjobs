from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol, Sequence, runtime_checkable

from ..models import ExecutionError, Job, Page
from ..status import JobStatus


@dataclass(frozen=True, slots=True)
class Capabilities:
    priority: bool = True
    delayed: bool = True
    dedup: bool = False
    transactional_enqueue: bool = False
    max_payload_bytes: int | None = None


@runtime_checkable
class Backend(Protocol):
    capabilities: Capabilities

    def setup(self) -> None: ...

    def close(self) -> None: ...

    def transaction(self) -> AbstractContextManager[Any]: ...

    def enqueue(self, job: Job) -> Job: ...

    def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]: ...

    def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]: ...

    def heartbeat(self, job_ids: Sequence[str], lease_seconds: float) -> None: ...

    def complete(self, job: Job, result: bytes | None) -> bool: ...

    def fail(
        self, job: Job, error: ExecutionError, retry_at: float | None
    ) -> bool: ...

    def release(self, job: Job, delay: float = 0.0) -> bool: ...

    def cancel(self, job_id: str) -> bool: ...

    def get(self, job_id: str) -> Job | None: ...

    def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page: ...

    def requeue(self, job_id: str) -> bool: ...

    def reap_expired(self) -> int: ...


class BaseBackend(ABC):
    capabilities: Capabilities = Capabilities()

    def setup(self) -> None:
        return None

    def close(self) -> None:
        return None

    @abstractmethod
    def transaction(self) -> AbstractContextManager[Any]: ...

    @abstractmethod
    def enqueue(self, job: Job) -> Job: ...

    def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        with self.transaction():
            return [self.enqueue(job) for job in jobs]

    def __enter__(self) -> "BaseBackend":
        self.setup()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
