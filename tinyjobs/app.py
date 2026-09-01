from __future__ import annotations

import threading
from typing import Any, Callable, Sequence

from .backends.base import Backend
from .backends.loader import load_backend
from .clock import Clock
from .codecs import CodecRegistry
from .models import Job, JobPayload, Page
from .options import EnqueueOptions
from .registry import TaskRegistry
from .retry import RetryPolicy
from .services import DispatchService, JobLifecycleService, SerializationService
from .status import JobStatus
from .task import Task
from .worker import Worker

DEFAULT_URL = "sqlite:///tinyjobs.db"


class TinyJobs:
    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        default_queue: str = "default",
        default_codec: str = "json",
        allow_pickle: bool = False,
        clock: Clock | None = None,
        **backend_options: Any,
    ) -> None:
        self._url = url
        self._backend_options = backend_options
        self._default_queue = default_queue
        self._clock = clock or Clock()
        self._codecs = CodecRegistry(default=default_codec, allow_pickle=allow_pickle)
        self._registry = TaskRegistry()
        self._serialization = SerializationService(self._codecs)
        self._backend: Backend | None = None
        self._dispatch: DispatchService | None = None
        self._lifecycle: JobLifecycleService | None = None
        self._guard = threading.Lock()

    @property
    def url(self) -> str:
        return self._url

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def codec(self) -> CodecRegistry:
        return self._codecs

    @property
    def registry(self) -> TaskRegistry:
        return self._registry

    @property
    def serialization(self) -> SerializationService:
        return self._serialization

    @property
    def backend(self) -> Backend:
        if self._backend is None:
            self._initialize()
        return self._backend

    @property
    def dispatch(self) -> DispatchService:
        if self._dispatch is None:
            self._initialize()
        return self._dispatch

    @property
    def lifecycle(self) -> JobLifecycleService:
        if self._lifecycle is None:
            self._initialize()
        return self._lifecycle

    def task(
        self,
        function: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        priority: int | None = None,
        codec: str | None = None,
        timeout: float | None = None,
        store_result: bool = False,
        max_attempts: int = 1,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        retry_backoff: Any = "exponential",
        retry_base: float = 1.0,
        retry_max: float = 600.0,
        retry_jitter: bool = True,
    ) -> Any:
        def decorate(target: Callable[..., Any]) -> Task:
            resolved = Task(
                self,
                target,
                name=name or f"{target.__module__}.{target.__qualname__}",
                options=EnqueueOptions(
                    queue=queue,
                    priority=priority,
                    max_attempts=max_attempts,
                    codec=codec,
                ),
                retry_policy=RetryPolicy(
                    max_attempts=max_attempts,
                    retry_on=retry_on,
                    backoff=retry_backoff,
                    base=retry_base,
                    maximum=retry_max,
                    jitter=retry_jitter,
                ),
                timeout=timeout,
                store_result=store_result,
            )
            self._registry.register(resolved)
            return resolved

        return decorate(function) if function is not None else decorate

    def send(
        self,
        name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        **options: Any,
    ) -> Job:
        payload = JobPayload(args=tuple(args), kwargs=dict(kwargs or {}))
        return self.dispatch.dispatch_by_name(
            name, payload, EnqueueOptions().merged_with(**options)
        )

    def get(self, job_id: str) -> Job | None:
        return self.backend.get(job_id)

    def jobs(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        return self.backend.list(
            status=status, queue=queue, limit=limit, cursor=cursor
        )

    def cancel(self, job_id: str) -> bool:
        return self.backend.cancel(job_id)

    def retry(self, job_id: str) -> bool:
        return self.backend.requeue(job_id)

    def decode(self, job: Job) -> JobPayload:
        return self._serialization.decode_payload(job)

    def worker(self, **options: Any) -> Worker:
        return Worker(self, **options)

    def close(self) -> None:
        if self._backend is not None:
            self._backend.close()
            self._backend = None
            self._dispatch = None
            self._lifecycle = None

    def __enter__(self) -> "TinyJobs":
        self.backend
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _initialize(self) -> None:
        with self._guard:
            if self._backend is not None:
                return
            backend = load_backend(
                self._url, clock=self._clock, **self._backend_options
            )
            backend.setup()
            self._dispatch = DispatchService(
                backend,
                self._serialization,
                self._clock,
                self._default_queue,
                self._registry,
            )
            self._lifecycle = JobLifecycleService(
                backend, self._serialization, self._clock
            )
            self._backend = backend
