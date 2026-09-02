from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, Coroutine, Sequence, TypeVar

from .backends.base import Backend
from .backends.loader import load_backend
from .clock import Clock
from .codecs import CodecRegistry
from .exceptions import BackendNotReadyError
from .models import Job, JobPayload, Page
from .options import EnqueueOptions
from .registry import TaskRegistry
from .retry import RetryPolicy
from .runtime import LoopRunner
from .services import DispatchService, JobLifecycleService, SerializationService
from .status import JobStatus
from .task import Task
from .worker import Worker

DEFAULT_URL = "sqlite:///tinyjobs.db"

T = TypeVar("T")


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
        self._runner = LoopRunner()
        self._backend: Backend | None = None
        self._dispatch: DispatchService | None = None
        self._lifecycle: JobLifecycleService | None = None
        self._lock: asyncio.Lock | None = None
        self._lock_guard = threading.Lock()

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
            raise BackendNotReadyError(
                "the backend is not connected yet; await app.ready() from async code "
                "or call app.start() from sync code"
            )
        return self._backend

    @property
    def dispatch(self) -> DispatchService:
        if self._dispatch is None:
            raise BackendNotReadyError("the backend is not connected yet")
        return self._dispatch

    @property
    def lifecycle(self) -> JobLifecycleService:
        if self._lifecycle is None:
            raise BackendNotReadyError("the backend is not connected yet")
        return self._lifecycle

    def run_sync(self, coroutine: Coroutine[Any, Any, T], *, name: str) -> T:
        return self._runner.run(coroutine, name=name)

    def bind(self, job: Job | None) -> Job | None:
        return job.bound_to(self) if job is not None else None

    async def ready(self) -> None:
        if self._backend is not None:
            return
        with self._lock_guard:
            if self._lock is None:
                self._lock = asyncio.Lock()
        async with self._lock:
            if self._backend is not None:
                return
            backend = load_backend(
                self._url, clock=self._clock, **self._backend_options
            )
            await backend.setup()
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

    def start(self) -> "TinyJobs":
        self.run_sync(self.ready(), name="start")
        return self

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

    async def asend(
        self,
        name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        **options: Any,
    ) -> Job:
        await self.ready()
        payload = JobPayload(args=tuple(args), kwargs=dict(kwargs or {}))
        return self.bind(
            await self.dispatch.dispatch_by_name(
                name, payload, EnqueueOptions().merged_with(**options)
            )
        )

    def send(self, name: str, *args: Any, **kwargs: Any) -> Job:
        return self.run_sync(self.asend(name, *args, **kwargs), name="send")

    async def aget(self, job_id: str) -> Job | None:
        await self.ready()
        return self.bind(await self.backend.get(job_id))

    def get(self, job_id: str) -> Job | None:
        return self.run_sync(self.aget(job_id), name="get")

    async def ajobs(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        await self.ready()
        page = await self.backend.list(
            status=status, queue=queue, limit=limit, cursor=cursor
        )
        for job in page.items:
            self.bind(job)
        return page

    def jobs(self, **options: Any) -> Page:
        return self.run_sync(self.ajobs(**options), name="jobs")

    async def acancel(self, job_id: str) -> bool:
        await self.ready()
        return await self.backend.cancel(job_id)

    def cancel(self, job_id: str) -> bool:
        return self.run_sync(self.acancel(job_id), name="cancel")

    async def aretry(self, job_id: str) -> bool:
        await self.ready()
        return await self.backend.requeue(job_id)

    def retry(self, job_id: str) -> bool:
        return self.run_sync(self.aretry(job_id), name="retry")

    async def aexecutions(self, job_id: str) -> list:
        await self.ready()
        return await self.backend.executions(job_id)

    def executions(self, job_id: str) -> list:
        return self.run_sync(self.aexecutions(job_id), name="executions")

    def decode(self, job: Job) -> JobPayload:
        return self._serialization.decode_payload(job)

    def worker(self, **options: Any) -> Worker:
        return Worker(self, **options)

    async def aclose(self) -> None:
        if self._backend is not None:
            await self._backend.close()
        self._backend = None
        self._dispatch = None
        self._lifecycle = None
        self._lock = None

    def close(self) -> None:
        if self._backend is not None:
            self.run_sync(self.aclose(), name="close")
        self._runner.close()

    async def __aenter__(self) -> "TinyJobs":
        await self.ready()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def __enter__(self) -> "TinyJobs":
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.close()
