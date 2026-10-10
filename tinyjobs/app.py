from __future__ import annotations

import asyncio
import threading
from typing import Any, Callable, Coroutine, Sequence, TypeVar

from .backends.base import Backend
from .backends.loader import load_backend
from .clock import Clock
from .codecs import CodecRegistry
from .exceptions import BackendNotReadyError, ConfigurationError
from .models import Job, JobPayload, Page, ScheduleState
from .options import EnqueueOptions
from .registry import TaskRegistry
from .retention import RetentionPolicy
from .retry import RetryPolicy
from .runtime import LoopRunner
from .scheduler import Scheduler
from .schedules import Schedule
from .services import (
    DispatchService,
    ExecutionService,
    Hooks,
    JobLifecycleService,
    SchedulingService,
    SerializationService,
)
from .services.hooks import FAILURE, START, SUCCESS
from .status import JobStatus
from .task import Task
from .worker import Worker

DEFAULT_URL = "sqlite:///tinyjobs.db"

T = TypeVar("T")
Listener = TypeVar("Listener", bound=Callable[..., Any])


class TinyJobs:
    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        default_queue: str = "default",
        default_codec: str = "json",
        allow_pickle: bool = False,
        retention: RetentionPolicy | None = None,
        clock: Clock | None = None,
        **backend_options: Any,
    ) -> None:
        self._url = url
        self._backend_options = backend_options
        self._default_queue = default_queue
        self._retention = retention or RetentionPolicy()
        self._clock = clock or Clock()
        self._codecs = CodecRegistry(default=default_codec, allow_pickle=allow_pickle)
        self._registry = TaskRegistry()
        self._serialization = SerializationService(self._codecs)
        self._hooks = Hooks()
        self._runner = LoopRunner()
        self._backend: Backend | None = None
        self._dispatch: DispatchService | None = None
        self._lifecycle: JobLifecycleService | None = None
        self._execution: ExecutionService | None = None
        self._scheduling: SchedulingService | None = None
        self._lock: asyncio.Lock | None = None
        self._lock_guard = threading.Lock()

    @property
    def url(self) -> str:
        return self._url

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def retention(self) -> RetentionPolicy:
        return self._retention

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

    @property
    def execution(self) -> ExecutionService:
        if self._execution is None:
            raise BackendNotReadyError("the backend is not connected yet")
        return self._execution

    @property
    def scheduling(self) -> SchedulingService:
        if self._scheduling is None:
            raise BackendNotReadyError("the backend is not connected yet")
        return self._scheduling

    def run_sync(
        self,
        coroutine: Coroutine[Any, Any, T],
        *,
        name: str,
        async_name: str | None = None,
    ) -> T:
        return self._runner.run(coroutine, name=name, async_name=async_name)

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
            self._execution = ExecutionService(
                self._serialization, self._lifecycle, self._hooks, self._clock
            )
            self._scheduling = SchedulingService(
                backend, self._dispatch, self._registry, self._clock
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
        cron: str | None = None,
        timezone: str = "UTC",
        misfire: str = "skip",
        overlap: bool = False,
    ) -> Any:
        def decorate(target: Callable[..., Any]) -> Task:
            task_name = name or f"{target.__module__}.{target.__qualname__}"
            resolved = Task(
                self,
                target,
                name=task_name,
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
                schedule=_declare_schedule(
                    task_name, cron, timezone, misfire, overlap
                ),
            )
            if cron is not None:
                _assert_callable_without_arguments(resolved)
            self._registry.register(resolved)
            return resolved

        return decorate(function) if function is not None else decorate

    def on_job_start(self, listener: Listener) -> Listener:
        return self._hooks.add(START, listener)

    def on_job_success(self, listener: Listener) -> Listener:
        return self._hooks.add(SUCCESS, listener)

    def on_job_failure(self, listener: Listener) -> Listener:
        return self._hooks.add(FAILURE, listener)

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

    async def apurge(self, policy: RetentionPolicy | None = None) -> int | None:
        await self.ready()
        return await self.backend.purge(
            policy or self._retention, self._clock.now()
        )

    def purge(self, policy: RetentionPolicy | None = None) -> int | None:
        return self.run_sync(self.apurge(policy), name="purge")

    async def acounts(self) -> dict[str, int]:
        await self.ready()
        return await self.backend.counts_by_status()

    def counts(self) -> dict[str, int]:
        return self.run_sync(self.acounts(), name="counts")

    async def aexecutions(self, job_id: str) -> list:
        await self.ready()
        return await self.backend.executions(job_id)

    def executions(self, job_id: str) -> list:
        return self.run_sync(self.aexecutions(job_id), name="executions")

    async def aschedules(self) -> list[ScheduleState]:
        await self.ready()
        return await self.backend.schedules()

    def schedules(self) -> list[ScheduleState]:
        return self.run_sync(self.aschedules(), name="schedules")

    def decode(self, job: Job) -> JobPayload:
        return self._serialization.decode_payload(job)

    def worker(self, **options: Any) -> Worker:
        options.setdefault("retention", self._retention)
        return Worker(self, **options)

    def scheduler(self, **options: Any) -> Scheduler:
        return Scheduler(self, **options)

    async def aclose(self) -> None:
        if self._backend is not None:
            await self._backend.close()
        self._backend = None
        self._dispatch = None
        self._lifecycle = None
        self._execution = None
        self._scheduling = None
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


def _declare_schedule(
    task: str, cron: str | None, timezone: str, misfire: str, overlap: bool
) -> Schedule | None:
    if cron is None:
        return None
    return Schedule.declare(
        task, cron, timezone=timezone, misfire=misfire, overlap=overlap
    )


def _assert_callable_without_arguments(task: Task) -> None:
    try:
        task.bind((), {})
    except TypeError:
        raise ConfigurationError(
            f"{task.name} runs on a cron schedule, which calls it with no "
            f"arguments, but its signature requires some"
        ) from None
