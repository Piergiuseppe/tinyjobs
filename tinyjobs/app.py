from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import time
import weakref
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    nullcontext,
    suppress,
)
from functools import partial
from typing import Any, AsyncIterator, Callable, Coroutine, Sequence, TypeVar

from .backends.base import Backend
from .backends.loader import check_options, load_backend
from .backends.managed import ManagedBackend
from .clock import Clock, FollowingClock
from .codecs import CodecRegistry
from .exceptions import (
    AppClosedError,
    BackendNotReadyError,
    ConfigurationError,
    UnsupportedCapabilityError,
)
from .executors import Spares
from .executors.thread import holds_spare, spared
from .models import Job, JobPayload, Page, ScheduleState, Stats
from .options import EnqueueOptions
from .registry import TaskRegistry
from .retention import RetentionPolicy
from .retry import RetryPolicy
from .runtime import (
    CLOSED_MESSAGE,
    LOOP_POLL_SECONDS,
    LoopRunner,
    after_fork_in_child,
    refuse_inside_loop,
    refuse_interrupted,
    drive,
    run_on,
    settled,
    tracked_lock,
    wake,
    when_collected,
)
from .scheduler import Scheduler
from .schedules import Schedule
from .services import (
    DispatchService,
    ExecutionService,
    Finding,
    HealthService,
    Hooks,
    JobLifecycleService,
    SchedulingService,
    SerializationService,
)
from .services.execution import current_runner
from .services.hooks import FAILURE, START, SUCCESS
from .status import JobStatus
from .task import Task
from .worker import Drain, EmbeddedWorker, Worker
from .worker.drain import DRAIN_SPARE_THREADS, InDrains

DEFAULT_URL = "sqlite:///tinyjobs.db"

WORKER_GRACE_SECONDS = 5.0
CLOSE_WAIT_SECONDS = 10.0

NOT_READY_MESSAGE = (
    "the backend is not connected yet; await app.ready() from async code "
    "or call app.start() from sync code"
)

EMBEDDED_SIGNALS_MESSAGE = (
    "an embedded worker installs no signal handlers: the server owns them, and "
    "leaving serve() or the lifespan is what stops the worker"
)

WRITING_CALLS = frozenset({"enqueue", "send", "cancel", "retry", "purge", "drain"})

T = TypeVar("T")

logger = logging.getLogger("tinyjobs.worker")

_apps: "weakref.WeakSet[TinyJobs]" = weakref.WeakSet()
_inherited: list[ManagedBackend] = []
Listener = TypeVar("Listener", bound=Callable[..., Any])
Attempt = tuple[concurrent.futures.Future, asyncio.AbstractEventLoop]


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
        eager: bool = False,
        **backend_options: Any,
    ) -> None:
        check_options(url, backend_options)
        self._url = url
        self._backend_options = backend_options
        self._default_queue = default_queue
        self._retention = retention or RetentionPolicy()
        self._eager = eager
        self._clock = FollowingClock() if clock is None else clock
        self._codecs = CodecRegistry(default=default_codec, allow_pickle=allow_pickle)
        self._registry = TaskRegistry()
        self._serialization = SerializationService(self._codecs)
        self._hooks = Hooks()
        self._runner = LoopRunner()
        when_collected(self, self._runner.close_here)
        self._backend: ManagedBackend | None = None
        self._dispatch: DispatchService | None = None
        self._lifecycle: JobLifecycleService | None = None
        self._execution: ExecutionService | None = None
        self._scheduling: SchedulingService | None = None
        self._health: HealthService | None = None
        self._connecting: Attempt | None = None
        self._lock_guard = tracked_lock()
        self._closed = False
        self._closes = 0
        self._backend_closing: concurrent.futures.Future | None = None
        self._sessions = 0
        self._workers: set[Worker | Drain] = set()
        self._thread_spares = Spares(DRAIN_SPARE_THREADS)
        self._in_drains = InDrains()
        self._closing_after: set[Worker | Drain] = set()
        _apps.add(self)

    @property
    def url(self) -> str:
        return self._url

    @property
    def thread_spares(self) -> Spares:
        return self._thread_spares

    @property
    def in_drains(self) -> InDrains:
        return self._in_drains

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def eager(self) -> bool:
        return self._eager

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
    def backend(self) -> ManagedBackend:
        return self._connected(self._backend)

    @property
    def dispatch(self) -> DispatchService:
        return self._connected(self._dispatch)

    @property
    def lifecycle(self) -> JobLifecycleService:
        return self._connected(self._lifecycle)

    @property
    def execution(self) -> ExecutionService:
        return self._connected(self._execution)

    @property
    def scheduling(self) -> SchedulingService:
        return self._connected(self._scheduling)

    @property
    def health(self) -> HealthService:
        return self._connected(self._health)

    def _connected(self, part: T | None) -> T:
        if part is not None:
            return part
        if self._closed or self._closes:
            raise AppClosedError(CLOSED_MESSAGE)
        raise BackendNotReadyError(NOT_READY_MESSAGE)

    def run_sync(
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
        *,
        name: str,
        async_name: str | None = None,
    ) -> T:
        if self._closed:
            raise AppClosedError(CLOSED_MESSAGE)
        if name in WRITING_CALLS:
            refuse = getattr(self._backend, "refuse_held_transaction", None)
            if refuse is not None:
                refuse()
        if holds_spare():
            factory = partial(_spared_call, factory)
        return self._runner.run(factory, name=name, async_name=async_name)

    def bind(self, job: Job | None) -> Job | None:
        return job.bound_to(self) if job is not None else None

    async def settle(self, job: Job) -> Job:
        bound = self.bind(job)
        if not self._eager:
            return bound
        while True:
            await Drain(self, quiet=True).run()
            bound = await bound.arefresh()
            draining = self._in_drains.running(bound.id)
            if bound.status is not JobStatus.RUNNING or draining is None:
                break
            await asyncio.wrap_future(draining)
        if self._left_for_a_spare(bound):
            logger.warning(
                "job %s stays queued: %d job thread(s) still run past their timeout, so jobs "
                "of sync tasks with a timeout wait until one returns and a later drain or "
                "eager call runs them",
                bound.id,
                self._thread_spares.limit,
            )
        return bound

    def _left_for_a_spare(self, job: Job) -> bool:
        if job.status is not JobStatus.QUEUED or not self._thread_spares.exhausted:
            return False
        if job.task not in self._registry or job.run_at > self._clock.now():
            return False
        task = self._registry.get(job.task)
        return not task.is_coroutine and task.timeout is not None

    async def ensure_connected(self) -> None:
        await settled()
        if self._closed:
            raise AppClosedError(CLOSED_MESSAGE)
        await self._connect()
        clock = self._clock
        if isinstance(clock, FollowingClock) and clock.following and clock.due():
            await self.backend.follow_clock()

    async def ready(self) -> None:
        await settled()
        self._closed = False
        await self._connect()

    async def _connect(self) -> None:
        attempt = await self._elect()
        if attempt is None:
            return
        try:
            raw = load_backend(self._url, clock=self._clock, **self._backend_options)
        except Exception as error:
            self._settle(attempt, error)
            raise
        try:
            if raw.capabilities.loop_bound:
                await self._connect_on_own_loop(raw, attempt)
            else:
                await self._attempt(raw, attempt)
        except BaseException:
            self._settle(attempt, None)
            raise

    async def _elect(self) -> concurrent.futures.Future | None:
        while True:
            with self._lock_guard:
                if self._backend is not None and self._backend.alive:
                    return None
                current = self._connecting
                if current is None or not current[1].is_running():
                    attempt: concurrent.futures.Future = concurrent.futures.Future()
                    self._connecting = (attempt, asyncio.get_running_loop())
                    if current is not None and not current[0].done():
                        current[0].set_result(None)
                    return attempt
            await self._outcome(current[0])

    async def _connect_on_own_loop(
        self, raw: Backend, attempt: concurrent.futures.Future
    ) -> None:
        home = self._runner.own_loop()
        with self._lock_guard:
            if self._connecting is not None and self._connecting[0] is attempt:
                self._connecting = (attempt, home)
        await run_on(home, self._attempt(raw, attempt), "connect")

    async def _outcome(self, attempt: concurrent.futures.Future) -> None:
        loop = asyncio.get_running_loop()
        waiting = loop.create_future()
        attempt.add_done_callback(partial(wake, loop, waiting))
        while not attempt.done():
            with self._lock_guard:
                current = self._connecting
            if current is None or current[0] is not attempt:
                break
            if not current[1].is_running():
                break
            await asyncio.wait({waiting}, timeout=LOOP_POLL_SECONDS)
        if attempt.done() and attempt.exception() is not None:
            raise attempt.exception()

    async def _attempt(self, raw: Backend, attempt: concurrent.futures.Future) -> None:
        try:
            await self._install(raw)
        except Exception as error:
            self._settle(attempt, error)
            raise
        except BaseException:
            self._settle(attempt, None)
            raise
        self._settle(attempt, None)

    def _settle(
        self, attempt: concurrent.futures.Future, error: Exception | None
    ) -> None:
        with self._lock_guard:
            if self._connecting is not None and self._connecting[0] is attempt:
                self._connecting = None
            if attempt.done():
                return
            if error is None:
                attempt.set_result(None)
            else:
                attempt.set_exception(error)

    async def _install(self, raw: Backend) -> None:
        backend = ManagedBackend(raw, self._runner)
        try:
            await backend.setup()
        except BaseException:
            with suppress(Exception):
                await backend.close()
            raise
        with self._lock_guard:
            closed = self._closed
            current = self._backend
            superseded = closed or (current is not None and current.alive)
            if not superseded:
                self._publish(backend)
        if superseded:
            with suppress(Exception):
                await backend.close()
        if closed:
            raise AppClosedError(CLOSED_MESSAGE)

    def _publish(self, backend: ManagedBackend) -> None:
        self._dispatch = DispatchService(
            backend,
            self._serialization,
            self._clock,
            self._default_queue,
            self._registry,
        )
        self._lifecycle = JobLifecycleService(backend, self._serialization, self._clock)
        self._execution = ExecutionService(
            self._serialization, self._lifecycle, self._hooks, self._clock
        )
        self._scheduling = SchedulingService(
            backend, self._dispatch, self._registry, self._clock
        )
        self._health = HealthService(backend, self._registry, self._clock)
        self._backend = backend
        self._closes = 0

    def forget_inherited_connection(self) -> None:
        self._thread_spares = Spares(DRAIN_SPARE_THREADS)
        self._in_drains = InDrains()
        self._workers = set()
        self._closing_after = set()
        if self._backend is not None:
            _inherited.append(self._backend)
        self._backend = None
        self._dispatch = None
        self._lifecycle = None
        self._execution = None
        self._scheduling = None
        self._health = None
        self._connecting = None
        self._backend_closing = None

    def start(self) -> "TinyJobs":
        self._runner.run(self.ready, name="start", async_name="ready")
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
        *,
        tx: Any = None,
        **options: Any,
    ) -> Job:
        payload = JobPayload(args=tuple(args), kwargs=dict(kwargs or {}))
        chosen = EnqueueOptions().merged_with(**options)
        if tx is not None:
            return await self.aenqueue_in(tx, name, payload, chosen)
        await self.ensure_connected()
        return await self.settle(
            await self.dispatch.dispatch_by_name(name, payload, chosen)
        )

    def send(
        self,
        name: str,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        tx: Any = None,
        **options: Any,
    ) -> Job:
        if tx is None:
            return self.run_sync(
                partial(self.asend, name, args, kwargs, **options), name="send"
            )
        payload = JobPayload(args=tuple(args), kwargs=dict(kwargs or {}))
        return self.enqueue_in(
            tx, name, payload, EnqueueOptions().merged_with(**options), call="send"
        )

    async def aenqueue_in(
        self, tx: Any, name: str, payload: JobPayload, options: EnqueueOptions
    ) -> Job:
        await self.ensure_connected()
        job = self._joining(name, payload, options)
        return self.bind((await self.backend.aenqueue_in(tx, [job]))[0])

    def enqueue_in(
        self,
        tx: Any,
        name: str,
        payload: JobPayload,
        options: EnqueueOptions,
        *,
        call: str = "enqueue",
    ) -> Job:
        self.run_sync(self.ensure_connected, name=call)
        job = self._joining(name, payload, options)
        return self.bind(self.backend.enqueue_in(tx, [job])[0])

    def _joining(self, name: str, payload: JobPayload, options: EnqueueOptions) -> Job:
        if self._eager:
            raise UnsupportedCapabilityError(
                "eager mode runs each job as it is sent, before any transaction "
                "could commit; tx is not available with eager=True"
            )
        if not self.backend.capabilities.transactional_enqueue:
            raise UnsupportedCapabilityError(
                "this backend does not support enqueueing inside a transaction"
            )
        return self.dispatch.prepare(name, payload, options)

    async def aget(self, job_id: str) -> Job | None:
        await self.ensure_connected()
        return self.bind(await self.backend.get(job_id))

    def get(self, job_id: str) -> Job | None:
        return self.run_sync(partial(self.aget, job_id), name="get")

    async def ajobs(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        await self.ensure_connected()
        page = await self.backend.list(
            status=status, queue=queue, limit=limit, cursor=cursor
        )
        for job in page.items:
            self.bind(job)
        return page

    def jobs(self, **options: Any) -> Page:
        return self.run_sync(partial(self.ajobs, **options), name="jobs")

    async def acancel(self, job_id: str) -> bool:
        await self.ensure_connected()
        return await self.backend.cancel(job_id)

    def cancel(self, job_id: str) -> bool:
        return self.run_sync(partial(self.acancel, job_id), name="cancel")

    async def aretry(self, job_id: str) -> bool:
        await self.ensure_connected()
        return await self.backend.requeue(job_id)

    def retry(self, job_id: str) -> bool:
        return self.run_sync(partial(self.aretry, job_id), name="retry")

    async def apurge(self, policy: RetentionPolicy | None = None) -> int | None:
        await self.ensure_connected()
        return await self.backend.purge(
            policy or self._retention, self._clock.now()
        )

    def purge(self, policy: RetentionPolicy | None = None) -> int | None:
        return self.run_sync(partial(self.apurge, policy), name="purge")

    async def acounts(self) -> dict[str, int]:
        await self.ensure_connected()
        return await self.backend.counts_by_status()

    def counts(self) -> dict[str, int]:
        return self.run_sync(self.acounts, name="counts")

    async def astats(self) -> Stats:
        await self.ensure_connected()
        return await self.health.stats()

    def stats(self) -> Stats:
        return self.run_sync(self.astats, name="stats")

    async def acheck(self, *, max_wait: float | None = None) -> list[Finding]:
        await self.ensure_connected()
        return await self.health.check(max_wait=max_wait)

    def check(self, *, max_wait: float | None = None) -> list[Finding]:
        return self.run_sync(partial(self.acheck, max_wait=max_wait), name="check")

    async def adrain(self, queues: Sequence[str] | None = None) -> int:
        return await Drain(self, queues).run()

    def drain(self, queues: Sequence[str] | None = None) -> int:
        try:
            return self.run_sync(partial(self.adrain, queues), name="drain")
        finally:
            if self._closed:
                self._runner.close()

    async def aexecutions(self, job_id: str) -> list:
        await self.ensure_connected()
        return await self.backend.executions(job_id)

    def executions(self, job_id: str) -> list:
        return self.run_sync(partial(self.aexecutions, job_id), name="executions")

    async def aschedules(self) -> list[ScheduleState]:
        await self.ensure_connected()
        return await self.backend.schedules()

    def schedules(self) -> list[ScheduleState]:
        return self.run_sync(self.aschedules, name="schedules")

    def decode(self, job: Job) -> JobPayload:
        return self._serialization.decode_payload(job)

    def worker(self, **options: Any) -> Worker:
        options.setdefault("retention", self._retention)
        return Worker(self, **options)

    def scheduler(self, **options: Any) -> Scheduler:
        return Scheduler(self, **options)

    @asynccontextmanager
    async def serve(
        self, *, worker: bool = False, **worker_options: Any
    ) -> AsyncIterator["TinyJobs"]:
        _assert_worker_requested(worker, worker_options)
        if worker_options.pop("handle_signals", False):
            raise ConfigurationError(EMBEDDED_SIGNALS_MESSAGE)
        embedded = (
            EmbeddedWorker(self.worker(**worker_options, handle_signals=False))
            if worker
            else nullcontext()
        )
        async with self, embedded:
            yield self

    def lifespan(
        self, *, worker: bool = False, **worker_options: Any
    ) -> Callable[[Any], AbstractAsyncContextManager[None]]:
        _assert_worker_requested(worker, worker_options)

        @asynccontextmanager
        async def bind(_: Any) -> AsyncIterator[None]:
            async with self.serve(worker=worker, **worker_options):
                yield

        return bind

    def attach(self, worker: Worker | Drain) -> None:
        with self._lock_guard:
            self._workers.add(worker)

    def detach(self, worker: Worker | Drain) -> bool:
        with self._lock_guard:
            self._workers.discard(worker)
            if worker not in self._closing_after:
                return False
            self._closing_after.discard(worker)
            return True

    async def aclose(self) -> None:
        await settled()
        await self._close(current_runner.get(None))

    async def _close(self, inside: object | None) -> None:
        if self._defer_close(inside):
            self._signal_workers()
            return
        try:
            await self._stop_workers()
            await self._shutdown()
        finally:
            if not self._runner.on_own_loop():
                self._runner.stop_own()

    def _defer_close(self, inside: object | None) -> bool:
        with self._lock_guard:
            if inside not in self._workers:
                return False
            self._closing_after.add(inside)
            return True

    def _closes_later(self, inside: object | None) -> bool:
        with self._lock_guard:
            return inside is not None and inside in self._closing_after

    def _signal_workers(self) -> list[Worker | Drain]:
        with self._lock_guard:
            workers = list(self._workers)
        for worker in workers:
            worker.stop()
        return workers

    async def _stop_workers(self) -> None:
        waiting = self._signal_workers()
        if not waiting:
            return
        deadline = time.monotonic() + WORKER_GRACE_SECONDS + max(
            worker.shutdown_timeout for worker in waiting
        )
        while time.monotonic() < deadline and not all(
            worker.finished for worker in waiting
        ):
            await asyncio.sleep(LOOP_POLL_SECONDS / 10)

    async def _shutdown(self) -> None:
        with self._lock_guard:
            backend, self._backend = self._backend, None
            self._dispatch = None
            self._lifecycle = None
            self._execution = None
            self._scheduling = None
            self._health = None
            self._closed = True
            self._closes += 1
            closing = self._backend_closing
            if backend is not None:
                closing = self._backend_closing = concurrent.futures.Future()
        if backend is None:
            while closing is not None and not closing.done():
                await asyncio.sleep(LOOP_POLL_SECONDS / 10)
            return
        try:
            await backend.close()
        finally:
            closing.set_result(None)

    def close(self) -> None:
        refuse_inside_loop("close")
        refuse_interrupted("close")
        drive(partial(self._close_app, current_runner.get(None)), name="close")

    def _close_app(self, inside: object | None) -> None:
        try:
            if self._backend is not None:
                with suppress(AppClosedError):
                    self._runner.run(partial(self._close, inside), name="close")
            elif self._backend_closing is not None:
                concurrent.futures.wait([self._backend_closing], timeout=CLOSE_WAIT_SECONDS)
        finally:
            if not self._closes_later(inside):
                self._closed = True
                self._runner.close_here()

    async def __aenter__(self) -> "TinyJobs":
        self._sessions += 1
        try:
            await self.ready()
        except BaseException:
            self._sessions -= 1
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._sessions -= 1
        if self._sessions == 0:
            await self.aclose()

    def __enter__(self) -> "TinyJobs":
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _forget_inherited_connections() -> None:
    for app in list(_apps):
        app.forget_inherited_connection()


after_fork_in_child(_forget_inherited_connections)


def _declare_schedule(
    task: str, cron: str | None, timezone: str, misfire: str, overlap: bool
) -> Schedule | None:
    if cron is None:
        return None
    return Schedule.declare(
        task, cron, timezone=timezone, misfire=misfire, overlap=overlap
    )


def _assert_worker_requested(worker: bool, options: dict[str, Any]) -> None:
    if options and not worker:
        raise ConfigurationError(
            f"worker options {sorted(options)} were given without worker=True, "
            f"so no worker would run to use them"
        )


def _assert_callable_without_arguments(task: Task) -> None:
    try:
        task.bind((), {})
    except TypeError:
        raise ConfigurationError(
            f"{task.name} runs on a cron schedule, which calls it with no "
            f"arguments, but its signature requires some"
        ) from None


def _spared_call(factory: Callable[[], Coroutine[Any, Any, T]]) -> Coroutine[Any, Any, T]:
    return spared(factory())
