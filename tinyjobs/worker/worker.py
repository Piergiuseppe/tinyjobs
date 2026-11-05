from __future__ import annotations

import asyncio
import contextvars
import logging
import math
import os
import sys
import sysconfig
import threading
import time
from contextlib import suppress
from dataclasses import replace
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Collection, Sequence

from ..exceptions import (
    AppClosedError,
    BackendNotReadyError,
    ConfigurationError,
    ExecutorClosedError,
    ForcedInterrupt,
    UnknownTaskError,
)
from ..ids import new_worker_id
from ..models import Job
from ..retention import RetentionPolicy
from ..retry import RetryPolicy
from ..runtime import inside_running_loop
from ..deadlines import Deadline, bounded, current, final, finishing, forget_loop, under
from ..duration import format_seconds
from ..executors import ExecutorResolver
from ..executors.process import assert_process_setup
from ..executors.thread import DaemonThreadPool
from ..flag import Flag
from ..logs import failure_fields, job_fields
from ..polling import AdaptiveInterval
from ..services.execution import current_runner
from ..services.lifecycle import VERIFY_SECONDS
from .lanes import Lanes, shares
from .signals import RunInterrupts, ShutdownWatcher

if TYPE_CHECKING:
    from ..app import TinyJobs

RELEASE_SECONDS = 1.0
RELEASE_SHARE = 4
CLOSE_SECONDS = 0.5
SETTLING_TICKS = 8
DEFAULT_EXECUTOR_THREADS = min(32, (os.cpu_count() or 1) + 4)
LOOPS_MESSAGE = "loops must be at least 1 and at most concurrency, got loops={loops} for concurrency={concurrency}"
PROCESSES_MESSAGE = (
    "processes must be at least 1, and at least loops so that every loop gets one, "
    "got processes={processes} for loops={loops}"
)
JOBS_PER_PROCESS_MESSAGE = "jobs_per_process must be at least 1, got {jobs}"
GIL_MESSAGE = (
    "this free-threaded Python runs with the GIL enabled, most likely because an extension "
    "module turned it back on, such as hiredis; uninstall it or set PYTHON_GIL=0 so the "
    "worker's threads and loops run in parallel"
)
ASYNC_LOOPS_MESSAGE = (
    "async_concurrency is shared out among the loops, so it must be at least loops, "
    "got async_concurrency={concurrency} for loops={loops}"
)
LOOP_BOUND_MESSAGE = (
    "loops above 1 need a backend reached over the network, such as redis or postgres, "
    "and a worker started with worker.run() or tinyjobs worker, "
    "since each loop opens its own connection"
)

_left_behind: set[asyncio.Future] = set()

logger = logging.getLogger("tinyjobs.worker")


class Worker:
    def __init__(
        self,
        app: "TinyJobs",
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 8,
        async_concurrency: int | None = None,
        lease_seconds: float = 60.0,
        reap_interval: float = 30.0,
        shutdown_timeout: float = 30.0,
        unknown_task_delay: float = 60.0,
        unknown_task_max_delay: float = 3600.0,
        retention: RetentionPolicy | None = None,
        scheduler: bool = True,
        misfire_grace: float = 60.0,
        poll_minimum: float = 0.005,
        poll_maximum: float = 0.5,
        worker_id: str | None = None,
        handle_signals: bool = True,
        loops: int = 1,
        processes: int | None = None,
        jobs_per_process: int | None = None,
        process_setup: Callable[[], Any] | None = None,
    ) -> None:
        if loops < 1 or (loops > 1 and loops > concurrency):
            raise ConfigurationError(LOOPS_MESSAGE.format(loops=loops, concurrency=concurrency))
        if processes is None:
            processes = max(loops, min(concurrency, os.cpu_count() or 1))
        elif processes < max(1, loops):
            raise ConfigurationError(
                PROCESSES_MESSAGE.format(processes=processes, loops=loops)
            )
        if jobs_per_process is not None and jobs_per_process < 1:
            raise ConfigurationError(JOBS_PER_PROCESS_MESSAGE.format(jobs=jobs_per_process))
        if process_setup is None:
            process_setup = app.process_setup
        elif process_setup is not None:
            assert_process_setup(process_setup)
        if loops > 1 and async_concurrency is not None and async_concurrency < loops:
            raise ConfigurationError(
                ASYNC_LOOPS_MESSAGE.format(loops=loops, concurrency=async_concurrency)
            )
        self._app = app
        self._queues = tuple(queues)
        sizes = shares(concurrency, loops)
        async_sizes = (
            [None] * loops
            if async_concurrency is None
            else shares(async_concurrency, loops)
        )
        concurrency, async_concurrency = sizes[0], async_sizes[0]
        process_sizes = shares(processes, loops)
        self._lane_options = [
            dict(
                queues=self._queues,
                concurrency=size,
                async_concurrency=async_size,
                processes=process_size,
                jobs_per_process=jobs_per_process,
                process_setup=process_setup,
                lease_seconds=lease_seconds,
                reap_interval=reap_interval,
                shutdown_timeout=shutdown_timeout,
                unknown_task_delay=unknown_task_delay,
                unknown_task_max_delay=unknown_task_max_delay,
                scheduler=False,
                poll_minimum=poll_minimum,
                poll_maximum=poll_maximum,
                handle_signals=False,
            )
            for size, async_size, process_size in zip(
                sizes[1:], async_sizes[1:], process_sizes[1:]
            )
        ]
        self._processes = process_sizes[0]
        self._jobs_per_process = jobs_per_process
        self._process_setup = process_setup
        self._lanes: Lanes | None = None
        self._parent: Worker | None = None
        self._home = app
        self._hosting: asyncio.AbstractEventLoop | None = None
        self._concurrency = concurrency
        self._lease_seconds = lease_seconds
        self._reap_interval = reap_interval
        self._shutdown_timeout = shutdown_timeout
        self._configured_shutdown_timeout = shutdown_timeout
        self._unknown_task_backoff = RetryPolicy(
            base=unknown_task_delay,
            maximum=max(unknown_task_max_delay, unknown_task_delay),
            jitter=False,
        )
        self._retention = retention or RetentionPolicy()
        self._misfire_grace = misfire_grace if scheduler else None
        self._interval = AdaptiveInterval(minimum=poll_minimum, maximum=poll_maximum)
        self._async_concurrency = async_concurrency
        self._id = worker_id or new_worker_id()
        self._handle_signals = handle_signals
        self._inflight: dict[tuple[str, int], Job] = {}
        self._lost: set[tuple[str, int]] = set()
        self._settling: dict[tuple[str, int], bool] = {}
        self._abandoned: set[tuple[str, int]] = set()
        self._misnamed = False
        self._running: dict[asyncio.Task, _Run] = {}
        self._releases: set[asyncio.Future] = set()
        self._unreleased: list[Job] = []
        self._finished = threading.Event()
        self._serving = False
        self._settle_by = Deadline()
        self._prepare_run()

    def _prepare_run(self) -> None:
        self._executors = ExecutorResolver(
            concurrency=self._concurrency,
            async_concurrency=self._async_concurrency,
            processes=self._processes,
            jobs_per_process=self._jobs_per_process,
            process_setup=self._process_setup,
            freed=self._freed,
        )
        self._stopping = Flag()
        self._forced = Flag()
        self._forcing = False
        self._capacity = Flag()
        self._capacity.set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._shutdown_timeout = self._configured_shutdown_timeout
        self._ending = Deadline()
        self._writing = Deadline()
        self._admitting = Deadline()
        self._beating = Deadline()
        self._sparing: asyncio.Task | None = None
        self._claiming: asyncio.Task | None = None
        self._drop_give_up()
        self._gave_up = False
        self._reserved: Any = None
        self._waves = 1
        self._transient: tuple[type[BaseException], ...] = ()

    @property
    def id(self) -> str:
        return self._id

    @property
    def shutdown_timeout(self) -> float:
        return self._shutdown_timeout

    @property
    def finished(self) -> bool:
        return self._finished.is_set()

    def run(self) -> None:
        if inside_running_loop():
            raise RuntimeError(
                "Worker.run() cannot be called from a running event loop; "
                "await worker.start() instead"
            )
        loop = asyncio.new_event_loop()
        loop.set_default_executor(DaemonThreadPool(DEFAULT_EXECUTOR_THREADS))
        asyncio.set_event_loop(loop)
        self._hosting = loop
        main = loop.create_task(self.start())
        stopping = self._stopping
        interrupts = RunInterrupts(
            loop,
            self._within_run(stopping, self.stop),
            self._within_run(stopping, self._request_force),
            self._within_run(stopping, self._mark_forcing),
            stopping.is_set,
            self._handle_signals,
        )
        clean = False
        try:
            loop.run_until_complete(main)
            clean = True
        except ForcedInterrupt:
            if not main.done():
                self._wind_down_interrupted(loop, main, forced=True)
            if not self._handle_signals:
                raise
            clean = True
        except BaseException:
            if not main.done():
                self._wind_down_interrupted(loop, main)
            raise
        finally:
            try:
                try:
                    self._leave_home(loop)
                finally:
                    self._settle(loop)
            except BaseException:
                clean = False
                raise
            finally:
                self._hosting = None
                interrupts.restore(exiting=clean and self._handle_signals)
                asyncio.set_event_loop(None)
                loop.close()
                forget_loop(loop)
        if interrupts.count and not self._handle_signals:
            raise KeyboardInterrupt

    def _leave_home(self, loop: asyncio.AbstractEventLoop) -> None:
        home, self._home = self._home, self._app
        if home is self._app:
            return
        try:
            with suppress(Exception, ForcedInterrupt), finishing(self._settle_by):
                loop.run_until_complete(home.aclose())
        finally:
            home.unhost(loop)

    def _settle(self, loop: asyncio.AbstractEventLoop) -> None:
        try:
            self._forget(loop.run_until_complete(_settled(self._settle_by)))
        except ForcedInterrupt:
            behind = {task for task in asyncio.all_tasks(loop) if not task.done()}
            _left_behind.update(behind)
            self._forget(behind | set(self._running))
            if not self._handle_signals:
                raise

    def _within_run(
        self, stopping: Flag, action: Callable[[], None]
    ) -> Callable[[], None]:
        def act() -> None:
            if self._stopping is stopping:
                action()

        return act

    def _forget(self, tasks: set[asyncio.Task]) -> None:
        for task in tasks:
            run = self._running.pop(task, None)
            if run is None:
                continue
            self._inflight.pop(run.key, None)
            self._lost.discard(run.key)
            self._settling.pop(run.key, None)
            self._abandoned.discard(run.key)
        self._capacity.set()

    def _wind_down_interrupted(
        self, loop: asyncio.AbstractEventLoop, main: asyncio.Task, forced: bool = False
    ) -> None:
        end = Deadline(time.monotonic() + self._budget())
        self.stop()
        if forced:
            loop.call_soon(self._request_force)
        with suppress(KeyboardInterrupt):
            loop.run_until_complete(asyncio.wait({main}, timeout=max(0.0, end.left())))
        if not main.done():
            self._settle_by = end

    def _budget(self) -> float:
        timeout = self._shutdown_timeout
        window = _release_window(timeout)
        return max(timeout, window + _close_window(window))

    def stop(self) -> None:
        stopping, ending = self._stopping, self._ending
        loop = self._loop or self._hosting
        if loop is None or (
            inside_running_loop() and loop is asyncio.get_running_loop()
        ):
            self._request_stop(stopping, ending)
            return
        try:
            loop.call_soon_threadsafe(self._request_stop, stopping, ending)
        except RuntimeError:
            if self._loop is None:
                self._request_stop(stopping, ending)

    async def start(self) -> None:
        if not await self._readied():
            self._prepare_run()
            self._finished.set()
            return
        if self._lane_options and self._home is self._app:
            self._prepare_run()
            self._finished.set()
            raise ConfigurationError(LOOP_BOUND_MESSAGE)
        if self._parent is None and gil_turned_back_on():
            logger.warning(GIL_MESSAGE)
        self._transient = tuple(getattr(self._home.backend, "transient_errors", ()))
        self._loop = asyncio.get_running_loop()
        self._finished.clear()
        self._settle_by = Deadline()
        try:
            self._app.attach(self)
        except BaseException:
            self._prepare_run()
            self._finished.set()
            raise
        if self._parent is None:
            self._app.close_by(None)
        self._serving = True
        try:
            self._open_lanes()
            await self._serve()
        finally:
            lanes = self._lanes
            try:
                if lanes is not None:
                    await lanes.finish()
            finally:
                self._lanes = None
                self._serving = False
                if self._reserved is not None:
                    self._reserved(self, -1)
                self._settle_by = self._closing_end()
                closing = self._app.detach(self, self._settle_by)
                self._prepare_run()
                self._finished.set()
                if closing and not (
                    self._parent is not None and self._app.close_after(self._parent)
                ):
                    await self._app.aclose()
        if lanes is not None and lanes.error is not None:
            raise lanes.error

    def _open_lanes(self) -> None:
        if not self._lane_options:
            return
        lanes = Lanes(
            [
                self._lane(index, options)
                for index, options in enumerate(self._lane_options, start=1)
            ],
            self.stop,
        )
        self._lanes = lanes
        lanes.start()

    def _lane(self, index: int, options: dict[str, Any]) -> "Worker":
        worker = self._app.worker(**options, worker_id=f"{self._id}-{index}")
        worker._parent = self
        return worker

    def force(self) -> None:
        self._relay(self._request_force)

    def mark_forcing(self) -> None:
        self._relay(self._mark_forcing)

    def _relay(self, action: Callable[[], None]) -> None:
        loop = self._loop or self._hosting
        if loop is None:
            return
        if inside_running_loop() and loop is asyncio.get_running_loop():
            action()
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(action)

    async def _ready_at_home(self) -> None:
        loop = self._hosting
        if (
            loop is not asyncio.get_running_loop()
            or self._home is not self._app
            or not self._app.loop_bound
        ):
            await self._app.ready()
            return
        home = self._app.lane()
        home.host(loop)
        self._home = home
        if self._parent is None:
            await self._app.reopen()
        await home.ready()

    async def _readied(self) -> bool:
        ready = asyncio.ensure_future(self._ready_at_home())
        try:
            await asyncio.wait(
                {ready, self._stopping.raised()}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            ready.cancel()
            raise
        if ready.done():
            ready.result()
            return True
        ready.cancel()
        await self._until({ready}, self._ending)
        if ready.done() and not ready.cancelled():
            ready.exception()
        logger.info("worker %s stopped before its backend was ready", self._id)
        return False

    async def _serve(self) -> None:
        watcher = ShutdownWatcher(
            self._request_stop, self._request_force, self._mark_forcing
        )
        background: list[asyncio.Task] = []
        heartbeat: list[asyncio.Task] = []
        with under(self._ending), finishing(self._ending):
            await self._served(watcher, background, heartbeat)

    async def _served(
        self,
        watcher: ShutdownWatcher,
        background: list[asyncio.Task],
        heartbeat: list[asyncio.Task],
    ) -> None:
        try:
            if self._handle_signals:
                watcher.install()
            logger.info(
                "worker %s listening on %s with concurrency %d",
                self._id,
                ",".join(self._queues),
                self._concurrency,
            )
            beating = contextvars.copy_context()
            beating.run(current.set, self._beating)
            beating.run(final.set, self._beating)
            heartbeat.append(
                asyncio.create_task(
                    self._heartbeat_loop(), name="tinyjobs-heartbeat", context=beating
                )
            )
            background.extend(self._start_background())
            await self._claim_loop()
        finally:
            for task in background:
                task.cancel()
            try:
                await asyncio.gather(*background, return_exceptions=True)
                await self._drain()
            finally:
                self._beating.end(time.monotonic())
                for task in heartbeat:
                    task.cancel()
                await asyncio.gather(*heartbeat, return_exceptions=True)
                watcher.uninstall()
                if self._sparing is not None:
                    self._sparing.cancel()
                    await asyncio.gather(self._sparing, return_exceptions=True)
                self._executors.shutdown()
                strays = self._executors.stray_names()
                if strays:
                    logger.warning(
                        "worker %s stops with %d job thread(s) still running after their job "
                        "ended (%s); the process exits without waiting for them",
                        self._id,
                        len(strays),
                        ", ".join(strays),
                    )
                logger.info("worker %s stopped", self._id)

    def _freed(self) -> None:
        self._capacity.set()

    def _start_background(self) -> list[asyncio.Task]:
        background = [asyncio.create_task(self._reaper_loop(), name="tinyjobs-reaper")]
        if self._parent is None:
            background.append(
                asyncio.create_task(self._housekeeping_loop(), name="tinyjobs-housekeeping")
            )
        if self._misfire_grace is not None:
            cron = self._home.scheduler(misfire_grace=self._misfire_grace)
            background.append(asyncio.create_task(cron.start(), name="tinyjobs-scheduler"))
        return background

    async def _claim_loop(self) -> None:
        await asyncio.sleep(0)
        while not self._stopping.is_set():
            capacity = self._concurrency - len(self._inflight) - self._executors.strays
            if capacity <= 0:
                self._capacity.clear()
                await self._wait_for(self._capacity)
                continue
            jobs = await self._claim(capacity)
            if not jobs:
                await self._idle(self._interval.next())
                continue
            self._interval.reset()
            for job in jobs:
                self._launch(job)

    async def _claim(self, capacity: int) -> list[Job]:
        try:
            return await self._claimed(capacity)
        except AppClosedError:
            logger.info("the app was closed; worker %s is stopping", self._id)
            self._request_stop()
            return []
        except (TimeoutError, *self._transient) as error:
            logger.warning(
                "claim got no answer in time (%s); backing off", str(error) or type(error).__name__
            )
            await self._pause(self._interval.next())
            return []
        except Exception:
            logger.exception("claim failed; backing off")
            await self._pause(self._interval.next())
            return []

    async def _claimed(self, capacity: int) -> list[Job]:
        claiming = asyncio.current_task()
        self._claiming = claiming
        try:
            with under(Deadline(math.inf, self._admitting)):
                return await self._home.backend.claim(
                    worker_id=self._id,
                    queues=self._queues,
                    limit=capacity,
                    lease_seconds=self._lease_seconds,
                )
        except asyncio.CancelledError:
            if not self._gave_up or claiming is None or claiming.uncancel() > 0:
                raise
            logger.warning("a claim was still waiting on the backend at shutdown; giving it up")
            return []
        finally:
            self._claiming = None
            self._gave_up = False
            self._drop_give_up()

    def _give_up_claim(self) -> None:
        self._drop_give_up()
        if self._claiming is not None and not self._gave_up:
            self._gave_up = True
            self._claiming.cancel()

    def _drop_give_up(self) -> None:
        giving_up = getattr(self, "_giving_up", None)
        self._giving_up: asyncio.TimerHandle | None = None
        if giving_up is not None:
            giving_up.cancel()

    def _bound_claim(self) -> None:
        if self._claiming is None or self._giving_up is not None:
            return
        left = self._admitting.left()
        if left <= 0:
            self._give_up_claim()
            return
        self._giving_up = asyncio.get_running_loop().call_later(left, self._give_up_claim)

    def _launch(self, job: Job) -> None:
        key = (job.id, job.claims)
        self._inflight[key] = job
        run = _Run(key, Deadline(math.inf, self._writing), Deadline(math.inf, self._ending))
        run.answers, run.settles = self._lease_seconds, self._settling
        context = contextvars.copy_context()
        context.run(run.enter)
        task = asyncio.create_task(
            self._run_job(job, run), name=f"tinyjobs-{job.id}", context=context
        )
        self._running[task] = run
        task.add_done_callback(self._finalize(job))

    def _finalize(self, job: Job):
        def done(task: asyncio.Task) -> None:
            key = (job.id, job.claims)
            self._running.pop(task, None)
            self._inflight.pop(key, None)
            self._lost.discard(key)
            self._capacity.set()
            settled = self._settling.pop(key, None)
            if key in self._abandoned:
                self._abandoned.discard(key)
                return
            cancelled = task.cancelled()
            error = None if cancelled else task.exception()
            stopped = cancelled or isinstance(error, ForcedInterrupt)
            if stopped and settled:
                return
            if stopped and settled is False:
                logger.warning(
                    "job %s ended while its outcome was being recorded; if it was not, "
                    "the reaper takes it back once its lease expires",
                    job.id,
                    extra=job_fields(job, worker_id=self._id),
                )
                return
            if stopped:
                self._release_later(job)
                return
            if error is not None:
                logger.exception(
                    "job %s crashed the worker loop",
                    job.id,
                    exc_info=error,
                    extra=job_fields(job, worker_id=self._id),
                )

        return done

    def _release_later(self, job: Job) -> None:
        self._unreleased.append(job)
        if len(self._unreleased) == 1:
            asyncio.get_running_loop().call_soon(self._release_all)

    def _release_all(self) -> None:
        jobs, self._unreleased = self._unreleased, []
        if not jobs:
            return
        release = asyncio.ensure_future(self._releasing(jobs))
        self._releases.add(release)
        release.add_done_callback(self._released(jobs))

    async def _releasing(self, jobs: list[Job]) -> None:
        with under(self._ending):
            async with bounded(self._ending.at):
                await self._home.backend.release_many(jobs)

    def _released(self, jobs: list[Job]):
        def done(release: asyncio.Future) -> None:
            self._releases.discard(release)
            failure = None if release.cancelled() else release.exception()
            if not release.cancelled() and failure is None:
                return
            one = len(jobs) == 1
            fields = job_fields(jobs[0], worker_id=self._id) if one else {"worker_id": self._id}
            if failure is None or isinstance(failure, TimeoutError):
                logger.warning(
                    "the release of %s did not answer in time; unless it went through, "
                    "the reaper takes %s back once %s lease expires",
                    _named(jobs),
                    "it" if one else "them",
                    "its" if one else "their",
                    extra=fields,
                )
                return
            logger.warning(
                "could not release %s (%s); the reaper takes %s back once %s lease expires",
                _named(jobs),
                str(failure) or type(failure).__name__,
                "it" if one else "them",
                "its" if one else "their",
                extra=fields,
            )

        return done

    async def _run_job(self, job: Job, run: _Run) -> None:
        if self._forcing:
            raise asyncio.CancelledError
        current_runner.set(self)
        try:
            task = self._app.registry.get(job.task)
        except UnknownTaskError:
            await self._release_unknown(job)
            return
        try:
            attempt = await self._home.execution.execute(
                self._app.bind(job),
                task,
                self._executors,
                held_until=run.held_until,
                settling=run.settling,
                settled=run.settled,
            )
        except ExecutorClosedError:
            await self._hand_back_unstarted(job)
            return
        except Exception:
            logger.exception(
                "could not record the outcome of job %s; the reaper takes it back "
                "once its lease expires",
                job.id,
                extra=job_fields(job, worker_id=self._id),
            )
            return
        fields = job_fields(
            job, worker_id=self._id, duration_ms=round(attempt.duration * 1000, 3)
        )
        if not attempt.recorded:
            logger.warning(
                "job %s attempt %d is no longer held by worker %s; its outcome "
                "was not recorded",
                job.id,
                job.attempts,
                self._id,
                extra=fields,
            )
            return
        if attempt.error is None:
            logger.info(
                "job %s succeeded on attempt %d", job.id, job.attempts, extra=fields
            )
            return
        failure = failure_fields(attempt.error, attempt.will_retry)
        logger.warning(
            "job %s failed on attempt %d/%d (%s)%s",
            job.id,
            job.attempts,
            job.max_attempts,
            failure["error"],
            "; will retry" if attempt.will_retry else "",
            exc_info=None if attempt.will_retry else attempt.error,
            extra={**fields, **failure},
        )

    async def _hand_back_unstarted(self, job: Job) -> None:
        logger.info(
            "the job threads are shut down; worker %s hands job %s back and stops",
            self._id,
            job.id,
            extra=job_fields(job, worker_id=self._id),
        )
        self._request_stop()
        try:
            async with bounded(self._answer_by()):
                await self._home.backend.hand_back(job)
        except Exception:
            logger.warning(
                "could not hand back job %s; the reaper takes it back once its "
                "lease expires",
                job.id,
                exc_info=True,
                extra=job_fields(job, worker_id=self._id),
            )

    async def _release_unknown(self, job: Job) -> None:
        delay = self._unknown_task_backoff.delay_for(job.hand_backs + 1)
        logger.error(
            "task %r is not registered in this worker; leaving job %s queued "
            "and retrying it in %s",
            job.task,
            job.id,
            format_seconds(delay),
            extra=job_fields(job, worker_id=self._id),
        )
        async with bounded(self._answer_by()):
            await self._home.backend.hand_back(job, delay)

    def _closing_end(self) -> Deadline:
        at = self._ending.at
        if at == math.inf:
            return Deadline()
        spare = _close_window(_release_window(self._configured_shutdown_timeout))
        return Deadline(max(at, time.monotonic()) + spare)

    def _answer_by(self) -> float:
        return time.monotonic() + self._lease_seconds

    @property
    def _beat_seconds(self) -> float:
        return max(self._lease_seconds / 3.0, 1.0)

    async def _heartbeat_loop(self) -> None:
        asked = renewed = time.monotonic()
        while True:
            await asyncio.sleep(max(0.0, asked + self._beat_seconds - time.monotonic()))
            asked = time.monotonic()
            releasing = self._admitting.left() <= 0
            kept = {run.key for run in self._running.values() if run.kept}
            beaten = [
                key
                for key in self._inflight
                if key not in self._lost and (not releasing or key in kept)
            ]
            job_ids = list({job_id for job_id, _ in beaten})
            if not job_ids:
                renewed = asked
                continue
            try:
                self._forget_lost(beaten, await self._beat(job_ids, asked, renewed))
            except AppClosedError:
                return
            except (TimeoutError, *self._transient) as error:
                logger.warning(
                    "heartbeat for %d job(s) got no answer in time (%s); the next one "
                    "tries again",
                    len(job_ids),
                    str(error) or type(error).__name__,
                )
                continue
            except Exception:
                logger.exception("heartbeat failed for %d jobs", len(job_ids))
                continue
            renewed = asked

    async def _beat(self, job_ids: list[str], asked: float, renewed: float) -> Any:
        hedge = asked + self._lease_seconds / 4
        async with bounded(max(hedge, renewed + self._lease_seconds)):
            attempts = {asyncio.ensure_future(self._renew(job_ids))}
            try:
                hedged, failure = False, None
                while attempts:
                    done, attempts = await asyncio.wait(
                        attempts,
                        timeout=None if hedged else max(0.0, hedge - time.monotonic()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for attempt in done:
                        if attempt.exception() is None:
                            return attempt.result()
                        failure = attempt.exception()
                    if not hedged and not done:
                        hedged = True
                        attempts.add(asyncio.ensure_future(self._renew(job_ids)))
                raise failure or TimeoutError()
            finally:
                for attempt in attempts:
                    attempt.cancel()
                await asyncio.gather(*attempts, return_exceptions=True)

    async def _renew(self, job_ids: list[str]) -> Any:
        return await self._home.backend.heartbeat(
            job_ids, self._lease_seconds, worker_id=self._id
        )

    def _forget_lost(self, beaten: list[tuple[str, int]], answer: Any) -> None:
        if answer is None:
            return
        if (
            isinstance(answer, (str, bytes))
            or not isinstance(answer, Collection)
            or not all(isinstance(job_id, str) for job_id in answer)
        ):
            if not self._misnamed:
                self._misnamed = True
                logger.warning(
                    "the backend's heartbeat returned %s rather than the ids of the leases it "
                    "renewed, so worker %s cannot tell when it loses a lease",
                    type(answer).__name__,
                    self._id,
                )
            return
        renewed = set(answer)
        for key in beaten:
            if key[0] in renewed or key not in self._inflight or key in self._settling:
                continue
            self._lost.add(key)
            logger.warning(
                "worker %s no longer holds job %s: its lease lapsed and another worker may "
                "run it; its outcome will not be recorded here",
                self._id,
                key[0],
                extra=job_fields(self._inflight[key], worker_id=self._id),
            )

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reap_interval)
            try:
                reaped = await self._home.backend.reap_expired()
            except AppClosedError:
                return
            except Exception:
                logger.exception("reaper pass failed")
                continue
            if reaped:
                logger.warning("reclaimed %d job(s) with an expired lease", reaped)

    async def _housekeeping_loop(self) -> None:
        if not self._retention.enabled:
            logger.info("retention disabled, nothing will be pruned")
            return
        if not self._home.backend.capabilities.retention:
            logger.info("this backend prunes on its own, skipping housekeeping")
            return
        logger.info("retention: %s", self._retention.describe())
        continuing = replace(self._retention, interval=0.0)
        while True:
            await asyncio.sleep(self._retention.check_interval)
            policy = self._retention
            while True:
                try:
                    removed = await self._home.backend.purge(policy, self._app.clock.now())
                except AppClosedError:
                    return
                except Exception:
                    logger.exception("retention sweep failed")
                    break
                if removed:
                    logger.info("retention: pruned %d job(s)", removed)
                if not removed or removed < self._retention.maximum_per_sweep:
                    break
                policy = continuing

    async def _drain(self) -> None:
        await self._wait_for_jobs()
        self._release_all()
        if not self._releases:
            return
        left = self._ending.left()
        _, late = await asyncio.wait(
            tuple(self._releases), timeout=None if left == math.inf else max(0.0, left)
        )
        for release in late:
            release.cancel()
        await asyncio.gather(*late, return_exceptions=True)

    async def _wait_for_jobs(self) -> None:
        if not self._running:
            return
        if self._ending.at == math.inf:
            self._request_stop()
        logger.info(
            "waiting up to %s for %d job(s)",
            format_seconds(max(0.0, self._admitting.left())),
            len(self._running),
        )
        await self._until(self._running, self._admitting)
        await self._finalized()
        stuck: dict[asyncio.Task, Any] = {}
        settling: set[asyncio.Task] = set()
        stopped: set[asyncio.Task] = set()
        for task, run in tuple(self._running.items()):
            thread = self._executors.keeps(task)
            if thread is not None:
                stuck[task] = thread
                self._keep(task, thread)
            elif self._settling.get(run.key) is False:
                settling.add(task)
            else:
                stopped.add(task)
        if stopped:
            logger.warning("releasing %d unfinished job(s) back to the queue", len(stopped))
            for task in stopped:
                task.cancel()
        if stopped or settling:
            await self._until(stopped | settling, self._ending)
            await self._finalized()
            self._leave_behind(stopped | settling)
        if stuck:
            await self._outlived(stuck)

    async def _finalized(self) -> None:
        for _ in range(SETTLING_TICKS):
            if not any(task.done() for task in self._running):
                return
            await asyncio.sleep(0)

    async def _until(self, tasks: Collection[asyncio.Task], deadline: Deadline) -> None:
        while pending := {task for task in tuple(tasks) if not task.done()}:
            left = deadline.left()
            if left <= 0:
                return
            forced = self._forced.raised()
            await asyncio.wait(
                pending if forced.done() else {*pending, forced},
                timeout=None if left == math.inf else left,
                return_when=asyncio.FIRST_COMPLETED,
            )

    def _leave_behind(self, tasks: set[asyncio.Task]) -> None:
        behind = [task for task in tasks if not task.done()]
        if not behind:
            return
        for task in behind:
            run = self._running.get(task)
            if run is not None:
                self._abandoned.add(run.key)
            task.cancel()
        logger.warning(
            "%d job(s) did not end within the shutdown timeout although cancelled; worker %s "
            "leaves them behind and the reaper takes them back once their lease expires: %s",
            len(behind),
            self._id,
            ", ".join(sorted(task.get_name() for task in behind)),
        )

    async def _outlived(self, stuck: dict[asyncio.Task, Any]) -> None:
        logger.warning(
            "%d job(s) run in threads that cannot be stopped; worker %s keeps their leases "
            "and records their outcome when they end",
            len(stuck),
            self._id,
        )
        await asyncio.gather(*stuck, return_exceptions=True)

    def _keep(self, task: asyncio.Task, thread: Any) -> None:
        run = self._running.get(task)
        if run is None or run.kept:
            return
        run.kept = True
        run.window = _release_window(self._configured_shutdown_timeout) * self._waves
        run.deadline.outer = None
        run.last.outer = None
        thread.add_done_callback(partial(_ended_by, asyncio.get_running_loop(), run))

    async def _wait_for(self, flag: Flag) -> None:
        await asyncio.wait(
            {flag.raised(), self._stopping.raised()}, return_when=asyncio.FIRST_COMPLETED
        )

    async def _idle(self, seconds: float) -> None:
        try:
            backend = self._home.backend
        except BackendNotReadyError:
            backend = None
        if not getattr(backend, "wakes_workers", False):
            await self._pause(seconds)
            return
        waiting = asyncio.create_task(
            backend.wait_for_jobs(worker_id=self._id, queues=self._queues, timeout=seconds)
        )
        try:
            await asyncio.wait(
                {waiting, self._stopping.raised()}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
        failure = None if waiting.cancelled() else waiting.exception()
        if failure is not None and not isinstance(failure, AppClosedError):
            logger.warning("waiting for new jobs failed: %s", failure)
            await self._pause(seconds)

    async def _pause(self, seconds: float) -> None:
        await asyncio.wait({self._stopping.raised()}, timeout=seconds)

    def _request_stop(
        self, stopping: Flag | None = None, ending: Deadline | None = None
    ) -> None:
        logger.info("shutdown requested; finishing in-flight jobs")
        window = _release_window(self._shutdown_timeout)
        end = time.monotonic() + self._budget() - _close_window(window)
        if ending is None or ending is self._ending:
            self._admitting.end(end - window)
            self._writing.end(end - min(VERIFY_SECONDS, window / 2))
        (self._ending if ending is None else ending).end(end)
        (self._stopping if stopping is None else stopping).set()
        if ending is None or ending is self._ending:
            self._bound_claim()
            if self._sparing is None:
                self._spare()
        if self._lanes is not None:
            self._lanes.stop()

    def _spare(self) -> None:
        if not self._serving:
            return
        try:
            backend = self._home.backend
            asyncio.get_running_loop()
        except (BackendNotReadyError, RuntimeError):
            return
        reserve = getattr(backend, "reserve", None)
        if reserve is not None:
            self._waves = reserve(self, len(self._running))
            self._reserved = reserve
        spare = getattr(backend, "spare", None)
        if spare is None:
            return
        context = contextvars.copy_context()
        context.run(current.set, self._ending)
        self._sparing = asyncio.create_task(
            self._spared(spare), name="tinyjobs-spare", context=context
        )

    async def _spared(self, spare: Any) -> None:
        try:
            async with bounded(self._admitting.at):
                await spare()
        except Exception as error:
            logger.debug("could not open a spare connection for the releases: %s", error)

    def _mark_forcing(self) -> None:
        self._forcing = True
        if self._lanes is not None:
            self._lanes.mark_forcing()

    def _request_force(self) -> None:
        if not self._forced.is_set():
            logger.warning("second signal received; forcing the stop")
        self._forcing = True
        self._forced.set()
        self._shutdown_timeout = 0.0
        now = time.monotonic()
        self._admitting.end(now)
        self._writing.end(now + RELEASE_SECONDS / 2)
        self._ending.end(now + RELEASE_SECONDS)
        self._give_up_claim()
        if self._lanes is not None:
            self._lanes.force()
        for task, run in tuple(self._running.items()):
            if self._settling.get(run.key) is False:
                continue
            thread = self._executors.keeps(task)
            if thread is not None:
                self._keep(task, thread)
                continue
            task.cancel()


def gil_turned_back_on() -> bool:
    checked = getattr(sys, "_is_gil_enabled", None)
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED")) and checked is not None and checked()


def _release_window(timeout: float) -> float:
    return max(RELEASE_SECONDS, timeout / RELEASE_SHARE)


def _close_window(window: float) -> float:
    return min(CLOSE_SECONDS, window / 4)


class _Run:
    __slots__ = ("key", "deadline", "last", "kept", "window", "answers", "settles")

    def __init__(self, key: tuple[str, int], deadline: Deadline, last: Deadline) -> None:
        self.key = key
        self.deadline = deadline
        self.last = last
        self.kept = False
        self.window = 0.0
        self.answers = 0.0
        self.settles: dict[tuple[str, int], bool] = {}

    def enter(self) -> None:
        current.set(self.deadline)
        final.set(self.last)

    def held_until(self) -> float:
        if self.kept:
            self.close()
        return time.monotonic() + self.answers

    def settling(self) -> None:
        self.settles[self.key] = False

    def settled(self) -> None:
        self.settles[self.key] = True

    def close(self) -> None:
        now = time.monotonic()
        self.deadline.end(now + self.window - min(VERIFY_SECONDS, self.window / 2))
        self.last.end(now + self.window)


def _ended_by(loop: asyncio.AbstractEventLoop, run: _Run, _: object) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(run.close)


async def _settled(end: Deadline) -> set[asyncio.Task]:
    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()
    tasks = {task for task in asyncio.all_tasks() if task is not current_task}
    for task in tasks:
        task.cancel()
    await _within(tasks, end)
    behind = {task for task in tasks if not task.done()}
    _left_behind.update(behind)
    for task in tasks:
        if task.done() and not task.cancelled() and task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during worker shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )
    await _within({asyncio.ensure_future(loop.shutdown_asyncgens())}, end)
    return behind


async def _within(tasks: set[asyncio.Future], end: Deadline) -> None:
    if not tasks:
        return
    left = end.left()
    await asyncio.wait(tasks, timeout=None if left == math.inf else max(left, 0.0))
    for _ in range(SETTLING_TICKS):
        if all(task.done() for task in tasks):
            return
        await asyncio.sleep(0)


def _named(jobs: Sequence[Job]) -> str:
    if len(jobs) == 1:
        return f"job {jobs[0].id}"
    return f"{len(jobs)} jobs ({', '.join(job.id for job in jobs)})"
