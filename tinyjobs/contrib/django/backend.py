from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
from dataclasses import replace
from contextlib import nullcontext, suppress
from typing import Any

from asgiref.sync import sync_to_async
from django.db import close_old_connections, connections
from django.tasks import Task, TaskContext, TaskResult, task_backends
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils.json import normalize_json

from ...app import TinyJobs
from ...exceptions import ConfigurationError
from ...models import Job
from ...status import JobStatus
from ...current import current_job
from .adapter import adapt
from .connections import joined_connection, shared_app
from .discovery import discover_tasks, find_task
from .results import build_result, raised
from .options import Task as TinyJobsTask, settings_tasks

logger = logging.getLogger("tinyjobs.django")

SIGNAL_THREAD = "tinyjobs-django-signal"

UNKNOWN_TASKS_MESSAGE = (
    'the "tasks" option of TASKS[{alias!r}] names {paths}, which no app of INSTALLED_APPS '
    "defines as a task"
)


def result_of(app: TinyJobs, alias: str, task: Task, job: Job, executions: list) -> TaskResult:
    value = None
    if job.result_payload is not None:
        value = app.serialization.decode_result(job.result_payload, job.codec)
    return build_result(task, job, app.decode(job), executions, alias=alias, value=value)


async def outcome_of(
    app: TinyJobs,
    alias: str,
    task: Task,
    job: Job,
    status: JobStatus,
    value: Any,
    error: BaseException | None,
) -> TaskResult:
    try:
        stored = await app.aget(job.id)
        if stored is not None:
            return result_of(app, alias, task, stored, await app.aexecutions(job.id))
    except Exception:
        logger.warning(
            "could not read job %s back for task_finished; reporting it from the claim",
            job.id,
            exc_info=True,
        )
    claimed = replace(job, status=status, finished_at=app.clock.now())
    result = result_of(app, alias, task, claimed, [])
    object.__setattr__(result, "_return_value", value)
    if error is not None:
        result.errors[:] = [raised(error)]
    return result


async def delivered(
    signal: Any, sender: Any, result: TaskResult, raised_by_task: BaseException | None = None
) -> None:
    loop = asyncio.get_running_loop()
    sent: asyncio.Future[None] = loop.create_future()
    context = contextvars.copy_context()

    def settle(error: BaseException | None) -> None:
        if sent.done():
            return
        if error is None:
            sent.set_result(None)
        else:
            sent.set_exception(error)

    def send() -> None:
        if raised_by_task is None:
            signal.send(sender, task_result=result)
            return
        try:
            raise raised_by_task
        except BaseException:
            signal.send(sender, task_result=result)

    def deliver() -> None:
        error: BaseException | None = None
        try:
            try:
                context.run(send)
            finally:
                connections.close_all()
        except Exception as raised_here:
            error = raised_here
        except BaseException as raised_here:
            error = RuntimeError(f"a receiver of the signal raised {raised_here!r}")
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(settle, error)

    threading.Thread(target=deliver, name=SIGNAL_THREAD, daemon=True).start()
    await sent


class TinyJobsBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True
    task_class = TinyJobsTask

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        options = dict(self.options)
        self.database: str | None = options.pop("database", None)
        declared = options.pop("tasks", None)
        self.tasks = settings_tasks(alias, declared)
        self.app = shared_app(alias, options, self._prepare, distinct=repr(declared))

    @classmethod
    def named(cls, alias: str) -> "TinyJobsBackend":
        return task_backends[alias]

    def _prepare(self, app: TinyJobs) -> None:
        alias, kind, tasks = self.alias, type(self), self.tasks

        def resolve(name: str):
            found = find_task(name, alias)
            if found is None:
                return None
            return adapt(found, app, kind, tasks.get(name, {}))

        app.registry.add_resolver(resolve)

        async def finished(
            job: Job, status: JobStatus, value: Any = None, error: BaseException | None = None
        ) -> None:
            task = find_task(job.task, alias)
            if task is None:
                return
            result = await outcome_of(app, alias, task, job, status, value, error)
            await delivered(task_finished, kind, result, error)

        async def failed(job: Job, error: BaseException, will_retry: bool) -> None:
            if not will_retry:
                await finished(job, JobStatus.FAILED, error=error)

        async def succeeded(job: Job, result: Any, duration: float) -> None:
            await finished(job, JobStatus.SUCCESS, result)

        app.on_job_failure(failed)
        app.on_job_success(succeeded)

    def discover(self) -> list[str]:
        names = dict.fromkeys(task.module_path for task in discover_tasks())
        unknown = sorted(path for path in self.tasks if path not in self.app.registry)
        if unknown:
            raise ConfigurationError(
                UNKNOWN_TASKS_MESSAGE.format(alias=self.alias, paths=", ".join(unknown))
            )
        return [name for name in names if name in self.app.registry]

    def validate_task(self, task: Task) -> None:
        super().validate_task(task)
        try:
            adapt(
                task,
                self.app,
                type(self),
                self.tasks.get(task.module_path, {}),
                importing=False,
            )
        except ConfigurationError as error:
            raise InvalidTask(str(error)) from None

    def enqueue(self, task: Task, args, kwargs) -> TaskResult:
        self.validate_task(task)
        if not self._known(task):
            raise InvalidTask(
                f"{task.module_path} is not in an app of INSTALLED_APPS, "
                "so no worker would run it"
            )
        with self._transaction() as tx:
            job = self.app.send(
                task.module_path, normalize_json(args), normalize_json(kwargs),
                tx=tx,
                **self._options_for(task),
            )
        executions = self.app.executions(job.id) if job.attempts else []
        return self._announce(self._result(task, job, executions))

    async def aenqueue(self, task: Task, args, kwargs) -> TaskResult:
        return await sync_to_async(
            self.enqueue, thread_sensitive=self.database is not None and not self.app.eager
        )(task, args, kwargs)

    def get_result(self, result_id: str) -> TaskResult:
        job, task = self._find(self.app.get(result_id), result_id)
        return self._result(task, job, self.app.executions(job.id))

    async def aget_result(self, result_id: str) -> TaskResult:
        return await sync_to_async(self.get_result, thread_sensitive=False)(result_id)

    def _known(self, task: Task) -> bool:
        try:
            return task.module_path in self.app.registry
        except ConfigurationError as error:
            raise InvalidTask(str(error)) from None

    def run(self, task: Task, args: tuple, kwargs: dict) -> Any:
        close_old_connections()
        try:
            job = current_job.get()
            result = self._result(task, job, self.app.executions(job.id))
            task_started.send(type(self), task_result=result)
            return normalize_json(task.func(*self._context(task, result), *args, **kwargs))
        finally:
            close_old_connections()

    async def arun(self, task: Task, args: tuple, kwargs: dict) -> Any:
        await sync_to_async(close_old_connections)()
        try:
            job = current_job.get()
            result = self._result(task, job, await self.app.aexecutions(job.id))
            await delivered(task_started, type(self), result)
            return normalize_json(
                await task.func(*self._context(task, result), *args, **kwargs)
            )
        finally:
            await sync_to_async(close_old_connections)()

    def _transaction(self) -> Any:
        if self.database is None or self.app.eager:
            return nullcontext()
        return joined_connection(self.database, self.app.url)

    def _options_for(self, task: Task) -> dict[str, Any]:
        return {
            "queue": task.queue_name,
            "priority": task.priority,
            "run_at": task.run_after.timestamp() if task.run_after else None,
        }

    def _find(self, job: Job | None, result_id: str) -> tuple[Job, Task]:
        task = find_task(job.task, self.alias) if job is not None else None
        if task is None:
            raise TaskResultDoesNotExist(result_id)
        return job, task

    def _result(self, task: Task, job: Job, executions: list) -> TaskResult:
        return result_of(self.app, self.alias, task, job, executions)

    def _announce(self, result: TaskResult) -> TaskResult:
        task_enqueued.send(type(self), task_result=result)
        return result

    def _context(self, task: Task, result: TaskResult) -> tuple:
        return (TaskContext(task_result=result),) if task.takes_context else ()
