from __future__ import annotations

import asyncio
import traceback
from typing import Any

from asgiref.sync import sync_to_async
from django.db import close_old_connections
from django.tasks import Task, TaskContext, TaskResult, task_backends
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils.json import normalize_json

from ...app import TinyJobs
from ...models import Job
from ...services.execution import current_job
from .adapter import adapt
from .connections import joined_connection, shared_app
from .discovery import discover_tasks, find_task
from .results import build_result, record_failure, record_success

INTERRUPTIONS = (KeyboardInterrupt, asyncio.CancelledError)


class TinyJobsBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        options = dict(self.options)
        self.database: str | None = options.pop("database", None)
        self.app = shared_app(alias, options, self._prepare)

    @classmethod
    def named(cls, alias: str) -> "TinyJobsBackend":
        return task_backends[alias]

    def _prepare(self, app: TinyJobs) -> None:
        alias, kind = self.alias, type(self)

        def resolve(name: str):
            found = find_task(name, alias)
            return adapt(found, app, kind) if found is not None else None

        app.registry.add_resolver(resolve)

    def discover(self) -> list[str]:
        names = dict.fromkeys(task.module_path for task in discover_tasks())
        return [name for name in names if name in self.app.registry]

    def enqueue(self, task: Task, args, kwargs) -> TaskResult:
        self.validate_task(task)
        if task.module_path not in self.app.registry:
            raise InvalidTask(
                f"{task.module_path} is not in an app of INSTALLED_APPS, "
                "so no worker would run it"
            )
        job = self.app.send(
            task.module_path, normalize_json(args), normalize_json(kwargs),
            tx=self._transaction(),
            **self._options_for(task),
        )
        executions = self.app.executions(job.id) if job.attempts else []
        return self._announce(self._result(task, job, executions))

    async def aenqueue(self, task: Task, args, kwargs) -> TaskResult:
        return await sync_to_async(
            self.enqueue, thread_sensitive=self.database is not None
        )(task, args, kwargs)

    def get_result(self, result_id: str) -> TaskResult:
        job, task = self._find(self.app.get(result_id), result_id)
        return self._result(task, job, self.app.executions(job.id))

    async def aget_result(self, result_id: str) -> TaskResult:
        return await sync_to_async(self.get_result, thread_sensitive=False)(result_id)

    def run(self, task: Task, args: tuple, kwargs: dict) -> Any:
        close_old_connections()
        try:
            job = current_job.get()
            result = self._started(task, job, self.app.executions(job.id))
            try:
                value = normalize_json(
                    task.func(*self._context(task, result), *args, **kwargs)
                )
            except INTERRUPTIONS:
                raise
            except BaseException as error:
                self._failed(result, error)
                raise
            return self._succeeded(result, value)
        finally:
            close_old_connections()

    async def arun(self, task: Task, args: tuple, kwargs: dict) -> Any:
        await sync_to_async(close_old_connections)()
        try:
            job = current_job.get()
            result = self._started(task, job, await self.app.aexecutions(job.id))
            try:
                value = normalize_json(
                    await task.func(*self._context(task, result), *args, **kwargs)
                )
            except INTERRUPTIONS:
                raise
            except BaseException as error:
                self._failed(result, error)
                raise
            return self._succeeded(result, value)
        finally:
            await sync_to_async(close_old_connections)()

    def _transaction(self) -> Any:
        if self.database is None or self.app.eager:
            return None
        return joined_connection(self.database)

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
        value = None
        if job.result_payload is not None:
            value = self.app.serialization.decode_result(job.result_payload, job.codec)
        return build_result(
            task, job, self.app.decode(job), executions, alias=self.alias, value=value
        )

    def _announce(self, result: TaskResult) -> TaskResult:
        task_enqueued.send(type(self), task_result=result)
        return result

    def _context(self, task: Task, result: TaskResult) -> tuple:
        return (TaskContext(task_result=result),) if task.takes_context else ()

    def _started(self, task: Task, job: Job, executions: list) -> TaskResult:
        result = self._result(task, job, executions)
        task_started.send(type(self), task_result=result)
        return result

    def _failed(self, result: TaskResult, error: BaseException) -> None:
        record_failure(
            result, error, "".join(traceback.format_exception(error)), self.app.clock.now()
        )
        task_finished.send(type(self), task_result=result)

    def _succeeded(self, result: TaskResult, value: Any) -> Any:
        record_success(result, value, self.app.clock.now())
        task_finished.send(type(self), task_result=result)
        return value
