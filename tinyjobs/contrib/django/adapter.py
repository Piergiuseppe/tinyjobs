from __future__ import annotations

from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any

from django.tasks import Task as DjangoTask

from ...app import TinyJobs
from ...options import EnqueueOptions
from ...retry import RetryPolicy
from ...task import Task

if TYPE_CHECKING:
    from .backend import TinyJobsBackend


def adapt(task: DjangoTask, app: TinyJobs, backend: "type[TinyJobsBackend]") -> Task:
    if iscoroutinefunction(task.func):

        async def run(*args: Any, **kwargs: Any) -> Any:
            return await backend.named(task.backend).arun(task, args, kwargs)

    else:

        def run(*args: Any, **kwargs: Any) -> Any:
            return backend.named(task.backend).run(task, args, kwargs)

    return Task(
        app,
        run,
        name=task.module_path,
        options=EnqueueOptions(),
        retry_policy=RetryPolicy(),
        store_result=True,
    )
