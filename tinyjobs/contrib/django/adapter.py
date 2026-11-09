from __future__ import annotations

from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Mapping

from django.tasks import Task as DjangoTask

from ...app import TinyJobs
from ...declaring import declared_task
from ...task import Task
from .options import options_of

if TYPE_CHECKING:
    from .backend import TinyJobsBackend

LOCATOR = ("tinyjobs.contrib.django.isolation", "located")


def adapt(
    task: DjangoTask,
    app: TinyJobs,
    backend: "type[TinyJobsBackend]",
    overrides: Mapping[str, Any],
    *,
    importing: bool = True,
) -> Task:
    if iscoroutinefunction(task.func):

        async def run(*args: Any, **kwargs: Any) -> Any:
            return await backend.named(task.backend).arun(task, args, kwargs)

    else:

        def run(*args: Any, **kwargs: Any) -> Any:
            return backend.named(task.backend).run(task, args, kwargs)

    return declared_task(
        app,
        run,
        name=task.module_path,
        registered_in=task.func.__module__,
        store_result=True,
        locator=(*LOCATOR, (task.backend, task.module_path)),
        **options_of(task, task.backend, overrides, importing=importing),
    )
