from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Callable, Mapping

from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, Task as DjangoTask, task_backends
from django.tasks.base import DEFAULT_TASK_PRIORITY, DEFAULT_TASK_QUEUE_NAME
from django.utils.module_loading import import_string

from ...exceptions import ConfigurationError, described


@dataclass(frozen=True, slots=True, kw_only=True)
class Task(DjangoTask):
    timeout: float | None = None
    max_attempts: int = 1
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    retry_backoff: Any = "exponential"
    retry_base: float = 1.0
    retry_max: float = 600.0
    retry_jitter: bool = True
    isolated: bool = False
    concurrency: int | None = None
    rate: str | None = None
    result_ttl: float | None = None


OPTIONS = tuple(
    field.name for field in fields(Task) if field.name not in {f.name for f in fields(DjangoTask)}
)

TASKS_MESSAGE = (
    'the "tasks" option of TASKS[{alias!r}] must map the import path of a task to a dict of its '
    "options, got {value}"
)
UNKNOWN_OPTION_MESSAGE = (
    'the "tasks" option of TASKS[{alias!r}] gives {path} the option {name!r}, which is not one '
    "of {known}"
)
RUN_AFTER_MESSAGE = (
    "run_after cannot be set on the decorator; use .using(run_after=...) when the task is sent"
)
RETRY_ON_PATH_MESSAGE = (
    "{path} on TASKS[{alias!r}] has a retry_on that names {value}, which is not an exception "
    "class: {problem}"
)


def options_of(
    task: DjangoTask, alias: str, overrides: Mapping[str, Any], *, importing: bool = True
) -> dict[str, Any]:
    options = {name: getattr(task, name) for name in OPTIONS if hasattr(task, name)}
    options.update(overrides)
    named = _named(options.get("retry_on"))
    if named:
        options["retry_on"] = tuple(
            _exception(alias, task.module_path, kind, importing) for kind in named
        )
    return options


def settings_tasks(alias: str, value: Any) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or not all(
        isinstance(path, str) and isinstance(options, Mapping)
        for path, options in value.items()
    ):
        raise ConfigurationError(TASKS_MESSAGE.format(alias=alias, value=described(value)))
    tasks: dict[str, dict[str, Any]] = {}
    for path, options in value.items():
        for name in options:
            if name not in OPTIONS:
                raise ConfigurationError(
                    UNKNOWN_OPTION_MESSAGE.format(
                        alias=alias, path=path, name=name, known=", ".join(OPTIONS)
                    )
                )
        tasks[path] = dict(options)
    return tasks


def _named(value: Any) -> tuple[Any, ...]:
    kinds = (value,) if isinstance(value, str) else value
    if isinstance(kinds, (list, tuple)) and any(isinstance(kind, str) for kind in kinds):
        return tuple(kinds)
    return ()


def _exception(alias: str, path: str, kind: Any, importing: bool) -> Any:
    if not isinstance(kind, str):
        return kind
    try:
        if not importing:
            module, _, name = kind.rpartition(".")
            if not module or not name.isidentifier():
                raise ValueError(f"{kind} is not a dotted path")
            return Exception
        return import_string(kind)
    except (ImportError, ValueError) as error:
        raise ConfigurationError(
            RETRY_ON_PATH_MESSAGE.format(
                alias=alias, path=path, value=described(kind), problem=error
            )
        ) from None


def task(
    function: Callable[..., Any] | None = None,
    *,
    priority: int = DEFAULT_TASK_PRIORITY,
    queue_name: str = DEFAULT_TASK_QUEUE_NAME,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    takes_context: bool = False,
    **options: Any,
) -> Any:
    if "run_after" in options:
        raise TypeError(RUN_AFTER_MESSAGE)

    def declare(target: Callable[..., Any]) -> DjangoTask:
        return task_backends[backend].task_class(
            func=target,
            priority=priority,
            queue_name=queue_name,
            backend=backend,
            takes_context=takes_context,
            run_after=None,
            **options,
        )

    return declare(function) if function is not None else declare
