from __future__ import annotations

import logging
import sys
from dataclasses import fields
from importlib import import_module
from typing import Any, Iterator

from django.apps import apps
from django.tasks import Task
from django.utils.module_loading import autodiscover_modules

logger = logging.getLogger("tinyjobs.django")


def find_task(path: str, alias: str) -> Task | None:
    module_name, _, attribute = path.rpartition(".")
    if not module_name or apps.get_containing_app_config(module_name) is None:
        return None
    try:
        module = import_module(module_name)
    except Exception:
        logger.exception(
            "could not import %r, which defines task %r", module_name, path
        )
        return None
    candidate = getattr(module, attribute, None)
    if not isinstance(candidate, Task):
        return None
    if candidate.module_path != path:
        return None
    if candidate.backend == alias:
        return candidate
    return stored(candidate, backend=alias)


def stored(task: Task, **changes: Any) -> Task:
    copied = object.__new__(type(task))
    for field in fields(task):
        value = changes.get(field.name, getattr(task, field.name))
        object.__setattr__(copied, field.name, value)
    return copied


def discover_tasks() -> Iterator[Task]:
    autodiscover_modules("tasks")
    for config in apps.get_app_configs():
        module = sys.modules.get(f"{config.name}.tasks")
        for candidate in vars(module).values() if module is not None else ():
            if isinstance(candidate, Task):
                yield candidate
