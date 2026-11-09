from __future__ import annotations

from typing import Any


def located(alias: str, path: str) -> Any:
    from django.tasks import task_backends

    return task_backends[alias].app.registry.get(path)
