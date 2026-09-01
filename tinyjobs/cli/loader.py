from __future__ import annotations

import importlib
import pkgutil
import sys
from pathlib import Path
from typing import Sequence

from ..app import TinyJobs
from ..exceptions import ConfigurationError

DEFAULT_ATTRIBUTE = "app"


def load_app(spec: str) -> TinyJobs:
    _ensure_cwd_importable()
    module_name, separator, attribute = spec.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ConfigurationError(
            f"cannot import {module_name!r} from {Path.cwd()}: {error}"
        ) from None
    name = attribute if separator else DEFAULT_ATTRIBUTE
    instance = getattr(module, name, None)
    if not isinstance(instance, TinyJobs):
        raise ConfigurationError(
            f"{spec!r} does not point at a TinyJobs instance; "
            f"expected something like 'myproject.tasks:app'"
        )
    return instance


def import_modules(patterns: Sequence[str]) -> list[str]:
    _ensure_cwd_importable()
    imported: list[str] = []
    for pattern in patterns:
        imported.extend(_import_pattern(pattern))
    return imported


def _import_pattern(pattern: str) -> list[str]:
    if not pattern.endswith(".*"):
        importlib.import_module(pattern)
        return [pattern]
    package_name = pattern[:-2]
    package = importlib.import_module(package_name)
    found = [package_name]
    for info in pkgutil.walk_packages(
        package.__path__, prefix=f"{package_name}."
    ):
        importlib.import_module(info.name)
        found.append(info.name)
    return found


def _ensure_cwd_importable() -> None:
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
