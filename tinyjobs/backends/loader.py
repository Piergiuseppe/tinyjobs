from __future__ import annotations

from importlib.metadata import entry_points
from typing import Any, Callable
from urllib.parse import urlsplit

from ..exceptions import UnknownBackendError

ENTRY_POINT_GROUP = "tinyjobs.backends"

_BUILTIN_FACTORIES: dict[str, Callable[..., Any]] = {}


def register_builtin(scheme: str, factory: Callable[..., Any]) -> None:
    _BUILTIN_FACTORIES[scheme] = factory


def scheme_of(url: str) -> str:
    scheme = urlsplit(url).scheme
    if not scheme:
        raise UnknownBackendError(
            f"{url!r} has no scheme; a backend url looks like 'sqlite:///jobs.db'"
        )
    return scheme


def load_backend(url: str, **options: Any) -> Any:
    scheme = scheme_of(url)
    factory = _BUILTIN_FACTORIES.get(scheme) or _load_plugin(scheme)
    if factory is None:
        raise UnknownBackendError(_unknown_scheme_message(scheme))
    return factory(url, **options)


def available_schemes() -> list[str]:
    plugins = {ep.name for ep in entry_points(group=ENTRY_POINT_GROUP)}
    return sorted(set(_BUILTIN_FACTORIES) | plugins)


def _load_plugin(scheme: str) -> Callable[..., Any] | None:
    for entry_point in entry_points(group=ENTRY_POINT_GROUP):
        if entry_point.name == scheme:
            return entry_point.load()
    return None


def _unknown_scheme_message(scheme: str) -> str:
    known = ", ".join(available_schemes())
    suggestion = f"; try 'pip install tinyjobs-{scheme}'" if scheme.isalnum() else ""
    return f"no backend registered for scheme {scheme!r} (available: {known}){suggestion}"
