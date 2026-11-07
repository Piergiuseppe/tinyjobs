from __future__ import annotations

import re
from typing import Any, Callable
from urllib.parse import urlsplit

from ..exceptions import UnknownBackendError

IN_MEMORY_JOIN_MESSAGE = (
    "an in-memory store cannot take a job from your transaction, since no worker could share "
    "it; give the app a file, as in sqlite:///app.db, to enqueue inside a transaction"
)

ENTRY_POINT_GROUP = "tinyjobs.backends"
SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*(?=:)")

_BUILTIN_FACTORIES: dict[str, Callable[..., Any]] = {}
_BUILTIN_CHECKS: dict[str, Callable[..., None]] = {}
_LOOP_BOUND: set[str] = set()
_COLD_JOINERS: dict[str, Callable[[str], bool]] = {}


def register_builtin(
    scheme: str,
    factory: Callable[..., Any],
    check: Callable[..., None] | None = None,
    *,
    loop_bound: bool = False,
    joins_cold: Callable[[str], bool] | None = None,
) -> None:
    _BUILTIN_FACTORIES[scheme] = factory
    if check is not None:
        _BUILTIN_CHECKS[scheme] = check
    if loop_bound:
        _LOOP_BOUND.add(scheme)
    if joins_cold is not None:
        _COLD_JOINERS[scheme] = joins_cold


def loop_bound(url: str) -> bool:
    return _scheme(url) in _LOOP_BOUND


def joins_cold(url: str) -> bool:
    decide = _COLD_JOINERS.get(_scheme(url))
    return decide is not None and decide(url)


def in_memory(url: str) -> bool:
    parts = urlsplit(url)
    return parts.scheme in ("memory", "sqlite") and parts.path[1:] in ("", ":memory:")


def check_options(url: str, options: dict[str, Any]) -> None:
    check = _BUILTIN_CHECKS.get(_scheme(url))
    if check is not None:
        check(url, **options)


def scheme_of(url: str) -> str:
    scheme = _scheme(url)
    if not scheme:
        raise UnknownBackendError(
            "the backend url has no scheme; a backend url looks like 'sqlite:///jobs.db'"
        )
    return scheme


def load_backend(url: str, **options: Any) -> Any:
    scheme = scheme_of(url)
    factory = _BUILTIN_FACTORIES.get(scheme) or _load_plugin(scheme)
    if factory is None:
        raise UnknownBackendError(_unknown_scheme_message(scheme))
    return factory(url, **options)


def available_schemes() -> list[str]:
    plugins = {point.name for point in _plugin_entry_points()}
    return sorted(set(_BUILTIN_FACTORIES) | plugins)


def _scheme(url: str) -> str:
    found = SCHEME.match(url.lstrip())
    return found.group(0).lower() if found else ""


def _plugin_entry_points():
    from importlib.metadata import entry_points

    return entry_points(group=ENTRY_POINT_GROUP)


def _load_plugin(scheme: str) -> Callable[..., Any] | None:
    for entry_point in _plugin_entry_points():
        if entry_point.name == scheme:
            return entry_point.load()
    return None


def _unknown_scheme_message(scheme: str) -> str:
    known = ", ".join(available_schemes())
    suggestion = f"; try 'pip install tinyjobs-{scheme}'" if scheme.isalnum() else ""
    return f"no backend registered for scheme {scheme!r} (available: {known}){suggestion}"
