from __future__ import annotations

from typing import Any, Callable

from ...app import TinyJobs

_apps: dict[tuple[str, str], TinyJobs] = {}


def shared_app(
    alias: str, options: dict[str, Any], prepare: Callable[[TinyJobs], None]
) -> TinyJobs:
    key = (alias, repr(sorted(options.items())))
    app = _apps.get(key)
    if app is None:
        app = TinyJobs(**options)
        prepare(app)
        app = _apps.setdefault(key, app)
    return app
