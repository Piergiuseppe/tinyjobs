from __future__ import annotations

import threading
from typing import Any, Callable

from ...app import TinyJobs

_apps: dict[tuple[str, str], TinyJobs] = {}
_guard = threading.Lock()


def shared_app(
    alias: str, options: dict[str, Any], prepare: Callable[[TinyJobs], None]
) -> TinyJobs:
    key = (alias, repr(sorted(options.items())))
    with _guard:
        app = _apps.get(key)
        if app is None:
            app = _apps[key] = TinyJobs(**options)
            prepare(app)
        return app
