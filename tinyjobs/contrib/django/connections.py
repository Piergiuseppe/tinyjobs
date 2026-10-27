from __future__ import annotations

from typing import Any, Callable

from ...app import TinyJobs
from ...exceptions import ConfigurationError

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


def joined_connection(alias: str) -> Any:
    from django.db import connections

    connection = connections[alias]
    if connection.vendor != "postgresql":
        raise ConfigurationError(
            f"the database {alias!r} is not PostgreSQL, so jobs cannot join its "
            "transactions"
        )
    connection.validate_thread_sharing()
    connection.ensure_connection()
    import psycopg

    raw = connection.connection
    if not isinstance(raw, psycopg.Connection):
        raise ConfigurationError(
            f"the database {alias!r} does not run on psycopg 3, so jobs cannot join "
            "its transactions"
        )
    return raw
