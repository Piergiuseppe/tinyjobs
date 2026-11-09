from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from ...app import TinyJobs
from ...backends.loader import scheme_of
from ...exceptions import ConfigurationError

VENDOR_OF = {"postgresql": "postgresql", "postgres": "postgresql", "sqlite": "sqlite"}

_apps: dict[tuple[str, str], TinyJobs] = {}


def shared_app(
    alias: str,
    options: dict[str, Any],
    prepare: Callable[[TinyJobs], None],
    *,
    distinct: str = "",
) -> TinyJobs:
    key = (alias, repr(sorted(options.items())) + distinct)
    app = _apps.get(key)
    if app is None:
        app = TinyJobs(**options)
        prepare(app)
        app = _apps.setdefault(key, app)
    return app


@contextmanager
def joined_connection(alias: str, url: str) -> Iterator[Any]:
    from django.db import connections

    connection = connections[alias]
    scheme = scheme_of(url)
    vendor = VENDOR_OF.get(scheme)
    if vendor is None:
        raise ConfigurationError(
            f"a job on {scheme}:// cannot join a database transaction, so \"database\" "
            "has no meaning here; leave it out of the backend's OPTIONS"
        )
    if connection.vendor != vendor:
        raise ConfigurationError(
            f"the database {alias!r} runs on {connection.vendor} and the app's url is on "
            f"{scheme}, so jobs cannot join its transactions; point \"database\" at an "
            "alias of the store in url"
        )
    connection.validate_thread_sharing()
    connection.close_if_health_check_failed()
    connection.ensure_connection()
    with connection.wrap_database_errors:
        yield _CHECKS[vendor](alias, connection)


def _psycopg(alias: str, wrapper: Any) -> Any:
    import psycopg

    raw = wrapper.connection
    if not isinstance(raw, psycopg.Connection):
        raise ConfigurationError(
            f"the database {alias!r} does not run on psycopg 3, so jobs cannot join "
            "its transactions"
        )
    return raw


def _sqlite(alias: str, wrapper: Any) -> Any:
    raw = wrapper.connection
    if not isinstance(raw, sqlite3.Connection):
        raise ConfigurationError(
            f"the database {alias!r} does not run on the sqlite3 module, so jobs cannot "
            "join its transactions"
        )
    if wrapper.is_in_memory_db():
        raise ConfigurationError(
            f"the database {alias!r} is in memory, so no worker could share the queue it "
            'would hold; point "database" at an alias whose NAME is the file in url, and in '
            "tests, where Django's test runner puts a SQLite alias in memory, set "
            '"eager": True'
        )
    return raw


_CHECKS = {"postgresql": _psycopg, "sqlite": _sqlite}
