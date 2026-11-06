from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any

from .exceptions import ConfigurationError
from .exiting import end_when_stdin_closes

VARIABLE_PREFIX = "TINYJOBS_VERIFY_"
URL_VARIABLE = "TINYJOBS_VERIFY_URL"
LEDGER_VARIABLE = "TINYJOBS_VERIFY_LEDGER"
PREFIX_VARIABLE = "TINYJOBS_VERIFY_PREFIX"
WATCH_VARIABLE = "TINYJOBS_VERIFY_WATCH"
TOKEN_VARIABLE = "TINYJOBS_VERIFY_TOKEN"

TASK = "tinyjobs.verify.mark"

ATTEMPTS = 20

LEDGER_TIMEOUT = 30.0

MISSING_MESSAGE = (
    "tinyjobs.verify:app is built from {url} and {ledger} in the environment, "
    "which tinyjobs verify sets for the workers it starts"
)


def build(
    url: str, ledger: str, prefix: str | None = None, token: str | None = None
) -> Any:
    from .app import TinyJobs

    options = {"prefix": prefix} if prefix else {}
    app = TinyJobs(url, **options)
    worker = token or str(os.getpid())

    @app.task(name=TASK, max_attempts=ATTEMPTS)
    def mark(number: int, sleep: float) -> None:
        time.sleep(sleep)
        record(ledger, number, worker)

    return app


def open_ledger(path: str) -> None:
    connection = sqlite3.connect(path, timeout=LEDGER_TIMEOUT)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("CREATE TABLE IF NOT EXISTS marks (number INTEGER PRIMARY KEY)")
        connection.execute("CREATE TABLE IF NOT EXISTS workers (token TEXT PRIMARY KEY)")
        connection.commit()
    finally:
        connection.close()


def record(path: str, number: int, token: str) -> None:
    connection = sqlite3.connect(path, timeout=LEDGER_TIMEOUT)
    try:
        with connection:
            connection.execute("INSERT OR IGNORE INTO marks (number) VALUES (?)", (number,))
            connection.execute("INSERT OR IGNORE INTO workers (token) VALUES (?)", (token,))
    finally:
        connection.close()


def marked(path: str) -> set[int]:
    return {int(value) for value in _column(path, "SELECT number FROM marks")}


def ready(path: str) -> set[str]:
    return {str(value) for value in _column(path, "SELECT token FROM workers")}


def _column(path: str, query: str) -> list[Any]:
    connection = sqlite3.connect(path, timeout=LEDGER_TIMEOUT)
    try:
        return [row[0] for row in connection.execute(query).fetchall()]
    finally:
        connection.close()


def __getattr__(name: str) -> Any:
    if name != "app":
        raise AttributeError(name)
    url, ledger = os.environ.get(URL_VARIABLE), os.environ.get(LEDGER_VARIABLE)
    if not url or not ledger:
        raise ConfigurationError(MISSING_MESSAGE.format(url=URL_VARIABLE, ledger=LEDGER_VARIABLE))
    app = build(url, ledger, os.environ.get(PREFIX_VARIABLE), os.environ.get(TOKEN_VARIABLE))
    globals()["app"] = app
    if os.environ.get(WATCH_VARIABLE):
        threading.Thread(
            target=end_when_stdin_closes, name="tinyjobs-verify-watch", daemon=True
        ).start()
    return app
