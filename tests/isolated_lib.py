from __future__ import annotations

import os


def imported_pid() -> int:
    return os.getpid()


def register(app, **options):
    return app.task(isolated=True, **options)
