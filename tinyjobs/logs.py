from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from .exceptions import error_message, error_traceback, error_type
from .models import Job

RECORD_ATTRIBUTES = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", (), None))
) | {"message", "asctime", "taskName"}


def job_fields(job: Job, **more: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "job_id": job.id,
        "task": job.task,
        "queue": job.queue,
        "attempt": job.attempts,
    }
    fields.update(more)
    return fields


def failure_fields(error: BaseException, will_retry: bool) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "error": error_type(error),
        "error_message": error_message(error),
        "will_retry": will_retry,
    }
    if not will_retry:
        fields["traceback"] = error_traceback(error)
    return fields


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        traceback = getattr(record, "traceback", None)
        if not traceback:
            return super().format(record)
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        line = self.formatMessage(record) + "\n" + traceback.rstrip("\n")
        if record.stack_info:
            line += "\n" + self.formatStack(record.stack_info)
        return line


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        traceback = getattr(record, "traceback", None)
        if traceback:
            entry["traceback"] = traceback
        elif record.exc_info:
            entry["traceback"] = self.formatException(record.exc_info)
        if record.stack_info:
            entry["stack"] = self.formatStack(record.stack_info)
        for key, value in vars(record).items():
            if key not in RECORD_ATTRIBUTES and key not in entry:
                entry[key] = _writable(value)
        return json.dumps(entry)


def _writable(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, default=str, allow_nan=False))
    except Exception:
        try:
            return repr(value)
        except Exception:
            return f"<unrepresentable {type(value).__name__}>"
