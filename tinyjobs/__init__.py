import sys

MINIMUM_PYTHON = (3, 11)

if sys.version_info < MINIMUM_PYTHON:
    raise RuntimeError(
        "tinyjobs needs Python {}.{} or newer, but this is Python {}.{} at {}. "
        "Run it with a newer interpreter, for example: "
        "python3.12 -m examples.demo".format(
            *MINIMUM_PYTHON,
            sys.version_info.major,
            sys.version_info.minor,
            sys.executable,
        )
    )

from .app import TinyJobs
from .exceptions import (
    Abandon,
    ConfigurationError,
    Retry,
    SerializationError,
    TinyJobsError,
    UnknownBackendError,
    UnknownTaskError,
    WaitTimeout,
)
from .models import Execution, ExecutionError, Job, JobPayload
from .retention import KEEP_EVERYTHING, RetentionPolicy
from .retry import RetryPolicy
from .status import ExecutionStatus, JobStatus
from .task import Task
from .worker import Worker

UNINSTALLED_VERSION = "0.0.0+unknown"

_version: str | None = None


def __getattr__(name: str) -> str:
    if name != "__version__":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    global _version
    if _version is None:
        from importlib.metadata import PackageNotFoundError, version

        try:
            _version = version("tinyjobs")
        except PackageNotFoundError:
            _version = UNINSTALLED_VERSION
    return _version

__all__ = [
    "Abandon",
    "ConfigurationError",
    "Execution",
    "ExecutionError",
    "ExecutionStatus",
    "KEEP_EVERYTHING",
    "Job",
    "JobPayload",
    "JobStatus",
    "Retry",
    "RetentionPolicy",
    "RetryPolicy",
    "SerializationError",
    "Task",
    "TinyJobs",
    "TinyJobsError",
    "UnknownBackendError",
    "UnknownTaskError",
    "WaitTimeout",
    "Worker",
    "__version__",
]
