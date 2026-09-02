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
)
from .models import Execution, ExecutionError, Job, JobPayload
from .retry import RetryPolicy
from .status import ExecutionStatus, JobStatus
from .task import Task
from .worker import Worker

__version__ = "0.1.0"

__all__ = [
    "Abandon",
    "ConfigurationError",
    "Execution",
    "ExecutionError",
    "ExecutionStatus",
    "Job",
    "JobPayload",
    "JobStatus",
    "Retry",
    "RetryPolicy",
    "SerializationError",
    "Task",
    "TinyJobs",
    "TinyJobsError",
    "UnknownBackendError",
    "UnknownTaskError",
    "Worker",
    "__version__",
]
