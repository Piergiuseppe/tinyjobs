import sys

MINIMUM_PYTHON = (3, 12)

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

__version__ = "0.13.0"

from .app import TinyJobs
from .clock import FrozenClock
from .exceptions import (
    Abandon,
    ConfigurationError,
    IsolatedTaskError,
    ProcessDied,
    ResultError,
    ResultTooLarge,
    Retry,
    SerializationError,
    TinyJobsError,
    UnencodableResult,
    UnknownBackendError,
    UnknownTaskError,
    WaitTimeout,
)
from .models import Execution, ExecutionError, Job, JobPayload, ScheduleState
from .retention import KEEP_EVERYTHING, RetentionPolicy
from .retry import RetryPolicy
from .scheduler import Scheduler
from .schedules import MisfirePolicy, Schedule
from .status import ExecutionStatus, JobStatus
from .task import Task
from .worker import Worker

__all__ = [
    "Abandon",
    "ConfigurationError",
    "Execution",
    "ExecutionError",
    "ExecutionStatus",
    "FrozenClock",
    "IsolatedTaskError",
    "KEEP_EVERYTHING",
    "Job",
    "JobPayload",
    "JobStatus",
    "MisfirePolicy",
    "ProcessDied",
    "ResultError",
    "ResultTooLarge",
    "Retry",
    "RetentionPolicy",
    "RetryPolicy",
    "Schedule",
    "ScheduleState",
    "Scheduler",
    "SerializationError",
    "Task",
    "TinyJobs",
    "TinyJobsError",
    "UnencodableResult",
    "UnknownBackendError",
    "UnknownTaskError",
    "WaitTimeout",
    "Worker",
    "__version__",
]
