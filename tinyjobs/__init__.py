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

__version__ = "0.18.0.dev0"

from .app import TinyJobs
from .clock import Clock, FrozenClock
from .limits import Limit
from .exceptions import (
    Abandon,
    AppClosedError,
    BackendNotReadyError,
    ConfigurationError,
    EventLoopStoppedError,
    ExecutorClosedError,
    ForcedInterrupt,
    InterruptedCallError,
    IsolatedTaskError,
    JobNotFoundError,
    NewerStoreError,
    ProcessDied,
    ResultError,
    ResultTooLarge,
    Retry,
    SerializationError,
    SynchronousCallInsideEventLoopError,
    TaskAlreadyRegisteredError,
    TinyJobsError,
    UnencodableResult,
    UnknownBackendError,
    UnknownCodecError,
    UnknownTaskError,
    UnsupportedCapabilityError,
    WaitTimeout,
)
from .models import (
    Execution,
    ExecutionError,
    Finding,
    Job,
    JobPayload,
    Page,
    QueueStats,
    ScheduleState,
    Stats,
)
from .retention import KEEP_EVERYTHING, RetentionPolicy
from .retry import RetryPolicy
from .scheduler import Scheduler
from .schedules import MisfirePolicy, Schedule
from .status import ExecutionStatus, JobStatus
from .task import Task
from .worker import Worker

__all__ = [
    "Abandon",
    "AppClosedError",
    "BackendNotReadyError",
    "Clock",
    "ConfigurationError",
    "EventLoopStoppedError",
    "Execution",
    "ExecutionError",
    "ExecutionStatus",
    "ExecutorClosedError",
    "Finding",
    "ForcedInterrupt",
    "FrozenClock",
    "InterruptedCallError",
    "IsolatedTaskError",
    "Job",
    "JobNotFoundError",
    "JobPayload",
    "JobStatus",
    "KEEP_EVERYTHING",
    "Limit",
    "MisfirePolicy",
    "NewerStoreError",
    "Page",
    "ProcessDied",
    "QueueStats",
    "ResultError",
    "ResultTooLarge",
    "RetentionPolicy",
    "Retry",
    "RetryPolicy",
    "Schedule",
    "Scheduler",
    "ScheduleState",
    "SerializationError",
    "Stats",
    "SynchronousCallInsideEventLoopError",
    "Task",
    "TaskAlreadyRegisteredError",
    "TinyJobs",
    "TinyJobsError",
    "UnencodableResult",
    "UnknownBackendError",
    "UnknownCodecError",
    "UnknownTaskError",
    "UnsupportedCapabilityError",
    "WaitTimeout",
    "Worker",
    "__version__",
]
