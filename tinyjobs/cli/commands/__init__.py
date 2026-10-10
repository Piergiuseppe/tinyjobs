from .base import Command
from .health import CheckCommand, StatsCommand
from .jobs import JobsCommand
from .purge import PurgeCommand
from .schedules import SchedulerCommand, SchedulesCommand
from .send import SendCommand
from .worker import WorkerCommand

COMMANDS: tuple[Command, ...] = (
    WorkerCommand(),
    SchedulerCommand(),
    JobsCommand(),
    SchedulesCommand(),
    StatsCommand(),
    CheckCommand(),
    SendCommand(),
    PurgeCommand(),
)

__all__ = [
    "COMMANDS",
    "CheckCommand",
    "Command",
    "JobsCommand",
    "PurgeCommand",
    "SchedulerCommand",
    "SchedulesCommand",
    "SendCommand",
    "StatsCommand",
    "WorkerCommand",
]
