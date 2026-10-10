from .base import Command
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
    SendCommand(),
    PurgeCommand(),
)

__all__ = [
    "COMMANDS",
    "Command",
    "JobsCommand",
    "PurgeCommand",
    "SchedulerCommand",
    "SchedulesCommand",
    "SendCommand",
    "WorkerCommand",
]
