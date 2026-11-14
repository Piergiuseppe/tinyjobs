from .base import Command
from .dashboard import DashboardCommand
from .health import CheckCommand, StatsCommand
from .jobs import JobsCommand
from .move import MoveCommand
from .purge import PurgeCommand
from .schedules import SchedulerCommand, SchedulesCommand
from .send import SendCommand
from .verify import VerifyCommand
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
    MoveCommand(),
    DashboardCommand(),
    VerifyCommand(),
)

__all__ = [
    "COMMANDS",
    "CheckCommand",
    "Command",
    "DashboardCommand",
    "JobsCommand",
    "MoveCommand",
    "PurgeCommand",
    "SchedulerCommand",
    "SchedulesCommand",
    "SendCommand",
    "StatsCommand",
    "VerifyCommand",
    "WorkerCommand",
]
