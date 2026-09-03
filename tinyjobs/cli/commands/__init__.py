from .base import Command
from .jobs import JobsCommand
from .purge import PurgeCommand
from .send import SendCommand
from .worker import WorkerCommand

COMMANDS: tuple[Command, ...] = (
    WorkerCommand(),
    JobsCommand(),
    SendCommand(),
    PurgeCommand(),
)

__all__ = [
    "COMMANDS",
    "Command",
    "JobsCommand",
    "PurgeCommand",
    "SendCommand",
    "WorkerCommand",
]
