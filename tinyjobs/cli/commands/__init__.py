from .base import Command
from .jobs import JobsCommand
from .send import SendCommand
from .worker import WorkerCommand

COMMANDS: tuple[Command, ...] = (WorkerCommand(), JobsCommand(), SendCommand())

__all__ = ["COMMANDS", "Command", "JobsCommand", "SendCommand", "WorkerCommand"]
