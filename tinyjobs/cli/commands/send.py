from __future__ import annotations

import argparse
import json
import sys

from ...status import JobStatus
from .base import Command


def _job_id(value: str) -> str:
    if not value:
        raise argparse.ArgumentTypeError("the id of a job, not an empty value")
    return value


class SendCommand(Command):
    name = "send"
    help = "enqueue a job by task name without importing its code"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument("task")
        parser.add_argument("--args", default="[]", help="JSON list of positional arguments")
        parser.add_argument("--kwargs", default="{}", help="JSON object of keyword arguments")
        parser.add_argument("--queue", default=None)
        parser.add_argument("--priority", type=int, default=None)
        parser.add_argument("--delay", type=float, default=None)
        parser.add_argument("--max-attempts", type=int, default=None)
        parser.add_argument("--key", default=None)
        parser.add_argument(
            "--after", default=None, type=_job_id, help="the id of a job this one runs after"
        )

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        job = app.send(
            args.task,
            args=json.loads(args.args),
            kwargs=json.loads(args.kwargs),
            queue=args.queue,
            priority=args.priority,
            delay=args.delay,
            max_attempts=args.max_attempts,
            key=args.key,
            after=args.after,
        )
        print(job.id)
        if job.status is JobStatus.CANCELLED:
            print(f"stored as cancelled: {job.last_error}", file=sys.stderr)
        return 0
