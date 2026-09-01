from __future__ import annotations

import argparse
import json

from .base import Command


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
        )
        print(job.id)
        return 0
