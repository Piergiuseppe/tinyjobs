from __future__ import annotations

import argparse
import logging

from .base import Command


class WorkerCommand(Command):
    name = "worker"
    help = "claim jobs from the queue and run them"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--queues",
            default="default",
            help="comma separated list of queues to consume (default: default)",
        )
        parser.add_argument("--concurrency", type=int, default=8)
        parser.add_argument("--async-concurrency", type=int, default=None)
        parser.add_argument("--lease", type=float, default=60.0, dest="lease_seconds")
        parser.add_argument("--reap-interval", type=float, default=30.0)
        parser.add_argument("--shutdown-timeout", type=float, default=30.0)
        parser.add_argument("--poll-min", type=float, default=0.005)
        parser.add_argument("--poll-max", type=float, default=0.5)

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        worker = app.worker(
            queues=[q.strip() for q in args.queues.split(",") if q.strip()],
            concurrency=args.concurrency,
            async_concurrency=args.async_concurrency,
            lease_seconds=args.lease_seconds,
            reap_interval=args.reap_interval,
            shutdown_timeout=args.shutdown_timeout,
            poll_minimum=args.poll_min,
            poll_maximum=args.poll_max,
        )
        worker.run()
        return 0
