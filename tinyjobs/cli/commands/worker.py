from __future__ import annotations

import argparse
import logging

from ...retention import RetentionPolicy
from ..duration import parse_duration
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
        parser.add_argument("--retain-success", type=parse_duration, default=None,
                            metavar="DURATION", help="e.g. 7d, 0 to keep forever")
        parser.add_argument("--retain-cancelled", type=parse_duration, default=None,
                            metavar="DURATION")
        parser.add_argument("--retain-failed", type=parse_duration, default=None,
                            metavar="DURATION", help="e.g. 30d")
        parser.add_argument("--purge-interval", type=parse_duration, default=None,
                            metavar="DURATION", help="how often to sweep, e.g. 1h")
        parser.add_argument("--no-purge", action="store_true",
                            help="never prune anything")
        parser.add_argument("--no-scheduler", action="store_true",
                            help="do not fire cron schedules from this worker")
        parser.add_argument("--misfire-grace", type=parse_duration, default=60.0,
                            metavar="DURATION",
                            help="how late a cron firing may be and still count "
                                 "as on time (default: 60s)")

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        worker = app.worker(
            retention=self._retention(args, app.retention),
            queues=[q.strip() for q in args.queues.split(",") if q.strip()],
            concurrency=args.concurrency,
            async_concurrency=args.async_concurrency,
            lease_seconds=args.lease_seconds,
            reap_interval=args.reap_interval,
            shutdown_timeout=args.shutdown_timeout,
            poll_minimum=args.poll_min,
            poll_maximum=args.poll_max,
            scheduler=not args.no_scheduler,
            misfire_grace=args.misfire_grace,
        )
        worker.run()
        return 0

    def _retention(
        self, args: argparse.Namespace, default: RetentionPolicy
    ) -> RetentionPolicy:
        if args.no_purge:
            return RetentionPolicy(success=0, cancelled=0, failed=0)
        return RetentionPolicy(
            success=_or(args.retain_success, default.success),
            cancelled=_or(args.retain_cancelled, default.cancelled),
            failed=_or(args.retain_failed, default.failed),
            interval=_or(args.purge_interval, default.interval),
            batch=default.batch,
            maximum_per_sweep=default.maximum_per_sweep,
        )


def _or(value: float | None, fallback: float) -> float:
    return fallback if value is None else value
