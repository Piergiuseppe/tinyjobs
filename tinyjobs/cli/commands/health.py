from __future__ import annotations

import argparse

from ...duration import format_age
from ...status import JobStatus
from ..duration import parse_duration
from ..formatting import render_table
from .base import Command

STATUSES = tuple(JobStatus)
STATS_HEADERS = ("QUEUE", *(str(status).upper() for status in STATUSES), "OLDEST DUE")


class StatsCommand(Command):
    name = "stats"
    help = "count jobs by queue and status, and show how long the oldest has waited"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        return None

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        stats = app.stats()
        if not stats.queues:
            print("no jobs yet")
            return 0
        now = app.clock.now()
        rows = [
            (
                queue.queue,
                *(str(queue.counts.get(str(status), 0)) for status in STATUSES),
                "-" if queue.oldest_due_at is None
                else format_age(now - queue.oldest_due_at),
            )
            for queue in stats.queues
        ]
        print(render_table(STATS_HEADERS, rows))
        print(f"\nrunning past their lease: {stats.expired_leases}")
        return 0


class CheckCommand(Command):
    name = "check"
    help = "check the store, the tasks and the queues; exits 1 on any problem"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--max-wait",
            type=parse_duration,
            default=None,
            metavar="DURATION",
            help="fail when a due job has waited longer than this",
        )

    def run(self, args: argparse.Namespace) -> int:
        findings = self.resolve_app(args).check(max_wait=args.max_wait)
        for finding in findings:
            print(f"{'ok  ' if finding.ok else 'FAIL'}  {finding.message}")
        return 0 if all(finding.ok for finding in findings) else 1
