from __future__ import annotations

import argparse

from ...retention import RetentionPolicy
from ..duration import format_duration, parse_duration
from .base import Command

EVERYTHING_TERMINAL = 1e-9


class PurgeCommand(Command):
    name = "purge"
    help = "prune terminal jobs older than their retention, once, now"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--retain-success", type=parse_duration, default=None, metavar="DURATION"
        )
        parser.add_argument(
            "--retain-cancelled", type=parse_duration, default=None, metavar="DURATION"
        )
        parser.add_argument(
            "--retain-failed", type=parse_duration, default=None, metavar="DURATION"
        )
        parser.add_argument(
            "--before",
            type=parse_duration,
            default=None,
            metavar="DURATION",
            help="shorthand: prune every terminal job older than this; "
                 "0 means every terminal job, whatever its age",
        )
        parser.add_argument("--dry-run", action="store_true")

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        policy = self._policy(args, app.retention)
        if not policy.enabled:
            print("retention is disabled by these settings, nothing to do")
            return 0

        counts = app.run_sync(app.backend.counts_by_status(), name="counts")
        print(f"before: {counts}")
        print(f"policy: {policy.describe()}")
        if args.dry_run:
            print("dry run, nothing removed")
            return 0

        removed = app.purge(policy)
        if removed is None:
            print("another process swept recently, nothing to do")
            return 0
        after = app.run_sync(app.backend.counts_by_status(), name="counts")
        print(f"removed {removed} job(s)")
        print(f"after:  {after}")
        return 0

    def _policy(
        self, args: argparse.Namespace, default: RetentionPolicy
    ) -> RetentionPolicy:
        if args.before is not None:
            horizon = max(args.before, EVERYTHING_TERMINAL)
            return RetentionPolicy(
                success=horizon,
                cancelled=horizon,
                failed=horizon,
                interval=0.0,
            )
        return RetentionPolicy(
            success=_or(args.retain_success, default.success),
            cancelled=_or(args.retain_cancelled, default.cancelled),
            failed=_or(args.retain_failed, default.failed),
            interval=0.0,
        )


def _or(value: float | None, fallback: float) -> float:
    return fallback if value is None else value
