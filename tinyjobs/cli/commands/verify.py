from __future__ import annotations

import argparse
from threading import Event

from ...exiting import stop_on_signals
from ...verifying import REAP_INTERVAL, Settings, Verification, masked
from ..duration import parse_duration
from .base import Command

DEFAULTS = Settings(url="")


def settled(settle: float | None, lease: float) -> float:
    if settle is not None:
        return settle
    return max(DEFAULTS.settle, 2 * lease + REAP_INTERVAL)


class VerifyCommand(Command):
    name = "verify"
    help = "prove the guarantees on a store: steady load, workers killed, nothing lost"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "url",
            help="the store to run on, holding no other jobs: a fresh file, a spare Redis "
                 "database or a scratch Postgres database",
        )
        parser.add_argument("--prefix", default=None, help="key or table prefix on that store")
        parser.add_argument(
            "--duration", type=parse_duration, default=DEFAULTS.duration, metavar="DURATION",
            help=f"how long to send jobs for (default: {DEFAULTS.duration:g}s)",
        )
        parser.add_argument(
            "--rate", type=float, default=DEFAULTS.rate, metavar="PER_SECOND",
            help=f"jobs sent per second (default: {DEFAULTS.rate:g})",
        )
        parser.add_argument(
            "--workers", type=int, default=DEFAULTS.workers,
            help=f"worker processes kept running (default: {DEFAULTS.workers})",
        )
        parser.add_argument(
            "--slots", type=int, default=DEFAULTS.slots,
            help=f"jobs each worker runs at once (default: {DEFAULTS.slots})",
        )
        parser.add_argument(
            "--kill-every", type=parse_duration, default=DEFAULTS.kill_every, metavar="DURATION",
            help=f"stop a worker this often, SIGTERM and SIGKILL in turn "
                 f"(default: {DEFAULTS.kill_every:g}s)",
        )
        parser.add_argument(
            "--lease", type=parse_duration, default=DEFAULTS.lease, metavar="DURATION",
            help=f"the workers' lease, what a SIGKILL costs (default: {DEFAULTS.lease:g}s)",
        )
        parser.add_argument(
            "--settle", type=parse_duration, default=None, metavar="DURATION",
            help=f"how long to wait for the last jobs after the sending stops, above the "
                 f"lease plus the reap interval (default: {DEFAULTS.settle:g}s, or twice the "
                 f"lease when that is more)",
        )
        parser.add_argument(
            "--keep", action="store_true",
            help="leave the jobs in the store instead of purging them",
        )

    def run(self, args: argparse.Namespace) -> int:
        settings = Settings(
            url=args.url,
            prefix=args.prefix,
            duration=args.duration,
            rate=args.rate,
            workers=args.workers,
            slots=args.slots,
            kill_every=args.kill_every,
            lease=args.lease,
            settle=settled(args.settle, args.lease),
            keep=args.keep,
        )
        print(
            f"{settings.workers} workers of {settings.slots} slots, {settings.rate:g} jobs/s "
            f"for {settings.duration:g}s, a SIGTERM or SIGKILL every {settings.kill_every:g}s, "
            f"on {masked(args.url)}",
            flush=True,
        )
        stopped = Event()
        restore = stop_on_signals(stopped, ends_process=getattr(args, "owns_process", False))
        try:
            report = Verification(
                settings, stopped=stopped, say=lambda line: print(line, flush=True)
            ).run()
        finally:
            restore()
        for line in report.lines():
            print(line)
        return 0 if report.ok else 1
