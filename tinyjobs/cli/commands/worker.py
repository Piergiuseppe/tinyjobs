from __future__ import annotations

import argparse
import asyncio
import errno
from contextlib import AbstractContextManager, nullcontext
from dataclasses import replace
from typing import Any, Callable

from ...app import TinyJobs
from ...exceptions import ConfigurationError
from ...models import Stats
from ...retention import RetentionPolicy
from ...worker.signals import owning_process
from ..duration import parse_duration
from ..loader import load_callable
from .base import Command


METRICS_BIND_MESSAGE = "cannot serve metrics on {address}: {error}; {remedy}"
ADDRESS_REMEDY = (
    "give --metrics-host an address of this machine, such as 127.0.0.1 or 0.0.0.0"
)
PORT_REMEDY = "pick a free --metrics-port, or 0 for any"
PRIVILEGED_REMEDY = "pick a --metrics-port above 1023, or 0 for any"
STATS_BUDGET = 5.0
STATS_TIMEOUT_MESSAGE = "the store did not answer the stats query within {budget:g}s"


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
        parser.add_argument("--loops", type=int, default=1,
                            help="event loops in this process, each with its own backend "
                                 "connection; above 1 pays off on free-threaded Python")
        parser.add_argument("--processes", type=int, default=None,
                            help="job processes for isolated=True tasks (default: "
                                 "concurrency or the CPU count, whichever is lower, "
                                 "and never fewer than loops)")
        parser.add_argument("--jobs-per-process", type=int, default=None,
                            help="replace a job process after this many jobs "
                                 "(default: never)")
        parser.add_argument("--process-setup", default=None, metavar="MODULE:FUNCTION",
                            help="called once in each job process before its first job")
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
        parser.add_argument("--metrics-port", type=port, default=None, metavar="PORT",
                            help="serve Prometheus metrics at /metrics on this port "
                                 "(default: off)")
        parser.add_argument("--metrics-host", default="127.0.0.1",
                            help="address the metrics server listens on "
                                 "(default: 127.0.0.1)")

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        worker = app.worker(
            retention=self._retention(args, app.retention),
            queues=[q.strip() for q in args.queues.split(",") if q.strip()],
            concurrency=args.concurrency,
            async_concurrency=args.async_concurrency,
            loops=args.loops,
            processes=args.processes,
            jobs_per_process=args.jobs_per_process,
            process_setup=load_callable(args.process_setup) if args.process_setup else None,
            lease_seconds=args.lease_seconds,
            reap_interval=args.reap_interval,
            shutdown_timeout=args.shutdown_timeout,
            poll_minimum=args.poll_min,
            poll_maximum=args.poll_max,
            scheduler=not args.no_scheduler,
            misfire_grace=args.misfire_grace,
        )
        owning = owning_process() if getattr(args, "owns_process", False) else nullcontext()
        with owning, self._metrics(app, args):
            try:
                worker.run()
            except KeyboardInterrupt:
                if not worker.finished:
                    raise
        return 0

    def _metrics(self, app: TinyJobs, args: argparse.Namespace) -> AbstractContextManager[Any]:
        if args.metrics_port is None:
            return nullcontext()
        from ...exporter import MetricsServer, address, unreachable

        try:
            return MetricsServer(
                app.metrics, stats_within(app, STATS_BUDGET), args.metrics_host, args.metrics_port
            )
        except OSError as error:
            if unreachable(error):
                remedy = ADDRESS_REMEDY
            elif error.errno == errno.EACCES:
                remedy = PRIVILEGED_REMEDY
            else:
                remedy = PORT_REMEDY
            raise ConfigurationError(
                METRICS_BIND_MESSAGE.format(
                    address=address(args.metrics_host, args.metrics_port),
                    error=error,
                    remedy=remedy,
                )
            ) from error

    def _retention(
        self, args: argparse.Namespace, default: RetentionPolicy
    ) -> RetentionPolicy:
        if args.no_purge:
            return replace(
                default,
                success=0,
                cancelled=0,
                failed=0,
                interval=_or(args.purge_interval, default.interval),
            )
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


def stats_within(app: TinyJobs, budget: float) -> Callable[[], Stats]:
    async def stats_or_timeout() -> Stats:
        query = asyncio.ensure_future(app.astats())
        query.add_done_callback(_settled)
        try:
            done, _ = await asyncio.wait({query}, timeout=budget)
            if done:
                return query.result()
        finally:
            query.cancel()
        raise TimeoutError(STATS_TIMEOUT_MESSAGE.format(budget=budget))

    def stats() -> Stats:
        return app.run_sync(stats_or_timeout, name="stats")

    return stats


def _settled(query: asyncio.Future[Stats]) -> None:
    if not query.cancelled():
        query.exception()


def port(value: str) -> int:
    number = int(value)
    if not 0 <= number <= 65535:
        raise argparse.ArgumentTypeError(f"a port is between 0 and 65535, not {value}")
    return number
