from __future__ import annotations

import argparse
from threading import Event

from ...dashboard.core import DEFAULT_HOST, DEFAULT_PORT, Dashboard
from ...exceptions import ConfigurationError
from ...exiting import stop_on_signals
from ...web import bind_problem
from ..address import parse_port
from .base import Command


class DashboardCommand(Command):
    name = "dashboard"
    help = "serve a page of queues, jobs and schedules, with retry, cancel, pause, resume and run"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--host",
            default=DEFAULT_HOST,
            help=f"address to listen on (default: {DEFAULT_HOST}; 0.0.0.0 exposes it)",
        )
        parser.add_argument(
            "--port",
            type=parse_port,
            default=DEFAULT_PORT,
            metavar="PORT",
            help=f"port to listen on (default: {DEFAULT_PORT}; 0 for any free one)",
        )

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        from ...dashboard.server import DashboardServer

        try:
            server = DashboardServer(Dashboard(app), args.host, args.port)
        except OSError as error:
            raise ConfigurationError(
                bind_problem(
                    "the dashboard", args.host, args.port, error,
                    host_flag="--host", port_flag="--port",
                )
            ) from error
        stopped = Event()
        restore = stop_on_signals(stopped, ends_process=getattr(args, "owns_process", False))
        try:
            with server:
                try:
                    stopped.wait()
                except KeyboardInterrupt:
                    pass
        finally:
            restore()
        return 0

