from __future__ import annotations

import argparse

from ..duration import parse_duration
from ..formatting import format_timestamp, render_table
from .base import Command

LIST_HEADERS = ("NAME", "CRON", "TIMEZONE", "NEXT RUN", "LAST RUN", "LAST JOB")
NO_SCHEDULES = (
    "no cron tasks in this app; declare one with @app.task(cron='*/5 * * * *')"
)
NOT_REGISTERED = "not registered yet"
PAUSED = "paused"
NOT_IN_APP = " (not in this app)"


class SchedulerCommand(Command):
    name = "scheduler"
    help = "fire cron schedules without running any jobs"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--misfire-grace",
            type=parse_duration,
            default=60.0,
            metavar="DURATION",
            help="how late a firing may be and still count as on time (default: 60s)",
        )

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        if not any(task.schedule for task in app.registry):
            print(NO_SCHEDULES)
            return 1
        app.scheduler(misfire_grace=args.misfire_grace).run()
        return 0


class SchedulesCommand(Command):
    name = "schedules"
    help = "list cron schedules, pause and resume them, or fire one now"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        actions = parser.add_subparsers(dest="action")
        actions.add_parser(
            "list", help="list cron schedules and when they fire next", parents=[shared]
        )
        for action, help in (
            ("pause", "stop a schedule from firing until it is resumed"),
            ("resume", "let a paused schedule fire again, from its next time on"),
            ("run", "enqueue a run of a cron task now, leaving its schedule as it is"),
        ):
            actions.add_parser(action, help=help, parents=[shared]).add_argument(
                "name", help="the cron task's name, as tinyjobs schedules lists it"
            )

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        handler = getattr(self, f"_{args.action or 'list'}")
        return handler(app, args)

    def _pause(self, app, args: argparse.Namespace) -> int:
        if app.pause_schedule(args.name):
            print(f"{args.name} paused")
            return 0
        print(f"{args.name} is already paused")
        return 0

    def _resume(self, app, args: argparse.Namespace) -> int:
        if app.resume_schedule(args.name):
            print(f"{args.name} resumed")
            return 0
        print(f"{args.name} is not paused")
        return 0

    def _run(self, app, args: argparse.Namespace) -> int:
        print(app.run_schedule(args.name).id)
        return 0

    def _list(self, app, args: argparse.Namespace) -> int:
        declared = {task.name: task for task in app.registry if task.schedule}
        stored = {state.name: state for state in app.schedules()}
        rows = []
        for name in sorted(set(declared) | set(stored)):
            state = stored.get(name)
            if state is None:
                schedule = declared[name].schedule
                rows.append(
                    (name, schedule.cron.source, schedule.timezone,
                     NOT_REGISTERED, "-", "-")
                )
                continue
            rows.append(
                (
                    name + ("" if name in declared else NOT_IN_APP),
                    state.cron,
                    state.timezone,
                    PAUSED if state.paused else format_timestamp(state.next_run_at),
                    format_timestamp(state.last_run_at),
                    state.last_job_id or "-",
                )
            )
        if not rows:
            print(NO_SCHEDULES)
            return 0
        print(render_table(LIST_HEADERS, rows))
        return 0
