from __future__ import annotations

import argparse

from ...status import JobStatus
from ..formatting import format_duration, format_timestamp, render_table, shorten
from .base import Command

LIST_HEADERS = ("ID", "TASK", "STATUS", "QUEUE", "ATT", "CREATED")


class JobsCommand(Command):
    name = "jobs"
    help = "inspect and control jobs"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        actions = parser.add_subparsers(dest="action", required=True)

        listing = actions.add_parser(
            "list", help="list jobs, newest first", parents=[shared]
        )
        listing.add_argument(
            "--status", choices=[str(s) for s in JobStatus], default=None
        )
        listing.add_argument("--queue", default=None)
        listing.add_argument("--limit", type=int, default=50)

        show = actions.add_parser(
            "show", help="show one job and every attempt", parents=[shared]
        )
        show.add_argument("job_id")

        retry = actions.add_parser(
            "retry", help="requeue a failed job", parents=[shared]
        )
        retry.add_argument("job_id")

        cancel = actions.add_parser(
            "cancel", help="cancel a queued or running job", parents=[shared]
        )
        cancel.add_argument("job_id")

    def run(self, args: argparse.Namespace) -> int:
        app = self.resolve_app(args)
        handler = getattr(self, f"_{args.action}")
        return handler(app, args)

    def _list(self, app, args: argparse.Namespace) -> int:
        page = app.jobs(
            status=JobStatus(args.status) if args.status else None,
            queue=args.queue,
            limit=args.limit,
        )
        rows = [
            (
                job.id,
                shorten(job.task, 34),
                str(job.status),
                job.queue,
                f"{job.attempts}/{job.max_attempts}",
                format_timestamp(job.created_at),
            )
            for job in page.items
        ]
        print(render_table(LIST_HEADERS, rows))
        if page.cursor:
            print(f"\nmore results available after {page.cursor}")
        return 0

    def _show(self, app, args: argparse.Namespace) -> int:
        job = app.get(args.job_id)
        if job is None:
            print(f"no job with id {args.job_id}")
            return 1
        payload = app.decode(job)
        print(f"id         {job.id}")
        print(f"task       {job.task}")
        print(f"status     {job.status}")
        print(f"queue      {job.queue}  priority {job.priority}")
        print(f"attempts   {job.attempts}/{job.max_attempts}")
        print(f"args       {payload.args!r}")
        print(f"kwargs     {payload.kwargs!r}")
        print(f"created    {format_timestamp(job.created_at)}")
        print(f"run at     {format_timestamp(job.run_at)}")
        print(f"finished   {format_timestamp(job.finished_at)}")
        if job.last_error:
            print(f"last error {job.last_error}")
        print("\nattempts")
        for execution in app.backend.executions(job.id):
            print(
                f"  {execution.attempt:>3}  {str(execution.status):<8} "
                f"{format_duration(execution.duration):>8}  "
                f"{execution.worker_id}  {execution.error_type or ''}"
            )
        return 0

    def _retry(self, app, args: argparse.Namespace) -> int:
        if not app.retry(args.job_id):
            print(f"job {args.job_id} is not in a retryable state")
            return 1
        print(f"job {args.job_id} requeued")
        return 0

    def _cancel(self, app, args: argparse.Namespace) -> int:
        if not app.cancel(args.job_id):
            print(f"job {args.job_id} is not queued or running")
            return 1
        print(f"job {args.job_id} cancelled")
        return 0
