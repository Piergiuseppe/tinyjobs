from __future__ import annotations

import argparse
import sys

from django.core.management.base import BaseCommand, CommandError
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.tasks.exceptions import InvalidTaskBackend

from ...backend import TinyJobsBackend
from .....cli.main import main


class Command(BaseCommand):
    help = (
        "Run the tinyjobs command line against a TASKS backend, for example "
        "'tinyjobs worker' or 'tinyjobs stats'."
    )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="alias of the TASKS backend to use (default: default)",
        )
        parser.add_argument("arguments", nargs=argparse.REMAINDER)

    def handle(self, *args, backend: str, arguments: list[str], **options) -> None:
        try:
            configured = task_backends[backend]
        except InvalidTaskBackend as error:
            raise CommandError(str(error)) from None
        if not isinstance(configured, TinyJobsBackend):
            raise CommandError(
                f"TASKS[{backend!r}] uses {type(configured).__name__}, "
                f"not tinyjobs.contrib.django.TinyJobsBackend"
            )
        configured.discover()
        code = main(arguments, app=configured.app)
        if code:
            sys.exit(code)
