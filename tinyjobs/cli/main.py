from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from .. import __version__
from ..app import TinyJobs
from ..exceptions import TinyJobsError
from .commands import COMMANDS
from .commands.base import build_app_options

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
DEFAULT_LOG_LEVEL = "INFO"


def build_global_options() -> argparse.ArgumentParser:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default=argparse.SUPPRESS,
        help=f"logging verbosity (default: {DEFAULT_LOG_LEVEL})",
    )
    return shared


def build_parser() -> argparse.ArgumentParser:
    shared = build_global_options()
    app_options = build_app_options()
    parser = argparse.ArgumentParser(
        prog="tinyjobs",
        description="background jobs and cron, without a broker",
        parents=[shared],
    )
    parser.add_argument(
        "--version", action="version", version=f"tinyjobs {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in COMMANDS:
        subparser = subparsers.add_parser(
            command.name, help=command.help, parents=[shared, app_options]
        )
        command.configure(subparser, argparse.ArgumentParser(
            add_help=False, parents=[shared, app_options]
        ))
        subparser.set_defaults(handler=command)
    return parser


def configure_own_logger(level: str) -> None:
    logger = logging.getLogger("tinyjobs")
    logger.setLevel(level)
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    logger.propagate = False


def main(argv: Sequence[str] | None = None, *, app: TinyJobs | None = None) -> int:
    parser = build_parser()
    parser.set_defaults(instance=app)
    args = parser.parse_args(argv)
    level = getattr(args, "log_level", DEFAULT_LOG_LEVEL)
    if app is None:
        logging.basicConfig(level=level, format=LOG_FORMAT, stream=sys.stderr)
    else:
        configure_own_logger(level)
    try:
        return args.handler.run(args)
    except TinyJobsError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
