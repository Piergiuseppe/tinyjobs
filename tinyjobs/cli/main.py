from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from .. import __version__
from ..app import TinyJobs
from ..exceptions import TinyJobsError
from ..logs import JSONFormatter, TextFormatter
from .commands import COMMANDS
from .commands.base import build_app_options

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s  %(message)s"
LOG_FORMATTERS = {"text": TextFormatter, "json": JSONFormatter}
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
LOG_FORMATS = tuple(LOG_FORMATTERS)
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "text"


def build_global_options() -> argparse.ArgumentParser:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default=argparse.SUPPRESS,
        help=f"logging verbosity (default: {DEFAULT_LOG_LEVEL})",
    )
    shared.add_argument(
        "--log-format",
        choices=LOG_FORMATS,
        default=argparse.SUPPRESS,
        help="text lines, or one JSON object per line with the job fields "
             f"(default: {DEFAULT_LOG_FORMAT})",
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


HOST_LOGGING_MESSAGE = (
    "--log-format is ignored: the {logger} logger already has handlers of its own; "
    "set tinyjobs.logs.{formatter} on them for {format} lines"
)


def host_logging_message(logger_name: str, log_format: str) -> str:
    return HOST_LOGGING_MESSAGE.format(
        logger=logger_name, formatter=LOG_FORMATTERS[log_format].__name__, format=log_format
    )


class OwnHandler(logging.StreamHandler):
    pass


def log_handler(log_format: str) -> logging.Handler:
    handler = OwnHandler(sys.stderr)
    handler.setFormatter(log_formatter(log_format))
    return handler


def log_formatter(log_format: str) -> logging.Formatter:
    return LOG_FORMATTERS[log_format](LOG_FORMAT)


def configure_logging(
    logger: logging.Logger, level: str, log_format: str, *, format_requested: bool
) -> None:
    logger.setLevel(level)
    if _reformat(logger, log_format):
        return
    if not logger.handlers:
        logger.addHandler(log_handler(log_format))
        logger.propagate = False
    elif format_requested:
        message = host_logging_message(logger.name, log_format)
        logger.handle(
            logger.makeRecord(logger.name, logging.WARNING, __file__, 0, message, (), None)
        )


def _reformat(logger: logging.Logger, log_format: str) -> bool:
    own = [handler for handler in logger.handlers if isinstance(handler, OwnHandler)]
    for handler in own:
        handler.setFormatter(log_formatter(log_format))
    return bool(own)


def main(
    argv: Sequence[str] | None = None,
    *,
    app: TinyJobs | None = None,
    owns_process: bool = False,
) -> int:
    parser = build_parser()
    parser.set_defaults(instance=app, owns_process=owns_process)
    args = parser.parse_args(argv)
    logger = logging.getLogger() if app is None else logging.getLogger("tinyjobs")
    configure_logging(
        logger,
        getattr(args, "log_level", DEFAULT_LOG_LEVEL),
        getattr(args, "log_format", DEFAULT_LOG_FORMAT),
        format_requested=hasattr(args, "log_format"),
    )
    try:
        return args.handler.run(args)
    except TinyJobsError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


def run() -> int:
    return main(owns_process=True)
