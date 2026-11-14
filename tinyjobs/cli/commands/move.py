from __future__ import annotations

import argparse
import logging
from pathlib import Path
from threading import Event
from typing import Callable
from urllib.parse import urlsplit

from ...app import TinyJobs
from ...backends.loader import in_memory, named
from ...exceptions import ConfigurationError, TinyJobsError
from ...exiting import stop_on_signals
from ...moving import Move
from .base import Command

OLD = "the old store"
NEW = "the new store"

MISSING_NEW = (
    "name the store to move to: --to with its address, or --app with the TinyJobs "
    "instance that already points at it"
)

SAME_ADDRESS = "--from and --to are the same address and prefix; there is nothing to move"

MISSING_FILE = (
    "the file --from names does not exist; a relative path is read from the directory "
    "the command runs in"
)


PREFIX_WITHOUT_TO = (
    "--to-prefix goes with --to; with --app the jobs go to the store and the prefix of "
    "the app"
)

TWO_NEW = "--to and --app both name a store to move to; give one"

STOPPED = "the move stopped on an error, {error}; --log-level DEBUG shows where"

logger = logging.getLogger("tinyjobs.move")


class _StoppedOnce(Event):
    restore: Callable[[], None] = staticmethod(lambda: None)

    def set(self) -> None:
        super().set()
        self.restore()


class MoveCommand(Command):
    name = "move"
    help = "move the jobs still to run from one store to another, once, now"

    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None:
        parser.add_argument(
            "--from", dest="old", required=True, metavar="URL",
            help="the store to empty: its jobs still to run are copied to the new store "
                 "and deleted here",
        )
        parser.add_argument(
            "--from-prefix", dest="old_prefix", default=None, metavar="PREFIX",
            help="key or table prefix on the store to empty",
        )
        parser.add_argument(
            "--to", dest="new", default=None, metavar="URL",
            help="the store to move to; without it, the store of --app",
        )
        parser.add_argument(
            "--to-prefix", dest="new_prefix", default=None, metavar="PREFIX",
            help="key or table prefix on the store --to names",
        )
        parser.add_argument(
            "--dry-run", action="store_true",
            help="count what would move now and move nothing",
        )

    def run(self, args: argparse.Namespace) -> int:
        opened: list[TinyJobs] = []
        try:
            _refuse_missing_file(args.old)
            old = _opened(args.old, args.old_prefix, opened)
            new, new_name = self._new(args, opened)
            stopped = _StoppedOnce()
            stopped.restore = restore = stop_on_signals(
                stopped, ends_process=getattr(args, "owns_process", False)
            )
            move = Move(
                old,
                new,
                old_name=named(OLD, args.old),
                new_name=new_name,
                dry_run=args.dry_run,
                stopped=stopped,
                say=lambda line: print(line, flush=True),
            )
            try:
                report = move.run()
            except BaseException as error:
                report = move.report
                report.cut_short, report.uncounted = not move.through, move.through
                if report.found:
                    print("\n".join(report.lines()), flush=True)
                if isinstance(error, TinyJobsError) or not isinstance(error, Exception):
                    raise
                logger.debug("the move stopped on an error", exc_info=error)
                raise TinyJobsError(STOPPED.format(error=_described(error))) from error
            finally:
                restore()
        finally:
            for app in opened:
                app.close()
        for line in report.lines():
            print(line)
        return 0 if report.ok else 1

    def _new(self, args: argparse.Namespace, opened: list[TinyJobs]) -> tuple[TinyJobs, str]:
        if args.new is None and args.new_prefix is not None:
            raise ConfigurationError(PREFIX_WITHOUT_TO)
        if args.new is not None and getattr(args, "app", None):
            raise ConfigurationError(TWO_NEW)
        if args.new is not None:
            if (args.new, args.new_prefix) == (args.old, args.old_prefix):
                raise ConfigurationError(SAME_ADDRESS)
            return _opened(args.new, args.new_prefix, opened), named(NEW, args.new)
        if not (getattr(args, "instance", None) or getattr(args, "app", None)):
            raise ConfigurationError(MISSING_NEW)
        app = self.resolve_app(args)
        return app, named("the store of the app", app.url)


def _described(error: Exception) -> str:
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def _refuse_missing_file(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "sqlite" or in_memory(url):
        return
    if not Path(parts.path[1:]).expanduser().exists():
        raise ConfigurationError(MISSING_FILE)


def _opened(url: str, prefix: str | None, opened: list[TinyJobs]) -> TinyJobs:
    app = TinyJobs(url, **({"prefix": prefix} if prefix else {}))
    opened.append(app)
    return app
