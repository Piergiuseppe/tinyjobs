from __future__ import annotations

import argparse
from abc import ABC, abstractmethod

from ...app import TinyJobs
from ...exceptions import ConfigurationError
from ..loader import import_modules, load_app

MISSING_APP = (
    "--app is required; point it at your TinyJobs instance, "
    "for example --app myproject.tasks:app"
)


class Command(ABC):
    name: str
    help: str

    @abstractmethod
    def configure(
        self, parser: argparse.ArgumentParser, shared: argparse.ArgumentParser
    ) -> None: ...

    @abstractmethod
    def run(self, args: argparse.Namespace) -> int: ...

    def resolve_app(self, args: argparse.Namespace) -> TinyJobs:
        app = getattr(args, "instance", None) or self._load(args)
        patterns = getattr(args, "imports", None)
        if patterns:
            import_modules(patterns)
        return app

    def _load(self, args: argparse.Namespace) -> TinyJobs:
        spec = getattr(args, "app", None)
        if not spec:
            raise ConfigurationError(MISSING_APP)
        return load_app(spec)


def build_app_options() -> argparse.ArgumentParser:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--app",
        metavar="MODULE:ATTR",
        default=argparse.SUPPRESS,
        help="import path of the TinyJobs instance, e.g. myproject.tasks:app",
    )
    shared.add_argument(
        "--import",
        dest="imports",
        action="append",
        default=argparse.SUPPRESS,
        metavar="PATTERN",
        help="extra module to import so its tasks register, e.g. 'myproject.jobs.*'",
    )
    return shared
