from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.backends import (
    Backend,
    SQLiteBackend,
    available_schemes,
    load_backend,
    register_builtin,
)
from tinyjobs.backends.loader import ENTRY_POINT_GROUP
from tinyjobs.exceptions import UnknownBackendError, UnsupportedCapabilityError


class BackendResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "jobs.db"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_sqlite_scheme_is_built_in(self):
        self.assertIn("sqlite", available_schemes())

    def test_url_resolves_to_the_sqlite_backend(self):
        backend = load_backend(f"sqlite:///{self.path}")
        self.assertIsInstance(backend, SQLiteBackend)
        asyncio.run(backend.close())

    def test_three_slashes_mean_a_relative_path(self):
        backend = SQLiteBackend.from_url("sqlite:///relative.db")
        self.assertEqual(backend.database, "relative.db")
        asyncio.run(backend.close())
        Path("relative.db").unlink(missing_ok=True)

    def test_four_slashes_mean_an_absolute_path(self):
        backend = SQLiteBackend.from_url(f"sqlite:///{self.path}")
        self.assertEqual(backend.database, str(self.path))
        self.assertTrue(backend.database.startswith("/"))
        asyncio.run(backend.close())

    def test_memory_urls_are_supported(self):
        for url in ("sqlite://", "sqlite:///:memory:"):
            app = TinyJobs(url)
            app.send("noop")
            self.assertEqual(len(app.jobs(limit=10).items), 1)
            app.close()

    def test_two_memory_backends_are_isolated(self):
        first, second = TinyJobs("sqlite://"), TinyJobs("sqlite://")
        first.send("noop")
        self.assertEqual(len(first.jobs(limit=10).items), 1)
        self.assertEqual(len(second.jobs(limit=10).items), 0)
        first.close()
        second.close()

    def test_unknown_scheme_names_the_missing_package(self):
        with self.assertRaises(UnknownBackendError) as caught:
            load_backend("mysql://localhost/jobs")
        message = str(caught.exception)
        self.assertIn("mysql", message)
        self.assertIn("pip install tinyjobs-mysql", message)
        self.assertIn("sqlite", message)

    def test_a_url_without_a_scheme_is_rejected(self):
        with self.assertRaises(UnknownBackendError):
            load_backend("jobs.db")

    def test_the_entry_point_group_is_stable(self):
        self.assertEqual(ENTRY_POINT_GROUP, "tinyjobs.backends")


class PluginSeamTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_a_third_party_scheme_works_end_to_end(self):
        created: list[str] = []

        def factory(url: str, **options):
            created.append(url)
            path = Path(self.directory.name) / "plugin.db"
            return SQLiteBackend(str(path), **options)

        register_builtin("acme", factory)
        self.assertIn("acme", available_schemes())

        app = TinyJobs("acme://wherever/it/wants")

        @app.task
        def handler(value):
            return value

        job = handler.enqueue(1)
        self.assertIsNotNone(app.get(job.id))
        self.assertEqual(created, ["acme://wherever/it/wants"])
        app.close()

    def test_the_sqlite_backend_satisfies_the_protocol(self):
        backend = SQLiteBackend(str(Path(self.directory.name) / "p.db"))
        self.assertIsInstance(backend, Backend)
        asyncio.run(backend.close())


class CapabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        path = Path(self.directory.name) / "jobs.db"

        def limited(url: str, **options):
            backend = SQLiteBackend(str(path), **options)
            backend.capabilities = replace(
                SQLiteBackend.capabilities, dedup=False, priority=False, delayed=False
            )
            return backend

        register_builtin("limited", limited)
        self.app = TinyJobs("limited://")

        @self.app.task
        def handler():
            return None

        self.handler = handler

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def test_requesting_dedup_on_a_backend_without_it_fails_at_enqueue(self):
        with self.assertRaises(UnsupportedCapabilityError):
            self.handler.with_options(key="k").enqueue()

    def test_requesting_priority_on_a_backend_without_it_fails_at_enqueue(self):
        with self.assertRaises(UnsupportedCapabilityError):
            self.handler.with_options(priority=5).enqueue()

    def test_requesting_a_delay_on_a_backend_without_it_fails_at_enqueue(self):
        with self.assertRaises(UnsupportedCapabilityError):
            self.handler.with_options(delay=60).enqueue()

    def test_a_plain_enqueue_still_works(self):
        self.assertIsNotNone(self.handler.enqueue())


if __name__ == "__main__":
    unittest.main()
