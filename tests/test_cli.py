from __future__ import annotations

import io
import contextlib
import tempfile
import unittest
from pathlib import Path

from tinyjobs.cli import build_parser, main
from tinyjobs.cli.formatting import format_duration, render_table, shorten
from tinyjobs.cli.loader import load_app
from tinyjobs.exceptions import ConfigurationError

FIXTURE = '''
from tinyjobs import TinyJobs

app = TinyJobs("sqlite:///{database}")


@app.task(name="fixture.echo")
def echo(value):
    return value


@app.task(name="fixture.tick", cron="*/5 * * * *", timezone="Europe/Rome")
def tick():
    return None
'''

PLAIN_FIXTURE = '''
from tinyjobs import TinyJobs

app = TinyJobs("sqlite:///{database}")
'''


class FixtureApp:
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.database = self.root / "jobs.db"
        (self.root / "fixture_app.py").write_text(
            FIXTURE.format(database=self.database)
        )
        self.previous = Path.cwd()
        import os

        os.chdir(self.root)

    def tearDown(self) -> None:
        import os
        import sys

        os.chdir(self.previous)
        sys.modules.pop("fixture_app", None)
        if str(self.root) in sys.path:
            sys.path.remove(str(self.root))
        self.directory.cleanup()

    def invoke(self, *argv: str) -> tuple[int, str]:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = main(list(argv))
        return code, buffer.getvalue()


class CommandLineTests(FixtureApp, unittest.TestCase):
    def test_send_enqueues_and_prints_the_job_id(self):
        code, output = self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(output.strip()), 36)

    def test_jobs_list_shows_the_enqueued_job(self):
        self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        code, output = self.invoke("jobs", "--app", "fixture_app:app", "list")
        self.assertEqual(code, 0)
        self.assertIn("fixture.echo", output)
        self.assertIn("queued", output)

    def test_jobs_show_renders_the_decoded_arguments(self):
        _, job_id = self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        code, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "show", job_id.strip()
        )
        self.assertEqual(code, 0)
        self.assertIn("fixture.echo", output)
        self.assertIn("'hi'", output)

    def test_jobs_show_reports_a_missing_id(self):
        code, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "show", "does-not-exist"
        )
        self.assertEqual(code, 1)
        self.assertIn("no job", output)

    def test_cancel_then_retry_round_trip(self):
        _, job_id = self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        job_id = job_id.strip()
        code, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "cancel", job_id
        )
        self.assertEqual(code, 0)
        self.assertIn("cancelled", output)
        code, output = self.invoke("jobs", "--app", "fixture_app:app", "retry", job_id)
        self.assertEqual(code, 0)
        self.assertIn("requeued", output)

    def test_send_accepts_queue_and_priority(self):
        _, job_id = self.invoke(
            "send",
            "--app",
            "fixture_app:app",
            "fixture.echo",
            "--args",
            '["hi"]',
            "--queue",
            "urgent",
            "--priority",
            "7",
        )
        _, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "show", job_id.strip()
        )
        self.assertIn("urgent", output)
        self.assertIn("priority 7", output)

    def test_a_missing_app_is_reported_clearly(self):
        import io, contextlib
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            code = main(["jobs", "list"])
        self.assertEqual(code, 2)
        self.assertIn("--app is required", buffer.getvalue())

    def test_app_is_accepted_after_a_nested_action(self):
        self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        code, output = self.invoke("jobs", "list", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertIn("fixture.echo", output)

    def test_log_level_is_accepted_at_every_level(self):
        for argv in (
            ["--log-level", "WARNING", "jobs", "--app", "fixture_app:app", "list"],
            ["jobs", "--app", "fixture_app:app", "--log-level", "WARNING", "list"],
            ["jobs", "list", "--app", "fixture_app:app", "--log-level", "WARNING"],
        ):
            with self.subTest(argv=argv):
                self.assertEqual(self.invoke(*argv)[0], 0)

    def test_filtering_by_status_narrows_the_listing(self):
        self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        _, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "list", "--status", "failed"
        )
        self.assertIn("no matching jobs", output)


class AppLoadingTests(unittest.TestCase):
    def test_a_bad_module_path_explains_itself(self):
        with self.assertRaises(ConfigurationError) as caught:
            load_app("definitely.not.a.module:app")
        self.assertIn("cannot import", str(caught.exception))

    def test_pointing_at_a_non_app_is_rejected(self):
        with self.assertRaises(ConfigurationError) as caught:
            load_app("unittest:TestCase")
        self.assertIn("TinyJobs instance", str(caught.exception))


class ParserTests(unittest.TestCase):
    def test_every_command_is_reachable(self):
        parser = build_parser()
        for command in (
            "worker", "scheduler", "jobs", "schedules", "stats", "check", "send", "purge"
        ):
            self.assertIn(command, parser.format_help())


class FormattingTests(unittest.TestCase):
    def test_an_empty_table_says_so(self):
        self.assertEqual(render_table(("A",), []), "no matching jobs")

    def test_columns_align_to_the_widest_cell(self):
        rendered = render_table(("ID", "NAME"), [("1", "long-value")])
        self.assertIn("long-value", rendered)
        self.assertEqual(len(rendered.splitlines()), 3)

    def test_durations_switch_units(self):
        self.assertEqual(format_duration(0.25), "250ms")
        self.assertEqual(format_duration(2.5), "2.50s")
        self.assertEqual(format_duration(None), "-")

    def test_long_values_are_truncated(self):
        self.assertEqual(shorten("abcdefghij", 5), "abcd…")
        self.assertEqual(shorten(None, 5), "-")


if __name__ == "__main__":
    unittest.main()


class ModuleEntryPointTests(unittest.TestCase):
    def test_the_package_is_runnable_with_dash_m(self):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "tinyjobs", "--version"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("tinyjobs", result.stdout)

    def test_the_cli_submodule_is_runnable_too(self):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "tinyjobs.cli", "--version"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class VersionTests(unittest.TestCase):
    def test_the_version_comes_from_the_package_metadata(self):
        import tinyjobs

        self.assertIsInstance(tinyjobs.__version__, str)
        self.assertTrue(tinyjobs.__version__)

    def test_it_is_not_duplicated_in_the_source(self):
        from pathlib import Path

        source = (Path(tinyjobs_dir()) / "__init__.py").read_text()
        self.assertNotIn('__version__ = "0.', source)

    def test_unknown_module_attributes_still_raise(self):
        import tinyjobs

        with self.assertRaises(AttributeError):
            tinyjobs.__does_not_exist__


def tinyjobs_dir() -> str:
    import tinyjobs
    from pathlib import Path

    return str(Path(tinyjobs.__file__).parent)


class PurgeCommandTests(FixtureApp, unittest.TestCase):
    def test_purge_works_as_the_very_first_command(self):
        code, output = self.invoke(
            "purge", "--app", "fixture_app:app", "--before", "1d"
        )
        self.assertEqual(code, 0, output)
        self.assertIn("removed 0", output)

    def test_dry_run_as_the_very_first_command(self):
        code, output = self.invoke(
            "purge", "--app", "fixture_app:app", "--before", "1d", "--dry-run"
        )
        self.assertEqual(code, 0, output)
        self.assertIn("dry run", output)

    def test_dry_run_removes_nothing(self):
        self.invoke("send", "--app", "fixture_app:app", "fixture.echo",
                    "--args", '["hi"]')
        code, output = self.invoke(
            "purge", "--app", "fixture_app:app", "--before", "0s", "--dry-run"
        )
        self.assertEqual(code, 0)
        self.assertIn("dry run", output)
        self.assertIn("fixture.echo", self.invoke(
            "jobs", "list", "--app", "fixture_app:app")[1])

    def test_it_refuses_to_prune_queued_jobs(self):
        self.invoke("send", "--app", "fixture_app:app", "fixture.echo",
                    "--args", '["hi"]')
        code, output = self.invoke(
            "purge", "--app", "fixture_app:app", "--before", "0s"
        )
        self.assertEqual(code, 0)
        self.assertIn("removed 0", output)

    def test_a_disabled_policy_says_so(self):
        code, output = self.invoke(
            "purge", "--app", "fixture_app:app",
            "--retain-success", "0", "--retain-cancelled", "0",
            "--retain-failed", "0",
        )
        self.assertEqual(code, 0)
        self.assertIn("disabled", output)

    def test_a_bad_duration_is_rejected(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["purge", "--app", "x:y", "--before", "domani"]
            )


class ScheduleCommandTests(FixtureApp, unittest.TestCase):
    def test_schedules_lists_a_declared_cron_before_any_worker_ran(self):
        code, output = self.invoke("schedules", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertIn("fixture.tick", output)
        self.assertIn("*/5 * * * *", output)
        self.assertIn("Europe/Rome", output)
        self.assertIn("not registered yet", output)

    def test_schedules_shows_the_next_run_once_registered(self):
        app = load_app("fixture_app:app").start()
        app.run_sync(app.scheduling.register(), name="register")
        code, output = self.invoke("schedules", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertNotIn("not registered yet", output)

    def test_the_scheduler_refuses_an_app_without_cron_tasks(self):
        import sys

        (self.root / "plain_app.py").write_text(
            PLAIN_FIXTURE.format(database=self.database)
        )
        try:
            code, output = self.invoke("scheduler", "--app", "plain_app:app")
        finally:
            sys.modules.pop("plain_app", None)
        self.assertEqual(code, 1)
        self.assertIn("no cron tasks", output)

    def test_the_worker_accepts_the_scheduler_flags(self):
        args = build_parser().parse_args(
            ["worker", "--app", "x:y", "--no-scheduler", "--misfire-grace", "5m"]
        )
        self.assertTrue(args.no_scheduler)
        self.assertEqual(args.misfire_grace, 300.0)


class HealthCommandTests(FixtureApp, unittest.TestCase):
    def test_stats_says_so_when_there_are_no_jobs(self):
        code, output = self.invoke("stats", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertIn("no jobs yet", output)

    def test_stats_counts_jobs_per_queue(self):
        self.invoke("send", "--app", "fixture_app:app", "fixture.echo", "--queue", "mail")
        code, output = self.invoke("stats", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertIn("OLDEST DUE", output)
        self.assertIn("mail", output)
        self.assertIn("running past their lease: 0", output)

    def test_check_passes_on_a_healthy_app(self):
        code, output = self.invoke("check", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertNotIn("FAIL", output)

    def test_check_exits_one_on_a_problem(self):
        self.invoke("send", "--app", "fixture_app:app", "nobody.home")
        code, output = self.invoke("check", "--app", "fixture_app:app")
        self.assertEqual(code, 1)
        self.assertIn("FAIL  1 queued job(s) for nobody.home", output)


if __name__ == "__main__":
    unittest.main()
