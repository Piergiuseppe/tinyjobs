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
'''


class CommandLineTests(unittest.TestCase):
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
        for command in ("worker", "jobs", "send"):
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
