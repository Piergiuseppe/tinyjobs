from __future__ import annotations

import io
import contextlib
import os
import logging
import tempfile
import time
import unittest
from pathlib import Path

from tinyjobs import TinyJobs
from tinyjobs.cli import build_parser, main
from tinyjobs.cli.formatting import format_duration, render_table, shorten
from tinyjobs.cli.main import host_logging_message
from tinyjobs.duration import format_seconds
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
            ["--log-format", "json", "jobs", "--app", "fixture_app:app", "list"],
            ["jobs", "list", "--app", "fixture_app:app", "--log-format", "json"],
        ):
            with self.subTest(argv=argv):
                self.assertEqual(self.invoke(*argv)[0], 0)

    def test_a_standalone_run_can_ask_for_json_logs(self):
        from tinyjobs.logs import JSONFormatter, TextFormatter

        root = logging.getLogger()
        self.addCleanup(setattr, root, "handlers", root.handlers[:])
        self.addCleanup(root.setLevel, root.level)
        root.handlers[:] = []
        self.assertEqual(self.invoke("--log-format", "json", "stats", "--app", "fixture_app:app")[0], 0)
        self.assertIsInstance(root.handlers[0].formatter, JSONFormatter)
        self.assertEqual(self.invoke("stats", "--app", "fixture_app:app")[0], 0)
        self.assertIsInstance(root.handlers[0].formatter, TextFormatter)
        self.assertEqual(len(root.handlers), 1)

    def test_a_standalone_run_sets_the_level_every_time(self):
        root = logging.getLogger()
        self.addCleanup(setattr, root, "handlers", root.handlers[:])
        self.addCleanup(root.setLevel, root.level)
        root.handlers[:] = []
        self.invoke("--log-level", "WARNING", "stats", "--app", "fixture_app:app")
        self.assertEqual(root.level, logging.WARNING)
        self.invoke("--log-level", "DEBUG", "stats", "--app", "fixture_app:app")
        self.assertEqual(root.level, logging.DEBUG)

    def test_a_standalone_run_under_foreign_root_handlers_is_told_the_format_is_ignored(self):
        root = logging.getLogger()
        self.addCleanup(setattr, root, "handlers", root.handlers[:])
        self.addCleanup(root.setLevel, root.level)
        seen = []
        handler = logging.Handler()
        handler.emit = lambda record: seen.append(record.getMessage())
        root.handlers[:] = [handler]
        self.invoke("--log-format", "json", "stats", "--app", "fixture_app:app")
        self.invoke("stats", "--app", "fixture_app:app")
        self.assertEqual(
            [m for m in seen if "--log-format is ignored" in m],
            [host_logging_message("root", "json")],
        )
        self.assertEqual(root.handlers, [handler])

    def test_filtering_by_status_narrows_the_listing(self):
        self.invoke(
            "send", "--app", "fixture_app:app", "fixture.echo", "--args", '["hi"]'
        )
        _, output = self.invoke(
            "jobs", "--app", "fixture_app:app", "list", "--status", "failed"
        )
        self.assertIn("no matching jobs", output)


class EmbeddedAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")
        self.logger = logging.getLogger("tinyjobs")
        self.saved = (self.logger.handlers[:], self.logger.level, self.logger.propagate)
        self.root_handlers = logging.getLogger().handlers[:]

    def tearDown(self) -> None:
        self.logger.handlers[:], self.logger.level, self.logger.propagate = (
            self.saved[0], self.saved[1], self.saved[2]
        )
        logging.getLogger().handlers[:] = self.root_handlers
        self.app.close()
        self.directory.cleanup()

    def test_a_host_can_hand_over_its_app_instead_of_an_import_path(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = main(["send", "hosted.task"], app=self.app)
        self.assertEqual(code, 0)
        self.assertEqual(self.app.get(buffer.getvalue().strip()).task, "hosted.task")

    def test_a_hosted_run_can_ask_for_json_logs(self):
        from tinyjobs.logs import JSONFormatter, TextFormatter

        self.logger.handlers[:] = []
        with contextlib.redirect_stdout(io.StringIO()):
            main(["--log-format", "json", "stats"], app=self.app)
            self.assertIsInstance(self.logger.handlers[0].formatter, JSONFormatter)
            main(["stats"], app=self.app)
            self.assertIsInstance(self.logger.handlers[0].formatter, TextFormatter)

    def test_a_host_that_configured_the_logger_is_told_the_format_is_ignored(self):
        seen = []
        handler = logging.Handler()
        handler.emit = lambda record: seen.append(record.getMessage())
        self.logger.handlers[:] = [handler]
        with contextlib.redirect_stdout(io.StringIO()):
            main(["--log-level", "ERROR", "--log-format", "json", "stats"], app=self.app)
            main(["--log-format", "text", "stats"], app=self.app)
            main(["stats"], app=self.app)
        self.assertEqual(
            [m for m in seen if "--log-format is ignored" in m],
            [host_logging_message("tinyjobs", "json"), host_logging_message("tinyjobs", "text")],
        )

    def test_a_hosted_run_leaves_the_root_logger_to_the_host(self):
        self.logger.handlers[:] = []
        with contextlib.redirect_stdout(io.StringIO()):
            main(["stats"], app=self.app)
        self.assertEqual(logging.getLogger().handlers, self.root_handlers)
        self.assertEqual(len(self.logger.handlers), 1)
        self.assertFalse(self.logger.propagate)


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
    def test_the_worker_takes_process_options(self):
        args = build_parser().parse_args(
            ["worker", "--app", "x:app", "--processes", "2", "--jobs-per-process", "50",
             "--process-setup", "tests.isolated:prepare"]
        )
        self.assertEqual((args.processes, args.jobs_per_process), (2, 50))
        self.assertEqual(args.process_setup, "tests.isolated:prepare")
        args = build_parser().parse_args(["worker", "--app", "x:app"])
        self.assertEqual((args.processes, args.jobs_per_process), (None, None))

    def test_every_command_is_reachable(self):
        parser = build_parser()
        for command in (
            "worker", "scheduler", "jobs", "schedules", "stats", "check", "send", "purge"
        ):
            self.assertIn(command, parser.format_help())

    def test_no_purge_keeps_the_sweep_settings_of_the_app(self):
        from tinyjobs.cli.commands.worker import WorkerCommand
        from tinyjobs.retention import RetentionPolicy

        default = RetentionPolicy(interval=60, batch=7, maximum_per_sweep=70)
        args = build_parser().parse_args(["worker", "--app", "x:app", "--no-purge"])
        policy = WorkerCommand()._retention(args, default)
        self.assertFalse(policy.enabled)
        self.assertEqual((policy.interval, policy.batch, policy.maximum_per_sweep), (60, 7, 70))
        args = build_parser().parse_args(
            ["worker", "--app", "x:app", "--no-purge", "--purge-interval", "2m"]
        )
        self.assertEqual(WorkerCommand()._retention(args, default).interval, 120)


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

    def test_log_durations_keep_their_precision_without_exponents(self):
        self.assertEqual(format_seconds(2592000.0), "2592000s")
        self.assertEqual(format_seconds(1234567.891), "1234567.891s")
        self.assertEqual(format_seconds(30.0), "30s")
        self.assertEqual(format_seconds(0.25), "0.25s")
        self.assertEqual(format_seconds(0.0001), "0.0001s")
        self.assertEqual(format_seconds(0.00001), "0.00001s")
        self.assertEqual(format_seconds(1.5e-9), "0.0000000015s")
        self.assertEqual(format_seconds(0.0), "0s")

    def test_retention_durations_never_use_exponents(self):
        from tinyjobs.duration import format_duration as retention_duration

        self.assertEqual(retention_duration(1234567.5), "1234567.5s")
        self.assertEqual(retention_duration(0.00001), "0.00001s")
        self.assertEqual(retention_duration(7200), "2h")

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
        stderr = io.StringIO()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(stderr):
            build_parser().parse_args(["purge", "--app", "x:y", "--before", "x" * 400])
        self.assertIn("'xxxxxxxxxxxxxxxxxxxxxxxx...' is not a duration", stderr.getvalue())

    def test_a_duration_beyond_counting_is_rejected_by_its_flag(self):
        stderr = io.StringIO()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(stderr):
            build_parser().parse_args(
                ["purge", "--app", "x:y", "--before", "9" * 400]
            )
        self.assertIn("--before", stderr.getvalue())
        self.assertIn("'999999999999999999999999...' is too long a duration", stderr.getvalue())


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
        app.run_sync(lambda: app.scheduling.register(), name="register")
        code, output = self.invoke("schedules", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertNotIn("not registered yet", output)

    def test_schedules_pause_resume_and_run_act_on_the_store(self):
        code, output = self.invoke("schedules", "pause", "fixture.tick", "--app", "fixture_app:app")
        self.assertEqual((code, output.strip()), (0, "fixture.tick paused"))
        code, output = self.invoke("schedules", "pause", "fixture.tick", "--app", "fixture_app:app")
        self.assertEqual((code, output.strip()), (0, "fixture.tick is already paused"))
        code, output = self.invoke("schedules", "list", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertIn("paused", output)
        code, output = self.invoke("schedules", "run", "fixture.tick", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertEqual(len(output.strip()), 36)
        code, output = self.invoke("schedules", "resume", "fixture.tick", "--app", "fixture_app:app")
        self.assertEqual((code, output.strip()), (0, "fixture.tick resumed"))
        code, output = self.invoke("schedules", "resume", "fixture.tick", "--app", "fixture_app:app")
        self.assertEqual((code, output.strip()), (0, "fixture.tick is not paused"))
        code, output = self.invoke("schedules", "--app", "fixture_app:app")
        self.assertEqual(code, 0)
        self.assertNotIn("paused", output)

    def test_schedules_refuses_a_name_it_does_not_know(self):
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            code, _ = self.invoke("schedules", "pause", "fixture.nope", "--app", "fixture_app:app")
        self.assertEqual(code, 2)
        self.assertIn("no schedule named 'fixture.nope'", errors.getvalue())
        with contextlib.redirect_stderr(errors):
            code, _ = self.invoke("schedules", "run", "fixture.echo", "--app", "fixture_app:app")
        self.assertEqual(code, 2)
        self.assertIn("does not declare a cron task named 'fixture.echo'", errors.getvalue())

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

    def test_a_ctrl_c_after_the_worker_finished_keeps_the_clean_exit(self):
        import signal
        from unittest import mock

        import threading

        from tinyjobs.cli import run
        from tinyjobs.worker.worker import Worker

        running = Worker.run

        def briefly(worker: Worker) -> None:
            threading.Timer(0.5, worker.stop).start()
            running(worker)

        for number in (signal.SIGINT, signal.SIGTERM):
            self.addCleanup(signal.signal, number, signal.getsignal(number))
        with mock.patch.object(Worker, "run", briefly), mock.patch(
            "sys.argv", ["tinyjobs", "worker", "--app", "fixture_app:app", "--no-scheduler"]
        ):
            self.assertEqual(run(), 0)
        from tinyjobs.worker.signals import _exit_cleanly

        self.assertIs(signal.getsignal(signal.SIGINT), _exit_cleanly)
        self.assertIs(signal.getsignal(signal.SIGTERM), _exit_cleanly)

    def test_main_called_in_process_leaves_the_signal_handlers_it_found(self):
        import signal
        from unittest import mock

        from tinyjobs.cli.main import main as entry
        from tinyjobs.worker.worker import Worker

        def finished(worker: Worker) -> None:
            worker._finished.set()

        found = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        for number, handler in found.items():
            self.addCleanup(signal.signal, number, handler)
        with mock.patch.object(Worker, "run", finished), mock.patch(
            "sys.argv", ["tinyjobs", "worker", "--app", "fixture_app:app"]
        ):
            self.assertEqual(entry(), 0)
        for number, handler in found.items():
            self.assertIs(signal.getsignal(number), handler)

    def test_a_stop_signal_ends_a_worker_whose_exit_waits_on_a_job_thread(self):
        import signal
        import subprocess
        import sys

        (self.root / "lingering_app.py").write_text(
            "import threading, time\n"
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "lingering.db"}'!r})\n"
            "@app.task(name='lingering')\n"
            "def lingering():\n"
            "    threading.Thread(target=time.sleep, args=(60,), daemon=False).start()\n"
        )
        import os

        import tinyjobs

        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join(
                [str(Path(tinyjobs.__file__).parents[1]), str(self.root)]
            ),
        )
        enqueue = "from lingering_app import app; app.send('lingering')"
        subprocess.run([sys.executable, "-c", enqueue], cwd=self.root, env=env, check=True)
        worker = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "worker", "--app", "lingering_app:app",
             "--shutdown-timeout", "1", "--no-scheduler"],
            cwd=self.root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(2.0)
            worker.send_signal(signal.SIGTERM)
            time.sleep(2.5)
            self.assertIsNone(worker.poll())
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(10), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)

    def test_a_stop_signal_keeps_the_failure_status_of_a_worker_that_crashed(self):
        import os
        import signal
        import subprocess
        import sys

        import tinyjobs

        (self.root / "occupied").write_text("")
        (self.root / "crashing_app.py").write_text(
            "import threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "threading.Thread(target=time.sleep, args=(8,), daemon=False).start()\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "occupied" / "jobs.db"}'!r})\n"
        )
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join(
                [str(Path(tinyjobs.__file__).parents[1]), str(self.root)]
            ),
        )
        worker = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "worker", "--app", "crashing_app:app"],
            cwd=self.root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(2.0)
            worker.send_signal(signal.SIGTERM)
            self.assertNotEqual(worker.wait(15), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)

    def owned_worker(self, module: str, source: str):
        import os
        import subprocess
        import sys

        import tinyjobs

        (self.root / f"{module}.py").write_text(source)
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join(
                [str(Path(tinyjobs.__file__).parents[1]), str(self.root)]
            ),
        )
        worker = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "worker", "--app", f"{module}:app",
             "--shutdown-timeout", "1", "--no-scheduler"],
            cwd=self.root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        worker.args_env = env
        return worker

    def ended(self, worker) -> tuple[int, str]:
        try:
            _, errors = worker.communicate(timeout=20)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=10)
        return worker.returncode, errors

    def test_a_stop_signal_while_the_run_tears_down_keeps_the_clean_exit(self):
        worker = self.owned_worker(
            "teardown_app",
            "import logging, os, signal, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "teardown.db"}'!r})\n"
            "class Late(logging.Handler):\n"
            "    def emit(self, record):\n"
            "        if record.getMessage().endswith(' stopped'):\n"
            "            os.kill(os.getpid(), signal.SIGTERM)\n"
            "            time.sleep(0.3)\n"
            "logging.getLogger('tinyjobs.worker').addHandler(Late())\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n",
        )
        status, errors = self.ended(worker)
        self.assertEqual(status, 0, errors[-2000:])

    def test_a_stop_signal_ends_a_process_that_waits_on_a_child_process_at_exit(self):
        import signal
        import subprocess
        import sys

        worker = self.owned_worker(
            "child_app",
            "import multiprocessing, signal, time\n"
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "child.db"}'!r})\n"
            "def child():\n"
            "    signal.signal(signal.SIGINT, signal.SIG_IGN)\n"
            "    time.sleep(12)\n"
            "@app.task(name='spawning')\n"
            "def spawning():\n"
            "    multiprocessing.get_context('fork').Process(target=child).start()\n",
        )
        try:
            subprocess.run(
                [sys.executable, "-c", "from child_app import app; app.send('spawning')"],
                cwd=self.root,
                env=worker.args_env,
                check=True,
            )
            time.sleep(3.0)
            worker.send_signal(signal.SIGTERM)
            time.sleep(2.5)
            self.assertIsNone(worker.poll())
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(5), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)

    def stalled(self, script: str, pipe: str):
        import os
        import subprocess
        import sys

        import tinyjobs

        (self.root / "entry.py").write_text(
            "import tinyjobs.cli\n"
            "def ran(run=tinyjobs.cli.run):\n"
            "    code = run()\n"
            "    open('ran', 'w').close()\n"
            "    return code\n"
            "tinyjobs.cli.run = ran\n" + script
        )
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(self.root)]),
        )
        streams = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, pipe: subprocess.PIPE}
        return subprocess.Popen([sys.executable, "entry.py"], cwd=self.root, env=env, **streams)

    def wait_for_the_run(self, worker) -> None:
        deadline = time.monotonic() + 30
        while not (self.root / "ran").exists():
            self.assertLess(time.monotonic(), deadline)
            self.assertIsNone(worker.poll())
            time.sleep(0.05)
        time.sleep(3.0)
        self.assertIsNone(worker.poll())

    def assert_one_signal_ends(self, worker, pipe) -> None:
        import signal

        try:
            self.wait_for_the_run(worker)
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(10), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)
            pipe.close()

    def test_a_stop_signal_ends_a_final_flush_stuck_on_a_stalled_stderr(self):
        worker = self.stalled(
            "import atexit, os, sys\n"
            "def fill():\n"
            "    sys.stderr.write('x' * 4000)\n"
            "    os.set_blocking(2, False)\n"
            "    try:\n"
            "        while True:\n"
            "            os.write(2, b'x' * 512)\n"
            "    except BlockingIOError:\n"
            "        pass\n"
            "    os.set_blocking(2, True)\n"
            "atexit.register(fill)\n"
            "import signal, threading\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "stderr.db"}'!r})\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    sys.exit(run())\n",
            "stderr",
        )
        self.assert_one_signal_ends(worker, worker.stderr)

    def test_one_stop_signal_ends_an_exit_hook_that_holds_a_stalled_stdout(self):
        worker = self.stalled(
            "import atexit, os, signal, sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "held.db"}'!r})\n"
            "def hold():\n"
            "    os.set_blocking(1, False)\n"
            "    try:\n"
            "        while True:\n"
            "            os.write(1, b'x' * 512)\n"
            "    except BlockingIOError:\n"
            "        pass\n"
            "    os.set_blocking(1, True)\n"
            "    sys.stdout.write('pending line\\n')\n"
            "    time.sleep(60)\n"
            "atexit.register(hold)\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    sys.exit(run())\n",
            "stdout",
        )
        self.assert_one_signal_ends(worker, worker.stdout)

    def test_one_stop_signal_ends_a_process_whose_thread_writes_to_a_stalled_stdout(self):
        worker = self.stalled(
            "import os, signal, sys, threading\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "spam.db"}'!r})\n"
            "def spam():\n"
            "    while True:\n"
            "        print('log ' + 'x' * 200, flush=True)\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    code = run()\n"
            "    threading.Thread(target=spam).start()\n"
            "    sys.exit(code)\n",
            "stdout",
        )
        self.assert_one_signal_ends(worker, worker.stdout)

    def test_one_stop_signal_ends_a_stalled_flush_after_the_wakeup_descriptor_was_reset(self):
        worker = self.stalled(
            "import asyncio, os, signal, sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "reset.db"}'!r})\n"
            "async def after():\n"
            "    loop = asyncio.get_running_loop()\n"
            "    loop.add_signal_handler(signal.SIGUSR1, lambda: None)\n"
            "    await asyncio.sleep(0)\n"
            "    loop.remove_signal_handler(signal.SIGUSR1)\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    code = run()\n"
            "    asyncio.run(after())\n"
            "    sys.stdout.write('y' * 4000)\n"
            "    os.set_blocking(1, False)\n"
            "    try:\n"
            "        while True:\n"
            "            os.write(1, b'x' * 512)\n"
            "    except BlockingIOError:\n"
            "        pass\n"
            "    os.set_blocking(1, True)\n"
            "    time.sleep(60)\n"
            "    sys.exit(code)\n",
            "stdout",
        )
        self.assert_one_signal_ends(worker, worker.stdout)

    def test_one_stop_signal_ends_a_forked_child_stalled_on_its_stdout(self):
        worker = self.stalled(
            "import os, signal, sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "child.db"}'!r})\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    code = run()\n"
            "    if os.fork():\n"
            "        os._exit(code)\n"
            "    sys.stdout.write('y' * 4000)\n"
            "    os.set_blocking(1, False)\n"
            "    try:\n"
            "        while True:\n"
            "            os.write(1, b'x' * 512)\n"
            "    except BlockingIOError:\n"
            "        pass\n"
            "    os.set_blocking(1, True)\n"
            f"    open({str(self.root / 'child.pid')!r}, 'w').write(str(os.getpid()))\n"
            "    time.sleep(60)\n"
            "    os._exit(3)\n",
            "stdout",
        )
        import signal

        try:
            self.assertEqual(worker.wait(10), 0)
            marker = self.root / "child.pid"
            deadline = time.monotonic() + 10
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            child = int(marker.read_text())
            os.kill(child, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    os.kill(child, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.05)
            else:
                os.kill(child, signal.SIGKILL)
                self.fail("the forked child ignored its stop signal")
        finally:
            worker.stdout.close()

    def test_a_second_stop_signal_inside_the_clean_exit_leaves_the_shared_stream_blocking(self):
        import os
        import subprocess
        import sys

        import tinyjobs

        reading, writing = os.pipe()
        self.addCleanup(os.close, reading)
        try:
            done = subprocess.run(
                [sys.executable, "-c",
                 "import os, signal, sys, threading, time\n"
                 "from tinyjobs.exiting import exit_cleanly\n"
                 "class Slow:\n"
                 "    def __init__(self, stream):\n"
                 "        self.stream = stream\n"
                 "    def fileno(self):\n"
                 "        return self.stream.fileno()\n"
                 "    def flush(self):\n"
                 "        time.sleep(0.2)\n"
                 "        self.stream.flush()\n"
                 "sys.stdout = Slow(sys.stdout)\n"
                 "signal.signal(signal.SIGINT, exit_cleanly)\n"
                 "signal.signal(signal.SIGTERM, exit_cleanly)\n"
                 "threading.Timer(0.1, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
                 "os.kill(os.getpid(), signal.SIGINT)\n"
                 "time.sleep(5)\n"],
                env=dict(os.environ, PYTHONPATH=str(Path(tinyjobs.__file__).parents[1])),
                stdout=writing,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
            )
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertTrue(os.get_blocking(writing))
        finally:
            os.close(writing)

    def test_a_clean_exit_leaves_a_shared_stdout_pipe_blocking(self):
        import os
        import subprocess
        import sys

        import tinyjobs

        (self.root / "entry.py").write_text(
            "import os, signal, sys, threading\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "shared.db"}'!r})\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            "    sys.exit(run())\n"
        )
        reading, writing = os.pipe()
        try:
            done = subprocess.run(
                [sys.executable, "entry.py"],
                cwd=self.root,
                env=dict(
                    os.environ,
                    PYTHONPATH=os.pathsep.join(
                        [str(Path(tinyjobs.__file__).parents[1]), str(self.root)]
                    ),
                ),
                stdout=writing,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            self.assertEqual(done.returncode, 0)
            self.assertTrue(os.get_blocking(writing))
        finally:
            os.close(reading)
            os.close(writing)

    def run_entry(self, body: str, stdout=None) -> int:
        import os
        import subprocess
        import sys

        import tinyjobs

        (self.root / "entry.py").write_text(
            "import os, signal, sys, threading, time\n"
            "from tinyjobs import TinyJobs\n"
            "from tinyjobs.cli import run\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "entry.db"}'!r})\n"
            "sys.argv = ['tinyjobs', 'worker', '--app', 'entry:app', '--shutdown-timeout', '1', '--no-scheduler']\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
            "if __name__ == '__main__':\n"
            "    sys.modules['entry'] = sys.modules['__main__']\n"
            + body
        )
        done = subprocess.run(
            [sys.executable, "entry.py"],
            cwd=self.root,
            env=dict(
                os.environ,
                PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(self.root)]),
            ),
            stdout=stdout or subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
        self.assertEqual(done.returncode, 0, done.stderr[-2000:])
        return done.returncode

    def test_a_stop_signal_during_a_slow_call_after_the_run_keeps_the_buffered_output(self):
        with open(self.root / "summary.txt", "w") as summary:
            self.run_entry(
                "    code = run()\n"
                "    print('summary line')\n"
                "    threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
                "    import sqlite3\n"
                "    sqlite3.connect(':memory:').execute(\n"
                "        'WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 30000000)'\n"
                "        ' SELECT count(*) FROM n').fetchall()\n"
                "    sys.exit(code)\n",
                stdout=summary,
            )
        self.assertEqual((self.root / "summary.txt").read_text(), "summary line\n")

    def test_a_stop_signal_while_the_main_thread_holds_the_thread_registry_keeps_the_buffered_output(self):
        with open(self.root / "limbo.txt", "w") as summary:
            self.run_entry(
                "    code = run()\n"
                "    sys.stdout.write('pending line\\n')\n"
                "    with threading._active_limbo_lock:\n"
                "        os.kill(os.getpid(), signal.SIGTERM)\n"
                "        time.sleep(0.01)\n"
                "    time.sleep(20)\n"
                "    sys.exit(code)\n",
                stdout=summary,
            )
        self.assertEqual((self.root / "limbo.txt").read_text(), "pending line\n")

    def test_a_stop_signal_ends_a_final_flush_stuck_on_a_stalled_pipe(self):
        import os
        import signal
        import subprocess
        import sys

        import tinyjobs

        (self.root / "stalled_app.py").write_text(
            "import atexit, os, sys, threading, signal\n"
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{self.root / "stalled.db"}'!r})\n"
            "def fill():\n"
            "    open('ran', 'w').close()\n"
            "    os.set_blocking(1, False)\n"
            "    try:\n"
            "        while True:\n"
            "            os.write(1, b'x' * 512)\n"
            "    except BlockingIOError:\n"
            "        pass\n"
            "    os.set_blocking(1, True)\n"
            "    sys.stdout.write('pending line\\n')\n"
            "atexit.register(fill)\n"
            "threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()\n"
        )
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(self.root)]),
        )
        worker = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "worker", "--app", "stalled_app:app",
             "--shutdown-timeout", "1", "--no-scheduler"],
            cwd=self.root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.wait_for_the_run(worker)
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(10), 0)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)
            worker.stdout.close()

    def test_a_ctrl_c_before_the_worker_finished_still_exits_130(self):
        from unittest import mock

        from tinyjobs.worker.worker import Worker

        def interrupted(worker: Worker) -> None:
            raise KeyboardInterrupt

        with mock.patch.object(Worker, "run", interrupted):
            code, _ = self.invoke("worker", "--app", "fixture_app:app")
        self.assertEqual(code, 130)

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
