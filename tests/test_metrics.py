from __future__ import annotations

import asyncio
import contextlib
import errno
import io
import logging
import math
import os
import queue
import re
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error
import urllib.request
from pathlib import Path

import tinyjobs
from tinyjobs import TinyJobs
from tinyjobs.cli.main import build_parser, main
from tinyjobs.exporter import MetricsServer
from tinyjobs.metrics import Metrics
from tinyjobs.models import Job, QueueStats, Stats
from tinyjobs.status import JobStatus


def job(task: str = "mail.send", queue: str = "default", id: str = "j1") -> Job:
    return Job(
        id=id, task=task, payload=b"{}", codec="json", queue=queue,
        status=JobStatus.RUNNING, attempts=1,
    )


class RenderTests(unittest.TestCase):
    def test_finished_attempts_are_counted_by_task_and_outcome(self):
        metrics = Metrics()
        metrics.succeeded(job(), None, 0.1)
        metrics.succeeded(job(), None, 0.2)
        metrics.failed(job(), ValueError(), True)
        metrics.failed(job("report"), ValueError(), False)
        text = metrics.render()
        self.assertIn('tinyjobs_jobs_total{task="mail.send",outcome="success"} 2\n', text)
        self.assertIn('tinyjobs_jobs_total{task="mail.send",outcome="retry"} 1\n', text)
        self.assertIn('tinyjobs_jobs_total{task="report",outcome="failure"} 1\n', text)
        self.assertIn("# TYPE tinyjobs_jobs_total counter\n", text)
        self.assertTrue(text.endswith("\n"))

    def test_durations_fill_a_cumulative_histogram(self):
        metrics = Metrics()
        metrics.succeeded(job(), None, 0.003)
        metrics.succeeded(job(), None, 2.0)
        text = metrics.render()
        self.assertIn('tinyjobs_job_duration_seconds_bucket{task="mail.send",le="0.005"} 1\n', text)
        self.assertIn('tinyjobs_job_duration_seconds_bucket{task="mail.send",le="1"} 1\n', text)
        self.assertIn('tinyjobs_job_duration_seconds_bucket{task="mail.send",le="2.5"} 2\n', text)
        self.assertIn('tinyjobs_job_duration_seconds_bucket{task="mail.send",le="+Inf"} 2\n', text)
        self.assertIn('tinyjobs_job_duration_seconds_sum{task="mail.send"} 2.003\n', text)
        self.assertIn('tinyjobs_job_duration_seconds_count{task="mail.send"} 2\n', text)

    def test_running_jobs_rise_on_start_and_fall_when_the_attempt_ends(self):
        metrics = Metrics()
        metrics.started(job(id="j1"))
        metrics.started(job(id="j2"))
        metrics.ended(job(id="j1"))
        self.assertIn('tinyjobs_jobs_running{task="mail.send"} 1\n', metrics.render())
        metrics.ended(job(id="j2"))
        self.assertIn('tinyjobs_jobs_running{task="mail.send"} 0\n', metrics.render())

    def test_an_end_without_a_start_or_a_second_end_leaves_the_gauge_alone(self):
        metrics = Metrics()
        metrics.ended(job())
        self.assertNotIn("tinyjobs_jobs_running{", metrics.render())
        metrics.started(job())
        metrics.ended(job())
        metrics.ended(job())
        self.assertIn('tinyjobs_jobs_running{task="mail.send"} 0\n', metrics.render())

    def test_store_stats_become_gauges(self):
        stats = Stats(
            queues=[
                QueueStats("default", {"queued": 3, "running": 1}, oldest_due_at=990.0),
                QueueStats("mail", {"failed": 2}),
            ],
            expired_leases=4,
            queued_tasks={"mail.send": 3},
        )
        text = Metrics().render(stats, now=1000.0)
        self.assertIn('tinyjobs_store_jobs{queue="default",status="queued"} 3\n', text)
        self.assertIn('tinyjobs_store_jobs{queue="default",status="running"} 1\n', text)
        self.assertIn('tinyjobs_store_jobs{queue="mail",status="failed"} 2\n', text)
        self.assertIn('tinyjobs_store_queued_jobs{task="mail.send"} 3\n', text)
        self.assertIn('tinyjobs_store_oldest_due_seconds{queue="default"} 10\n', text)
        self.assertNotIn('tinyjobs_store_oldest_due_seconds{queue="mail"}', text)
        self.assertIn("tinyjobs_store_expired_leases 4\n", text)
        self.assertIn(f'tinyjobs_info{{version="{tinyjobs.__version__}"}} 1\n', text)

    def test_without_stats_only_the_process_series_are_written(self):
        text = Metrics().render()
        self.assertNotIn("tinyjobs_store_", text)
        self.assertIn("tinyjobs_info{", text)

    def test_special_floats_use_the_prometheus_spelling(self):
        stats = Stats(queues=[QueueStats("q", {}, oldest_due_at=-math.inf)])
        self.assertIn('tinyjobs_store_oldest_due_seconds{queue="q"} +Inf\n', Metrics().render(stats, now=0.0))

    def test_label_values_are_escaped(self):
        metrics = Metrics()
        metrics.started(job('odd"name\\with\nbreak'))
        self.assertIn(
            'tinyjobs_jobs_running{task="odd\\"name\\\\with\\nbreak"} 1\n', metrics.render()
        )


class WorkerMetricsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")

    async def asyncTearDown(self) -> None:
        await self.app.aclose()
        self.directory.cleanup()

    def test_the_collector_is_one_per_app_and_shared_by_its_lanes(self):
        metrics = self.app.metrics
        self.assertIs(self.app.metrics, metrics)
        self.assertIs(self.app.lane().metrics, metrics)

    async def test_an_outcome_the_store_refuses_counts_as_nothing_and_ends_the_run(self):
        @self.app.task
        def handler():
            return None

        await handler.aenqueue()
        await self.app.ready()
        backend = self.app.backend
        refused = asyncio.Event()

        async def broken(*args, **kwargs):
            refused.set()
            raise ValueError("not a connection problem")

        self.addCleanup(setattr, backend, "complete", backend.complete)
        backend.complete = broken
        worker = self.app.worker(concurrency=2, shutdown_timeout=5.0)
        with self.assertLogs("tinyjobs.worker", "ERROR"):
            runner = asyncio.create_task(worker.start())
            await asyncio.wait_for(refused.wait(), timeout=10)
            worker.stop()
            await asyncio.wait_for(runner, timeout=10)
        text = self.app.metrics.render()
        self.assertIn(f'tinyjobs_jobs_running{{task="{handler.name}"}} 0\n', text)
        self.assertNotIn("tinyjobs_jobs_total{", text)

    async def test_a_cancelled_attempt_ends_the_run(self):
        @self.app.task
        async def handler():
            await asyncio.sleep(30)

        await handler.aenqueue()
        worker = self.app.worker(concurrency=2, shutdown_timeout=0.5)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            if f'tinyjobs_jobs_running{{task="{handler.name}"}} 1\n' in self.app.metrics.render():
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        self.assertIn(
            f'tinyjobs_jobs_running{{task="{handler.name}"}} 0\n', self.app.metrics.render()
        )

    async def test_jobs_run_by_the_worker_show_up(self):
        @self.app.task(max_attempts=2, retry_base=0.01)
        def handler(value):
            if value == "fail":
                raise ValueError("boom")
            return value

        metrics = self.app.metrics
        done = await handler.aenqueue("ok")
        failing = await handler.aenqueue("fail")
        worker = self.app.worker(concurrency=2, shutdown_timeout=5.0)
        runner = asyncio.create_task(worker.start())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            statuses = [(await self.app.aget(j.id)).status for j in (done, failing)]
            if all(status.is_terminal for status in statuses):
                break
        worker.stop()
        await asyncio.wait_for(runner, timeout=10)
        text = metrics.render(await self.app.astats())
        self.assertIn(f'tinyjobs_jobs_total{{task="{handler.name}",outcome="success"}} 1\n', text)
        self.assertIn(f'tinyjobs_jobs_total{{task="{handler.name}",outcome="retry"}} 1\n', text)
        self.assertIn(f'tinyjobs_jobs_total{{task="{handler.name}",outcome="failure"}} 1\n', text)
        self.assertIn(f'tinyjobs_job_duration_seconds_count{{task="{handler.name}"}} 1\n', text)
        self.assertIn(f'tinyjobs_jobs_running{{task="{handler.name}"}} 0\n', text)
        self.assertIn('tinyjobs_store_jobs{queue="default",status="success"} 1\n', text)
        self.assertIn('tinyjobs_store_jobs{queue="default",status="failed"} 1\n', text)


class ServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def test_a_scrape_asks_the_store_for_its_stats(self):
        @self.app.task(name="mail.send")
        def send():
            return None

        send.enqueue()
        with MetricsServer(self.app.metrics, self.app.stats) as server:
            with urllib.request.urlopen(server.url, timeout=5) as response:
                body = response.read().decode()
                content_type = response.headers["Content-Type"]
        self.assertEqual(content_type, "text/plain; version=0.0.4; charset=utf-8")
        self.assertIn('tinyjobs_store_jobs{queue="default",status="queued"} 1\n', body)
        self.assertIn('tinyjobs_store_queued_jobs{task="mail.send"} 1\n', body)
        self.assertIn("tinyjobs_store_expired_leases 0\n", body)

    def test_an_ipv6_host_is_served_and_named_in_brackets(self):
        try:
            server = MetricsServer(self.app.metrics, None, "::1", 0)
        except OSError as error:
            self.skipTest(f"no IPv6 loopback: {error}")
        with server:
            self.assertEqual(server.url, f"http://[::1]:{server.port}/metrics")
            with urllib.request.urlopen(server.url, timeout=5) as response:
                self.assertEqual(response.status, 200)

    def test_a_host_name_binds_ipv4_when_it_has_an_ipv4_address(self):
        families = {family for family, *_ in socket.getaddrinfo("localhost", 0)}
        if socket.AF_INET not in families:
            self.skipTest("localhost has no IPv4 address here")
        with MetricsServer(self.app.metrics, None, "localhost", 0) as server:
            self.assertEqual(server.host, "127.0.0.1")
            with urllib.request.urlopen(server.url, timeout=5) as response:
                self.assertEqual(response.status, 200)

    def test_a_host_name_prefers_ipv4_whatever_order_the_resolver_gives(self):
        infos = socket.getaddrinfo("localhost", 0, type=socket.SOCK_STREAM)
        if {info[0] for info in infos} != {socket.AF_INET, socket.AF_INET6}:
            self.skipTest("localhost does not have both families here")
        ordered = sorted(infos, key=lambda info: info[0] != socket.AF_INET6)
        with mock.patch("tinyjobs.exporter.socket.getaddrinfo", return_value=ordered):
            with MetricsServer(self.app.metrics, None, "localhost", 0) as server:
                self.assertEqual(server.host, "127.0.0.1")

    def test_a_taken_port_is_refused_even_when_another_family_is_free(self):
        with MetricsServer(self.app.metrics, None, "localhost", 0) as first:
            with self.assertRaises(OSError):
                MetricsServer(self.app.metrics, None, "localhost", first.port)

    def test_other_paths_are_not_found(self):
        with MetricsServer(self.app.metrics, None) as server:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(f"http://{server.host}:{server.port}/", timeout=5)
        self.assertEqual(caught.exception.code, 404)

    def test_a_client_that_leaves_before_the_answer_is_not_a_traceback(self):
        gone = threading.Event()

        def slow() -> Stats:
            gone.wait(5)
            return Stats(queues=[])

        stderr = io.StringIO()
        with MetricsServer(self.app.metrics, slow) as server:
            with contextlib.redirect_stderr(stderr), self.assertLogs("tinyjobs.exporter", "DEBUG") as logs:
                client = socket.create_connection((server.host, server.port), timeout=5)
                client.sendall(b"GET /metrics HTTP/1.0\r\n\r\n")
                client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                client.close()
                gone.set()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not any(
                    "went away" in r.getMessage() or "request from" in r.getMessage()
                    for r in logs.records
                ):
                    time.sleep(0.02)
        self.assertEqual(stderr.getvalue(), "")
        self.assertTrue(
            any("went away" in r.getMessage() or "request from" in r.getMessage()
                for r in logs.records)
        )

    def test_a_stopped_server_that_never_started_does_not_hang(self):
        server = MetricsServer(self.app.metrics, None)
        server.stop()
        server.stop()

    def test_a_render_that_fails_is_a_500_not_a_dropped_connection(self):
        with MetricsServer(self.app.metrics, lambda: "not stats") as server:
            with self.assertLogs("tinyjobs.exporter", "ERROR"):
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    urllib.request.urlopen(server.url, timeout=5)
        self.assertEqual(caught.exception.code, 500)

    def test_an_unreachable_store_still_serves_the_process_counters(self):
        def broken() -> Stats:
            raise ConnectionError("store unreachable")

        with MetricsServer(self.app.metrics, broken) as server:
            with self.assertLogs("tinyjobs.exporter", "DEBUG") as logs:
                for _ in range(2):
                    with urllib.request.urlopen(server.url, timeout=5) as response:
                        body = response.read().decode()
        self.assertIn("tinyjobs_info{", body)
        self.assertNotIn("tinyjobs_store_", body)
        failures = [r for r in logs.records if "did not answer the stats query" in r.getMessage()]
        self.assertEqual([r.levelname for r in failures], ["WARNING", "DEBUG"])


class CommandLineTests(unittest.TestCase):
    def sqlite_app(self) -> TinyJobs:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        app = TinyJobs(f"sqlite:///{Path(directory.name) / 'jobs.db'}")
        self.addCleanup(app.close)
        logger = logging.getLogger("tinyjobs")
        saved = (logger.handlers[:], logger.level, logger.propagate)

        def restore() -> None:
            logger.handlers[:], logger.level, logger.propagate = saved

        self.addCleanup(restore)
        return app

    def test_the_worker_takes_the_metrics_options(self):
        args = build_parser().parse_args(
            ["worker", "--app", "x:app", "--metrics-port", "9464", "--metrics-host", "0.0.0.0"]
        )
        self.assertEqual((args.metrics_port, args.metrics_host), (9464, "0.0.0.0"))
        args = build_parser().parse_args(["worker", "--app", "x:app"])
        self.assertEqual((args.metrics_port, args.metrics_host), (None, "127.0.0.1"))

    def test_a_port_outside_the_range_is_refused_by_the_parser(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as exit:
            build_parser().parse_args(["worker", "--app", "x:app", "--metrics-port", "70000"])
        self.assertEqual(exit.exception.code, 2)
        self.assertIn("between 0 and 65535", stderr.getvalue())

    def test_a_port_already_in_use_is_a_configuration_error(self):
        app = self.sqlite_app()
        taken = socket.socket()
        self.addCleanup(taken.close)
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = main(["worker", "--metrics-port", str(port), "--no-scheduler"], app=app)
        self.assertEqual(code, 2)
        self.assertIn(f"cannot serve metrics on 127.0.0.1:{port}", stderr.getvalue())
        self.assertIn("pick a free --metrics-port", stderr.getvalue())

    def test_a_host_that_does_not_resolve_is_a_configuration_error(self):
        app = self.sqlite_app()
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = main(
                ["worker", "--metrics-port", "0", "--metrics-host", "no-such-host.invalid",
                 "--no-scheduler"],
                app=app,
            )
        self.assertEqual(code, 2)
        self.assertIn("cannot serve metrics on no-such-host.invalid:0", stderr.getvalue())
        self.assertIn("an address of this machine", stderr.getvalue())

    def test_a_privileged_port_names_the_range_to_pick_from(self):
        app = self.sqlite_app()
        refused = PermissionError(errno.EACCES, "Permission denied")
        stderr = io.StringIO()
        with mock.patch("tinyjobs.exporter.MetricsServer", side_effect=refused):
            with contextlib.redirect_stderr(stderr):
                code = main(["worker", "--metrics-port", "80", "--no-scheduler"], app=app)
        self.assertEqual(code, 2)
        self.assertIn("cannot serve metrics on 127.0.0.1:80", stderr.getvalue())
        self.assertIn("above 1023", stderr.getvalue())

    def test_a_store_that_does_not_answer_in_time_costs_the_scrape_its_gauges(self):
        from tinyjobs.cli.commands import worker as command

        app = self.sqlite_app()

        async def hanging() -> Stats:
            try:
                await asyncio.sleep(30)
            finally:
                await asyncio.shield(asyncio.sleep(1))
            raise AssertionError("the scrape waited for the store")

        args = build_parser().parse_args(["worker", "--app", "x:app", "--metrics-port", "0"])
        with mock.patch.object(command, "STATS_BUDGET", 0.2), mock.patch.object(app, "astats", hanging):
            with command.WorkerCommand()._metrics(app, args) as server:
                started = time.monotonic()
                with self.assertLogs("tinyjobs.exporter", "WARNING") as logs:
                    with urllib.request.urlopen(server.url, timeout=5) as response:
                        body = response.read().decode()
                elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.8)
        self.assertIn("within 0.2s", str(logs.records[0].exc_info[1]))
        self.assertIn("tinyjobs_info{", body)
        self.assertNotIn("tinyjobs_store_", body)

    def test_importing_the_library_or_the_cli_does_not_load_the_http_server(self):
        checked = subprocess.run(
            [sys.executable, "-c",
             "import sys, tinyjobs, tinyjobs.cli; print('http.server' in sys.modules)"],
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(checked.stdout.strip(), "False")

    def test_a_worker_serves_metrics_while_it_runs(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "metrics_app.py").write_text(
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{root / "jobs.db"}'!r})\n"
            "@app.task(name='metrics.echo')\n"
            "def echo(value):\n"
            "    return value\n"
        )
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )
        subprocess.run(
            [sys.executable, "-c", "from metrics_app import app; app.send('metrics.echo', args=(1,))"],
            cwd=root, env=env, check=True,
        )
        worker = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "worker", "--app", "metrics_app:app",
             "--metrics-port", "0", "--no-scheduler", "--shutdown-timeout", "2"],
            cwd=root, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        lines: queue.Queue[str | None] = queue.Queue()

        def read_stderr() -> None:
            for line in worker.stderr:
                lines.put(line)
            lines.put(None)

        reader = threading.Thread(target=read_stderr, daemon=True)
        reader.start()
        try:
            url = None
            deadline = time.monotonic() + 20
            while url is None:
                line = lines.get(timeout=max(0.0, deadline - time.monotonic()))
                self.assertIsNotNone(line, "the worker ended before serving metrics")
                found = re.search(r"serving metrics at (http://127\.0\.0\.1:\d+/metrics)", line)
                if found:
                    url = found.group(1)
            deadline = time.monotonic() + 10
            body = ""
            while time.monotonic() < deadline and "outcome=\"success\"} 1" not in body:
                time.sleep(0.1)
                with urllib.request.urlopen(url, timeout=5) as response:
                    body = response.read().decode()
            self.assertIn('tinyjobs_jobs_total{task="metrics.echo",outcome="success"} 1\n', body)
            self.assertIn('tinyjobs_store_jobs{queue="default",status="success"} 1\n', body)
            worker.send_signal(signal.SIGTERM)
            self.assertEqual(worker.wait(15), 0)
            with self.assertRaises(urllib.error.URLError):
                urllib.request.urlopen(url, timeout=2)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(10)
            reader.join(5)
            worker.stderr.close()


if __name__ == "__main__":
    unittest.main()
