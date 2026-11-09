from __future__ import annotations

import asyncio
import contextlib
import io
import json
import logging
import os
import queue
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from wsgiref.util import setup_testing_defaults

import tinyjobs
from tinyjobs import TinyJobs
from tinyjobs.cli.main import build_parser, main
from tinyjobs.dashboard import Dashboard, asgi, wsgi
from tinyjobs.dashboard.server import DashboardServer
from tinyjobs.web import Request, Response


def request(path: str, *, method: str = "GET", query: dict | None = None, root: str = "") -> Request:
    return Request(method, path, query or {}, root)


class DashboardCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")
        self.dashboard = Dashboard(self.app, page_size=10)

        @self.app.task(name="math.add", store_result=True)
        def add(a, b):
            return a + b

        @self.app.task(name="math.boom", max_attempts=1)
        def boom():
            raise ValueError("nope <b>bold</b>")

        @self.app.task(name="clock.tick", cron="*/5 * * * *")
        def tick():
            return "tick"

        @self.app.task(name="nightly report (eu)", cron="0 3 * * *")
        def nightly():
            return "report"

        @self.app.task(name="reports/daily", cron="0 4 * * *")
        def daily():
            return "daily"

        self.add = add
        self.boom = boom

    def tearDown(self) -> None:
        self.app.close()
        self.directory.cleanup()

    def get(self, path: str, **query: str) -> Response:
        return self.dashboard.respond(request(path, query=query))

    def post(self, path: str) -> Response:
        return self.dashboard.respond(request(path, method="POST"))

    def html(self, path: str, **query: str) -> str:
        response = self.get(path, **query)
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.content_type, "text/html; charset=utf-8")
        return response.body.decode()

    def data(self, path: str, **query: str) -> dict:
        response = self.get(path, **query)
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.content_type, "application/json")
        return json.loads(response.body)


class OverviewTests(DashboardCase):
    def test_an_empty_store_shows_the_checks_and_no_queues(self):
        page = self.html("/")
        self.assertIn("<h1>Overview</h1>", page)
        self.assertIn("no jobs in the store yet", page)
        self.assertIn('class="ok"', page)
        self.assertIn("nothing waiting", page)

    def test_queue_counts_link_to_the_filtered_job_list(self):
        self.add.enqueue(1, 2)
        self.add.with_options(queue="mail").enqueue(3, 4)
        self.boom.enqueue()
        self.app.drain()
        page = self.html("/")
        self.assertIn('<a href="/jobs?queue=default&amp;status=success">1</a>', page)
        self.assertIn('<a href="/jobs?queue=default&amp;status=failed">1</a>', page)
        self.assertIn('<a href="/jobs?queue=mail&amp;status=success">1</a>', page)
        self.assertIn("running past their lease: <strong>0</strong>", page)

    def test_the_overview_is_also_served_as_json(self):
        self.add.enqueue(1, 2)
        data = self.data("/api/overview")
        self.assertEqual(data["queues"][0]["queue"], "default")
        self.assertEqual(data["queues"][0]["counts"]["queued"], 1)
        self.assertEqual(data["queued_tasks"], {"math.add": 1})
        self.assertEqual(data["expired_leases"], 0)
        self.assertTrue(all(finding["ok"] for finding in data["findings"]))
        self.assertIsInstance(data["now"], float)

    def test_a_store_that_does_not_answer_in_time_is_a_503(self):
        job = self.add.enqueue(1, 2)
        for name in ("schedules", "list", "get"):
            self.slow_down(name)
        dashboard = Dashboard(self.app, budget=0.05)
        for path in ("/", "/api/overview", "/jobs", "/api/jobs", f"/jobs/{job.id}", "/schedules"):
            with self.assertLogs("tinyjobs.dashboard", "WARNING") as logs:
                response = dashboard.respond(request(path))
            self.assertEqual(response.status, 503, path)
            self.assertIn(b"the store did not answer within 0.05s", response.body)
            self.assertIn(f"could not answer GET {path}: the store did not answer", logs.output[0])
            self.assertEqual(len(logs.output), 1)

    def test_the_overview_queries_the_stats_once(self):
        self.app.start()
        calls = []
        stats = self.app.backend.stats

        async def counted(*args, **kwargs):
            calls.append(args)
            return await stats(*args, **kwargs)

        self.app.backend.stats = counted
        self.html("/")
        self.assertEqual(len(calls), 1)

    def test_a_wrong_method_is_refused_before_the_store_is_asked(self):
        self.app.close()
        for path in ("/", "/schedules", "/api/overview", "/api/schedules"):
            response = self.post(path)
            self.assertEqual(response.status, 405, path)

    def slow_down(self, name: str) -> None:
        self.app.start()
        original = getattr(self.app.backend, name)

        async def slow(*args, **kwargs):
            await asyncio.sleep(1)
            return await original(*args, **kwargs)

        setattr(self.app.backend, name, slow)


class JobListTests(DashboardCase):
    def test_jobs_are_listed_newest_first_with_a_next_page(self):
        ids = [self.add.enqueue(i, i).id for i in range(25)]
        page = self.html("/jobs")
        self.assertIn(f'<a href="/jobs/{ids[-1]}">', page)
        self.assertNotIn(ids[0], page)
        self.assertIn("Next page", page)
        first = self.data("/api/jobs")
        self.assertEqual([job["id"] for job in first["items"]], ids[::-1][:10])
        second = self.data("/api/jobs", cursor=first["cursor"])
        self.assertEqual([job["id"] for job in second["items"]], ids[::-1][10:20])
        third = self.data("/api/jobs", cursor=second["cursor"])
        self.assertEqual(len(third["items"]), 5)
        self.assertIsNone(third["cursor"])
        self.assertNotIn("Next page", self.html("/jobs", cursor=second["cursor"]))

    def test_the_next_page_link_keeps_the_filters(self):
        for i in range(12):
            self.add.with_options(queue="mail").enqueue(i, i)
        page = self.html("/jobs", queue="mail", status="queued")
        self.assertIn('href="/jobs?status=queued&amp;queue=mail&amp;cursor=', page)
        self.assertIn('<option value="queued" selected>', page)
        self.assertNotIn('<option value="" selected>', page)
        self.assertIn('value="mail"', page)
        self.assertIn('<option value="" selected>any status</option>', self.html("/jobs"))
        self.assertIn('<option value="" selected>any status</option>', self.html("/jobs", queue="mail"))

    def test_filters_narrow_the_list(self):
        self.add.enqueue(1, 2)
        failing = self.boom.enqueue()
        self.app.drain()
        self.assertEqual(
            [job["id"] for job in self.data("/api/jobs", status="failed")["items"]], [failing.id]
        )
        self.assertEqual(self.data("/api/jobs", queue="other")["items"], [])
        self.assertIn("no jobs match", self.html("/jobs", queue="other"))

    def test_an_unknown_status_is_a_400_that_names_the_choices(self):
        response = self.get("/jobs", status="done")
        self.assertEqual(response.status, 400)
        self.assertIn(b"one of queued, held, running, success, failed, cancelled", response.body)
        self.assertEqual(self.get("/api/jobs", status="done").status, 400)


class JobPageTests(DashboardCase):
    def test_a_finished_job_shows_its_arguments_result_and_attempt(self):
        job = self.add.with_options(key="sum-1").enqueue(1, b=2)
        self.app.drain()
        page = self.html(f"/jobs/{job.id}")
        self.assertIn(f"<h1>Job {job.id}</h1>", page)
        self.assertIn("<dt>args</dt><dd><pre>(1, 2)</pre></dd>", page)
        self.assertIn("<dt>kwargs</dt><dd><pre>{}</pre></dd>", page)
        self.assertIn("<h2>Result</h2><pre>3</pre>", page)
        self.assertIn("sum-1", page)
        self.assertIn("status-success", page)
        self.assertNotIn("<h2>Last error</h2>", page)
        self.assertNotIn("Retry", page)
        self.assertNotIn("Cancel", page)
        data = self.data(f"/api/jobs/{job.id}")
        self.assertEqual(data["result"], "3")
        self.assertEqual(data["actions"], {"retry": False, "cancel": False})
        self.assertEqual(len(data["executions"]), 1)
        self.assertEqual(data["executions"][0]["status"], "success")

    def test_a_failed_job_shows_the_escaped_error_and_the_traceback(self):
        job = self.boom.enqueue()
        self.app.drain()
        page = self.html(f"/jobs/{job.id}")
        self.assertIn("nope &lt;b&gt;bold&lt;/b&gt;", page)
        self.assertNotIn("<b>bold</b>", page)
        self.assertIn('<details id="run-1"><summary>run 1, attempt 1, ValueError</summary>', page)
        self.assertIn('<a href="#run-1">ValueError</a>', page)
        self.assertIn(f'action="/jobs/{job.id}/retry"', page)
        self.assertNotIn(f'action="/jobs/{job.id}/cancel"', page)

    def test_a_queued_job_offers_cancel_only(self):
        job = self.add.enqueue(1, 2)
        page = self.html(f"/jobs/{job.id}")
        self.assertIn(f'action="/jobs/{job.id}/cancel"', page)
        self.assertNotIn(f'action="/jobs/{job.id}/retry"', page)
        self.assertIn("not run yet", page)

    def test_arguments_that_cannot_be_decoded_are_reported_not_fatal(self):
        job = self.add.enqueue(1, 2)
        with contextlib.closing(
            __import__("sqlite3").connect(Path(self.directory.name) / "jobs.db")
        ) as connection:
            connection.execute("UPDATE tinyjobs_jobs SET payload = X'00ff' WHERE id = ?", (job.id,))
            connection.commit()
        page = self.html(f"/jobs/{job.id}")
        self.assertIn("the arguments could not be decoded", page)
        data = self.data(f"/api/jobs/{job.id}")
        self.assertIsNone(data["args"])
        self.assertIsNotNone(data["payload_error"])

    def test_arguments_whose_repr_fails_are_reported_not_fatal(self):
        job = self.add.enqueue(1, 2)

        class Broken:
            def __repr__(self):
                raise RuntimeError("no repr")

        decode = self.app.decode
        self.app.decode = lambda job: type("P", (), {"args": (Broken(),), "kwargs": {}})()
        try:
            data = self.data(f"/api/jobs/{job.id}")
        finally:
            self.app.decode = decode
        self.assertEqual(data["payload_error"], "no repr")
        self.assertIsNone(data["args"])

    def test_a_result_that_cannot_be_decoded_is_reported_not_fatal(self):
        job = self.add.enqueue(1, 2)
        self.app.drain()
        with contextlib.closing(
            __import__("sqlite3").connect(Path(self.directory.name) / "jobs.db")
        ) as connection:
            connection.execute("UPDATE tinyjobs_jobs SET result = X'00ff' WHERE id = ?", (job.id,))
            connection.commit()
        page = self.html(f"/jobs/{job.id}")
        self.assertIn("the result could not be decoded", page)
        data = self.data(f"/api/jobs/{job.id}")
        self.assertIsNone(data["result"])
        self.assertIsNotNone(data["result_error"])

    def test_a_missing_job_is_a_404(self):
        self.assertEqual(self.get("/jobs/nope").status, 404)
        self.assertEqual(self.get("/api/jobs/nope").status, 404)
        self.assertEqual(self.post("/jobs/nope/retry").status, 404)
        self.assertEqual(self.get("/jobs/nope").body, b"no such job\n")

    def test_a_missing_job_page_does_not_echo_the_id_as_html(self):
        response = self.get("/jobs/<script>")
        self.assertEqual(response.status, 404)
        self.assertEqual(response.content_type, "text/plain; charset=utf-8")
        self.assertNotIn(b"<script>", response.body)


class JobActionTests(DashboardCase):
    def test_retrying_a_failed_job_queues_it_again(self):
        job = self.boom.enqueue()
        self.app.drain()
        response = self.post(f"/jobs/{job.id}/retry")
        self.assertEqual(response.status, 303)
        self.assertEqual(dict(response.headers)["Location"], f"/jobs/{job.id}?notice=retried")
        self.assertEqual(self.app.get(job.id).status.value, "queued")
        self.assertIn("the job is queued again", self.html(f"/jobs/{job.id}", notice="retried"))

    def test_retrying_a_finished_job_is_refused_with_a_notice(self):
        job = self.add.enqueue(1, 2)
        self.app.drain()
        response = self.post(f"/jobs/{job.id}/retry")
        self.assertEqual(dict(response.headers)["Location"], f"/jobs/{job.id}?notice=retry-refused")
        self.assertIn("was not retried", self.html(f"/jobs/{job.id}", notice="retry-refused"))

    def test_a_held_job_offers_cancel_and_takes_it(self):
        @self.app.task(rate="1/h", name="spaced")
        def spaced():
            return None

        spaced.enqueue()
        held = spaced.enqueue()
        self.app.drain()
        self.assertEqual(self.app.get(held.id).status.value, "held")
        page = self.html(f"/jobs/{held.id}")
        self.assertIn(f'action="/jobs/{held.id}/cancel"', page)
        self.assertIn("status-held", page)
        response = self.post(f"/jobs/{held.id}/cancel")
        self.assertEqual(dict(response.headers)["Location"], f"/jobs/{held.id}?notice=cancelled")
        self.assertEqual(self.app.get(held.id).status.value, "cancelled")

    def test_cancelling_a_queued_job(self):
        job = self.add.enqueue(1, 2)
        response = self.post(f"/jobs/{job.id}/cancel")
        self.assertEqual(dict(response.headers)["Location"], f"/jobs/{job.id}?notice=cancelled")
        self.assertEqual(self.app.get(job.id).status.value, "cancelled")
        again = self.post(f"/jobs/{job.id}/cancel")
        self.assertEqual(dict(again.headers)["Location"], f"/jobs/{job.id}?notice=cancel-refused")
        self.assertIn("had already finished", self.html(f"/jobs/{job.id}", notice="cancel-refused"))

    def test_a_retry_blocked_by_the_dedup_key_says_so(self):
        failed = self.boom.with_options(key="k").enqueue()
        self.app.drain()
        self.add.with_options(key="k").enqueue(1, 2)
        response = self.post(f"/jobs/{failed.id}/retry")
        self.assertEqual(dict(response.headers)["Location"], f"/jobs/{failed.id}?notice=retry-refused")
        page = self.html(f"/jobs/{failed.id}", notice="retry-refused")
        self.assertIn("another queued, held or running job holds its dedup key", page)

    def test_actions_need_post_and_pages_need_get(self):
        job = self.add.enqueue(1, 2)
        response = self.get(f"/jobs/{job.id}/retry")
        self.assertEqual(response.status, 405)
        self.assertEqual(dict(response.headers)["Allow"], "POST")
        for path in ("/", "/jobs", f"/jobs/{job.id}", "/schedules", "/api/overview", "/static/app.js"):
            response = self.post(path)
            self.assertEqual(response.status, 405, path)
            self.assertEqual(dict(response.headers)["Allow"], "GET, HEAD")
        with DashboardServer(self.dashboard, "127.0.0.1", 0) as server:
            for method, path in (("PUT", "/jobs"), ("DELETE", f"/jobs/{job.id}"), ("OPTIONS", "/"), ("TRACE", "/")):
                with socket.create_connection((server.host, server.port), timeout=5) as client:
                    client.sendall(f"{method} {path} HTTP/1.0\r\nContent-Length: 0\r\n\r\n".encode())
                    answer = whole_answer(client)
                self.assertTrue(answer.startswith(b"HTTP/1.0 405"), (method, answer[:60]))
                self.assertIn(b"Allow: GET, HEAD", answer)

    def test_an_unknown_notice_is_ignored(self):
        page = self.html("/jobs", notice="<script>alert(1)</script>")
        self.assertNotIn("<script>alert", page)
        self.assertNotIn('class="notice"', page)


class ScheduleTests(DashboardCase):
    def test_a_declared_schedule_is_listed_before_a_scheduler_stores_it(self):
        page = self.html("/schedules")
        self.assertIn("clock.tick", page)
        self.assertIn("<code>*/5 * * * *</code>", page)
        self.assertIn("not registered yet", page)
        self.assertIn('action="/schedules/clock.tick/pause"', page)
        self.assertIn('action="/schedules/clock.tick/run"', page)
        data = self.data("/api/schedules")
        self.assertEqual(data["items"][0]["stored"], False)
        self.assertEqual(data["items"][0]["declared"], True)

    def test_a_schedule_name_with_a_slash_is_listed_without_buttons(self):
        page = self.html("/schedules")
        row = page[page.index("reports/daily"):]
        row = row[: row.index("</tr>")]
        self.assertNotIn("<form", row)
        self.assertIn('action="/schedules/clock.tick/pause"', page)

    def test_a_schedule_name_with_spaces_and_brackets_round_trips(self):
        page = self.html("/schedules")
        self.assertIn('action="/schedules/nightly%20report%20%28eu%29/pause"', page)
        response = self.post("/schedules/nightly report (eu)/pause")
        self.assertEqual(dict(response.headers)["Location"], "/schedules?notice=paused")
        with DashboardServer(self.dashboard, "127.0.0.1", 0) as server:
            opener = urllib.request.build_opener(NoRedirect)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                opener.open(
                    server.url + "schedules/nightly%20report%20%28eu%29/resume", data=b"", timeout=5
                )
            with caught.exception as answer:
                self.assertEqual(answer.status, 303)
                self.assertEqual(answer.headers["Location"], "/schedules?notice=resumed")

    def test_pause_resume_and_run_round_trip(self):
        response = self.post("/schedules/clock.tick/pause")
        self.assertEqual(response.status, 303)
        self.assertEqual(dict(response.headers)["Location"], "/schedules?notice=paused")
        page = self.html("/schedules", notice="paused")
        self.assertIn("the schedule is paused", page)
        self.assertIn("status-paused", page)
        self.assertIn('action="/schedules/clock.tick/resume"', page)
        self.assertIn('action="/schedules/clock.tick/run"', page)
        again = self.post("/schedules/clock.tick/pause")
        self.assertEqual(dict(again.headers)["Location"], "/schedules?notice=pause-refused")
        resumed = self.post("/schedules/clock.tick/resume")
        self.assertEqual(dict(resumed.headers)["Location"], "/schedules?notice=resumed")
        page = self.html("/schedules")
        self.assertIn("status-active", page)
        self.assertIn('action="/schedules/clock.tick/pause"', page)
        ran = self.post("/schedules/clock.tick/run")
        location = dict(ran.headers)["Location"]
        self.assertTrue(location.startswith("/schedules?notice=ran&job="), location)
        job_id = location.rpartition("=")[2]
        self.assertEqual(self.app.get(job_id).task, "clock.tick")
        page = self.html("/schedules", notice="ran", job=job_id)
        self.assertIn(f'a run is queued as job <a href="/jobs/{job_id}">{job_id}</a>', page)
        self.assertEqual(self.data("/api/schedules")["items"][0]["stored"], True)

    def test_a_schedule_this_app_does_not_declare_is_a_404(self):
        for action in ("pause", "resume", "run"):
            response = self.post(f"/schedules/ghost/{action}")
            self.assertEqual(response.status, 404, action)
            self.assertIn(b"ghost", response.body)

    def test_a_stored_schedule_of_a_task_this_app_lacks_is_marked(self):
        self.app.pause_schedule("clock.tick")
        other = TinyJobs(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")
        self.addCleanup(other.close)
        page = Dashboard(other).respond(request("/schedules")).body.decode()
        self.assertIn("clock.tick (not in this app)", page)
        self.assertIn('action="/schedules/clock.tick/resume"', page)
        self.assertNotIn('action="/schedules/clock.tick/run"', page)


class PlumbingTests(DashboardCase):
    def test_static_files_are_served_with_their_types_and_cached(self):
        for name, content_type in (("style.css", "text/css"), ("app.js", "text/javascript")):
            response = self.get(f"/static/{name}")
            self.assertEqual(response.status, 200)
            self.assertTrue(response.content_type.startswith(content_type))
            self.assertEqual(dict(response.headers)["Cache-Control"], "max-age=86400")
        self.assertEqual(self.get("/static/other.txt").status, 404)
        self.assertEqual(self.get("/static/../core.py").status, 404)

    def test_unknown_paths_are_404(self):
        for path in ("/nope", "/jobs/x/y/z", "/api", "/api/nope", "/schedules/x"):
            self.assertEqual(self.get(path).status, 404, path)

    def test_links_and_actions_carry_the_mount_root(self):
        job = self.boom.enqueue()
        self.app.drain()
        page = self.dashboard.respond(request(f"/jobs/{job.id}", root="/admin/jobs")).body.decode()
        self.assertIn('href="/admin/jobs/static/style.css', page)
        self.assertIn('href="/admin/jobs/jobs"', page)
        self.assertIn(f'action="/admin/jobs/jobs/{job.id}/retry"', page)
        response = self.dashboard.respond(
            request(f"/jobs/{job.id}/retry", method="POST", root="/admin/jobs")
        )
        self.assertEqual(
            dict(response.headers)["Location"], f"/admin/jobs/jobs/{job.id}?notice=retried"
        )
        response = self.dashboard.respond(
            request(f"/jobs/{job.id}/cancel", method="POST", root="/caf\u00e9 bar")
        )
        self.assertEqual(
            dict(response.headers)["Location"], f"/caf%C3%A9%20bar/jobs/{job.id}?notice=cancelled"
        )

    def test_head_is_answered_like_get_without_a_body(self):
        job = self.add.enqueue(1, 2)
        with DashboardServer(self.dashboard, "127.0.0.1", 0) as server:
            head = urllib.request.Request(server.url + "jobs/" + job.id, method="HEAD")
            with urllib.request.urlopen(head, timeout=5) as response:
                self.assertEqual(response.status, 200)
                self.assertGreater(int(response.headers["Content-Length"]), 1000)
                self.assertEqual(response.read(), b"")

    def test_a_bad_content_length_is_a_400(self):
        job = self.add.enqueue(1, 2)
        with DashboardServer(self.dashboard, "127.0.0.1", 0) as server:
            for value in ("abc", "-5"):
                with socket.create_connection((server.host, server.port), timeout=5) as client:
                    client.sendall(
                        f"POST /jobs/{job.id}/cancel HTTP/1.0\r\nContent-Length: {value}\r\n\r\n".encode()
                    )
                    answer = whole_answer(client)
                self.assertTrue(answer.startswith(b"HTTP/1.0 400"), answer[:60])
            for size in (70000, 600000):
                with socket.create_connection((server.host, server.port), timeout=5) as client:
                    client.sendall(
                        f"POST /jobs/{job.id}/cancel HTTP/1.0\r\nContent-Length: {size}\r\n\r\n".encode()
                        + b"x" * size
                    )
                    answer = whole_answer(client)
                self.assertTrue(answer.startswith(b"HTTP/1.0 413"), (size, answer[:60]))
            with socket.create_connection((server.host, server.port), timeout=5) as client:
                client.sendall(
                    f"GET http://dashboard.test{'/jobs/' + job.id}#x HTTP/1.1\r\nHost: dashboard.test\r\n\r\n".encode()
                )
                answer = whole_answer(client)
            self.assertTrue(answer.startswith(b"HTTP/1.0 200"), answer[:60])
        self.assertEqual(self.app.get(job.id).status.value, "queued")

    def test_a_closed_app_is_a_503_not_a_crash(self):
        self.app.close()
        with self.assertNoLogs("tinyjobs.dashboard", "WARNING"):
            response = self.get("/")
        self.assertEqual(response.status, 503)
        self.assertIn(b"this app was closed", response.body)

    def test_the_async_entry_point_serves_on_the_caller_loop(self):
        job = self.add.enqueue(1, 2)

        async def ask():
            return await self.dashboard.handle(request(f"/api/jobs/{job.id}"))

        response = asyncio.run(ask())
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.body)["id"], job.id)


class ServerTests(DashboardCase):
    def test_pages_and_actions_are_served_over_http(self):
        job = self.boom.enqueue()
        self.app.drain()
        with DashboardServer(self.dashboard, "127.0.0.1", 0) as server:
            self.assertTrue(server.url.endswith("/"), server.url)
            with urllib.request.urlopen(server.url + "jobs", timeout=5) as response:
                self.assertEqual(response.headers["Content-Type"], "text/html; charset=utf-8")
                self.assertIn(job.id, response.read().decode())
            opener = urllib.request.build_opener(NoRedirect)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                opener.open(server.url + f"jobs/{job.id}/retry", data=b"", timeout=5)
            with caught.exception as response:
                self.assertEqual(response.status, 303)
                self.assertEqual(response.headers["Location"], f"/jobs/{job.id}?notice=retried")
            with urllib.request.urlopen(server.url + "api/jobs/" + job.id, timeout=5) as response:
                self.assertEqual(json.loads(response.read())["status"], "queued")
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(server.url + "nope", timeout=5)
            with caught.exception as response:
                self.assertEqual(response.code, 404)


def whole_answer(client: socket.socket) -> bytes:
    chunks = []
    while True:
        chunk = client.recv(65536)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class MountingTests(DashboardCase):
    def test_the_wsgi_app_serves_under_its_script_name(self):
        job = self.add.enqueue(1, 2)
        application = wsgi(self.app)
        environ = {}
        setup_testing_defaults(environ)
        environ["SCRIPT_NAME"] = "/admin"
        environ["PATH_INFO"] = "/jobs"
        environ["QUERY_STRING"] = "status=queued"
        started = []
        body = b"".join(application(environ, lambda status, headers: started.append((status, headers))))
        self.assertEqual(started[0][0], "200 OK")
        self.assertIn(("Content-Type", "text/html; charset=utf-8"), started[0][1])
        self.assertIn(f'href="/admin/jobs/{job.id}"'.encode(), body)
        self.assertIn(b'<option value="queued" selected>', body)

    def test_the_wsgi_app_answers_a_posted_action_with_a_redirect(self):
        job = self.add.enqueue(1, 2)
        application = wsgi(self.app)
        environ = {}
        setup_testing_defaults(environ)
        environ.update(
            REQUEST_METHOD="POST", SCRIPT_NAME="/admin/", PATH_INFO=f"/jobs/{job.id}/cancel",
            CONTENT_TYPE="application/x-www-form-urlencoded", CONTENT_LENGTH="abc",
        )
        started = []
        list(application(environ, lambda status, headers: started.append((status, headers))))
        self.assertEqual(started[0][0], "303 See Other")
        self.assertIn(("Location", f"/admin/jobs/{job.id}?notice=cancelled"), started[0][1])
        self.assertEqual(self.app.get(job.id).status.value, "cancelled")

    def test_the_wsgi_app_decodes_the_latin_1_path_and_answers_head(self):
        application = wsgi(self.app)
        environ = {}
        setup_testing_defaults(environ)
        environ.update(
            REQUEST_METHOD="POST",
            PATH_INFO="/schedules/nightly report (eu)/pause".encode().decode("latin-1"),
        )
        started = []
        list(application(environ, lambda status, headers: started.append((status, headers))))
        self.assertEqual(started[0][0], "303 See Other")
        environ = {}
        setup_testing_defaults(environ)
        environ.update(REQUEST_METHOD="HEAD", PATH_INFO="/schedules")
        started = []
        body = list(application(environ, lambda status, headers: started.append((status, headers))))
        self.assertEqual(started[0][0], "200 OK")
        self.assertEqual(body, [])
        self.assertTrue(any(name == "Content-Length" and int(value) > 500 for name, value in started[0][1]))

    def test_the_asgi_app_strips_the_root_path_and_answers_lifespan(self):
        job = self.add.enqueue(1, 2)
        application = asgi(self.app)

        async def drive(scope, incoming):
            sent = []
            received = iter(incoming)

            async def receive():
                return next(received)

            async def send(message):
                sent.append(message)

            await application(scope, receive, send)
            return sent

        async def scenario():
            lifespan = await drive(
                {"type": "lifespan"},
                [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}],
            )
            self.assertEqual(
                [message["type"] for message in lifespan],
                ["lifespan.startup.complete", "lifespan.shutdown.complete"],
            )
            page = await drive(
                {
                    "type": "http", "method": "GET", "path": "/admin/jobs", "root_path": "/admin/",
                    "query_string": "status=&from=caf\xe9".encode("latin-1"),
                    "headers": [],
                },
                [],
            )
            self.assertEqual(page[0]["status"], 200)
            self.assertIn(f'href="/admin/jobs/{job.id}"'.encode(), page[1]["body"])
            self.assertIn(b'href="/admin/static/style.css', page[1]["body"])
            mounted_at_jobs = await drive(
                {
                    "type": "http", "method": "GET", "path": "/jobs/jobs", "root_path": "/jobs",
                    "query_string": b"", "headers": [],
                },
                [],
            )
            self.assertIn(b"<h1>Jobs</h1>", mounted_at_jobs[1]["body"])
            sibling = await drive(
                {
                    "type": "http", "method": "GET", "path": "/jobs", "root_path": "/j",
                    "query_string": b"", "headers": [],
                },
                [],
            )
            self.assertIn(b"<h1>Jobs</h1>", sibling[1]["body"])
            head = await drive(
                {
                    "type": "http", "method": "HEAD", "path": "/jobs", "root_path": "",
                    "query_string": b"", "headers": [],
                },
                [],
            )
            self.assertEqual(head[0]["status"], 200)
            self.assertEqual(head[1]["body"], b"")
            action = await drive(
                {
                    "type": "http", "method": "POST", "path": f"/jobs/{job.id}/cancel",
                    "root_path": "", "query_string": b"",
                    "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
                },
                [],
            )
            self.assertEqual(action[0]["status"], 303)
            self.assertIn((b"location", f"/jobs/{job.id}?notice=cancelled".encode()), action[0]["headers"])
            ignored = await drive({"type": "websocket"}, [])
            self.assertEqual(ignored, [])

        asyncio.run(scenario())
        self.assertEqual(self.app.get(job.id).status.value, "cancelled")


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

    def test_the_command_takes_a_host_and_a_port(self):
        args = build_parser().parse_args(["dashboard", "--app", "x:app"])
        self.assertEqual((args.host, args.port), ("127.0.0.1", 8070))
        args = build_parser().parse_args(["dashboard", "--host", "0.0.0.0", "--port", "0"])
        self.assertEqual((args.host, args.port), ("0.0.0.0", 0))
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                build_parser().parse_args(["dashboard", "--port", "70000"])

    def test_the_command_runs_from_a_thread_and_restores_the_handlers(self):
        app = self.sqlite_app()
        before = signal.getsignal(signal.SIGINT)

        class Instant(threading.Event):
            def wait(self, timeout=None):
                return True

        outcome = []
        with mock.patch("tinyjobs.cli.commands.dashboard.Event", Instant):
            thread = threading.Thread(
                target=lambda: outcome.append(main(["dashboard", "--port", "0"], app=app))
            )
            thread.start()
            thread.join(timeout=20)
            self.assertEqual(outcome, [0])
            self.assertEqual(main(["dashboard", "--port", "0"], app=app), 0)
        self.assertIs(signal.getsignal(signal.SIGINT), before)

    def test_a_port_already_in_use_is_a_configuration_error(self):
        app = self.sqlite_app()
        taken = socket.socket()
        self.addCleanup(taken.close)
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            code = main(["dashboard", "--port", str(port)], app=app)
        self.assertEqual(code, 2)
        self.assertIn(f"cannot serve the dashboard on 127.0.0.1:{port}", stderr.getvalue())
        self.assertIn("pick a free --port", stderr.getvalue())

    def test_importing_the_dashboard_package_does_not_load_the_http_server(self):
        checked = subprocess.run(
            [sys.executable, "-c",
             "import sys, tinyjobs, tinyjobs.cli, tinyjobs.dashboard; "
             "print('http.server' in sys.modules)"],
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(checked.stdout.strip(), "False")

    def test_the_command_serves_until_a_stop_signal(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "board_app.py").write_text(
            "from tinyjobs import TinyJobs\n"
            f"app = TinyJobs({f'sqlite:///{root / "jobs.db"}'!r})\n"
            "@app.task(name='board.echo')\n"
            "def echo(value):\n"
            "    return value\n"
        )
        env = dict(
            os.environ,
            PYTHONPATH=os.pathsep.join([str(Path(tinyjobs.__file__).parents[1]), str(root)]),
        )
        subprocess.run(
            [sys.executable, "-c", "from board_app import app; app.send('board.echo', args=(1,))"],
            cwd=root, env=env, check=True,
        )
        server = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "dashboard", "--app", "board_app:app", "--port", "0"],
            cwd=root, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        lines: queue.Queue[str | None] = queue.Queue()

        def read_stderr() -> None:
            for line in server.stderr:
                lines.put(line)
            lines.put(None)

        reader = threading.Thread(target=read_stderr, daemon=True)
        reader.start()
        try:
            url = None
            deadline = time.monotonic() + 20
            while url is None:
                line = lines.get(timeout=max(0.0, deadline - time.monotonic()))
                self.assertIsNotNone(line, "the command ended before serving")
                found = re.search(r"serving the dashboard at (http://127\.0\.0\.1:\d+/)", line)
                if found:
                    url = found.group(1)
            with urllib.request.urlopen(url + "api/jobs", timeout=5) as response:
                self.assertEqual(json.loads(response.read())["items"][0]["task"], "board.echo")
            server.send_signal(signal.SIGTERM)
            self.assertEqual(server.wait(timeout=10), 0)
        finally:
            if server.poll() is None:
                server.kill()
                server.wait()
            reader.join(timeout=5)
            server.stderr.close()
