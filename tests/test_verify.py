from __future__ import annotations

import asyncio
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from threading import Event, Thread, Timer
from types import SimpleNamespace
from unittest import mock

from tinyjobs import TinyJobs
from tinyjobs import verify
from tinyjobs.cli.commands.verify import settled
from tinyjobs.cli.main import main
from tinyjobs.exceptions import ConfigurationError
from tinyjobs.models import Job, JobPayload, Page
from tinyjobs.status import JobStatus
from tinyjobs.verifying import (
    KILL,
    PAGE,
    PURGE_POLICY,
    TERM,
    Report,
    Settings,
    Verification,
    _pid_of,
    _Workers,
    in_memory,
    masked,
)

REDIS_URL = os.environ.get("TINYJOBS_REDIS_URL")
POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")

QUICK = dict(duration=6.0, rate=30.0, workers=2, slots=4, kill_every=1.0, lease=3.0, settle=30.0)

ENV = {
    key: value for key, value in os.environ.items() if not key.startswith("TINYJOBS_VERIFY_")
}


class ScratchCase(unittest.TestCase):
    def scratch_url(self) -> str:
        self.folder = tempfile.mkdtemp(prefix="tinyjobs-verify-test-")
        self.addCleanup(shutil.rmtree, self.folder, True)
        return f"sqlite:///{self.folder}/jobs.db"

    def counts(self, url: str, **options) -> dict[str, int]:
        app = TinyJobs(url, **options)
        try:
            return app.counts()
        finally:
            app.close()


@unittest.skipUnless(os.name == "posix", "the verifier stops workers with signals")
class VerificationTests(ScratchCase):
    def test_a_short_run_under_kills_loses_nothing_and_cleans_up(self):
        url = self.scratch_url()
        with mock.patch.dict(os.environ, {verify.PREFIX_VARIABLE: "inherited"}):
            report = Verification(Settings(url, **QUICK)).run()
        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.sent, report.attempted)
        self.assertGreater(report.sent, 0)
        self.assertEqual(report.marked, report.sent)
        self.assertEqual((report.lost, report.unfinished, report.extra), (0, 0, 0))
        self.assertEqual((report.missing, report.phantoms, report.duplicated), (0, 0, 0))
        self.assertEqual(report.left, 0)
        self.assertGreaterEqual(report.ready, 2)
        self.assertGreaterEqual(report.terms, 1)
        self.assertGreaterEqual(report.kills, 1)
        self.assertIsNotNone(report.p95)
        self.assertEqual(report.lines()[-1], "ok")
        self.assertEqual(sum(self.counts(url).values()), 0)

    def test_a_store_holding_jobs_is_refused(self):
        url = self.scratch_url()
        app = TinyJobs(url)
        app.send("someone.elses.task")
        app.close()
        with self.assertRaises(ConfigurationError) as caught:
            Verification(Settings(url, **QUICK)).run()
        self.assertIn("holds 1 job(s)", str(caught.exception))
        self.assertEqual(self.counts(url), {"queued": 1})

    def test_a_store_the_workers_cannot_share_is_refused(self):
        for url in ("memory://", "sqlite://", "sqlite:///:memory:"):
            self.assertTrue(in_memory(url), url)
            with self.assertRaises(ConfigurationError) as caught:
                Verification(Settings(url))
            self.assertIn("in memory", str(caught.exception))
        for url in ("sqlite:///jobs.db", "sqlite:///x?mode=memory", "memory:///x.db",
                    "sqlite::memory:", "redis://localhost/1", "postgresql://h/d"):
            self.assertFalse(in_memory(url), url)

    def test_a_store_that_cannot_be_reached_is_a_configuration_error(self):
        with self.assertRaises(ConfigurationError) as caught:
            Verification(Settings("redis://127.0.0.1:1/0", **QUICK)).run()
        self.assertIn("cannot use the store at redis://127.0.0.1:1/0", str(caught.exception))

    def test_keep_leaves_the_jobs_in_the_store(self):
        url = self.scratch_url()
        settings = Settings(url, duration=6.0, rate=20.0, workers=2, kill_every=1.0, keep=True)
        report = Verification(settings).run()
        self.assertTrue(report.ok, report.lines())
        self.assertIn("the finished jobs are kept in the store", report.lines()[-2])
        self.assertEqual(self.counts(url), {"success": report.sent})
        stopped = Event()
        Timer(1.0, stopped.set).start()
        settings = Settings(url, duration=60.0, rate=200.0, workers=1, slots=1, kill_every=60.0,
                            keep=True)
        app = TinyJobs(url)
        self.addCleanup(app.close)
        app.purge(PURGE_POLICY)
        report = Verification(settings, stopped=stopped).run()
        self.assertTrue(report.interrupted)
        self.assertEqual(self.counts(url).get("queued", 0), 0)
        self.assertEqual(self.counts(url).get("cancelled", 0), report.unfinished)

    def test_a_worker_that_dies_on_its_own_fails_the_run(self):
        url = self.scratch_url()
        settings = Settings(url, duration=1.0, rate=5.0, workers=1, kill_every=60.0, lease=2.0,
                            settle=5.0)
        dying = [sys.executable, "-c", "import sys, time; time.sleep(0.3); sys.exit(3)"]
        report = Verification(settings, command=dying).run()
        self.assertFalse(report.ok)
        self.assertGreaterEqual(report.deaths, 1)
        self.assertFalse(report.settled)
        self.assertEqual(report.lost, 0)
        self.assertEqual(report.unfinished, report.sent)
        last = report.lines()[-1]
        self.assertIn("died unexpectedly", last)
        self.assertIn("did not settle", last)
        self.assertIn("unfinished", last)
        self.assertNotIn("lost", last)
        self.assertEqual(sum(self.counts(url).values()), 0)

    def test_a_worker_ignoring_sigterm_is_reported_and_killed(self):
        url = self.scratch_url()
        marker = os.path.join(self.folder, "deaf")
        settings = Settings(url, duration=60.0, rate=5.0, workers=1, kill_every=60.0)
        deaf = [sys.executable, "-c",
                "import pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"pathlib.Path({marker!r}).touch(); time.sleep(60)"]
        stopped = Event()
        stopped_at = []

        def once_deaf():
            deadline = time.monotonic() + 30
            while not os.path.exists(marker) and time.monotonic() < deadline:
                time.sleep(0.05)
            stopped_at.append(time.monotonic())
            stopped.set()

        Thread(target=once_deaf, daemon=True).start()
        with mock.patch("tinyjobs.verifying.SHUTDOWN_TIMEOUT", 0.5), \
                mock.patch("tinyjobs.verifying.EXIT_GRACE", 0.5):
            report = Verification(settings, stopped=stopped, command=deaf).run()
        ended = time.monotonic()
        self.assertTrue(os.path.exists(marker), "the deaf worker never started")
        self.assertEqual(report.bad_exits, 1)
        self.assertGreaterEqual(ended - stopped_at[0], 1.0)
        self.assertIn("did not stop cleanly on SIGTERM", report.lines()[-1])
        self.assertEqual(sum(self.counts(url).values()), 0)

    def test_the_load_keeps_the_rate_and_skips_the_kills_it_missed(self):
        class Fake:
            def __init__(self):
                self.sent = []
                self.stops = []
                self.busy = []

            def send(self, task, args, *, queue):
                self.sent.append(args[0])

            def jobs(self, *, queue, status, limit):
                return Page([Job(id="r", task=verify.TASK, payload=b"", codec="json",
                                 queue=queue, status=status, worker_id="host/7/abcd")], None)

            def stop_one(self, how, ready, busy):
                if "t" not in ready:
                    return False
                self.stops.append(how)
                self.busy.append(busy)
                return True

            def sweep(self):
                pass

        fake = Fake()
        settings = Settings("sqlite:///jobs.db", duration=1.0, rate=100.0, kill_every=0.3)
        report = Report(settings)
        Verification(settings)._load(fake, fake, report, lambda: {"t"})
        self.assertEqual(report.attempted, report.sent)
        self.assertEqual(report.numbers, set(fake.sent))
        self.assertGreaterEqual(report.sent, 50)
        self.assertLessEqual(report.sent, 101)
        self.assertEqual(fake.stops[:3], [TERM, KILL, TERM][:len(fake.stops)])
        self.assertEqual(fake.busy, [{7}] * len(fake.stops))
        self.assertEqual(len(fake.stops), report.terms + report.kills)
        self.assertGreaterEqual(len(fake.stops), 1)
        self.assertLessEqual(len(fake.stops), 3)
        unready = Fake()
        report = Report(settings)
        polls = []
        Verification(settings)._load(unready, unready, report, lambda: polls.append(1) or set())
        self.assertEqual(unready.stops, [])
        self.assertEqual(report.terms + report.kills, 0)
        self.assertLessEqual(len(polls), 6)
        clock = [0.0]

        class Stalling(Fake):
            def send(self, task, args, *, queue):
                super().send(task, args, queue=queue)
                if len(self.sent) == 1:
                    clock[0] += 3.5

            def stop_one(self, how, ready, busy):
                self.stops.append(clock[0])
                return True

        class Clock:
            def is_set(self):
                return False

            def wait(self, timeout):
                clock[0] += timeout

        stalled = Stalling()
        settings = Settings("sqlite:///jobs.db", duration=10.0, rate=1.0, kill_every=1.0)
        report = Report(settings)
        fake_time = SimpleNamespace(monotonic=lambda: clock[0])
        with mock.patch("tinyjobs.verifying.time", fake_time):
            Verification(settings, stopped=Clock())._load(stalled, stalled, report, lambda: {"t"})
        self.assertEqual(stalled.stops, [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])
        self.assertEqual(report.terms + report.kills, 7)

    def test_a_kill_prefers_a_ready_worker_holding_a_job(self):
        class Process:
            def __init__(self, pid):
                self.pid = pid
                self.stdin = None
                self.signals = []

            def poll(self):
                return None

            def terminate(self):
                self.signals.append(TERM)

            def kill(self):
                self.signals.append(KILL)

        workers = _Workers.__new__(_Workers)
        processes = [Process(1), Process(2), Process(3)]
        workers._command, workers._env = [], {}
        workers._alive = list(processes)
        workers._tokens = {process: f"t{process.pid}" for process in processes}
        workers._signalled, workers._ready, workers._deadlines = {}, set(), {}
        workers._stopping, workers.deaths, workers.bad_exits = False, 0, 0
        started = []
        workers.start_one = lambda: started.append(1)
        self.assertFalse(workers.stop_one(KILL, set(), {1, 2, 3}))
        self.assertFalse(workers.stop_one(KILL, {"t1", "t2", "t3"}, set()))
        self.assertTrue(workers.stop_one(KILL, {"t1", "t2", "t3"}, {3}))
        self.assertEqual([p.signals for p in processes], [[], [], [KILL]])
        self.assertTrue(workers.stop_one(TERM, {"t1", "t2", "t3"}, {3}))
        self.assertEqual([p.signals for p in processes], [[TERM], [], [KILL]])
        self.assertTrue(workers.stop_one(TERM, {"t2"}, set()))
        self.assertEqual([p.signals for p in processes], [[TERM], [TERM], [KILL]])
        self.assertFalse(workers.stop_one(TERM, {"t1", "t2", "t3"}, {1, 2, 3}))
        self.assertEqual(started, [1, 1, 1])
        for worker_id, pid in (("host/7/ab", 7), ("host/7/ab-0", 7), ("drain", None),
                               (None, None), ("host/x/ab", None)):
            self.assertEqual(_pid_of(worker_id), pid, worker_id)

    def test_a_stop_ends_the_run_early_and_clears_what_was_queued(self):
        url = self.scratch_url()
        stopped = Event()
        Timer(1.0, stopped.set).start()
        settings = Settings(url, duration=60.0, rate=200.0, workers=1, slots=1, kill_every=60.0)
        report = Verification(settings, stopped=stopped).run()
        self.assertTrue(report.interrupted)
        self.assertEqual(report.lost, 0)
        self.assertEqual(report.sent, report.counted)
        self.assertIn("stopped before the end", report.lines()[-1])
        self.assertEqual(sum(self.counts(url).values()), 0)

    def test_settings_must_make_sense(self):
        for wrong in (
            dict(rate=0), dict(duration=-1), dict(workers=0), dict(slots=0.5),
            dict(kill_every=float("inf")), dict(settle=0), dict(lease=90.0),
            dict(lease=5.0, settle=6.0),
        ):
            with self.assertRaises(ConfigurationError):
                Settings("sqlite:///jobs.db", **wrong)
        with self.assertRaises(ConfigurationError) as caught:
            Settings("sqlite:///jobs.db", lease=90.0)
        self.assertIn("settle must be above lease plus the reap interval (91s)",
                      str(caught.exception))
        Settings("sqlite:///jobs.db", lease=90.0, settle=92.0)

    def test_the_report_reads_as_prose(self):
        settings = Settings("sqlite:///jobs.db")
        report = Report(
            settings, attempted=1500, sent=1500, succeeded=1500, terms=3, kills=2, reruns=17,
            marked=1500, p50=0.048, p95=1.3, read_back=True, ready=3, left=0,
        )
        self.assertEqual(
            report.lines(),
            [
                "sent 1,500 jobs: 1,500 succeeded, 0 failed, 0 cancelled, 0 unfinished, 0 lost",
                "workers stopped 5 times, 3 by SIGTERM and 2 by SIGKILL, 0 unexpected deaths",
                "17 jobs ran more than once, after the kills; every job applied its side "
                "effect once",
                "end-to-end p50 48 ms, p95 1.3 s, kills included",
                "0 enqueue failures; the store is left as it was found",
                "ok",
            ],
        )
        broken = Report(
            settings, attempted=12, sent=10, succeeded=8, failed=1, lost=1, marked=9, missing=1,
            kills=1, ready=1, enqueue_failures=2, last_enqueue_error="OSError: no", left=1,
            read_back=True,
        )
        lines = broken.lines()
        self.assertEqual(lines[2], "0 jobs ran more than once, after the kills; 9 side effects "
                                   "recorded for 10 jobs sent, 1 missing and 0 never acknowledged")
        self.assertEqual(lines[3], "2 enqueue failures, the last one OSError: no; 1 job(s) "
                                   "left in the store")
        self.assertEqual(
            lines[-1],
            "FAIL: 1 job(s) lost; 1 job(s) failed; 1 job(s) sent without their side effect; "
            "1 job(s) left in the store; 2 enqueue(s) failed",
        )
        extra = Report(settings, sent=10, succeeded=11, extra=1, marked=11, phantoms=1, kills=1,
                       ready=1, read_back=True, left=0)
        self.assertEqual(extra.lines()[-1], "FAIL: 1 job(s) never acknowledged: an enqueue "
                         "reported as failed had gone through; 1 side effect(s) of jobs never "
                         "acknowledged")
        twice = Report(settings, sent=10, succeeded=11, duplicated=1, marked=10, kills=1, ready=1,
                       read_back=True, left=0)
        self.assertEqual(twice.lines()[-1], "FAIL: 1 job(s) enqueued more than once")
        kept = Report(Settings("sqlite:///jobs.db", keep=True), sent=1, succeeded=1, marked=1,
                      kills=1, ready=1, read_back=True, left=1)
        self.assertTrue(kept.ok, kept.lines())
        unread = Report(settings, sent=10, kills=1, error="OSError: gone")
        self.assertEqual(unread.lines()[0], "sent 10 jobs, not read back")
        self.assertEqual(unread.lines()[3], "0 enqueue failures; the store was not cleared")
        self.assertEqual(
            unread.lines()[-1],
            "FAIL: failed: OSError: gone; the jobs could not be read back; the ledger could "
            "not be read",
        )
        unledgered = Report(settings, sent=10, succeeded=10, kills=1, read_back=True, left=0)
        self.assertEqual(unledgered.lines()[-1], "FAIL: the ledger could not be read")
        unsent = Report(settings, attempted=5, enqueue_failures=5, last_enqueue_error="no",
                        marked=0, read_back=True, left=0)
        self.assertEqual(unsent.lines()[-1], "FAIL: 5 enqueue(s) failed")
        meddled = Report(settings, sent=10, succeeded=9, cancelled=1, marked=10, kills=1, ready=1,
                         read_back=True, left=0)
        self.assertEqual(meddled.lines()[0], "sent 10 jobs: 9 succeeded, 0 failed, 1 cancelled, "
                                             "0 unfinished, 0 lost")
        self.assertEqual(meddled.lines()[-1], "FAIL: 1 job(s) cancelled by someone else")
        unsettled = Report(settings, sent=10, succeeded=9, unfinished=1, marked=9, missing=1,
                           kills=1, ready=1, read_back=True, settled=False, left=0)
        self.assertIn("did not settle within 60s, the lease being 5s", unsettled.lines()[-1])
        quiet = Report(settings, sent=10, succeeded=10, marked=10, ready=1, read_back=True, left=0)
        self.assertEqual(quiet.lines()[-1], "FAIL: no worker was SIGKILLed: lengthen --duration "
                                            "or shorten --kill-every")
        idle = Report(settings, sent=10, unfinished=10, marked=0, missing=10, ready=0,
                      read_back=True, left=0)
        self.assertEqual(idle.lines()[-1], "FAIL: 10 job(s) unfinished; 10 job(s) sent without "
                                           "their side effect; no worker ran a job")
        others = Report(settings, sent=1, succeeded=1, marked=1, kills=1, ready=1, read_back=True,
                        others=2)
        self.assertIn("2 other job(s) appeared, so nothing was purged", others.lines()[-2])
        self.assertTrue(others.ok, others.lines())

    def test_the_read_back_pages_through_the_jobs_and_counts_others(self):
        settings = Settings("sqlite:///jobs.db")
        verification = Verification(settings)
        queue = verification._queue
        jobs = [
            Job(id=f"j{n}", task=verify.TASK, payload=str(n).encode(), codec="json", queue=queue,
                status=JobStatus.SUCCESS, created_at=100.0, finished_at=100.0 + n / 1000,
                claims=2 if n % 100 == 0 else 1)
            for n in range(PAGE + 7)
        ]
        jobs.append(Job(id="q", task=verify.TASK, payload=str(PAGE + 7).encode(), codec="json",
                        queue=queue, status=JobStatus.QUEUED))
        jobs.append(Job(id="x", task="other", payload=b"", codec="json", queue=queue,
                        status=JobStatus.SUCCESS))
        jobs.append(Job(id="d", task=verify.TASK, payload=b"3", codec="json", queue=queue,
                        status=JobStatus.SUCCESS, created_at=100.0, finished_at=100.0))
        jobs.append(Job(id="r", task=verify.TASK, payload=str(PAGE + 8).encode(), codec="json",
                        queue=queue, status=JobStatus.RUNNING, worker_id="host/1/ab"))

        class Serialization:
            def decode_payload(self, job):
                return JobPayload(args=(int(job.payload), 0.05))

        class Store:
            pages = []
            serialization = Serialization()

            def jobs(self, *, queue, limit, cursor):
                self.pages.append(cursor)
                start = int(cursor or 0)
                items = jobs[start:start + limit]
                return Page(items, str(start + limit) if len(items) == limit else None)

            def counts(self):
                return {"success": PAGE + 10, "queued": 1, "running": 1}

        report = Report(settings, numbers=set(range(PAGE + 10)))
        pending = verification._read_back(Store(), report)
        self.assertEqual(Store.pages, [None, str(PAGE)])
        self.assertEqual((report.succeeded, report.unfinished, report.reruns), (PAGE + 8, 2, 6))
        self.assertEqual(pending, ["q", "r"])
        self.assertEqual(report.others, 2)
        self.assertEqual((report.lost, report.extra, report.duplicated), (1, 0, 1))
        report.others = 0
        Store.purge = lambda self, policy: 0
        verification._clear(Store(), report)
        self.assertEqual(report.left, PAGE + 12)
        self.assertEqual(report.p50, jobs[(PAGE + 7) // 2].finished_at - 100.0)
        self.assertTrue(report.read_back)
        report = Report(settings, numbers=set(range(1, PAGE + 9)))
        verification._read_back(Store(), report)
        self.assertEqual((report.lost, report.extra), (0, 1))

    def test_the_workers_app_comes_from_the_environment(self):
        with mock.patch.dict(os.environ, ENV, clear=True):
            with self.assertRaises(ConfigurationError):
                verify.app
        url = self.scratch_url()
        ledger = os.path.join(tempfile.mkdtemp(), "ledger.db")
        self.addCleanup(shutil.rmtree, os.path.dirname(ledger), True)
        verify.open_ledger(ledger)
        variables = {**ENV, verify.URL_VARIABLE: url, verify.LEDGER_VARIABLE: ledger}
        with mock.patch.dict(os.environ, variables, clear=True):
            app = verify.app
        self.addCleanup(app.close)
        self.assertIs(verify.app, app)
        self.assertIn(verify.TASK, [task.name for task in app.registry])
        verify.record(ledger, 7, "one")
        verify.record(ledger, 7, "two")
        self.assertEqual(verify.marked(ledger), {7})
        self.assertEqual(verify.ready(ledger), {"one", "two"})
        del verify.app

    def test_the_command_prints_the_report_and_exits_on_the_verdict(self):
        url = self.scratch_url()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = main(["verify", url, "--duration", "6s", "--rate", "10",
                         "--workers", "2", "--kill-every", "1s"])
        self.assertEqual(code, 0, out.getvalue())
        lines = out.getvalue().splitlines()
        self.assertTrue(lines[0].startswith("2 workers of 4 slots, 10 jobs/s for 6s"))
        self.assertEqual(lines[-1], "ok")
        self.assertIn("waiting for the store to settle", lines[1])

    def test_the_command_needs_a_url(self):
        out = io.StringIO()
        with contextlib.redirect_stderr(out), self.assertRaises(SystemExit):
            main(["verify"])
        self.assertIn("url", out.getvalue())

    def test_the_command_hides_the_password_and_sizes_the_settle_to_the_lease(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["verify", "postgresql://u:hunter2@127.0.0.1:1/db", "--duration", "1s"])
        self.assertEqual(code, 2)
        printed = out.getvalue() + err.getvalue()
        self.assertNotIn("hunter2", printed)
        self.assertIn("postgresql://u:***@127.0.0.1:1/db", printed)
        self.assertEqual(masked("redis://:secret@[::1]:6379/0"), "redis://:***@[::1]:6379/0")
        self.assertEqual(masked("sqlite:///jobs.db"), "sqlite:///jobs.db")
        self.assertEqual(settled(None, 5.0), 60.0)
        self.assertEqual(settled(None, 60.0), 121.0)
        self.assertEqual(settled(7.0, 60.0), 7.0)


@unittest.skipUnless(os.name == "posix", "the verifier stops workers with signals")
@unittest.skipUnless(shutil.which("pgrep"), "needs pgrep to look for leftover workers")
class SignalledVerifierTests(ScratchCase):
    def start(self, url: str) -> subprocess.Popen:
        process = subprocess.Popen(
            [sys.executable, "-m", "tinyjobs", "verify", url, "--duration", "60s",
             "--rate", "10", "--workers", "1", "--kill-every", "60s"],
            env={**ENV, "TMPDIR": self.folder}, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True,
        )
        self.addCleanup(self.end, process)
        self.pids: dict[str, str] = {}
        deadline = time.monotonic() + 20
        while not self.pids and time.monotonic() < deadline:
            self.pids = {pid: self.started_at(pid) for pid in self.found(["-P", str(process.pid)])}
            time.sleep(0.1)
        self.assertTrue(self.pids, "no worker started")
        return process

    def end(self, process: subprocess.Popen) -> None:
        if process.poll() is None:
            process.kill()
        process.communicate()
        for pid in self.workers():
            with contextlib.suppress(OSError):
                os.kill(int(pid), 9)

    def found(self, options: list[str]) -> list[str]:
        found = subprocess.run(
            ["pgrep", *options, "-f", "tinyjobs.verify:app"], capture_output=True, text=True
        )
        return found.stdout.split()

    def started_at(self, pid: str) -> str:
        shown = subprocess.run(["ps", "-o", "lstart=", "-p", pid], capture_output=True, text=True)
        return shown.stdout.strip()

    def workers(self) -> list[str]:
        alive = set(self.found([]))
        return sorted(
            pid for pid, started in self.pids.items()
            if pid in alive and self.started_at(pid) == started
        )

    def test_sigterm_ends_the_run_and_stops_the_workers(self):
        url = self.scratch_url()
        process = self.start(url)
        time.sleep(1.0)
        process.terminate()
        output, _ = process.communicate(timeout=60)
        self.assertEqual(process.returncode, 1, output)
        self.assertIn("stopped before the end", output.splitlines()[-1])
        self.assertEqual(self.workers(), [])
        self.assertEqual(sum(self.counts(url).values()), 0)
        self.assertEqual(
            [name for name in os.listdir(self.folder) if name.startswith("tinyjobs-verify-")], []
        )

    def test_workers_follow_a_verifier_that_dies(self):
        url = self.scratch_url()
        process = self.start(url)
        process.kill()
        process.communicate(timeout=60)
        deadline = time.monotonic() + 30
        while self.workers() and time.monotonic() < deadline:
            time.sleep(0.2)
        self.assertEqual(self.workers(), [])


@unittest.skipUnless(os.name == "posix", "the verifier stops workers with signals")
@unittest.skipUnless(REDIS_URL, "set TINYJOBS_REDIS_URL to run the redis tests")
class RedisVerificationTests(unittest.TestCase):
    def test_a_short_run_on_redis(self):
        prefix = f"tinyjobs-verify-{uuid.uuid4().hex}"
        self.addCleanup(asyncio.run, self.forget(prefix))
        report = Verification(Settings(REDIS_URL, prefix=prefix, **QUICK)).run()
        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.marked, report.sent)

    async def forget(self, prefix: str) -> None:
        from redis.asyncio import Redis

        client = Redis.from_url(REDIS_URL)
        try:
            keys = [key async for key in client.scan_iter(match=f"{{{prefix}}}:*", count=10_000)]
            if keys:
                await client.delete(*keys)
        finally:
            await client.aclose()


@unittest.skipUnless(os.name == "posix", "the verifier stops workers with signals")
@unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")
class PostgresVerificationTests(unittest.TestCase):
    def test_a_short_run_on_postgres(self):
        prefix = f"tinyjobs_verify_{uuid.uuid4().hex[:16]}"
        self.addCleanup(self.forget, prefix)
        report = Verification(Settings(POSTGRES_URL, prefix=prefix, **QUICK)).run()
        self.assertTrue(report.ok, report.lines())
        self.assertEqual(report.marked, report.sent)

    def forget(self, prefix: str) -> None:
        import psycopg

        with psycopg.connect(POSTGRES_URL, autocommit=True) as connection:
            for table in ("executions", "jobs", "schedules", "meta", "ran"):
                connection.execute(f'DROP TABLE IF EXISTS "{prefix}_{table}" CASCADE')


if __name__ == "__main__":
    unittest.main()
