from __future__ import annotations

import os
import random
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from threading import Event
from typing import IO, Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from .backends.loader import in_memory
from .duration import is_finite_seconds
from .exceptions import ConfigurationError
from .retention import RetentionPolicy
from .status import JobStatus
from .verify import (
    LEDGER_VARIABLE,
    PREFIX_VARIABLE,
    TASK,
    TOKEN_VARIABLE,
    URL_VARIABLE,
    VARIABLE_PREFIX,
    WATCH_VARIABLE,
    build,
    marked,
    open_ledger,
    ready,
)

SLEEP_RANGE = (0.02, 0.1)

REAP_INTERVAL = 1.0

POLL_MAXIMUM = 0.05

SHUTDOWN_TIMEOUT = 10.0

EXIT_GRACE = 5.0

SETTLE_POLL = 0.5

KILL_RETRY = 0.25

PAGE = 500

PURGE_PASSES = 1000

PURGE_POLICY = RetentionPolicy(
    success=1e-9, cancelled=1e-9, failed=1e-9, interval=0.0, maximum_per_sweep=1_000_000
)

LOG_TAIL = 20

PENDING = (JobStatus.QUEUED, JobStatus.HELD, JobStatus.WAITING, JobStatus.RUNNING)

QUEUE_PREFIX = "tinyjobs-verify-"

TERM = "term"
KILL = "kill"
FORCED = "forced"

POSIX_MESSAGE = "tinyjobs verify needs a POSIX platform: it stops workers with signals"

IN_MEMORY_MESSAGE = (
    "tinyjobs verify needs a store its workers can share, and {url} is in memory: "
    "give it a file, a Redis database or a Postgres database"
)

OCCUPIED_MESSAGE = (
    "the store holds {count} job(s): tinyjobs verify runs on a store of its own, "
    "a fresh file, a spare Redis database or a scratch Postgres database, "
    "and leaves it as it found it; a new file, a flushed Redis database or dropped "
    "Postgres tables clear one, which a verify run killed outright leaves occupied"
)

SETTLE_MESSAGE = (
    "settle must be above lease plus the reap interval ({least:g}s): a job a SIGKILL cut "
    "short comes back only when its lease ends, got {settle!r}"
)


@dataclass(frozen=True, slots=True)
class Settings:
    url: str
    prefix: str | None = None
    duration: float = 30.0
    rate: float = 50.0
    workers: int = 3
    slots: int = 4
    kill_every: float = 5.0
    lease: float = 5.0
    settle: float = 60.0
    keep: bool = False

    def __post_init__(self) -> None:
        for name in ("duration", "rate", "kill_every", "lease", "settle"):
            value = getattr(self, name)
            if not is_finite_seconds(value) or value <= 0:
                raise ConfigurationError(f"{name} must be above zero, got {value!r}")
        for name in ("workers", "slots"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ConfigurationError(
                    f"{name} must be a whole number of at least 1, got {value!r}"
                )
        least = self.lease + REAP_INTERVAL
        if self.settle <= least:
            raise ConfigurationError(SETTLE_MESSAGE.format(least=least, settle=self.settle))


@dataclass(slots=True)
class Report:
    settings: Settings
    attempted: int = 0
    sent: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0
    unfinished: int = 0
    terms: int = 0
    kills: int = 0
    deaths: int = 0
    bad_exits: int = 0
    reruns: int = 0
    marked: int | None = None
    p50: float | None = None
    p95: float | None = None
    enqueue_failures: int = 0
    last_enqueue_error: str = ""
    settled: bool = True
    interrupted: bool = False
    error: str = ""
    read_back: bool = False
    lost: int = 0
    extra: int = 0
    duplicated: int = 0
    missing: int = 0
    phantoms: int = 0
    ready: int | None = None
    others: int = 0
    purged: int = 0
    left: int | None = None
    last_lines: list[str] = field(default_factory=list)
    numbers: set[int] = field(default_factory=set)

    @property
    def counted(self) -> int:
        return self.succeeded + self.failed + self.cancelled + self.unfinished

    @property
    def problems(self) -> list[str]:
        problems = []
        if self.interrupted:
            problems.append("stopped before the end")
        if self.error:
            problems.append(f"failed: {self.error}")
        if not self.settled:
            problems.append(
                f"the store did not settle within {self.settings.settle:g}s, the lease "
                f"being {self.settings.lease:g}s"
            )
        if not self.read_back:
            problems.append("the jobs could not be read back")
        else:
            if self.lost:
                problems.append(f"{self.lost:,} job(s) lost")
            if self.extra:
                problems.append(
                    f"{self.extra:,} job(s) never acknowledged: an enqueue reported as failed "
                    f"had gone through"
                )
            if self.duplicated:
                problems.append(f"{self.duplicated:,} job(s) enqueued more than once")
            if self.cancelled:
                problems.append(f"{self.cancelled:,} job(s) cancelled by someone else")
            if self.failed:
                problems.append(f"{self.failed:,} job(s) failed")
            if self.unfinished:
                problems.append(f"{self.unfinished:,} job(s) unfinished")
        if self.marked is None:
            problems.append("the ledger could not be read")
        else:
            if self.missing:
                problems.append(f"{self.missing:,} job(s) sent without their side effect")
            if self.phantoms:
                problems.append(
                    f"{self.phantoms:,} side effect(s) of jobs never acknowledged"
                )
        if self.left and not self.settings.keep and not self.others:
            problems.append(f"{self.left:,} job(s) left in the store")
        if self.deaths:
            problems.append(f"{self.deaths} worker(s) died unexpectedly")
        if self.bad_exits:
            problems.append(f"{self.bad_exits} worker(s) did not stop cleanly on SIGTERM")
        if self.enqueue_failures:
            problems.append(f"{self.enqueue_failures:,} enqueue(s) failed")
        if not self.interrupted and not self.error and self.sent:
            if self.ready == 0:
                problems.append("no worker ran a job")
            elif self.ready and not self.kills:
                problems.append(
                    "no worker was SIGKILLed: lengthen --duration or shorten --kill-every"
                )
        return problems

    @property
    def ok(self) -> bool:
        return not self.problems

    def lines(self) -> list[str]:
        if self.read_back:
            first = (
                f"sent {self.sent:,} jobs: {self.succeeded:,} succeeded, {self.failed:,} failed, "
                f"{self.cancelled:,} cancelled, {self.unfinished:,} unfinished, "
                f"{self.lost:,} lost"
            )
        else:
            first = f"sent {self.sent:,} jobs, not read back"
        lines = [
            first,
            f"workers stopped {self.terms + self.kills} times, {self.terms} by SIGTERM and "
            f"{self.kills} by SIGKILL, {self.deaths} unexpected deaths",
            f"{self.reruns:,} jobs ran more than once, after the kills; " + self._marked(),
        ]
        if self.p50 is not None and self.p95 is not None:
            lines.append(
                f"end-to-end p50 {_seconds(self.p50)}, p95 {_seconds(self.p95)}, kills included"
            )
        failures = f"{self.enqueue_failures:,} enqueue failures"
        if self.enqueue_failures:
            failures += f", the last one {self.last_enqueue_error}"
        lines.append(f"{failures}; {self._store()}")
        lines.extend(self.last_lines)
        lines.append("ok" if self.ok else "FAIL: " + "; ".join(self.problems))
        return lines

    def _marked(self) -> str:
        if self.marked is None:
            return "the ledger could not be read"
        if not self.missing and not self.phantoms:
            return "every job applied its side effect once"
        return (
            f"{self.marked:,} side effects recorded for {self.sent:,} jobs sent, "
            f"{self.missing:,} missing and {self.phantoms:,} never acknowledged"
        )

    def _store(self) -> str:
        if self.settings.keep:
            return "the finished jobs are kept in the store"
        if self.others:
            return f"{self.others:,} other job(s) appeared, so nothing was purged"
        if self.left is None:
            return "the store was not cleared"
        if self.left:
            return f"{self.left:,} job(s) left in the store"
        return "the store is left as it was found"


class Verification:
    def __init__(
        self,
        settings: Settings,
        *,
        stopped: Event | None = None,
        command: Sequence[str] | None = None,
        say: Callable[[str], None] = lambda line: None,
    ) -> None:
        if os.name != "posix":
            raise ConfigurationError(POSIX_MESSAGE)
        if in_memory(settings.url):
            raise ConfigurationError(IN_MEMORY_MESSAGE.format(url=settings.url))
        self._settings = settings
        self._stopped = stopped or Event()
        self._command = list(command) if command else self._worker_command()
        self._say = say
        self._queue = QUEUE_PREFIX + secrets.token_hex(4)

    def run(self) -> Report:
        settings = self._settings
        report = Report(settings)
        folder = tempfile.mkdtemp(prefix="tinyjobs-verify-")
        try:
            ledger = os.path.join(folder, "ledger.db")
            app = build(settings.url, ledger, settings.prefix)
            try:
                self._refuse_occupied(app)
                open_ledger(ledger)
                workers = _Workers(
                    [*self._command, "--queues", self._queue],
                    {
                        **_without_verify_variables(os.environ),
                        URL_VARIABLE: settings.url,
                        LEDGER_VARIABLE: ledger,
                        WATCH_VARIABLE: "1",
                        **({PREFIX_VARIABLE: settings.prefix} if settings.prefix else {}),
                    },
                    os.path.join(folder, "workers.log"),
                )
                try:
                    for _ in range(settings.workers):
                        workers.start_one()
                    self._load(app, workers, report, partial(ready, ledger))
                    if not self._stopped.is_set():
                        self._say(f"sent {report.sent:,} jobs, waiting for the store to settle")
                        self._settle(app, workers, report)
                except Exception as error:
                    report.error = _described(error)
                finally:
                    workers.stop_all(_ready(ledger))
                    report.deaths, report.bad_exits = workers.deaths, workers.bad_exits
                    report.last_lines = (
                        workers.tail() if report.deaths or report.bad_exits else []
                    )
                with suppress(Exception):
                    report.ready = len(ready(ledger))
                with suppress(Exception):
                    marks = marked(ledger)
                    report.marked = len(marks)
                    report.missing = len(report.numbers - marks)
                    report.phantoms = len(marks - report.numbers)
                try:
                    pending = self._read_back(app, report)
                    for job_id in pending:
                        app.cancel(job_id)
                    if not settings.keep:
                        self._clear(app, report)
                except Exception as error:
                    report.error = report.error or _described(error)
            finally:
                app.close()
        finally:
            shutil.rmtree(folder, ignore_errors=True)
        return report

    def _refuse_occupied(self, app: Any) -> None:
        try:
            held = sum(app.counts().values())
        except Exception as error:
            raise ConfigurationError(
                f"cannot use the store at {masked(self._settings.url)}: {_described(error)}"
            ) from error
        if held:
            raise ConfigurationError(OCCUPIED_MESSAGE.format(count=held))

    def _load(
        self, app: Any, workers: "_Workers", report: Report, ready: Callable[[], set[str]]
    ) -> None:
        settings = self._settings
        start = time.monotonic()
        end = start + settings.duration
        next_send = start
        next_kill = start + settings.kill_every
        while not self._stopped.is_set():
            now = time.monotonic()
            if now >= end:
                return
            if now >= next_kill:
                terminating = report.terms <= report.kills
                if workers.stop_one(TERM if terminating else KILL, ready(), self._busy(app)):
                    if terminating:
                        report.terms += 1
                    else:
                        report.kills += 1
                    next_kill = now + settings.kill_every
                else:
                    next_kill = now + KILL_RETRY
            workers.sweep()
            if now >= next_send:
                self._send(app, report)
                next_send = max(now, next_send + 1.0 / settings.rate)
                continue
            self._stopped.wait(min(next_send, next_kill, end) - now)
        report.interrupted = True

    def _send(self, app: Any, report: Report) -> None:
        number = report.attempted
        report.attempted += 1
        try:
            app.send(TASK, (number, random.uniform(*SLEEP_RANGE)), queue=self._queue)
        except Exception as error:
            report.enqueue_failures += 1
            report.last_enqueue_error = _described(error)
            return
        report.sent += 1
        report.numbers.add(number)

    def _settle(self, app: Any, workers: "_Workers", report: Report) -> None:
        deadline = time.monotonic() + self._settings.settle
        while not self._stopped.is_set():
            workers.sweep()
            if not self._unsettled(app):
                return
            if time.monotonic() >= deadline:
                report.settled = False
                return
            self._stopped.wait(SETTLE_POLL)
        report.interrupted = True

    def _unsettled(self, app: Any) -> bool:
        if any(self._pending(app, status) for status in PENDING):
            return True
        cursor = None
        while True:
            page = app.jobs(queue=self._queue, limit=PAGE, cursor=cursor)
            if any(job.status in PENDING for job in page.items):
                return True
            cursor = page.cursor
            if not cursor or not page.items:
                return False

    def _pending(self, app: Any, status: JobStatus) -> bool:
        return bool(app.jobs(queue=self._queue, status=status, limit=1).items)

    def _busy(self, app: Any) -> set[int]:
        try:
            running = app.jobs(queue=self._queue, status=JobStatus.RUNNING, limit=PAGE).items
        except Exception:
            return set()
        return {pid for job in running if (pid := _pid_of(job.worker_id)) is not None}

    def _read_back(self, app: Any, report: Report) -> list[str]:
        latencies = []
        pending = []
        seen = set()
        counted = 0
        cursor = None
        while True:
            page = app.jobs(queue=self._queue, limit=PAGE, cursor=cursor)
            for job in page.items:
                if job.task != TASK:
                    continue
                counted += 1
                seen.add(int(app.serialization.decode_payload(job).args[0]))
                if job.status is JobStatus.SUCCESS:
                    report.succeeded += 1
                    if job.finished_at is not None:
                        latencies.append(job.finished_at - job.created_at)
                elif job.status is JobStatus.FAILED:
                    report.failed += 1
                elif job.status is JobStatus.CANCELLED:
                    report.cancelled += 1
                else:
                    report.unfinished += 1
                    if job.status in PENDING:
                        pending.append(job.id)
                if job.claims > 1:
                    report.reruns += 1
            cursor = page.cursor
            if not cursor or not page.items:
                break
        report.others = max(0, sum(app.counts().values()) - counted)
        report.lost = len(report.numbers - seen)
        report.extra = len(seen - report.numbers)
        report.duplicated = counted - len(seen)
        if latencies:
            latencies.sort()
            report.p50 = latencies[len(latencies) // 2]
            report.p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
        report.read_back = True
        return pending

    def _clear(self, app: Any, report: Report) -> None:
        if report.others:
            return
        for _ in range(PURGE_PASSES):
            removed = app.purge(PURGE_POLICY) or 0
            report.purged += removed
            if not removed:
                break
        report.left = sum(app.counts().values())

    def _worker_command(self) -> list[str]:
        settings = self._settings
        return [
            sys.executable, "-m", "tinyjobs", "worker",
            "--app", "tinyjobs.verify:app",
            "--concurrency", str(settings.slots),
            "--lease", f"{settings.lease:g}",
            "--reap-interval", f"{REAP_INTERVAL:g}",
            "--poll-max", f"{POLL_MAXIMUM:g}",
            "--shutdown-timeout", f"{SHUTDOWN_TIMEOUT:g}",
            "--no-scheduler", "--no-purge",
            "--log-level", "WARNING",
        ]


class _Workers:
    def __init__(self, command: list[str], env: dict[str, str], log_path: str) -> None:
        self._command = command
        self._env = env
        self._log_path = log_path
        self._log: IO[bytes] = open(log_path, "ab")
        self._alive: list[subprocess.Popen] = []
        self._tokens: dict[subprocess.Popen, str] = {}
        self._signalled: dict[subprocess.Popen, str] = {}
        self._ready: set[subprocess.Popen] = set()
        self._deadlines: dict[subprocess.Popen, float] = {}
        self._stopping = False
        self.deaths = 0
        self.bad_exits = 0

    def start_one(self) -> None:
        token = secrets.token_hex(8)
        process = subprocess.Popen(
            self._command, env={**self._env, TOKEN_VARIABLE: token}, stdin=subprocess.PIPE,
            stdout=self._log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        self._alive.append(process)
        self._tokens[process] = token

    def stop_one(self, how: str, ready: set[str], busy: set[int] = frozenset()) -> bool:
        self.sweep()
        candidates = [
            process
            for process in self._alive
            if process not in self._signalled
            and self._tokens[process] in ready
            and process.poll() is None
        ]
        if how == KILL:
            candidates = [process for process in candidates if process.pid in busy]
        for process in candidates:
            self._signal(process, how, True)
            self.start_one()
            return True
        return False

    def _signal(self, process: subprocess.Popen, how: str, ready: bool) -> None:
        self._signalled[process] = how
        if ready:
            self._ready.add(process)
        if how == TERM:
            self._deadlines[process] = time.monotonic() + SHUTDOWN_TIMEOUT + EXIT_GRACE
        with suppress(OSError):
            if how == KILL:
                process.kill()
            else:
                process.terminate()

    def sweep(self) -> None:
        now = time.monotonic()
        for process in list(self._alive):
            status = process.poll()
            if status is None:
                if self._signalled.get(process) == TERM and now >= self._deadlines[process]:
                    self.bad_exits += 1
                    self._signalled[process] = FORCED
                    with suppress(OSError):
                        process.kill()
                continue
            self._alive.remove(process)
            self._release(process)
            self._tokens.pop(process, None)
            kind = self._signalled.pop(process, None)
            if kind is None:
                self.deaths += 1
                if not self._stopping:
                    self.start_one()
            elif kind == TERM and status != 0 and process in self._ready:
                self.bad_exits += 1

    def stop_all(self, ready: set[str]) -> None:
        self._stopping = True
        self.sweep()
        for process in self._alive:
            if process not in self._signalled:
                self._signal(process, TERM, self._tokens[process] in ready)
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT + EXIT_GRACE
        for process in list(self._alive):
            with suppress(OSError, subprocess.TimeoutExpired):
                process.wait(max(0.0, deadline - time.monotonic()))
        self.sweep()
        for process in self._alive:
            with suppress(OSError):
                process.kill()
            with suppress(OSError):
                process.wait()
            self._release(process)
        self._alive.clear()
        self._log.close()

    def _release(self, process: subprocess.Popen) -> None:
        if process.stdin is not None:
            with suppress(OSError):
                process.stdin.close()

    def tail(self) -> list[str]:
        try:
            text = Path(self._log_path).read_text(encoding="utf-8", errors="replace")
        except OSError:
            return []
        return text.splitlines()[-LOG_TAIL:]


def masked(url: str) -> str:
    parts = urlsplit(url)
    if parts.password is None:
        return url
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    netloc = f"{parts.username or ''}:***@{host}" + (f":{parts.port}" if parts.port else "")
    return parts._replace(netloc=netloc).geturl()


def _ready(ledger: str) -> set[str]:
    try:
        return ready(ledger)
    except Exception:
        return set()


def _pid_of(worker_id: str | None) -> int | None:
    parts = (worker_id or "").split("/")
    if len(parts) < 3 or not parts[1].isdigit():
        return None
    return int(parts[1])


def _without_verify_variables(environment: Mapping[str, str]) -> dict[str, str]:
    return {
        key: value for key, value in environment.items() if not key.startswith(VARIABLE_PREFIX)
    }


def _described(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def _seconds(value: float) -> str:
    if value < 1.0:
        return f"{value * 1000:.0f} ms"
    return f"{value:.1f} s"
