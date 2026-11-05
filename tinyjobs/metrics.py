from __future__ import annotations

import math
import threading
import time
from typing import Any, Iterable

from .models import Job, Stats

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
)

SUCCESS = "success"
FAILURE = "failure"
RETRY = "retry"


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(**labels: Any) -> str:
    return "{" + ",".join(f'{key}="{_label(str(value))}"' for key, value in labels.items()) + "}"


def _number(value: float) -> str:
    if value != value:
        return "NaN"
    if value in (math.inf, -math.inf):
        return "+Inf" if value > 0 else "-Inf"
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


class _Histogram:
    __slots__ = ("counts", "total", "count")

    def __init__(self) -> None:
        self.counts = [0] * len(DURATION_BUCKETS)
        self.total = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        for index, bound in enumerate(DURATION_BUCKETS):
            if value <= bound:
                self.counts[index] += 1
        self.total += value
        self.count += 1


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._finished: dict[tuple[str, str], int] = {}
        self._durations: dict[str, _Histogram] = {}
        self._running: dict[str, int] = {}
        self._attempts: set[tuple[str, int]] = set()

    def started(self, job: Job) -> None:
        with self._lock:
            if (job.id, job.claims) not in self._attempts:
                self._attempts.add((job.id, job.claims))
                self._running[job.task] = self._running.get(job.task, 0) + 1

    def ended(self, job: Job) -> None:
        with self._lock:
            if (job.id, job.claims) in self._attempts:
                self._attempts.discard((job.id, job.claims))
                self._running[job.task] -= 1

    def succeeded(self, job: Job, result: Any, duration: float) -> None:
        self._finish(job.task, SUCCESS, duration)

    def failed(self, job: Job, error: BaseException, will_retry: bool) -> None:
        self._finish(job.task, RETRY if will_retry else FAILURE, None)

    def _finish(self, task: str, outcome: str, duration: float | None) -> None:
        with self._lock:
            self._finished[task, outcome] = self._finished.get((task, outcome), 0) + 1
            if duration is not None:
                self._durations.setdefault(task, _Histogram()).observe(duration)

    def render(self, stats: Stats | None = None, *, now: float | None = None) -> str:
        from . import __version__

        with self._lock:
            lines = list(self._process_lines())
        lines.extend(_store_lines(stats, time.time() if now is None else now))
        lines.append("# HELP tinyjobs_info The tinyjobs version serving these metrics.")
        lines.append("# TYPE tinyjobs_info gauge")
        lines.append(f"tinyjobs_info{_labels(version=__version__)} 1")
        return "\n".join(lines) + "\n"

    def _process_lines(self) -> Iterable[str]:
        yield "# HELP tinyjobs_jobs_total Job attempts finished in this process, by task and outcome."
        yield "# TYPE tinyjobs_jobs_total counter"
        for (task, outcome), count in sorted(self._finished.items()):
            yield f"tinyjobs_jobs_total{_labels(task=task, outcome=outcome)} {count}"
        yield "# HELP tinyjobs_job_duration_seconds Time a successful job attempt took, by task."
        yield "# TYPE tinyjobs_job_duration_seconds histogram"
        for task, histogram in sorted(self._durations.items()):
            for bound, count in zip(DURATION_BUCKETS, histogram.counts):
                yield (
                    f"tinyjobs_job_duration_seconds_bucket{_labels(task=task, le=_number(bound))}"
                    f" {count}"
                )
            yield (
                f"tinyjobs_job_duration_seconds_bucket{_labels(task=task, le='+Inf')}"
                f" {histogram.count}"
            )
            yield f"tinyjobs_job_duration_seconds_sum{_labels(task=task)} {_number(histogram.total)}"
            yield f"tinyjobs_job_duration_seconds_count{_labels(task=task)} {histogram.count}"
        yield "# HELP tinyjobs_jobs_running Jobs running in this process, by task."
        yield "# TYPE tinyjobs_jobs_running gauge"
        for task, count in sorted(self._running.items()):
            yield f"tinyjobs_jobs_running{_labels(task=task)} {count}"


def _store_lines(stats: Stats | None, now: float) -> Iterable[str]:
    if stats is None:
        return
    yield "# HELP tinyjobs_store_jobs Jobs in the store, by queue and status."
    yield "# TYPE tinyjobs_store_jobs gauge"
    for queue in stats.queues:
        for status, count in sorted(queue.counts.items()):
            yield f"tinyjobs_store_jobs{_labels(queue=queue.queue, status=status)} {count}"
    yield "# HELP tinyjobs_store_queued_jobs Jobs waiting in the store, by task."
    yield "# TYPE tinyjobs_store_queued_jobs gauge"
    for task, count in sorted(stats.queued_tasks.items()):
        yield f"tinyjobs_store_queued_jobs{_labels(task=task)} {count}"
    yield (
        "# HELP tinyjobs_store_oldest_due_seconds How long the oldest due job has waited, "
        "by queue."
    )
    yield "# TYPE tinyjobs_store_oldest_due_seconds gauge"
    for queue in stats.queues:
        if queue.oldest_due_at is not None:
            age = max(0.0, now - queue.oldest_due_at)
            yield f"tinyjobs_store_oldest_due_seconds{_labels(queue=queue.queue)} {_number(age)}"
    yield "# HELP tinyjobs_store_expired_leases Running jobs whose lease has expired."
    yield "# TYPE tinyjobs_store_expired_leases gauge"
    yield f"tinyjobs_store_expired_leases {stats.expired_leases}"
