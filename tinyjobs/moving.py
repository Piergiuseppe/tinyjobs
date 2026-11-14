from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from threading import Event
from typing import Any, Callable, Sequence

from .backends.loader import in_memory
from .exceptions import ConfigurationError
from .ids import new_job_id
from .models import Job, JobPayload
from .status import JobStatus

MOVABLE = (JobStatus.QUEUED, JobStatus.HELD, JobStatus.WAITING)

PAGE = 500

GROUP = 200

PROGRESS_SECONDS = 2.0

NAMED = 20

PROBE_TASK = "tinyjobs.move.probe"

PROBE_QUEUE = "tinyjobs-move"

PROBE_DELAY = 10 * 365 * 86400.0

PROBE_STALE_SECONDS = 3600.0

NEEDS_AFTER = "run after another job, which the new store cannot do"
NEEDS_DEDUP = "hold a deduplication key, which the new store cannot do"
NEEDS_DELAYED = "are scheduled for later, which the new store cannot do"
TOO_LARGE = "carry a payload larger than the new store takes"

SAME_STORE_MESSAGE = (
    "{old} and {new} are one store under two addresses, so there is nothing to move; "
    "no job was touched"
)

IN_MEMORY_MESSAGE = (
    "{store} is in memory: it lives and ends with this command, so jobs moved to it "
    "would be gone, and one moved from holds none"
)

EMPTY_STORE_MESSAGE = (
    "{old} holds no job of any status: nothing was ever sent to it, everything has "
    "left it, or its address or prefix names another store"
)


@dataclass(slots=True)
class Report:
    old: str
    new: str
    dry_run: bool = False
    reaped: int = 0
    found: int = 0
    moved: int = 0
    merged: int = 0
    doubled: list[str] = field(default_factory=list)
    changed: int = 0
    behind: int = 0
    joined: int = 0
    expired: int = 0
    cancelled: list[str] = field(default_factory=list)
    unsupported: dict[str, int] = field(default_factory=dict)
    running: int = 0
    pending: int = 0
    cut_short: bool = False
    uncounted: bool = False

    @property
    def ok(self) -> bool:
        if self.cut_short or self.uncounted or self.dry_run:
            return not (self.cut_short or self.uncounted)
        return not (self.running or self.pending)

    def lines(self) -> list[str]:
        if self.dry_run:
            return [
                f"{self.moved:,} job(s) would move from {self.old} to {self.new}",
                *self._left(),
                "stopped before the end: the count is partial, and no job moved"
                if self.cut_short else "dry run, no job moved",
            ]
        lines = []
        if self.reaped:
            lines.append(
                f"{self.reaped:,} job(s) whose worker had stopped reporting went back "
                f"to the queue first, or failed for good if they had no attempt left"
            )
        lines.append(f"moved {self.moved:,} job(s) from {self.old} to {self.new}")
        if self.merged:
            lines.append(
                f"{self.merged:,} of them met a job holding the same deduplication key "
                f"in {self.new}, which stands for both"
            )
        return [*lines, *self._left(), self._verdict()]

    def _left(self) -> list[str]:
        lines = []
        if self.doubled:
            lines.append(
                f"{len(self.doubled):,} job(s) were started by a worker of {self.old} "
                f"while they moved, and run in both stores: {_some(self.doubled)}; stop "
                f"its workers before moving"
            )
        if self.cancelled:
            lines.append(
                f"{len(self.cancelled):,} job(s) were cancelled in {self.old} while they "
                f"moved, and their copies are still to run in {self.new}: "
                f"{_some(self.cancelled)}"
            )
        if self.expired:
            lines.append(
                f"{self.expired:,} job(s) whose worker has stopped reporting are not "
                f"counted: a real run returns them to the queue first, with what waits "
                f"for them"
            )
        if self.changed:
            lines.append(
                f"{self.changed:,} job(s) left: they wait for a job a worker of "
                f"{self.old} took before they were read"
            )
        if self.behind:
            lines.append(
                f"{self.behind:,} job(s) left: they wait for a job that is not moving, "
                f"still running in {self.old} or sent there a moment ago"
            )
        if self.joined:
            lines.append(
                f"{self.joined:,} job(s) left: a job was sent to {self.old} behind them "
                f"while they moved; the next run takes them together"
            )
        for reason, count in sorted(self.unsupported.items()):
            lines.append(f"{count:,} job(s) left: they {reason}")
        if self.running > self.expired:
            lines.append(
                f"{self.running - self.expired:,} job(s) still running in {self.old}; "
                f"they finish there, or come back when their lease ends"
            )
        return lines

    def _verdict(self) -> str:
        if self.cut_short:
            return "stopped before the end; run it again to move the rest"
        if self.uncounted:
            return f"stopped while counting what is left in {self.old}; run it again to know"
        if self.ok:
            return f"nothing left to run in {self.old}"
        left = self.pending + self.running
        if left <= sum(self.unsupported.values()):
            return f"{left:,} job(s) left to run in {self.old}"
        return f"{left:,} job(s) left to run in {self.old}; run it again"


class Move:
    def __init__(
        self,
        old: Any,
        new: Any,
        *,
        old_name: str = "the old store",
        new_name: str = "the new store",
        dry_run: bool = False,
        stopped: Event | None = None,
        say: Callable[[str], None] = lambda line: None,
    ) -> None:
        self._old = old
        self._new = new
        self._dry_run = dry_run
        self._stopped = stopped or Event()
        self._say = say
        self._stands_for: dict[str, str] = {}
        self._doubled: set[str] = set()
        self._report = Report(old_name, new_name, dry_run=dry_run)
        self.through = False

    @property
    def report(self) -> Report:
        self._report.doubled = sorted(self._doubled)
        return self._report

    def run(self) -> Report:
        report = self._report
        for app, name in ((self._old, report.old), (self._new, report.new)):
            if in_memory(app.url):
                raise ConfigurationError(IN_MEMORY_MESSAGE.format(store=name))
        self._on(self._new, _nothing)
        if not sum(self._counts().values()):
            raise ConfigurationError(EMPTY_STORE_MESSAGE.format(old=report.old))
        if not self._dry_run:
            self._tell_apart()
            for app in (self._old, self._new):
                self._on(app, lambda: self._forget_probes(app))
            report.reaped = self._on(self._old, lambda: self._old.backend.reap_expired())
        plan = self._on(self._old, self._plan)
        report.found = len(plan)
        told = time.monotonic()
        for group in _groups(_families(plan)):
            if self._stopped.is_set():
                report.cut_short = True
                break
            self._move(group)
            if time.monotonic() - told >= PROGRESS_SECONDS:
                told = time.monotonic()
                done = "counted" if self._dry_run else "moved"
                self._say(f"{done} {report.moved:,} of {report.found:,}")
        else:
            self.through = True
        counts = self._counts()
        report.running = counts.get(JobStatus.RUNNING, 0)
        if report.running:
            running = self._on(self._old, lambda: self._ids(JobStatus.RUNNING))
            self._doubled.update(self._copies(running))
        if self._dry_run and report.running:
            report.expired = self._on(self._old, self._expired)
        report.pending = sum(counts.get(status, 0) for status in MOVABLE)
        return self.report

    def _on(self, app: Any, call: Callable[[], Any]) -> Any:
        async def connected() -> Any:
            await app.ensure_connected()
            return await call()

        return app.run_sync(connected, name="move")

    def _counts(self) -> dict[str, int]:
        return self._on(self._old, lambda: self._old.backend.counts_by_status())

    async def _plan(self) -> dict[str, str | None]:
        plan: dict[str, str | None] = {}
        def note(job: Job) -> None:
            if job.task != PROBE_TASK:
                plan[job.id] = job.after

        for status in MOVABLE:
            await self._each(status, note)
        return plan

    async def _expired(self) -> int:
        now = self._old.clock.now()
        late: list[str] = []

        def note(job: Job) -> None:
            if job.lease_expires_at is not None and job.lease_expires_at < now:
                late.append(job.id)

        await self._each(JobStatus.RUNNING, note)
        return len(late)

    async def _ids(self, status: JobStatus) -> list[str]:
        found: list[str] = []
        await self._each(status, lambda job: found.append(job.id))
        return found

    async def _each(self, status: JobStatus, note: Callable[[Job], Any]) -> None:
        cursor = None
        while True:
            page = await self._old.backend.list(status=status, limit=PAGE, cursor=cursor)
            for job in page.items:
                note(job)
            cursor = page.cursor
            if not cursor or not page.items:
                return

    async def _forget_probes(self, app: Any) -> None:
        stale = time.time() - PROBE_STALE_SECONDS
        cursor = None
        while True:
            page = await app.backend.list(queue=PROBE_QUEUE, limit=PAGE, cursor=cursor)
            for job in page.items:
                if job.task == PROBE_TASK and job.created_at < stale:
                    await app.backend.delete(job.id)
            cursor = page.cursor
            if not cursor or not page.items:
                return

    def _tell_apart(self) -> None:
        now = time.time()
        payload, codec = self._new.serialization.encode_payload(JobPayload())
        probe = Job(
            id=new_job_id(),
            task=PROBE_TASK,
            payload=payload,
            codec=codec,
            queue=PROBE_QUEUE,
            status=JobStatus.QUEUED,
            run_at=now + PROBE_DELAY,
            created_at=now,
        )
        self._on(self._new, lambda: self._new.backend.enqueue(probe))
        try:
            same = self._on(self._old, lambda: self._old.backend.get(probe.id)) is not None
        finally:
            self._on(self._new, lambda: self._new.backend.delete(probe.id))
        if same:
            report = self._report
            raise ConfigurationError(SAME_STORE_MESSAGE.format(old=report.old, new=report.new))

    def _move(self, families: Sequence[Sequence[str]]) -> None:
        report = self._report
        ready, changed = self._on(self._old, lambda: self._read(families))
        if changed:
            copied = self._copies(list(changed))
            report.cancelled.extend(
                job_id for job_id in copied if changed[job_id] is JobStatus.CANCELLED
            )
            self._doubled.update(set(copied).difference(report.cancelled))
        if self._dry_run:
            report.moved += sum(map(len, ready))
        elif ready:
            self._on(self._new, lambda: self._copy(ready))
            self._on(self._old, lambda: self._take(ready))

    def _copies(self, job_ids: Sequence[str]) -> list[str]:
        async def copies() -> list[str]:
            backend = self._new.backend
            return [job_id for job_id in job_ids if await backend.get(job_id) is not None]

        return self._on(self._new, copies)

    async def _read(
        self, families: Sequence[Sequence[str]]
    ) -> tuple[list[list[Job]], dict[str, JobStatus]]:
        report = self._report
        backend = self._old.backend
        ready: list[list[Job]] = []
        changed: dict[str, JobStatus] = {}
        for family in families:
            jobs = [await backend.get(job_id) for job_id in family]
            if any(job is None or job.status not in MOVABLE for job in jobs):
                taken = {job.id: job.status for job in jobs if job and job.status not in MOVABLE}
                report.changed += len(taken) and sum(
                    job is not None and job.status in MOVABLE for job in jobs
                )
                changed.update(taken)
                continue
            if jobs[0].status is JobStatus.WAITING:
                report.behind += len(family)
                continue
            freed = [replace(jobs[0], after=None), *jobs[1:]]
            reason = self._refused(freed)
            if reason:
                report.unsupported[reason] = report.unsupported.get(reason, 0) + len(family)
                continue
            ready.append(freed)
        return ready, changed

    def _refused(self, jobs: Sequence[Job]) -> str | None:
        capabilities = self._new.backend.capabilities
        limit = capabilities.max_payload_bytes
        now = self._new.clock.now()
        for job in jobs:
            if job.after and not capabilities.after:
                return NEEDS_AFTER
            if job.dedup_key and not capabilities.dedup:
                return NEEDS_DEDUP
            if job.run_at > now and not capabilities.delayed:
                return NEEDS_DELAYED
            if limit is not None and len(job.payload) > limit:
                return TOO_LARGE
        return None

    async def _copy(self, families: Sequence[Sequence[Job]]) -> None:
        backend = self._new.backend
        for family in families:
            for job in family:
                stored = await backend.enqueue(_fresh(job, self._stands_for))
                if stored.id != job.id:
                    self._stands_for[job.id] = stored.id

    async def _take(self, families: Sequence[Sequence[Job]]) -> None:
        report = self._report
        backend = self._old.backend
        for family in families:
            for job in reversed(family):
                if await backend.delete(job.id):
                    report.moved += 1
                    report.merged += job.id in self._stands_for
                    continue
                left = await backend.get(job.id)
                if left is None:
                    continue
                if left.status in MOVABLE:
                    report.joined += 1
                elif left.status is JobStatus.CANCELLED:
                    report.cancelled.append(job.id)
                else:
                    self._doubled.add(job.id)


def _some(job_ids: Sequence[str]) -> str:
    shown = ", ".join(job_ids[:NAMED])
    return shown if len(job_ids) <= NAMED else f"{shown} and {len(job_ids) - NAMED:,} more"


async def _nothing() -> None:
    return None


def _fresh(job: Job, stands_for: dict[str, str]) -> Job:
    return replace(
        job,
        status=JobStatus.QUEUED,
        after=stands_for.get(job.after, job.after) if job.after else None,
        finished_at=None,
        lease_expires_at=None,
        worker_id=None,
        result_payload=None,
        _app=None,
    )


def _families(plan: dict[str, str | None]) -> list[list[str]]:
    children: dict[str, list[str]] = {}
    roots = []
    for job_id in sorted(plan):
        parent = plan[job_id]
        if parent in plan:
            children.setdefault(parent, []).append(job_id)
        else:
            roots.append(job_id)
    families = []
    for root in roots:
        family, waiting = [], [root]
        while waiting:
            job_id = waiting.pop()
            family.append(job_id)
            waiting.extend(reversed(children.get(job_id, ())))
        families.append(family)
    return families


def _groups(families: Sequence[Sequence[str]]) -> list[list[Sequence[str]]]:
    groups: list[list[Sequence[str]]] = []
    group: list[Sequence[str]] = []
    size = 0
    for family in families:
        group.append(family)
        size += len(family)
        if size >= GROUP:
            groups.append(group)
            group, size = [], 0
    if group:
        groups.append(group)
    return groups
