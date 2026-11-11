from __future__ import annotations

import json
import logging
from functools import partial
from importlib import resources
from typing import Any, Callable, Coroutine, TypeVar
from urllib.parse import quote

from ..app import TinyJobs
from ..deadlines import STATS_BUDGET, within
from ..exceptions import AppClosedError, UnknownTaskError, error_message
from ..models import Execution, Job, ScheduleState
from ..status import JobStatus
from ..web import Request, Response
from . import pages

logger = logging.getLogger("tinyjobs.dashboard")

T = TypeVar("T")

PAGE_SIZE = 50
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8070
TIMEOUT_MESSAGE = "the store did not answer within {budget:g}s"
HTML = "text/html; charset=utf-8"
JSON = "application/json"
STATIC = {"style.css": "text/css; charset=utf-8", "app.js": "text/javascript; charset=utf-8"}
STATIC_CACHE = "max-age=86400"
JOB_ACTIONS = ("retry", "cancel")
SCHEDULE_ACTIONS = {
    "pause": "pause_schedule",
    "resume": "resume_schedule",
    "run": "run_schedule",
}
RETRYABLE = frozenset({JobStatus.FAILED, JobStatus.CANCELLED})
CANCELLABLE = frozenset(
    {JobStatus.QUEUED, JobStatus.HELD, JobStatus.WAITING, JobStatus.RUNNING}
)


class Dashboard:
    def __init__(
        self, app: TinyJobs, *, page_size: int = PAGE_SIZE, budget: float = STATS_BUDGET
    ) -> None:
        self._app = app
        self._page_size = page_size
        self._budget = budget

    def respond(self, request: Request) -> Response:
        refusal = refused(request)
        if refusal is not None:
            return refusal
        try:
            return self._app.run_sync(partial(self.handle, request), name=call_name(request))
        except Exception as error:
            return unavailable(request, error)

    async def handle(self, request: Request) -> Response:
        refusal = refused(request)
        if refusal is not None:
            return refusal
        try:
            return await self._route(request)
        except Exception as error:
            return unavailable(request, error)

    async def _route(self, request: Request) -> Response:
        parts = segments(request.path)
        match parts:
            case []:
                return page(request, pages.overview, await self._read(self._overview))
            case ["jobs"]:
                data = await self._read(partial(self._jobs, request))
                return page(request, pages.jobs, data) if data is not None else bad_status()
            case ["jobs", job_id]:
                data = await self._read(partial(self._job, job_id))
                return page(request, pages.job, data) if data is not None else no_job()
            case ["jobs", job_id, action]:
                return await self._job_action(request, job_id, action)
            case ["schedules"]:
                return page(request, pages.schedules, await self._read(self._schedules))
            case ["schedules", name, action]:
                return await self._schedule_action(request, name, action)
            case ["api", "overview"]:
                return as_json(await self._read(self._overview))
            case ["api", "jobs"]:
                data = await self._read(partial(self._jobs, request))
                return as_json(data) if data is not None else bad_status()
            case ["api", "jobs", job_id]:
                data = await self._read(partial(self._job, job_id))
                return as_json(data) if data is not None else no_job()
            case ["api", "schedules"]:
                return as_json(await self._read(self._schedules))
            case ["static", name]:
                return static(name)
        return Response(404, b"not found\n")

    async def _read(self, start: Callable[[], Coroutine[Any, Any, T]]) -> T:
        return await within(self._budget, start, TIMEOUT_MESSAGE.format(budget=self._budget))

    async def _overview(self) -> dict[str, Any]:
        app = self._app
        stats = await app.astats()
        findings = await app.health.check(stats=stats)
        return {
            "now": app.clock.now(),
            "queues": [
                {
                    "queue": queue.queue,
                    "counts": {status.value: queue.counts.get(status.value, 0) for status in JobStatus},
                    "oldest_due_at": queue.oldest_due_at,
                }
                for queue in stats.queues
            ],
            "expired_leases": stats.expired_leases,
            "queued_tasks": dict(sorted(stats.queued_tasks.items())),
            "findings": [{"ok": finding.ok, "message": finding.message} for finding in findings],
        }

    async def _jobs(self, request: Request) -> dict[str, Any] | None:
        wanted = request.query.get("status") or None
        try:
            status = JobStatus(wanted) if wanted else None
        except ValueError:
            return None
        queue = request.query.get("queue") or None
        listing = await self._app.ajobs(
            status=status,
            queue=queue,
            limit=self._page_size,
            cursor=request.query.get("cursor") or None,
        )
        return {
            "now": self._app.clock.now(),
            "status": status.value if status else None,
            "queue": queue,
            "items": [describe_job(job) for job in listing.items],
            "cursor": listing.cursor,
        }

    async def _job(self, job_id: str) -> dict[str, Any] | None:
        app = self._app
        job = await app.aget(job_id)
        if job is None:
            return None
        data = describe_job(job)
        data["now"] = app.clock.now()
        data.update(describe_payload(app, job))
        data["executions"] = [describe_execution(execution) for execution in await app.aexecutions(job_id)]
        data["actions"] = {"retry": job.status in RETRYABLE, "cancel": job.status in CANCELLABLE}
        return data

    async def _job_action(self, request: Request, job_id: str, action: str) -> Response:
        app = self._app
        if await app.aget(job_id) is None:
            return no_job()
        done = await (app.aretry(job_id) if action == "retry" else app.acancel(job_id))
        notice = {"retry": "retried", "cancel": "cancelled"}[action] if done else f"{action}-refused"
        return redirect(f"{quote(request.root, safe='/')}/jobs/{quote(job_id, safe='')}?notice={notice}")

    async def _schedules(self) -> dict[str, Any]:
        app = self._app
        declared = {task.name: task for task in app.registry if task.schedule}
        stored = {state.name: state for state in await app.aschedules()}
        items = []
        for name in sorted(set(declared) | set(stored)):
            state = stored.get(name)
            if state is None:
                schedule = declared[name].schedule
                items.append(
                    {
                        "name": name,
                        "cron": schedule.cron.source,
                        "timezone": schedule.timezone,
                        "declared": True,
                        "stored": False,
                        "paused": False,
                        "next_run_at": None,
                        "last_run_at": None,
                        "last_job_id": None,
                    }
                )
            else:
                items.append(describe_schedule(state, declared=name in declared))
        return {"now": app.clock.now(), "items": items}

    async def _schedule_action(self, request: Request, name: str, action: str) -> Response:
        app = self._app
        try:
            if action == "run":
                job = await app.arun_schedule(name)
                return redirect(f"{quote(request.root, safe='/')}/schedules?notice=ran&job={quote(job.id, safe='')}")
            done = await (app.apause_schedule(name) if action == "pause" else app.aresume_schedule(name))
        except UnknownTaskError as error:
            return Response(404, f"{error_message(error)}\n".encode("utf-8"))
        notice = {"pause": "paused", "resume": "resumed"}[action] if done else f"{action}-refused"
        return redirect(f"{quote(request.root, safe='/')}/schedules?notice={notice}")


def segments(path: str) -> list[str]:
    return [part for part in path.split("/") if part]


def refused(request: Request) -> Response | None:
    expected = allowed_method(segments(request.path))
    if expected is None:
        return Response(404, b"not found\n")
    return None if request.method in expected.split(", ") else not_allowed(expected)


def allowed_method(parts: list[str]) -> str | None:
    match parts:
        case [] | ["jobs"] | ["jobs", _] | ["schedules"] | ["api", "overview"] | ["api", "jobs"]:
            return "GET, HEAD"
        case ["api", "jobs", _] | ["api", "schedules"]:
            return "GET, HEAD"
        case ["static", name] if name in STATIC:
            return "GET, HEAD"
        case ["jobs", _, action] if action in JOB_ACTIONS:
            return "POST"
        case ["schedules", _, action] if action in SCHEDULE_ACTIONS:
            return "POST"
    return None


def call_name(request: Request) -> str:
    match segments(request.path):
        case ["jobs", _, action] if action in JOB_ACTIONS:
            return action
        case ["schedules", _, action] if action in SCHEDULE_ACTIONS:
            return SCHEDULE_ACTIONS[action]
    return "dashboard"


def describe_job(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "task": job.task,
        "status": job.status.value,
        "queue": job.queue,
        "priority": job.priority,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "run_at": job.run_at,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "lease_expires_at": job.lease_expires_at,
        "worker_id": job.worker_id,
        "dedup_key": job.dedup_key,
        "after": job.after,
        "last_error": job.last_error,
        "has_result": job.result_payload is not None,
    }


def describe_payload(app: TinyJobs, job: Job) -> dict[str, Any]:
    described: dict[str, Any] = {
        "args": None,
        "kwargs": None,
        "payload_error": None,
        "result": None,
        "result_error": None,
    }
    try:
        payload = app.decode(job)
        described["args"] = repr(payload.args)
        described["kwargs"] = repr(payload.kwargs)
    except Exception as error:
        described["payload_error"] = error_message(error)
    if job.result_payload is not None:
        try:
            described["result"] = repr(job.result())
        except Exception as error:
            described["result_error"] = error_message(error)
    return described


def describe_execution(execution: Execution) -> dict[str, Any]:
    return {
        "attempt": execution.attempt,
        "status": execution.status.value,
        "worker_id": execution.worker_id,
        "started_at": execution.started_at,
        "finished_at": execution.finished_at,
        "duration": execution.duration,
        "error_type": execution.error_type,
        "traceback": execution.traceback,
    }


def describe_schedule(state: ScheduleState, *, declared: bool) -> dict[str, Any]:
    return {
        "name": state.name,
        "cron": state.cron,
        "timezone": state.timezone,
        "declared": declared,
        "stored": True,
        "paused": state.paused,
        "next_run_at": None if state.paused else state.next_run_at,
        "last_run_at": state.last_run_at,
        "last_job_id": state.last_job_id,
    }


def page(request: Request, render: Any, data: dict[str, Any]) -> Response:
    notice = pages.notice_line(request.query.get("notice"), request.query.get("job"), request.root)
    return Response(200, render(data, request.root, notice).encode("utf-8"), HTML)


def as_json(data: dict[str, Any]) -> Response:
    return Response(200, (json.dumps(data, default=str) + "\n").encode("utf-8"), JSON)


def static(name: str) -> Response:
    body = resources.files(__package__).joinpath("static", name).read_bytes()
    return Response(200, body, STATIC[name], (("Cache-Control", STATIC_CACHE),))


def unavailable(request: Request, error: Exception) -> Response:
    level = logging.DEBUG if isinstance(error, AppClosedError) else logging.WARNING
    logger.log(
        level,
        "the dashboard could not answer %s %s: %s",
        request.method,
        request.path,
        error_message(error),
    )
    logger.debug("the failed request in full", exc_info=error)
    return Response(503, f"{error_message(error)}\n".encode("utf-8"))


def redirect(location: str) -> Response:
    return Response(303, b"", headers=(("Location", location),))


def not_allowed(allowed: str) -> Response:
    return Response(405, b"method not allowed\n", headers=(("Allow", allowed),))


def no_job() -> Response:
    return Response(404, b"no such job\n")


def bad_status() -> Response:
    statuses = ", ".join(status.value for status in JobStatus)
    return Response(400, f"no such status; one of {statuses}\n".encode("utf-8"))
