from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Sequence
from urllib.parse import quote, urlencode

from .. import __version__
from ..duration import format_age, format_seconds
from ..status import JobStatus

STATUSES = tuple(status.value for status in JobStatus)
NOTICES = {
    "retried": "the job is queued again, or waits for the job it runs after",
    "retry-refused": "the job was not retried: it is not failed or cancelled, the job it runs after did not succeed, or another job that has not finished holds its dedup key",
    "cancelled": "the job is cancelled, or will be when it next checks",
    "cancel-refused": "the job was not cancelled: it had already finished",
    "paused": "the schedule is paused",
    "pause-refused": "the schedule was already paused",
    "resumed": "the schedule fires again from its next time",
    "resume-refused": "the schedule was not paused",
    "ran": "a run is queued as job {job}",
}
NAVIGATION = (("", "Overview"), ("jobs", "Jobs"), ("schedules", "Schedules"))


def overview(data: dict[str, Any], root: str, notice: str) -> str:
    findings = "".join(
        f'<li class="{"ok" if finding["ok"] else "problem"}">{escape(finding["message"])}</li>'
        for finding in data["findings"]
    )
    rows = []
    for queue in data["queues"]:
        cells = [link(f"{root}/jobs?{urlencode({'queue': queue['queue']})}", queue["queue"])]
        for status in STATUSES:
            count = queue["counts"].get(status, 0)
            href = f"{root}/jobs?{urlencode({'queue': queue['queue'], 'status': status})}"
            cells.append(link(href, str(count)) if count else "0")
        oldest = queue["oldest_due_at"]
        cells.append("-" if oldest is None else format_age(data["now"] - oldest))
        rows.append(cells)
    queues = (
        table(("Queue", *STATUSES, "Oldest due"), rows, numbers=range(1, len(STATUSES) + 1))
        if rows
        else '<p class="empty">no jobs in the store yet</p>'
    )
    tasks = [
        [escape(task), str(count)] for task, count in data["queued_tasks"].items()
    ]
    queued_tasks = (
        table(("Task", "Queued"), tasks, numbers=(1,))
        if tasks
        else '<p class="empty">nothing queued</p>'
    )
    body = f"""<h1>Overview</h1>
{notice}
<h2>Health</h2>
<ul class="findings">{findings}</ul>
<h2>Queues</h2>
{queues}
<p>running past their lease: <strong>{data["expired_leases"]}</strong></p>
<h2>Queued by task</h2>
{queued_tasks}"""
    return layout("Overview", "", body, root)


def jobs(data: dict[str, Any], root: str, notice: str) -> str:
    options = "".join(
        f'<option value="{status}"{" selected" if status == data["status"] else ""}>{status}</option>'
        for status in STATUSES
    )
    filters = f"""<form class="filters" method="get" action="{escape(root, quote=True)}/jobs" autocomplete="off">
<select name="status" aria-label="status"><option value=""{" selected" if not data["status"] else ""}>any status</option>{options}</select>
<input name="queue" aria-label="queue" placeholder="queue" value="{escape(data["queue"] or "")}">
<button type="submit">Filter</button>
</form>"""
    rows = [
        [
            link(f"{root}/jobs/{segment(job['id'])}", job["id"]),
            escape(job["task"]),
            badge(job["status"]),
            escape(job["queue"]),
            f"{job['attempts']}/{job['max_attempts']}",
            age(job["created_at"], data["now"]),
            age(job["run_at"], data["now"]),
            age(job["finished_at"], data["now"]),
            escape(job["worker_id"] or "-"),
        ]
        for job in data["items"]
    ]
    listing = (
        table(
            ("Id", "Task", "Status", "Queue", "Attempts", "Created", "Run at", "Finished", "Worker"),
            rows,
            numbers=(4,),
        )
        if rows
        else '<p class="empty">no jobs match</p>'
    )
    pager = ""
    if data["cursor"]:
        query = {
            key: value
            for key, value in (("status", data["status"]), ("queue", data["queue"]))
            if value
        }
        query["cursor"] = data["cursor"]
        pager = f'<p class="pager">{link(f"{root}/jobs?{urlencode(query)}", "Next page")}</p>'
    body = f"<h1>Jobs</h1>\n{notice}\n{filters}\n{listing}\n{pager}"
    return layout("Jobs", "jobs", body, root)


def job(data: dict[str, Any], root: str, notice: str) -> str:
    now = data["now"]
    fields = (
        ("Task", escape(data["task"])),
        ("Status", badge(data["status"])),
        ("Queue", f"{escape(data['queue'])}, priority {data['priority']}"),
        ("Attempts", f"{data['attempts']} of {data['max_attempts']}"),
        ("Created", when(data["created_at"], now)),
        ("Run at", when(data["run_at"], now)),
        ("Finished", when(data["finished_at"], now)),
        ("Lease", when(data["lease_expires_at"], now)),
        ("Worker", escape(data["worker_id"] or "-")),
        ("Dedup key", escape(data["dedup_key"] or "-")),
        (
            "Runs after",
            link(f"{root}/jobs/{segment(data['after'])}", data["after"]) if data["after"] else "-",
        ),
    )
    details = "".join(f"<dt>{name}</dt><dd>{value}</dd>" for name, value in fields)
    actions = "".join(
        action_form(f"{root}/jobs/{segment(data['id'])}/{action}", label)
        for action, label in (("retry", "Retry"), ("cancel", "Cancel"))
        if data["actions"][action]
    )
    if data["payload_error"] is None:
        payload = (
            f"<dl><dt>args</dt><dd><pre>{escape(data['args'])}</pre></dd>"
            f"<dt>kwargs</dt><dd><pre>{escape(data['kwargs'])}</pre></dd></dl>"
        )
    else:
        payload = f'<p class="problem">the arguments could not be decoded: {escape(data["payload_error"])}</p>'
    result = ""
    if data["result"] is not None:
        result = f"<h2>Result</h2><pre>{escape(data['result'])}</pre>"
    elif data["result_error"] is not None:
        result = (
            f'<h2>Result</h2><p class="problem">the result could not be decoded: '
            f"{escape(data['result_error'])}</p>"
        )
    error = ""
    if data["last_error"]:
        error = f"<h2>Last error</h2><pre>{escape(data['last_error'])}</pre>"
    rows = []
    tracebacks = []
    for index, execution in enumerate(data["executions"], 1):
        error_type = escape(execution["error_type"] or "")
        if execution["traceback"]:
            error_type = f'<a href="#run-{index}">{error_type}</a>'
            tracebacks.append(
                f'<details id="run-{index}"><summary>run {index}, attempt {execution["attempt"]}, '
                f'{escape(execution["error_type"] or "error")}</summary>'
                f"<pre>{escape(execution['traceback'])}</pre></details>"
            )
        rows.append(
            [
                str(index),
                str(execution["attempt"]),
                badge(execution["status"]),
                when(execution["started_at"], now),
                "-" if execution["duration"] is None else format_seconds(execution["duration"]),
                escape(execution["worker_id"]),
                error_type,
            ]
        )
    executions = (
        table(("Run", "Attempt", "Status", "Started", "Took", "Worker", "Error"), rows, numbers=(0, 1))
        + "".join(tracebacks)
        if rows
        else '<p class="empty">not run yet</p>'
    )
    body = f"""<h1>Job {escape(data["id"])}</h1>
{notice}
<div class="actions">{actions}</div>
<dl>{details}</dl>
<h2>Arguments</h2>
{payload}
{result}
{error}
<h2>Attempts</h2>
{executions}"""
    return layout(f"Job {data['id']}", "jobs", body, root)


def schedules(data: dict[str, Any], root: str, notice: str) -> str:
    now = data["now"]
    rows = []
    for schedule in data["items"]:
        if schedule["stored"]:
            state = badge("paused" if schedule["paused"] else "active")
            next_run = "-" if schedule["paused"] else when(schedule["next_run_at"], now)
        else:
            state = "not registered yet"
            next_run = "-"
        allowed = [] if "/" in schedule["name"] else ["resume" if schedule["paused"] else "pause"]
        if allowed and schedule["declared"]:
            allowed.append("run")
        actions = "".join(
            action_form(
                f"{root}/schedules/{segment(schedule['name'])}/{action}", action.capitalize()
            )
            for action in allowed
        )
        name = escape(schedule["name"]) + ("" if schedule["declared"] else " (not in this app)")
        last_job = (
            link(f"{root}/jobs/{segment(schedule['last_job_id'])}", schedule["last_job_id"])
            if schedule["last_job_id"]
            else "-"
        )
        rows.append(
            [
                name,
                f"<code>{escape(schedule['cron'])}</code>",
                escape(schedule["timezone"]),
                next_run,
                age(schedule["last_run_at"], now),
                last_job,
                state,
                actions,
            ]
        )
    listing = (
        table(("Name", "Cron", "Timezone", "Next run", "Last run", "Last job", "State", "Actions"), rows, quiet=(7,))
        if rows
        else '<p class="empty">no cron tasks in this app; declare one with @app.task(cron="*/5 * * * *")</p>'
    )
    body = f"<h1>Schedules</h1>\n{notice}\n{listing}"
    return layout("Schedules", "schedules", body, root)


def notice_line(key: str | None, job_id: str | None, root: str) -> str:
    if key not in NOTICES:
        return ""
    text = escape(NOTICES[key])
    if key == "ran":
        if not job_id:
            return ""
        text = text.format(job=link(f"{root}/jobs/{segment(job_id)}", job_id))
    return f'<p class="notice">{text}</p>'


def layout(title: str, current: str, body: str, root: str) -> str:
    links = "".join(
        f'<a href="{escape(root, quote=True)}/{path}"{" class=\"current\" aria-current=\"page\"" if path == current else ""}>{label}</a>'
        for path, label in NAVIGATION
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} - tinyjobs</title>
<link rel="stylesheet" href="{escape(root, quote=True)}/static/style.css?v={__version__}">
</head>
<body>
<header><a class="brand" href="{escape(root, quote=True)}/">tinyjobs</a><nav>{links}</nav></header>
<main>
{body}
</main>
<footer>tinyjobs {__version__}</footer>
<script src="{escape(root, quote=True)}/static/app.js?v={__version__}"></script>
</body>
</html>
"""


def table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    *,
    numbers: Sequence[int] = (),
    quiet: Sequence[int] = (),
) -> str:
    head = "".join(
        f'<th><span class="hidden">{escape(header)}</span></th>'
        if index in quiet
        else f"<th>{escape(header)}</th>"
        for index, header in enumerate(headers)
    )
    body = []
    for row in rows:
        cells = []
        for index, cell in enumerate(row):
            attribute = ' class="number"' if index in numbers else ""
            cells.append(f"<td{attribute}>{cell}</td>")
        body.append(f"<tr>{''.join(cells)}</tr>")
    return (
        f'<div class="tablewrap"><table><thead><tr>{head}</tr></thead>'
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


def link(href: str, text: str) -> str:
    return f'<a href="{escape(href, quote=True)}">{escape(text)}</a>'


def action_form(action: str, label: str) -> str:
    return (
        f'<form class="actions" method="post" action="{escape(action, quote=True)}">'
        f'<button type="submit">{label}</button></form> '
    )


def segment(value: str) -> str:
    return quote(value, safe="")


def badge(status: str) -> str:
    return f'<span class="status status-{escape(status)}">{escape(status)}</span>'


def when(moment: float | None, now: float) -> str:
    if moment is None:
        return "-"
    stamp = clock(moment)
    return f'<time datetime="{stamp.replace(" ", "T")}Z">{stamp} UTC</time><span class="age">{distance(moment, now)}</span>'


def age(moment: float | None, now: float) -> str:
    if moment is None:
        return "-"
    stamp = clock(moment)
    return f'<time datetime="{stamp.replace(" ", "T")}Z" title="{stamp} UTC">{distance(moment, now)}</time>'


def clock(moment: float) -> str:
    return datetime.fromtimestamp(moment, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def distance(moment: float, now: float) -> str:
    seconds = now - moment
    return f"{format_age(seconds)} ago" if seconds >= 0 else f"in {format_age(-seconds)}"
