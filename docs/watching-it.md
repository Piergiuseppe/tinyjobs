# Watching it

```python
@app.on_job_failure
def alert(job, error, will_retry):
    if not will_retry:
        sentry_sdk.capture_exception(error)


@app.on_job_success
def timing(job, result, duration):
    statsd.timing(job.task, duration)
```

`on_job_start(job)` completes the set. Hooks run in the worker and can be `def` or `async def`. One
that raises is logged and ignored, so a broken metrics client never fails a job. A `def` hook runs on
the worker's event loop, so keep it quick.

```bash
tinyjobs stats --app tasks:app
tinyjobs check --app tasks:app --max-wait 5m      # exits 1 on any problem
```

```
QUEUE    QUEUED  HELD  WAITING  RUNNING  SUCCESS  FAILED  CANCELLED  OLDEST DUE
-------  ------  ----  -------  -------  -------  ------  ---------  ----------
default  2       0     0        2        0        0       0          16s
mail     1       0     0        0        0        0       0          16s

running past their lease: 2
```

A job past its lease means the worker holding it stopped reporting, usually because it died. `check`
fails on that, on an app that registers no tasks, on queued or held jobs for a task the app doesn't register
(a missing `--import`), on a due job, queued or [held](limits.md), left longer than `--max-wait`, and on a cron schedule more
than a minute overdue, which means no worker or scheduler is running. A job
[waiting on another](jobs.md#one-job-after-another) is not due, so it never counts as a backlog. Both are `app.stats()` and
`app.check()` in code.

Every log record about a job carries the job as fields, so a log pipeline can filter on them without
parsing the message. `--log-format json` writes one object per line, on every command (wrapped
here); a `tinyjobs` logger your settings configured, as under `manage.py tinyjobs`, or a root
logger that already has handlers in a standalone run, is left alone, with a warning when the flag
was given:

```bash
tinyjobs worker --app tasks:app --log-format json
```

```json
{"time": "2026-11-05T09:12:44.118Z", "level": "INFO", "logger": "tinyjobs.worker",
 "message": "job 01a1ab2e-... succeeded on attempt 1", "job_id": "01a1ab2e-...",
 "task": "tasks.generate_pdf", "queue": "default", "attempt": 1,
 "worker_id": "laptop/8821/9389541d", "duration_ms": 12.4}
```

A failure adds `error`, `error_message` and `will_retry`, with the traceback in `traceback` on
the last attempt, which the text format prints under the line; a record about a schedule carries
`schedule`. The fields are `extra` on the standard `logging` record, so your own handlers see
them too, and the last attempt also carries the exception as `exc_info`, for Sentry and plain
formatters. `tinyjobs.logs.JSONFormatter` and `TextFormatter` are what the flag installs, for a
logging config of your own.

Prometheus can scrape the worker:

```bash
tinyjobs worker --app tasks:app --metrics-port 9464        # --metrics-host 0.0.0.0 to expose it
curl -s localhost:9464/metrics
```

```
tinyjobs_jobs_total{task="tasks.generate_pdf",outcome="retry"} 3
tinyjobs_jobs_total{task="tasks.generate_pdf",outcome="success"} 1842
tinyjobs_job_duration_seconds_bucket{task="tasks.generate_pdf",le="0.5"} 1790
tinyjobs_jobs_running{task="tasks.generate_pdf"} 2
tinyjobs_store_jobs{queue="default",status="queued"} 14
tinyjobs_store_queued_jobs{task="tasks.generate_pdf"} 14
tinyjobs_store_oldest_due_seconds{queue="default"} 0.8
tinyjobs_store_expired_leases 0
tinyjobs_info{version="0.18.0"} 1
```

The counters, the duration histogram and the running gauge are this process's, fed by the job
events behind the hooks above. The `tinyjobs_store_*` gauges are the store's, read with one `stats`
query per scrape that gets five seconds to answer, so a slow store costs the scrape its gauges and
not the target; they are the same from every worker, so scrape them from one, or take `max()`
across instances rather than `sum()`. An embedded worker has no port of its own: return
`app.metrics.render(await app.astats())` from a route, with `tinyjobs.metrics.CONTENT_TYPE` as
the content type, and give the stats query the budget and the fallback to `app.metrics.render()`
that your route gives any store query.
