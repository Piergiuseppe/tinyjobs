# Django

Django 6 ships `django.tasks`, a standard API for background tasks, without a worker to run them.
tinyjobs is that worker. Your tasks stay plain Django code:

```python
# settings.py
INSTALLED_APPS = [..., "tinyjobs.contrib.django"]

TASKS = {
    "default": {
        "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
        "OPTIONS": {"url": "sqlite:///jobs.db"},
    }
}
```

```python
# shop/tasks.py
from django.tasks import task


@task
def send_receipt(order_id: int) -> None:
    ...


send_receipt.enqueue(42)
```

```bash
python manage.py tinyjobs worker
```

`manage.py tinyjobs` takes every `tinyjobs` command, such as `worker`, `jobs`, `stats` and `check`,
against the configured backend, with `--backend` to pick a `TASKS` alias other than `default`.
`OPTIONS` takes the arguments of `TinyJobs(...)`: a `postgresql://` or `redis://` URL, `"eager": True`
for tests.
Queues, priorities, `run_after`, async tasks, `takes_context` and `get_result()` all work, and the
`task_enqueued`, `task_started` and `task_finished` signals fire. A result shows its `run_after`
until the job is first picked up.

Tasks are found by their import path, and only inside your `INSTALLED_APPS`: a job can never make
the worker import code your project does not already trust, and enqueueing a task defined elsewhere
raises `InvalidTask`.

## Timeouts, retries, limits and isolation

`django.tasks` knows a priority and a queue. The rest of what tinyjobs does for a task goes on the
same decorator, since Django, from 6.1, hands the keywords it does not know to the backend:

```python
from django.tasks import task  # on Django 6.0: from tinyjobs.contrib.django import task


@task(timeout=30, max_attempts=3)
def send_receipt(order_id: int) -> None: ...


@task(rate="10/m", concurrency=2)
def call_partner_api(order_id: int) -> None: ...


@task(isolated=True, timeout=120)
def render(document_id: int) -> None: ...
```

| option | what it does |
| --- | --- |
| `timeout` | seconds one attempt may take |
| `max_attempts`, `retry_on`, `retry_backoff`, `retry_base`, `retry_max`, `retry_jitter` | [retries](jobs.md), off unless `max_attempts` is above 1 |
| `concurrency`, `rate` | [limits](limits.md) counted across every worker |
| `isolated` | a [job process](isolated-tasks.md) of its own, killed at the timeout |
| `result_ttl` | seconds the result is kept after the job finishes |

`after` sends a task to run once another has succeeded, as
[it does on a tinyjobs task](jobs.md#one-job-after-another):

```python
report = build_report.enqueue(42)
email_report.after(report).enqueue(42)
```

It takes the `TaskResult` or its id and chains with `using`. The result reads `READY` while it
waits, and `FAILED` with a `JobCancelled` error if the task before it did not succeed. A result
that is not in the queue raises `tinyjobs.JobNotFoundError`, and a task sent this way shows its
`run_after` only while it waits.

The options mean what they mean on `@app.task`, and a value that cannot be, such as a malformed rate, is
an `InvalidTask` where the task is declared. `retry_on` also takes dotted paths, which the
settings need; they are imported when the task is first looked up.

The same options can live in the settings, by import path, where they win over the decorator
option by option:

```python
TASKS = {
    "default": {
        "BACKEND": "tinyjobs.contrib.django.TinyJobsBackend",
        "OPTIONS": {
            "url": "sqlite:///jobs.db",
            "tasks": {
                "newsletter.tasks.send_issue": {"rate": "30/m"},
                "shop.tasks.send_receipt": {"max_attempts": 5, "retry_on": ["smtplib.SMTPException"]},
            },
        },
    }
}
```

That is the way to limit a task of an app you did not write, and the way to keep your own
decorators plain when some environment runs another backend, such as Django's
`ImmediateBackend`, which would refuse a keyword it does not know. A path that names no task of
your `INSTALLED_APPS` stops `manage.py tinyjobs` with the path in the message.

- **Retries.** A failed attempt that will run again reads as `READY`, with the attempt's error in
  `errors`, and `task_finished` fires once, after the attempt that ends the task, a timeout
  included.
  `task_started` fires for every attempt and `context.attempt` counts them. `raise Retry(in_=60)`
  and `raise Abandon("...")` from `tinyjobs` work inside a Django task.
- **Limits.** A job a limit stops is `held` in `manage.py tinyjobs stats` and `READY` to Django.
  Keep a limited task in its app's `tasks.py`, which `manage.py tinyjobs` imports before the
  worker starts. A limited task defined elsewhere is unknown until its first job arrives; the
  worker sends that job back to the queue once, to be claimed again under the limit.
- **Isolated tasks** run inside the job process, with `django.setup()` done there first:
  the task, its database connections and its `task_started` signal. The job process opens a
  connection of its own to the queue to read the job's earlier attempts. `manage.py` must keep
  its `if __name__ == "__main__":` guard, as the one Django generates does, since a job process
  imports it.
- **`task_finished`** fires in the worker, after the queue recorded how the task ended, with
  the result `get_result()` would return, so it also reports a timeout and a job process that
  died. Receivers run in a thread started for each finishing task, and one that raises is logged and
  does not change the task. A job cancelled while it runs, one failed because its worker
  died, and one cancelled because the task it was to run `after` did not succeed, end without
  the signal.

`"database": "default"` in `OPTIONS` makes every enqueue join that database's transaction, so a
task enqueued inside `transaction.atomic()` exists only if the block commits. It works on a
Postgres alias and on a SQLite alias whose `NAME` is the file in `url`. On SQLite the enqueue
writes before it reads, so it can open the block, but a block that reads before it enqueues meets
"database is locked" when a worker committed in between: give the alias
`"transaction_mode": "IMMEDIATE"` in its own `OPTIONS`, and keep the block short, since it holds
the file's write lock until it commits. Django's test runner turns a SQLite alias into an
in-memory database, which no worker could share, so an enqueue that joins it is refused there:
give the test settings `"eager": True`, which runs each task inline and keeps test jobs out of
your queue. An alias with a `TEST` `NAME` gets a file instead, but a `TestCase` holds that file's
write lock inside its transaction, so the app's own calls wait on it; `"eager": True` is the
setting for tests. On Redis, enqueue from
`transaction.on_commit`, or the worker may run before the row it needs is committed.
