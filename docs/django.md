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
raises `InvalidTask`. Django has no retries, so a failed task
stays failed.

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
