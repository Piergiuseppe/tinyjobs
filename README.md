# tinyjobs

Background jobs and cron for Python, backed by SQLite, with no dependencies.

```bash
pip install tinyjobs
```

That's the whole setup. No broker, no result backend, no daemon. The queue is a file.

Celery is the right tool for a lot of systems, but it asks for a Redis or RabbitMQ instance before it
will run a single job. This is for the projects that never needed that: a Django app that sends
emails, a scraper with a nightly refresh, a CLI that offloads slow work. When you outgrow SQLite you
swap the backend for a plugin and your task code doesn't change.

Requires Python 3.11 or newer.

## Quick start

```python
# tasks.py
from tinyjobs import TinyJobs

app = TinyJobs("sqlite:///jobs.db")


@app.task
async def generate_pdf(document_id: int) -> str:
    ...


@app.task(max_attempts=5, timeout=300)
def transcode(video_id: int) -> None:
    ...
```

Enqueue from anywhere, including plain sync code:

```python
from tasks import generate_pdf

job = generate_pdf.enqueue(123)
print(job.id)          # 01a061bf-c08a-738d-...
```

`enqueue` gives you back a snapshot of the row it just wrote, not a live handle. The `Job` you are
holding will say `queued` forever, however long you wait, because nothing updates it: the worker that
runs your task is a different process writing to the database. To see where a job got to, read it
back:

```python
app.get(job.id).status     # queued -> running -> success
```

Or from async code, which is the native path:

```python
job = await generate_pdf.aenqueue(123)
```

Run a worker:

```bash
tinyjobs worker --app tasks:app --concurrency 8
```

`tinyjobs` is installed as a command, but `python -m tinyjobs` does the same thing and is handy when
you have not activated a virtualenv:

```bash
python3.13 -m tinyjobs worker --app tasks:app --concurrency 8
```

Look at what happened:

```bash
tinyjobs jobs list --app tasks:app
tinyjobs jobs list --app tasks:app --status failed
tinyjobs jobs show <job-id> --app tasks:app
```

`jobs show` prints every attempt, not just the last one:

```
attempts
    1  failed        4ms  laptop/8821/9389541d  ConnectionError
    2  failed        3ms  laptop/8821/9389541d  ConnectionError
    3  success      12ms  laptop/8821/9389541d
```

There's a runnable example in `examples/demo.py` covering retries, timeouts, priorities, delays and
deduplication:

```bash
python -m examples.demo
tinyjobs worker --app examples.demo:app --concurrency 4
```

## What you get

| | |
| --- | --- |
| Async and sync tasks | `async def` runs on the worker's event loop, `def` in a thread pool |
| Retries | exponential backoff with jitter, per-exception policy, `Retry` and `Abandon` |
| Delayed jobs | `delay=60` or `run_at=<datetime>` |
| Priorities and queues | integers, highest first; workers subscribe to a subset |
| Timeouts | hard for async tasks, soft for threaded sync ones (see below) |
| Crash recovery | leases plus heartbeats, so a killed worker's jobs come back |
| Deduplication | `key="invoice:123"` collapses duplicate enqueues |
| Attempt history | every attempt is a row, with its traceback and timings |
| Multi-worker | multiple processes and machines over one file or backend |
| Async throughout | the backend protocol is `async`, with a sync facade for ordinary code |
| Cancellation | before a job starts; cooperative after |

Delivery is **at-least-once**. A worker can finish a job and die before recording it, in which case
the job runs again. Write your tasks so that running twice is the same as running once. There's no
way around that without a transaction spanning both the queue and whatever your task touches, and
anything claiming exactly-once is either lying to you or a great deal more complicated than this.

Two things are worth knowing before you hit them:

**Timeouts on sync tasks are advisory.** You can't kill a running Python thread. When a threaded task
exceeds its timeout the job is marked failed and the worker moves on, but the thread keeps going until
the function returns. Pass timeouts to the library you are calling, or write the task as `async def`
where cancellation actually works.

**Arguments are serialized as JSON**, extended with tags for `datetime`, `date`, `Decimal`, `UUID`,
`Path`, `set`, `tuple`, `bytes` and non-string dict keys. Pickle is available behind
`TinyJobs(..., allow_pickle=True)` but it's off by default, because deserializing a pickle runs arbitrary
code and the roadmap includes backends that reach over a network. Dataclasses and enums need one line
of registration:

```python
app.codec.register_dataclass(Money)
app.codec.register_enum(Priority)
```

## How it works

`jobs.db` holds two tables. `jobs` is the queue, `executions` is the history of attempts.

A worker claims work with a single statement, which is what makes it safe to run as many workers as you like:

```sql
UPDATE jobs SET status = 'running', worker_id = ?, attempts = attempts + 1, lease_expires_at = ?
 WHERE id IN (SELECT id FROM jobs
               WHERE status = 'queued' AND run_at <= ? AND queue IN (?)
               ORDER BY priority DESC, run_at ASC LIMIT ?)
RETURNING *;
```

Claiming a job takes a lease. The worker refreshes it while the job runs, and if the worker dies the
lease expires and another worker picks the job up. On SQLite older than 3.35 the same claim runs as
two statements inside `BEGIN IMMEDIATE`.

Because it's just SQLite, you can read the queue with anything:

```bash
sqlite3 jobs.db "SELECT task, status, attempts, payload FROM jobs WHERE status = 'failed';"
```

```
sync_invoice|failed|3|{"args":[8842],"kwargs":{}}
```

That readability is most of the reason for preferring JSON over pickle.

## Other backends

Backend choice is a URL:

```python
app = TinyJobs("sqlite:///jobs.db")
app = TinyJobs("redis://localhost/0")
```

The core package doesn't import or know about optional backends. It looks the scheme up in the
`tinyjobs.backends` entry point group, so a third-party package registers itself with:

```toml
[project.entry-points."tinyjobs.backends"]
redis = "tinyjobs_redis:RedisBackend"
```

Backends declare what they support (`priority`, `delayed`, `dedup`, ...) and asking for something
unsupported raises when you enqueue rather than failing quietly later. No Redis or Postgres backend
exists yet.

The protocol is `async`, so a backend built on a network client talks to it directly with no threads
in the way. SQLite is the odd one out: `sqlite3` is a blocking C library, so that backend does its own
thread hop internally, on a dedicated single-worker executor. `tests/test_async_backend.py` implements
a working in-memory backend in about 120 lines if you want a template.

## Status

v0.1. Working and tested, but young. What's in:

- task registry, `enqueue`, `app.send`
- SQLite backend, atomic claim, leases, reaper
- worker with async and threaded executors, timeouts, graceful shutdown on `SIGTERM`
- retries with backoff and jitter
- priorities, named queues, delayed jobs, deduplication keys
- `tinyjobs worker` / `jobs` / `send`
- the backend plugin seam

What's not in yet:

- cron schedules and the scheduler (v0.2)
- transactional enqueue, so a job is only created if your own transaction commits (v0.3)
- reading results back, a process executor for CPU-bound work, `memory://` for tests (v0.4)
- `tinyjobs stats` / `check` / `purge`

`docs/design.md` explains the design and why it looks like this, including the parts not built yet.

## Working on it

Nothing to install beyond an interpreter, but `python3` on macOS is still 3.9, which is too old. A
venv saves you from remembering:

```bash
uv venv --python 3.12 .venv     # or: python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Then `python`, `tinyjobs` and the example all point at the right interpreter:

```bash
python -m examples.demo
tinyjobs worker --app examples.demo:app --concurrency 4
```

Importing the package on anything older than 3.11 raises with the version it found and the path to it,
rather than an unhelpful error about `StrEnum`.

## Tests

```bash
python -m unittest discover -s tests -t .
```

134 tests, no test dependencies. The one that matters most is
`tests/test_claim_concurrency.py`, which runs six processes and eight threads against one database and
asserts every job was claimed exactly once.

## License

MIT.
