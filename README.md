# tinyjobs

[![PyPI Downloads](https://static.pepy.tech/personalized-badge/tinyjobs?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/tinyjobs)

Background jobs and cron for Python, backed by SQLite, with no dependencies.

```bash
pip install tinyjobs
```

That's the whole setup. No broker, no result backend, no daemon. The queue is a file.

Celery is the right tool for a lot of systems, but it asks for a Redis or RabbitMQ instance before it
will run a single job. This is for the projects that never needed that: a Django app that sends
emails, a scraper with a nightly refresh, a CLI that offloads slow work. When you outgrow SQLite you
switch the URL to Postgres or Redis and your task code doesn't change.

Requires Python 3.12 or newer.

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

`enqueue` gives you back a snapshot of the row it just wrote, not a live handle, so the `Job` you are
holding keeps saying `queued`: the worker that runs your task is a different process writing to the
database, and nothing reaches back into your object. Ask for the current state when you want it:

```python
job.refresh()          # updates this instance in place, and returns it
job.status             # queued -> running -> success
job.attempts           # 3, if it took three tries
job.last_error         # 'ConnectionError: refused', when it failed

app.get(job.id)        # or a fresh object, if you prefer not to mutate
```

Attribute access never does I/O on its own. `refresh()` is the one place a round trip happens, which
keeps `for job in jobs: print(job.status)` from quietly becoming a thousand queries.

If the task returns something you want back, ask for it to be kept:

```python
@app.task(store_result=True)
async def generate_pdf(document_id: int) -> str:
    return f"/tmp/{document_id}.pdf"

job = generate_pdf.enqueue(123)
...
job.refresh().result()     # '/tmp/123.pdf'
```

Results are off by default because most jobs are run for their side effects, and storing return
values nobody reads is write amplification on the hot path.

## Keeping the database from growing forever

Terminal jobs are pruned on a schedule. Successes and cancellations are kept for a week, failures for
thirty days, because failures are the ones you come back to:

```python
app = TinyJobs("sqlite:///jobs.db", retention=RetentionPolicy(failed=90 * 86400))
app = TinyJobs("sqlite:///jobs.db", retention=KEEP_EVERYTHING)   # prune nothing
```

```bash
tinyjobs worker --app tasks:app --retain-success 7d --retain-failed 30d
tinyjobs worker --app tasks:app --no-purge

tinyjobs purge --app tasks:app --before 30d --dry-run
```

Every worker checks whether a sweep is due, and exactly one of them wins, because claiming the sweep
is a single conditional `UPDATE` on a marker row. So running forty workers still means one sweep per
interval, with no leader to elect. Queued and running jobs are never touched at any horizon, and each
sweep deletes in small batches with a cap, so it never holds the write lock long enough to stall a
claim.

Note that pruning does not shrink the file. SQLite reuses the freed pages, so the database
**plateaus** rather than shrinking. Handing space back to the filesystem needs `VACUUM`, which wants
exclusive access, so it stays a manual thing.

If you want to block until a job is finished, `wait` polls for you:

```python
job = generate_pdf.enqueue(123)
print(job.wait(timeout=30).result())     # '/tmp/123.pdf'

await job.awaited(timeout=30)            # from async code
```

It returns the job whatever the outcome, rather than raising, so a failed job leaves you holding
`job.status`, `job.attempts` and `job.last_error` to look at. Only the timeout raises, as
`WaitTimeout`, which is also a `TimeoutError` if you would rather catch that.

The async twin is called `awaited` and not `await` for the boring reason that `await` is a keyword.

Waiting on a job from the request that created it means you did not need a background job. It is meant
for scripts, tests and glue code.

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

There's a runnable example in `examples/demo.py` covering retries, timeouts, priorities, delays,
deduplication and cron:

```bash
python -m examples.demo
tinyjobs worker --app examples.demo:app --concurrency 4
```

## Cron

```python
@app.task(cron="*/15 * * * *")
async def refresh_cache() -> None:
    ...


@app.task(cron="30 2 * * *", timezone="Europe/Rome")
def nightly_report() -> None:
    ...
```

There is no extra process to run. Every `tinyjobs worker` also runs the scheduler, and however many
workers you start, each firing creates exactly one job: the schedule moves on with a single
conditional `UPDATE`, and only the worker whose update matched writes the job, in the same
transaction. Kill any of them, even with `kill -9`, and the rest carry on.

What it creates is an ordinary job, with the task's retries, queue and priority.

```bash
tinyjobs schedules --app tasks:app
```

```
NAME                  CRON          TIMEZONE     NEXT RUN             LAST RUN             LAST JOB
--------------------  ------------  -----------  -------------------  -------------------  ------------------------------------
tasks.nightly_report  30 2 * * *    Europe/Rome  2026-10-11 02:30:00  -                    -
tasks.refresh_cache   */15 * * * *  UTC          2026-10-10 14:15:00  2026-10-10 14:09:00  01a125b7-6b62-7135-b9a0-b7707bf39c1c
```

```bash
tinyjobs worker --app tasks:app --no-scheduler     # runs jobs, fires nothing
tinyjobs scheduler --app tasks:app                 # fires schedules, runs nothing
```

When nothing was running at the moment a schedule was due, `misfire` decides what happens on restart:

```python
@app.task(cron="0 * * * *")                                   # skip, the default
@app.task(cron="0 * * * *", misfire="run_once")               # one catch-up run
@app.task(cron="0 0 * * *", misfire="run_all", overlap=True)  # every missed day, oldest first
```

A firing only counts as missed when it is more than a minute late (`--misfire-grace`). A schedule
won't start a run while the previous one is still queued or running, unless `overlap=True`.

A malformed expression, an unknown timezone, `0 0 30 2 *` (which never fires) or a cron task that
needs arguments all raise at import, not at 2 a.m.

Worth knowing:

- **The resolution is one minute.**
- **An `async` task that blocks the event loop also blocks its worker's scheduler**, which then skips
  the firings it slept through. Write blocking work as `def`, or run a standalone `tinyjobs scheduler`.
- **Daylight saving.** A time of day (`30 2 * * *`) fires once, even when 02:30 happens twice or not
  at all. A frequency (`*/10 * * * *`) follows the real clock: the repeated hour runs twice, the
  skipped one not at all. Local timezones on Windows need `pip install tzdata`.
- **Clocks** that disagree by less than the grace are harmless. A jump forward counts as an outage; a
  jump back is noticed on the next tick.
- **One schedule per task name.** For two cadences, register the function twice under two names.
- **Volume.** Ten thousand schedules on the same minute all fire exactly once, the last few seconds
  late. Numbers in [Measured](#measured).

## Watching it

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
QUEUE    QUEUED  RUNNING  SUCCESS  FAILED  CANCELLED  OLDEST DUE
-------  ------  -------  -------  ------  ---------  ----------
default  2       2        0        0       0          16s
mail     1       0        0        0       0          16s

running past their lease: 2
```

A job past its lease means the worker holding it stopped reporting, usually because it died. `check`
fails on that, on queued jobs for a task the app doesn't register (a missing `--import`), on a due job
waiting longer than `--max-wait`, and on a cron schedule more than a minute overdue, which means no
worker or scheduler is running. Both are `app.stats()` and `app.check()` in code.

## FastAPI

```python
from fastapi import FastAPI
from tasks import app as jobs, generate_pdf

api = FastAPI(lifespan=jobs.lifespan(worker=True))


@api.post("/documents/{document_id}/pdf")
async def render(document_id: int) -> dict:
    job = await generate_pdf.aenqueue(document_id)
    return {"job": job.id}
```

`uvicorn main:api` is the whole deployment. The lifespan connects when the server starts, runs a
worker inside it, and on shutdown lets in-flight jobs finish before closing. Plain `def` endpoints
can call `enqueue()` too, on every backend.

`worker=True` takes the options of `app.worker()`, such as `concurrency`, `queues` and
`shutdown_timeout`. Under `uvicorn --workers 4` each process runs its own worker on the same queue,
and cron still fires once. If you already have a lifespan, nest it:

```python
@asynccontextmanager
async def lifespan(api: FastAPI):
    async with jobs.serve(worker=True):
        yield
```

Shutdown waits up to `shutdown_timeout` (30 s) for running jobs, and a second Ctrl-C does not cut it
short. Keep it under your orchestrator's grace period: `docker stop` allows 10 s, and past it the
process is killed and its jobs wait out their leases instead of going straight back to the queue.

Starlette takes the same `lifespan`. Leave out `worker=True` and the server only enqueues, with
`tinyjobs worker --app tasks:app` running the jobs elsewhere.

The worker shares the process with your API: an async task that blocks the loop stalls requests too,
and CPU-heavy `def` tasks compete for the GIL. That is the point where a separate worker pays off.
Numbers in [Measured](#measured).

## Django

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

On Postgres, `"database": "default"` in `OPTIONS` makes every enqueue join that database's
transaction, so a task enqueued inside `transaction.atomic()` exists only if the block commits. On
SQLite and Redis, enqueue from `transaction.on_commit`, or the worker may run before the row it needs
is committed.

## Testing your tasks

```python
app = TinyJobs("memory://", eager=True)

job = generate_pdf.enqueue(123)     # runs before enqueue returns
job.status                          # success
job.result()                        # '/tmp/123.pdf'
```

Or keep the queue and run it when you choose, with a clock you control:

```python
from tinyjobs import FrozenClock

clock = FrozenClock(time.time())
app = TinyJobs("memory://", clock=clock)

flaky.enqueue()
app.drain()             # runs everything due, highest priority first; returns how many ran
clock.advance(10)       # the retry's backoff
app.drain()
```

`memory://` is the SQLite backend on a private in-memory database, so tests run the same claim,
dedup and retries as production. Delays and backoff are respected in eager mode too: advance the clock
and drain. A failing task is recorded on the job, not raised, as it would be in a worker.

## What you get

| | |
| --- | --- |
| Async and sync tasks | `async def` runs on the worker's event loop, `def` in a thread pool |
| Retries | exponential backoff with jitter, per-exception policy, `Retry` and `Abandon` |
| Cron | `cron="*/15 * * * *"`, timezones and DST, misfire and overlap policies, in every worker |
| Delayed jobs | `delay=60` or `run_at=<datetime>` (a naive one is local time) |
| Priorities and queues | integers, highest first; workers subscribe to a subset |
| Timeouts | hard for async tasks, soft for threaded sync ones (see below) |
| Crash recovery | leases plus heartbeats, so a killed worker's jobs come back |
| Deduplication | `key="invoice:123"` collapses duplicate enqueues |
| Attempt history | every attempt is a row, with its traceback and timings |
| Multi-worker | multiple processes over one SQLite file, or machines over one Postgres or Redis |
| Transactional enqueue | on Postgres, a job commits or rolls back with your own transaction |
| Async throughout | the backend protocol is `async`, with a sync facade for ordinary code |
| Cancellation | before a job starts; cooperative after |
| Retention | old terminal jobs are pruned so the database stops growing |
| Hooks | `on_job_start`, `on_job_success`, `on_job_failure`, for metrics and alerts |
| Health | `tinyjobs stats`, and `tinyjobs check` for probes |
| Testing | `memory://`, `eager=True`, `app.drain()` and a frozen clock |
| FastAPI | `lifespan()`, with an optional worker inside the server process |
| Django | a `django.tasks` backend and `manage.py tinyjobs` |

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

## How it compares

[Huey](https://github.com/coleifer/huey) is the closest relative and worth a look: it also runs on
SQLite with no dependencies, and has cron, retries and a `django.tasks` backend. tinyjobs adds three
things. `async def` tasks run natively on the worker's loop, where Huey has no asyncio pipeline for
running tasks, only helpers to await their results. FastAPI and Starlette can run the worker inside
the server's lifespan. And cron fires from every worker without a leader, with timezones and misfire
policies, on SQLite or on Redis. [LiteQ](https://github.com/ddreamboy/liteq), at 0.2, makes much
the same pitch on SQLite.

[RQ](https://python-rq.org) and [arq](https://github.com/python-arq/arq) need Redis. RQ's cron
scheduler is still marked beta, and its retries take a fixed interval or a list of delays, with no
built-in exponential backoff. arq is asyncio-only and in maintenance mode, with security fixes only.
[Celery](https://docs.celeryq.dev) and [Dramatiq](https://dramatiq.io) need a broker; Celery is
still the answer for RabbitMQ routing, workflows and large fleets.

On Postgres, [Procrastinate](https://procrastinate.readthedocs.io) is the closest: async, with cron,
retries, `LISTEN`/`NOTIFY` and jobs deferred inside your own transaction, but Postgres only. tinyjobs
does the same on Postgres and keeps SQLite and Redis behind one API. In Django,
[django-tasks-db](https://github.com/RealOrangeOne/django-tasks-db) keeps `django.tasks` tasks in
your database through the ORM, so an enqueue can share your transaction, and
[django-q2](https://github.com/GDay/django-q2) can use the ORM as its broker and has cron schedules.

As of October 2026, from each project's documentation.

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
lease expires and another worker picks the job up. On Redis and Postgres, a worker that cannot
record a job's outcome because the server went away keeps trying until a second before its lease
would run out, so a restart or a failover that ends by then does not run the finished job again,
unless the worker is shut down first and its `shutdown_timeout` ends the wait. On SQLite older than
3.35 the same claim runs as two statements inside `BEGIN IMMEDIATE`.

Because it's just SQLite, you can read the queue with anything:

```bash
sqlite3 jobs.db "SELECT task, status, attempts, payload FROM jobs WHERE status = 'failed';"
```

```
sync_invoice|failed|3|{"args":[8842],"kwargs":{}}
```

That readability is most of the reason for preferring JSON over pickle.

## Redis

```bash
pip install 'tinyjobs[redis]'
```

```python
app = TinyJobs("redis://localhost:6379/0")
app = TinyJobs("redis://localhost:6379/0", prefix="billing")    # several apps on one server
```

Nothing else changes: the same tasks, workers, cron, hooks, `stats` and `check`. Every state change
is one Lua script, so a claim is as atomic as on SQLite, and any number of workers on any number of
machines can share the queue. With the default clock every machine keeps Redis's time, so their own
clocks need not agree; a `run_at` or `Retry(at=...)` you pass is read on the local clock and moved
onto Redis's. A `clock=` you pass yourself is used as it is, and must then agree across machines.
Leases hold even while a machine still runs on an old reading: the scripts never reap by a time
ahead of Redis's and never stamp a lease from one behind it. Keep the Redis server and its replicas
on NTP: after a failover to a node whose clock is ahead by more than a lease, every running job looks
abandoned and runs again, or fails when it has no attempts left. When upgrading from 0.7.0, which
used each machine's own clock, stop or upgrade every 0.7.0 process, workers and senders alike, before
starting the new workers if their clocks drift from Redis's by more than a lease, and grant `TIME`
to a Redis user restricted by ACLs. It passes the same contract suite as SQLite.

**Turn persistence on.** Redis keeps the queue in memory. Killed with `SIGKILL` mid-run, a Redis on
its default snapshots lost the whole queue; with `appendonly yes` the same crash lost nothing, and
the jobs that were running came back once their leases expired. A restart, graceful or not, is
otherwise harmless: workers log the outage, back off, and carry on.

Worth knowing: a job costs about 1 KB of Redis memory, retention prunes it like on SQLite, and a job
cannot join your database's transaction. All keys share one hash slot (`{prefix}`), so it runs on a
single Redis or a primary with replicas, not spread across a cluster.

Each process holds at most 100 connections (`max_connections=`). A burst beyond that waits for a free
one rather than failing, and gives up with redis-py's `ConnectionError` after `pool_timeout=` seconds
(20).

## Postgres

```bash
pip install 'tinyjobs[postgres]'
```

```python
app = TinyJobs("postgresql://app@localhost/shop")
app = TinyJobs("postgresql://app@localhost/shop", prefix="billing", schema="jobs")
```

The same tasks, workers, cron, hooks, `stats` and `check` again. The tables (`tinyjobs_jobs`,
`tinyjobs_executions`, `tinyjobs_schedules`, `tinyjobs_meta`) are created on first connect, and any
number of processes can start at once. Every machine keeps the server's time, as on Redis. An idle
worker is woken by `LISTEN`/`NOTIFY` and starts a new job in a few milliseconds instead of at its next
poll; `notify=False` turns that off.

**Enqueue inside your own transaction.** Pass your psycopg connection or cursor, sync or async, as
`tx`. The job commits or rolls back with everything else, and workers only hear of it at commit:

```python
with psycopg.connect(DATABASE_URL) as connection, connection.transaction():
    connection.execute("INSERT INTO orders (id) VALUES (%s)", (42,))
    send_receipt.with_options(tx=connection).enqueue(42)
```

`app.send(name, tx=connection)` and the async calls take it too. The connection must reach the
database tinyjobs uses, not a copy of it, and the tables must belong to its role or to a role that
can become it, the same rule as below, or the enqueue raises `ConfigurationError`; a connection
with `client_encoding` `SQL_ASCII` is refused too. In Django, add
`"database": "default"` to `OPTIONS` and every enqueue joins `transaction.atomic()`.

Each process holds at most 10 connections (`max_connections=`) plus one for `LISTEN`, every
statement times out after 30 seconds (`statement_timeout=`), a server that stops answering fails
the call instead of hanging it, and connections are named `tinyjobs` in
`pg_stat_activity`. The tables and their schema must belong to the connecting role or a role that
can become it (`SET` on Postgres 16 and later), every role that inherits the owner's privileges must
be able to become it too, and no other role may hold `TRIGGER` or `REFERENCES` on them or own code that runs on
them, such as a trigger, a constraint's function or an index's; otherwise pass `schema=` with one it
owns. Behind PgBouncer use session
pooling. A transaction left open for hours anywhere on the server stops autovacuum from cleaning the
jobs table. Postgres 14 to 18 are supported, and CI tests 14 and 18.

## Other backends

The core package doesn't know about third-party backends. It looks an unknown URL scheme up in the
`tinyjobs.backends` entry point group, so a package registers itself with:

```toml
[project.entry-points."tinyjobs.backends"]
mysql = "tinyjobs_mysql:MySQLBackend.from_url"
```

Backends declare what they support (`priority`, `delayed`, `dedup`, ...) and asking for something
unsupported raises when you enqueue rather than failing quietly later. The protocol is `async`, and
`tinyjobs.testing.BackendContract` is the test suite a backend has to pass, the same one SQLite,
Postgres and Redis run. `tests/test_async_backend.py` implements a minimal in-memory backend in about 130 lines if
you want a template.

## Measured

Ten hours of steady load on a shared four-core ARM Linux VPS, run twice side by side on 0.7.0: once
on one SQLite file and once on Redis 7.4. Each run had its own container capped at 1.25 CPUs and
1.5 GB, three worker processes of eight slots, a deliberate 25 jobs per second, a graceful restart
every fifteen minutes and a `SIGKILL` every twenty.

```
SQLite   897,300 jobs   24.9/s sustained, zero enqueue failures
Redis    881,375 jobs   24.5/s sustained, zero enqueue failures
each     68 worker restarts, 39 by SIGTERM, 29 by SIGKILL, zero unexpected deaths
```

Sampled once a minute for the whole run, comparing the medians of the first hour and the last:

| | SQLite hour 0 | SQLite hour 9 | Redis hour 0 | Redis hour 9 |
| --- | --- | --- | --- | --- |
| worker RSS | 37.1 MB | 37.3 MB | 41.6 MB | 42.1 MB |
| threads per worker | 12 | 12 | 12 | 13 |
| open file descriptors | 12 | 12 | 17 | 17 |
| store size | 39 MB | 42 MB | 43 MB | 47 MB |
| end-to-end p95 | 402 ms | 399 ms | 392 ms | 403 ms |

Nothing drifted. The store size is the database file on SQLite and Redis `used_memory` on Redis;
with retention on, both stayed within a few MB for ten hours. The SQLite workers had 12 file descriptors open in all 597 samples.

A separate burst harness pushes the other end: 2,398 enqueues per second from 24 threads, 572 jobs
per second end-to-end with six workers, and a crash recovery check that kills every worker while 48
jobs are mid-flight and confirms all 48 come back and finish. Some of them run twice, which is what
at-least-once means; the check is that each one applied its side effect once, because the handlers
are idempotent.

Retention was also measured on its own, with the burst harness: with it off the database climbed
from 4.7 to 10.2 MB in three minutes and kept going, and with it on the same curve flattened at 6.9
MB and stayed there. Under a `SIGKILL` on a worker every thirty seconds it moved 0.6% across eight
minutes.

Redis was measured with the same kind of harness, on a laptop with Redis 7.4 in Docker: 20,000 jobs
over four workers of eight slots in 2.1 seconds, against 4 seconds on SQLite, each run exactly once.
Twelve `SIGKILL`s on workers during 4,000 jobs left none lost; 43 came back through an expired lease,
and 2 applied their side effect twice. Claiming stays at 0.7 ms with 200,000 jobs queued.

Postgres 17 was measured the same way on the same laptop: 20,000 jobs over four workers of eight slots
in 2.1 seconds, each run exactly once, and 6,850 enqueues a second from 24 threads. An idle worker
polling every five seconds started a new job 6.6 ms after it was sent (p95 8.4 ms). Claiming stays at
0.23 ms with 200,000 jobs queued. Twelve `SIGKILL`s on workers during 4,000 jobs left none lost and
none run twice; 88 came back through an expired lease. Postgres stopped three times during 6,000
jobs, once cleanly and twice with `-m immediate`, down three seconds each: the workers carried on,
and nothing was lost or ran twice.

Two machines sharing one Redis were measured too: a laptop and a Linux VPS 52 ms apart, two workers
of eight slots on each, a `SIGKILL` on one of them every 25 to 30 seconds, 3,000 jobs a run. With the
laptop's clock set five minutes behind and the VPS's fifteen minutes ahead, all 3,000 succeeded and
none applied its side effect twice; the 23 attempts cut short by the kills were picked up again. With
both clocks right the result was the same, but for one job killed between its side effect and its
completion. The same skewed run on 0.7.0, which still used each machine's own clock, ran 492 jobs
twice and failed 2, because the VPS took the laptop's live jobs for dead.

Cron was measured on its own. Ten thousand schedules all due on the same minute fire in about a
second from one scheduler, roughly 110 microseconds each. With four workers racing for them, 2,000
schedules a minute were fired 8,000 times in four minutes, each exactly once, p95 0.57 seconds late;
10,000 a minute, 40,000 times, each exactly once, p95 6.4 seconds late. Fifty-six schedulers racing on
one schedule for twelve minutes produced twelve jobs, and fifty `SIGKILL`s across four workers and a
standalone scheduler in ten minutes left no gap and no duplicate.

FastAPI was measured under uvicorn in Docker, with the worker embedded. 3,000 jobs through one
process on SQLite and 4,000 through `--workers 4` each ran exactly once, and cron fired once in each
of 13 minutes across the four processes. Three API containers on one Redis took 5,638 jobs while one
was `SIGKILL`ed; the eight it was running came back on the others when their leases expired, and
none ran twice. An 8-second Redis outage turned enqueues into 500s while it lasted, then everything
resumed; the one job that finished during it ran again, which no longer happens: with the outcome
retried, a job finishing as an 8-second outage began was recorded once. `docker stop` waited only
for the jobs in flight, 1.5 seconds.

Sharing the process costs the API little: `/ping` p95 went from 2.1 ms to 12.9 ms under eight
CPU-bound `def` tasks, and back to 1.6 ms with the worker moved out. Six `async def` tasks calling
`time.sleep(1)` froze it for six seconds.

Django was measured with a real project under uvicorn `--workers 2` and under `runserver`: 1,000
requests split between sync and async views, each enqueuing a task that writes a row, with two
`manage.py tinyjobs worker` processes on SQLite and then on Redis. Every task ran exactly once.
Enqueueing through `django.tasks` costs about what a direct `send` does, 6,000 a second against
6,600. A worker `SIGKILL`ed mid-task leaves that task `FAILED` once its lease expires, since Django
tasks are not retried.

Two caveats worth stating. The tasks are synthetic, with `sleep` standing in for real I/O. And 25
jobs per second was chosen as a comfortable rate to hold for ten hours, not as a ceiling.

## Status

v0.7.2. Working and measured, but young. What's in:

- task registry, `enqueue`, `app.send`
- SQLite backend, atomic claim, leases, reaper
- async backend protocol with a synchronous facade
- worker with async and threaded executors, timeouts, graceful shutdown on `SIGTERM`
- retries with backoff and jitter, `Retry` and `Abandon`
- priorities, named queues, delayed jobs, deduplication keys
- `job.refresh()`, `job.wait()` and `job.result()`
- retention, so the database plateaus instead of growing
- lifecycle hooks for metrics and alerting
- `memory://`, eager mode and `app.drain()` for testing
- cron schedules, fired leader-free from every worker, with timezones, misfire and overlap policies
- `tinyjobs worker` / `scheduler` / `schedules` / `jobs` / `stats` / `check` / `send` / `purge`
- a Redis backend, `pip install 'tinyjobs[redis]'`
- a Postgres backend, `pip install 'tinyjobs[postgres]'`, with `LISTEN`/`NOTIFY` wake-ups
- transactional enqueue on Postgres, also through Django's `transaction.atomic()`
- the backend plugin seam and its contract suite
- `app.lifespan()` and `app.serve()` for FastAPI and Starlette, with an embedded worker
- a `django.tasks` backend and `manage.py tinyjobs`

What's not in yet:

- transactional enqueue on SQLite
- a process executor for CPU-bound work

Upgrading from 0.6: stop every 0.6 worker before starting 0.7. The first 0.7 process migrates the
store, and from then on a 0.6 process refuses to start against it. 0.7 also needs Python 3.12 or
newer.

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

Importing the package on anything older than 3.12 raises with the version it found and the path to it.

## Tests

```bash
python -m unittest discover -s tests -t .
```

1349 tests, no test dependencies. The one that matters most is
`tests/test_claim_concurrency.py`, which runs six processes and eight threads against one database and
asserts every job was claimed exactly once.

Interruptions get tests of their own. One kind raises an exception at every call and loop edge inside
tinyjobs in turn, during a send, an enqueue, a start, a close, a drain, a transaction and a fork, and
after each one checks that no lock is held, no transaction is open and no job is stuck running. The
other fires a real `SIGALRM` every 20 µs to 1.5 ms while calls run.

The Postgres and Redis tests skip unless they have a server and the client library, and the FastAPI
ones unless `fastapi` and `httpx` are installed:

```bash
docker run -d -p 6379:6379 redis:7.4-alpine
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=tinyjobs postgres:17-alpine
pip install -e '.[redis,postgres]' fastapi httpx
export TINYJOBS_REDIS_URL=redis://localhost:6379/0
export TINYJOBS_POSTGRES_URL=postgresql://postgres:tinyjobs@localhost:5432/postgres
python -m unittest discover -s tests -t .
```

## License

MIT.
