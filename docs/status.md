# Status

v0.17.0. Working and measured, but young. What's in:

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
- transactional enqueue on Postgres and SQLite, also through Django's `transaction.atomic()`
- the backend plugin seam and its contract suite
- `app.lifespan()` and `app.serve()` for FastAPI and Starlette, with an embedded worker
- a `django.tasks` backend and `manage.py tinyjobs`
- a worker that keeps the GIL off on free-threaded builds, with `--loops` for several event loops in
  one process
- `isolated=True`, a job process per task that is killed at its timeout and replaced when it crashes,
  with `--processes`, `--jobs-per-process` and `process_setup`
- `tinyjobs schedules pause` / `resume` / `run`, and the same from the app
- `max_result_bytes` and a per-task `result_ttl`
- the job fields on every log record and `--log-format json`
- a Prometheus exporter on `tinyjobs worker --metrics-port`
- `tinyjobs dashboard`, and `wsgi(app)` / `asgi(app)` to mount its pages in an existing app
- `tinyjobs verify`, and the store audit in `tinyjobs check`, with `--store-warnings` for a
  development store
- `synchronous_commit` kept at least `on` on every Postgres connection
- prefixed tables on SQLite, so the queue lives in the application's own file
- per-task limits: `concurrency=` and `rate=`, enforced by the store across every worker, with a
  `held` status for the jobs a limit stops
- the task options on `django.tasks`: timeout, retries, limits and `isolated` on Django's own
  `@task`, or by import path in the settings
- jobs that run after another: `after=job`, with a `waiting` status, on every store and on
  `django.tasks`

Upgrading to 0.17 adds the `waiting` status and a column (a field, on Redis) to every store. Stop
every tinyjobs process on a store before starting the first 0.17 one: it upgrades the store on its
first call, and an older process refuses to start against the upgraded store, since it would not
release the jobs waiting on the ones it finishes. This is meant to be the last upgrade that needs a
stop: from 0.17 a store records the oldest version of its layout whose code can work on it, and a release that changes
the store keeps the previous minor working beside it unless its upgrade note says otherwise, so a
rolling deploy from one minor to the next needs no stop. A release older than the store allows
still refuses to start. That holds for 0.16 too once the store is upgraded, so going back to 0.16
means restoring a backup taken before the upgrade.

What changes for an application that never sends a job `after` another:

- `JobStatus` has a new member, `waiting`. `tinyjobs stats` and the dashboard's table of queues
  have a column for it between held and running, so whatever reads those tables by position reads
  one column off, and `/api/overview` counts it in every queue.
- The figures for queued and held jobs are called queued everywhere, now that waiting is a status:
  the dashboard's "Queued by task", the `check` finding "N queued job(s) for X", which said
  "waiting", and the help text of `tinyjobs_store_queued_jobs`. An alert matching the old words
  needs the new ones.
- `jobs retry` and `jobs cancel` word their refusals and help differently, and so do the
  dashboard's notices after a retry. A job's page and every job in `/api/jobs` gain the job it
  runs after, `/jobs?status=waiting` is a page and no longer a 400, and `tinyjobs_store_jobs`
  can carry `status="waiting"`.
- On Django every task of the backend has a `follows` field, `None` unless `.after()` set it.
- On Postgres a completion reads one more row, which measured about 6% on a loop that only claims
  and completes; [Measured](measured.md) has the numbers.

Upgrading to 0.16 needs no stop. On Django, `task_finished` now fires after the queue recorded
the outcome, from a thread of the worker, with a result read back from the queue rather than the
object the task's context holds; a receiver that raises no longer fails the task; a timeout now
fires it, and a job cancelled while it runs no longer does. `@app.task` now refuses options it used to accept in silence:
a `max_attempts` below 1, a `timeout`, `retry_base` or `retry_max` that is not a finite number of
seconds (`timeout` above zero), a `retry_backoff` other than `"exponential"`, `"fixed"` or a
function, a `retry_jitter` that is not a bool, and a `retry_on` holding anything but exception
classes.

Upgrading to 0.15 adds a status and a limits table (keys, on Redis) to every store, a stop-the-world step; [Limits](limits.md) has it.

Upgrading to 0.14 renames the SQLite tables, a stop-the-world step; [SQLite](sqlite.md) has it.

Upgrading from 0.6: stop every 0.6 worker before starting 0.7. The first 0.7 process migrates the
store, and from then on a 0.6 process refuses to start against it. 0.7 also needs Python 3.12 or
newer.

The [design notes](design.md) explain the design and why it looks like this, including the parts not
built yet.
