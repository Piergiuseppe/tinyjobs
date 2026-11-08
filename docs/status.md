# Status

v0.14.0. Working and measured, but young. What's in:

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

What's not in yet:

- per-task concurrency and rate limits
- jobs that run after another

Upgrading to 0.14 renames the SQLite tables, a stop-the-world step; [SQLite](sqlite.md) has it.

Upgrading from 0.6: stop every 0.6 worker before starting 0.7. The first 0.7 process migrates the
store, and from then on a 0.6 process refuses to start against it. 0.7 also needs Python 3.12 or
newer.

The [design notes](design.md) explain the design and why it looks like this, including the parts not
built yet.
