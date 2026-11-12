# Changelog

What changed in each release of tinyjobs, newest first, in the layout of [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/). What a release is allowed to change is on the
[compatibility page](https://tinyjobs.readthedocs.io/en/latest/compatibility/). Store schema
versions are the `SCHEMA_VERSION` of each backend.

## 0.18.0 - unreleased

### Added
- `tinyjobs.NewerStoreError`, raised when a store was upgraded by a release this one cannot work
  beside.
- Importable from `tinyjobs`: the exceptions a caller can meet and could only import from
  `tinyjobs.exceptions` (`AppClosedError`, `BackendNotReadyError`, `UnsupportedCapabilityError`,
  `SynchronousCallInsideEventLoopError`, `InterruptedCallError`, `EventLoopStoppedError`,
  `ExecutorClosedError`, `TaskAlreadyRegisteredError`, `UnknownCodecError`, `ForcedInterrupt`), and
  the types the app returns or takes: `Stats`, `QueueStats`, `Page`, `Finding` and `Clock`.
- A compatibility page: what is public, what a release may change, and what has to be true before
  1.0. This changelog.
- For backend authors: a store without `claim_job` passes the contract, as the documentation said;
  the tests that claim a job by its id are skipped for it or run without that step. The in-memory
  template in `tests/test_async_backend.py` now passes the contract.

### Changed
- A store too new for the running release is refused with `NewerStoreError` instead of a bare
  `RuntimeError`. It is still a `RuntimeError`, so code catching that keeps working. A command
  that meets one prints `error: ...` and exits with status 2, where it printed a traceback and
  exited with status 1.
- `from tinyjobs import *` brings in the names listed under Added, among them `Page`, `Clock`,
  `Stats` and `Finding`, which can shadow a name of your own imported before it.

### Upgrading
- No schema change and no stop needed: 0.17 and 0.18 processes work on the same store, which every
  release now checks on SQLite, Postgres and Redis. From 0.16 or earlier, follow the 0.17 note
  first.

## 0.17.0 - 2026-11-12

### Added
- `after=job` (the `Job` or its id): a job waits in the new `waiting` status until another job
  succeeds, and is cancelled, with every job waiting behind it, when that job fails for good or is
  cancelled. Works with `enqueue`, `app.send` and `tinyjobs send --after`.
- `tinyjobs.JobNotFoundError`, raised by `enqueue` when the job named in `after` is not in the store.
- On Django, `task.after(result)`; every task result reports the task it ran after and why it was
  cancelled.
- Every store records `compatible_since`, the oldest schema version whose code can run beside it,
  so later upgrades between neighbouring minors need no stop.

### Changed
- `JobStatus` has a new member, `waiting`. `tinyjobs stats` and the dashboard's queue table gain a
  column between held and running, so anything reading those tables by position is one column off;
  `/api/overview` counts it in every queue.
- Queued and held jobs are called "queued" everywhere: the dashboard's "Queued by task" (it was
  "Waiting by task"), the `check` finding "N queued job(s) for X" (it said "waiting"), and the
  help text of `tinyjobs_store_queued_jobs`. Alerts matching the old words need the new ones.
- `jobs retry` and `jobs cancel` word their refusals and help differently, and so do the
  dashboard's notices after a retry.
- A job's page and every job in `/api/jobs` show the job it runs after; `/jobs?status=waiting` is a
  page instead of a 400; `tinyjobs_store_jobs` can carry `status="waiting"`.
- On Django every task has a `follows` field, `None` unless `.after()` set it.
- On Postgres a completion reads one more row (about 6% on a loop that only claims and completes).

### Fixed
- `ready()` right after a close no longer raises the `AppClosedError` of a connection attempt that
  the close had beaten.
- A job process killed by a signal is reported with the signal's name even when its exit status
  arrives late.

### Upgrading
- Schema: SQLite 5 to 6, Postgres 2 to 3, Redis 3 to 4 (a new column, a field on Redis).
- Stop every tinyjobs process on a store before starting the first 0.17 one. It upgrades the store
  on its first call, and older processes then refuse to start against it. Going back to 0.16 means
  restoring a backup taken before the upgrade.

## 0.16.0 - 2026-11-11

### Added
- The tinyjobs task options on `django.tasks`: `timeout`, `max_attempts` and the `retry_*`
  options, `concurrency`, `rate`, `isolated` and `result_ttl`, on Django's own `@task` (or
  `tinyjobs.contrib.django.task` on Django 6.0), or by import path under the `"tasks"` option in
  the settings, where they win over the decorator.
- Isolated tasks on Django run inside the job process, with `django.setup()` done there first.

### Changed
- On Django, `task_finished` fires from a worker thread after the queue has recorded the outcome,
  with the result read back from the queue. A receiver that raises no longer fails the task; a
  timeout now fires it; a job cancelled while it runs no longer does.
- `@app.task` raises `ConfigurationError` for options it used to accept silently: `max_attempts`
  below 1; a `timeout`, `retry_base` or `retry_max` that is not a finite number of seconds
  (`timeout` above zero); a `retry_backoff` other than `"exponential"`, `"fixed"` or a function; a
  `retry_jitter` that is not a bool; a `retry_on` holding anything but exception classes.

### Upgrading
- No schema change and no stop needed.

## 0.15.0 - 2026-11-09

### Added
- Per-task limits: `@app.task(concurrency=N, rate="10/m")`, stored in the store and enforced at
  the claim across every worker. `tinyjobs.Limit` is exported.
- A `held` job status for jobs a limit stops; they keep their dedup key, can be cancelled, and are
  admitted in queue order when the task has room.

### Changed
- `JobStatus` has a new member, `held`. `tinyjobs stats` and the dashboard's queue table gain a
  column for it between queued and running.
- The `check` finding about jobs for an unregistered task says "waiting job(s)" instead of
  "queued job(s)"; the dashboard's "Queued by task" became "Waiting by task".
- `jobs cancel` and the dashboard accept held jobs; refusal messages mention "held".
- Eager mode ignores limits; `app.drain()` honours them.
- On Django a held job reads as `READY`.

### Upgrading
- Schema: SQLite 4 to 5, Postgres 1 to 2, Redis 2 to 3 (the held status and a limits table, keys
  on Redis).
- Stop every tinyjobs process on a store before starting the first 0.15 one; older processes
  refuse to start against the upgraded store.

## 0.14.0 - 2026-11-08

### Added
- Transactional enqueue on SQLite: `tx=` takes a `sqlite3` connection or cursor on the app's file,
  and the job commits or rolls back with the caller's transaction. A process whose first call is
  such an enqueue installs the schema through the caller's connection.
- `prefix=` on SQLite, so two apps can share one file or the queue can live in the application's
  own database.
- Django's `"database"` option also works on a SQLite alias whose `NAME` is the file in `url`.

### Changed
- The SQLite tables are renamed `tinyjobs_jobs`, `tinyjobs_executions`, `tinyjobs_schedules` and
  `tinyjobs_meta` (they were `jobs`, `executions`, `schedules` and `meta`).

### Upgrading
- Schema: SQLite 3 to 4.
- Stop every tinyjobs process on the file, workers and enqueuing application processes alike,
  then start one 0.14 process: it renames the tables in place and keeps every row. A worker left
  running fails every call until stopped, and a job it held runs again when its lease expires. A
  pre-0.14 file is migrated under the default prefix only.

## 0.13.0 - 2026-11-06

### Added
- `tinyjobs check` audits the store's configuration against the guarantees: SQLite journal mode,
  `synchronous`, filesystem and free space; Redis persistence, eviction policy, memory and role;
  Postgres `fsync`, `synchronous_commit` and standby.
- `tinyjobs check --store-warnings` (`app.check(store_warnings=True)`) reports those findings as
  `WARN` without failing.
- `tinyjobs verify`, which kills workers under steady load on a store of its own and checks that no
  job is lost and every side effect is applied once.
- A guarantees page in the documentation.

### Changed
- `tinyjobs check` exits 1 on store configuration findings (for example a Redis without
  `appendonly`, or a SQLite file not in WAL mode) that passed before; use `--store-warnings` on
  development setups.
- Every Postgres connection tinyjobs opens sets `synchronous_commit` to `on` where the server
  default is `off`.
- A SQLite app refuses options the SQLite backend does not take (anything but `busy_timeout`) with
  `ConfigurationError` when it is built.
- The documentation moved from the README into one page per topic.

### Upgrading
- No schema change. Check readiness probes that run `tinyjobs check` against development stores.

## 0.12.0 - 2026-11-06

### Added
- `tinyjobs dashboard` (on `127.0.0.1:8070` by default, `--host`/`--port`): queues and health,
  jobs with their attempts and tracebacks, schedules, with retry, cancel, pause, resume and run.
  No dependencies; no login, meant to sit behind a reverse proxy.
- JSON endpoints `/api/overview`, `/api/jobs`, `/api/jobs/<id>` and `/api/schedules`.
- `tinyjobs.dashboard.wsgi(app)` and `asgi(app)` to mount the pages in an existing app at any
  prefix.

### Upgrading
- No schema change.

## 0.11.0 - 2026-11-05

### Added
- `tinyjobs schedules pause` / `resume` / `run`, and `app.pause_schedule()`,
  `app.resume_schedule()`, `app.run_schedule()`. A paused schedule lists as `paused` and shows as
  an ok line in `check`.
- `TinyJobs(max_result_bytes=)`: a larger result fails the job with `ResultTooLarge`, without
  retry. New exceptions `ResultError`, `ResultTooLarge`, `UnencodableResult`.
- `@app.task(result_ttl=)` to drop a stored result after a while while keeping the job.
- Job fields (`job_id`, `task`, `queue`, `attempt`, `worker_id`, `duration_ms`, and on failure
  `error`, `error_message`, `will_retry`, `traceback`) on every log record, and `--log-format json`
  on every command; `tinyjobs.logs.JSONFormatter` and `TextFormatter`.
- A Prometheus exporter: `tinyjobs worker --metrics-port` (`--metrics-host`), and
  `app.metrics.render()` for embedded workers.
- An operating guide in the documentation.

### Changed
- `tinyjobs schedules` takes subcommands; with none it lists, as before.
- Durations given on the command line must be finite.

### Upgrading
- No schema change. Upgrade every worker, scheduler and `tinyjobs schedules` to 0.11 before pausing
  a schedule: older ones read a pause as a clock that moved back and un-pause it, and an older
  `tinyjobs schedules` fails to list it.

## 0.10.0 - 2026-11-05

### Added
- `@app.task(isolated=True)` runs a `def` task in a reusable job process: hard timeouts (the
  process is killed), a crash fails the job with `ProcessDied`, and the process group is killed on
  stop.
- Worker options `processes`, `jobs_per_process` and `process_setup`, with `--processes`,
  `--jobs-per-process` and `--process-setup`; `TinyJobs(process_setup=)`.
- `ProcessDied` and `IsolatedTaskError` exceptions. Exceptions raised in a job process come back
  with their own type and traceback, so `retry_on` matches them.

### Upgrading
- No schema change.

## 0.9.0 - 2026-11-04

### Added
- On free-threaded builds such as 3.14t the worker keeps the GIL off, and `def` tasks run in
  parallel.
- `loops=` on the worker and `tinyjobs worker --loops N`, to run several event loops in one
  process (above 1 it is refused on any store but Redis and Postgres, and in an embedded worker).

### Changed
- `worker.run()` and `tinyjobs worker` open a connection pool of their own for claims, outcomes and
  cron, plus one per extra loop, besides the app's pool, which opens only when your code uses it.
  Size Postgres `max_connections` or the PgBouncer pool per pool, not per process.

### Fixed
- On Redis, jobs whose claim reply was lost are handed back when the worker closes.

### Upgrading
- No schema change.

## 0.8.0 - 2026-11-03

### Added
- A Postgres backend (`pip install 'tinyjobs[postgres]'`, Postgres 14 to 18), with
  `LISTEN`/`NOTIFY` wake-ups (`notify=False` to turn off), `prefix=` and `schema=`, and the
  server's clock.
- Transactional enqueue on Postgres: `tx=` takes a psycopg connection or cursor, sync or async.
- Django's `"database"` option: every enqueue joins that Postgres database's
  `transaction.atomic()`.

### Changed
- Eager mode runs only the job it sent, ahead of any older job, and returns it finished. Earlier
  releases ran every due job on each eager call; call `app.drain()` to run the rest of the queue.
  A backend plugin without `claim_job` keeps the old behaviour.
- Shutdown fits inside `shutdown_timeout` (30 s), with time kept for handing back running jobs and
  closing connections. A second Ctrl-C under `tinyjobs worker` now cancels running jobs and hands
  them back at once; a job that ignores interrupts makes the process exit with status 130 a second
  later.
- A threaded `def` task past its timeout counts against `concurrency` until its thread returns, and
  a warning names the task. The timeout counts from when the job's thread starts.
- A retention sweep that reaches its cap of 50,000 jobs runs another straight away.
- On Redis and Postgres a worker keeps retrying a job's outcome during a server outage, up to a
  second before the lease runs out, so a job finished during a short outage is not run again.

### Fixed
- Redis scripts the server lost (after a restart or `SCRIPT FLUSH`) are reloaded.
- Every backend call and every stop is bounded by a deadline, so an outage, a stop signal or a stuck
  thread no longer hangs a worker.

### Upgrading
- No schema change (Postgres starts at schema 1). Check tests that relied on eager mode running
  older queued jobs.

## 0.7.3 - 2026-10-27

### Added
- Documentation published on Read the Docs, linked from PyPI.

## 0.7.2 - 2026-10-27

### Changed
- README soak figures measured on a Linux VPS, on SQLite and on Redis. No code changes.

## 0.7.1 - 2026-10-27

### Changed
- On Redis, leases, due times, reaping, retention and job ids follow the Redis server's clock
  instead of each machine's, so machines with skewed clocks no longer reclaim each other's running
  jobs. A `run_at` or `Retry(at=...)` you pass is moved onto the server's clock. A `clock=` you
  pass is used as given.
- On Django, results report `run_after` only while the job waits for its first claim.

### Upgrading
- On Redis, 0.7.0 used each machine's own clock. Where machine clocks drift from the Redis server's
  by more than a lease, stop or upgrade every 0.7.0 process, workers and senders alike, before
  starting 0.7.1 workers. A Redis user restricted by ACLs needs `TIME` granted.

## 0.7.0 - 2026-10-26

### Added
- A `django.tasks` backend, `tinyjobs.contrib.django.TinyJobsBackend`, with queues, priorities,
  `run_after`, async tasks, `takes_context`, `get_result`, the task signals, and
  `manage.py tinyjobs` for every CLI command.

### Changed
- Requires Python 3.12 or newer.
- A tinyjobs call made from a signal handler that lands while the thread holds a tinyjobs lock or
  write raises `InterruptedCallError` instead of hanging.
- A call on a closed thread executor raises `ExecutorClosedError` (a subclass of `AppClosedError`).

### Fixed
- "database is locked" when two processes open a fresh SQLite file at once.
- Interrupted or abandoned SQLite writes are rolled back; concurrent and cancelled closes no longer
  stall the loop or leak threads; a forked child does not inherit a pending close.

### Upgrading
- Schema: SQLite 2 to 3, Redis 1 to 2.
- Stop every 0.6 worker before starting 0.7. The first 0.7 process migrates the store, and from
  then on a 0.6 process refuses to start against it.

## 0.6.0 - 2026-10-22

### Added
- `app.lifespan()` and `app.serve()` for FastAPI and Starlette, with an optional embedded worker.
- `max_connections=` and `pool_timeout=` for the Redis pool.

### Changed
- The Redis pool waits for a free connection instead of failing when all 100 are taken.
- A job whose task the worker does not know is handed back with a delay that doubles each time, up
  to `unknown_task_max_delay` (an hour), instead of every minute.
- A closed app no longer reconnects: `enqueue()`, `drain()` and a scheduler tick raise
  `AppClosedError`; `start()`, `serve()` or starting a worker open it again.

### Fixed
- Sync calls from a `def` endpoint on Redis failed every time; the sync facade now runs on the loop
  that connected the backend, and raises `EventLoopStoppedError` if that loop stops.
- `RetryPolicy` no longer overflows past attempt 1024.

## 0.5.0 - 2026-10-12

### Added
- A Redis backend, `pip install 'tinyjobs[redis]'` (schema 1).
- `tinyjobs.testing.BackendContract`, the test suite any backend plugin can run.

### Fixed
- A job whose outcome could not be recorded because the store was unreachable is no longer
  recorded as a task failure; it runs again once its lease expires.

## 0.4.0 - 2026-10-10

### Added
- Cron schedules (`@app.task(cron=..., timezone=..., misfire=..., overlap=...)`), fired
  leader-free from every worker, with `tinyjobs scheduler`, `tinyjobs schedules`,
  `--no-scheduler` and `--misfire-grace`.
- Lifecycle hooks `on_job_start`, `on_job_success`, `on_job_failure`.
- `memory://`, `eager=True`, `app.drain()` and `FrozenClock`, now exported, for testing.
- `tinyjobs stats` and `tinyjobs check` (with `--max-wait`), and `app.stats()` / `app.check()`.

## 0.3.1 - 2026-09-03

### Changed
- `tinyjobs purge` prints durations as `1w`, `30d` instead of seconds.

### Fixed
- `tinyjobs purge` failed with `BackendNotReadyError` when it was the first command run.

## 0.3.0 - 2026-09-03

### Added
- Retention: finished jobs are pruned automatically, successes and cancellations after a week,
  failures after thirty days (`RetentionPolicy`, `KEEP_EVERYTHING`), with worker flags
  `--retain-success`, `--retain-cancelled`, `--retain-failed`, `--purge-interval`, `--no-purge`.
- `tinyjobs purge`, with `--before` and `--dry-run`.

### Changed
- Finished jobs are now deleted after the retention period by default.

### Fixed
- `tinyjobs jobs list --status` sorted every matching row on large databases; it now has an index
  in the order it lists.

### Upgrading
- Schema: SQLite 1 to 2, migrated automatically on first open.

## 0.2.0 - 2026-09-02

### Added
- `Job.wait()` and `Job.awaited()`, with an optional timeout that raises `WaitTimeout` (also a
  `TimeoutError`).

### Fixed
- The CLI reported the wrong version (0.1.1 said 0.1.0); the version now comes from the package
  metadata.

## 0.1.1 - 2026-09-02

### Added
- `Job.refresh()` and `Job.result()`, which returns the decoded result of a `store_result=True`
  task.

### Changed
- `Job.result` (the stored bytes) is renamed `Job.result_payload`.

## 0.1.0 - 2026-09-02

### Added
- First release: background jobs on SQLite, with no dependencies, on Python 3.11 or newer.
- Task registry, `task.enqueue()` with signature validation, `app.send()` by name.
- Async backend protocol with a synchronous facade; atomic claim, leases and a reaper.
- Worker with async and threaded executors, timeouts and graceful shutdown on `SIGTERM`.
- Retries with backoff and jitter, priorities, named queues, delayed jobs, deduplication keys.
- `tinyjobs worker`, `jobs` and `send`, also as `python -m tinyjobs`; the backend plugin seam.
