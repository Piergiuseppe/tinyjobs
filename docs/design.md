# tinyjobs design notes

Why tinyjobs is built the way it is. The README covers usage; this covers the reasoning, including
for the parts that are not written yet. Sections describing unbuilt features say so.

Contents:

- [Delivery semantics](#delivery-semantics)
- [Core concepts](#core-concepts)
- [Data model](#data-model)
- [Lifecycle](#lifecycle)
- [Layering: who knows what](#layering-who-knows-what)
- [Serialization](#serialization)
- [The backend protocol](#the-backend-protocol)
- [The SQLite backend](#the-sqlite-backend)
- [The scheduler](#the-scheduler)
- [Execution model](#execution-model)
- [Retries](#retries)
- [Results](#results)
- [Public API](#public-api)
- [Operations](#operations)
- [Testing](#testing)
- [On writing C extensions](#on-writing-c-extensions)
- [Roadmap](#roadmap)

---

## Delivery semantics

**tinyjobs guarantees at-least-once execution.**

The failure window is narrow but real: a worker can finish a job and die before it writes `SUCCESS`
back to the store. The lease on that job eventually expires, a reaper requeues it, and the job runs a
second time. There is no way to close this window without a transaction that spans both the queue and
whatever your task touches.

The practical consequence is a contract with you, the author of the task:

> **Task functions must be idempotent.** Running one twice with the same arguments must be
> indistinguishable from running it once.

In practice this means: upsert instead of insert, check-then-act inside a transaction, use the job id
as a natural deduplication key for external calls, and make external API calls with an idempotency
key of their own.

Where we *can* help:

- **Enqueue-side deduplication.** `with_options(key="invoice-sync:123")` refuses to create a second
  job while one with the same key is pending or running. This makes *enqueueing* effectively-once,
  which is the half of the problem that actually shows up in practice: a retried HTTP request
  submitting the same job three times.
- **Attempt visibility.** Every attempt is recorded as an `Execution` row, so a duplicate run is
  visible after the fact rather than silent.
- **Transactional enqueue.** With the SQLite backend, `enqueue` can join your application's own
  transaction: if the transaction rolls back, the job is never created. This removes the classic
  dual-write bug where a job starts running against a row that was never committed. It is a genuine
  advantage of a database-backed queue over a broker-backed one; see
  [Transactional enqueue](#transactional-enqueue) for the API and its one hard constraint.

---

## Core concepts

Four entities. Keeping them distinct is what makes retries, cron and monitoring simple instead of
tangled.

### Task: the registered function

```python
@app.task
async def generate_pdf(document_id: int) -> str:
    ...
```

A Task is a declaration: a callable, a stable name, and its default options (retries, timeout, queue,
priority). It lives for the lifetime of the process and is never persisted. Only its *name* is.

That name is the contract between the code and the store. It defaults to `module.function`, but it is
overridable (`@app.task(name="pdf.generate")`) and **should be** for anything long-lived: renaming a
module must not orphan the jobs already sitting in the queue.

### Job: a request to execute

```python
job = generate_pdf.enqueue(123)
```

A Job is a persistent record: this task, these arguments, this status. It is created by `enqueue`,
survives restarts, and is the unit users cancel, inspect and query.

### Execution: a single attempt

One Job produces one or more Executions:

```
Job #123
├── attempt 1 → FAILED    (ConnectionError, 0.4s, worker-a)
├── attempt 2 → FAILED    (ConnectionError, 0.4s, worker-a)
└── attempt 3 → SUCCESS   (2.1s, worker-b)
```

This is the piece most queue libraries omit, and its absence is felt immediately in production: with
attempts collapsed into the job row, you lose every traceback but the last one, you cannot tell a job
that succeeded first try from one that barely survived, and you cannot chart failure rates. An
Execution row carries its own status, timings, worker id, and traceback on failure.

### Schedule: a rule that creates jobs

```python
@app.task(cron="*/5 * * * *")
async def refresh_cache():
    ...
```

A Schedule is *not* a Job. It is a rule with a next firing time; when that time arrives it **creates**
a Job and computes the next one. Keeping it separate means cron jobs get ordinary retry behaviour,
ordinary history, and ordinary cancellation, with no special-casing anywhere in the worker.

### How they flow

```
   @app.task                      Schedule (a coroutine in every worker)
       │                               │
       │ registers                     │ cron fires
       ▼                               │
     Task ─────── enqueue() ──────►  Job  ◄────────────┘
                                      │
                                      │ claimed by a worker
                                      ▼
                                  Execution ── retry ──► Execution
```

---

## Data model

Sketch of the SQLite schema. Other backends are free to store this differently, as long as the
protocol contract holds.

```sql
CREATE TABLE jobs (
    id              TEXT PRIMARY KEY,        -- UUIDv7: sortable by creation time
    task            TEXT NOT NULL,           -- registered task name
    args            BLOB NOT NULL,           -- serialized (args, kwargs)
    codec           TEXT NOT NULL,           -- 'json' | 'pickle' | ...
    queue           TEXT NOT NULL DEFAULT 'default',
    priority        INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 1,
    run_at          REAL NOT NULL,           -- earliest execution time (unix)
    created_at      REAL NOT NULL,
    finished_at     REAL,
    lease_expires_at REAL,                   -- NULL unless RUNNING
    worker_id       TEXT,
    dedup_key       TEXT,
    result          BLOB,
    last_error      TEXT
);

-- The claim query is the hottest path in the system; this index defines it.
-- Partial, and in exactly the ORDER BY's column order. See the note below.
CREATE INDEX idx_jobs_claim ON jobs (queue, priority DESC, run_at ASC)
    WHERE status = 'queued';
CREATE INDEX idx_jobs_lease ON jobs (status, lease_expires_at);
CREATE UNIQUE INDEX idx_jobs_dedup ON jobs (dedup_key)
    WHERE dedup_key IS NOT NULL AND status IN ('queued', 'running');

CREATE TABLE executions (
    id          INTEGER PRIMARY KEY,
    job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INTEGER NOT NULL,
    status      TEXT NOT NULL,               -- running | success | failed | timeout | lost
    worker_id   TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL,
    error_type  TEXT,
    traceback   TEXT
);
CREATE INDEX idx_exec_job ON executions (job_id, attempt);

CREATE TABLE schedules (
    name         TEXT PRIMARY KEY,           -- task name
    cron         TEXT NOT NULL,
    timezone     TEXT NOT NULL,
    next_run_at  REAL NOT NULL,              -- the compare-and-set column
    last_run_at  REAL,
    last_job_id  TEXT
);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);  -- schema_version, ...
```

Notes on a few choices:

- **UUIDv7 job ids**, not autoincrement integers. They are sortable by time (so they behave like
  integers for pagination and index locality) but they can be generated client-side and remain unique
  across backends and across a future sharded store.
- **`run_at` on every job.** Delayed jobs, retry backoff and cron output are all just "a queued job
  with a future `run_at`". This is the single most load-bearing column in the schema: it lets one
  state (`QUEUED`) cover three features that would otherwise need states of their own.
- **`codec` per row**, not global. You can flip the default codec without breaking jobs already in
  flight, and a mixed-codec queue during a migration is legal by construction.
- **The claim index is partial, and column order matters.** `WHERE status = 'queued'` keeps the index
  holding only claimable rows, so completed history, however much of it accumulates, is invisible to
  the hot path. Measured on 200k rows of which 500 were queued: the index carried 500 entries and the
  claim `SELECT` ran in ~5 µs. The column order must match `ORDER BY priority DESC, run_at ASC`
  exactly; get it wrong and SQLite silently adds `USE TEMP B-TREE FOR ORDER BY`, sorting the result
  set on every single claim. This is what makes retention a pure disk-space policy rather than a
  performance one.
- **Partial unique index for deduplication.** Enforced by the database, not by a read-then-write in
  Python, which would race under concurrency.
- Timestamps are unix floats. Comparisons stay cheap, no timezone ambiguity in storage, and the
  presentation layer formats them.

---

## Lifecycle

### Job status

```
                          ┌─────────┐
                          │ CREATED │   transient: the row is written as QUEUED
                          └────┬────┘
                               ↓
                          ┌─────────┐
      ┌──────────────────►│ QUEUED  │◄─────────────────┐
      │                   └────┬────┘                  │
      │                        │ claim (run_at <= now) │
      │                        ↓                       │
      │                   ┌─────────┐                  │
      │                   │ RUNNING │                  │ lease expired
      │                   └────┬────┘                  │ (worker died)
      │                        │                       │
      │      ┌─────────────────┼─────────────────┐     │
      │      ↓                 ↓                 └─────┘
      │  ┌─────────┐      ┌────────┐
      │  │ SUCCESS │      │ FAILED │ (this attempt)
      │  └─────────┘      └───┬────┘
      │                       │
      │                  attempts < max
      │                  and retryable?
      │                    ╱        ╲
      │                  yes         no
      │                   │           │
      └───────────────────┘           ↓
        run_at = now + backoff   ┌────────┐
                                 │ FAILED │  terminal
                                 └────────┘

  From QUEUED or RUNNING, an explicit request moves the job to CANCELLED (terminal).
```

Terminal states: `SUCCESS`, `FAILED`, `CANCELLED`. Everything else is in flight.

**Job status and Execution status are different enums, and they answer different questions.** A job
whose attempt 1 failed is not `FAILED`. It goes back to `QUEUED` with a future `run_at`, while the
*execution* row for attempt 1 stays `FAILED` forever. `Job.FAILED` means one thing only: retries are
exhausted, this job is dead. That distinction is why a separate `RETRYING` job status is unnecessary.

**Cancellation is included from day one.** It costs an enum value and one predicate in the claim
query today; retrofitting it means migrating a live table later. Semantics: cancelling a `QUEUED` job
prevents it from ever being claimed. Cancelling a `RUNNING` job sets the flag and stops future
retries. The in-flight attempt is *not* killed, because Python cannot safely interrupt arbitrary
code. Async tasks may cooperate by checking `job.is_cancelled()`.

### Execution status

`RUNNING → SUCCESS | FAILED | TIMEOUT | LOST`

`LOST` is what the reaper writes when a lease expires: we know the attempt started, we will never
know how it ended. Keeping it distinct from `FAILED` matters: a spike in `LOST` means workers are
being OOM-killed, which is a completely different investigation from a spike in `FAILED`.

---

## Layering: who knows what

```
  user code
      │
   Task           name, callable, default options
      │
    App           registry, configuration, codec selection
      │
  Backend         jobs, state, leases, persistence      ← the plugin seam
      │
   Worker         claim → execute → record → repeat
      │
  Executor        async / thread / process
      │
  callable
```

The rule that keeps this honest: **the Backend never knows how a function is executed, and the
Executor never knows how state is stored.** The Worker is the only component that touches both, and
it is kept as dumb as possible: a loop.

The Scheduler sits beside the Worker, not above it. It knows about cron expressions and next firing
times. It never executes anything; it only creates jobs.

---

## Serialization

This is the decision with the longest shadow, so the reasoning is written out in full.

### Decision

**JSON with typed extensions is the default codec. Pickle is available, per-task or per-job, and is
disabled unless explicitly enabled in configuration.** The codec name is stored on every job row.

### Why not pickle by default

Pickle is the obvious choice. It serializes almost anything, it is in the standard library, and it
is what Celery originally did. It is still the wrong default, for four reasons that compound:

1. **It becomes a remote code execution sink the moment the store is shared.** `pickle.loads` will
   execute arbitrary code by design (`__reduce__`), so deserializing is equivalent to executing
   whatever produced the payload. In a local SQLite file that is roughly as trusted as your own
   source tree. But the roadmap here includes Redis and RabbitMQ backends: network
   services, frequently unauthenticated in development, occasionally exposed by accident. Anyone able
   to write a job row would gain code execution inside every worker. Choosing pickle now is designing
   in a vulnerability that opens by itself later, when the plugin ships.

2. **It throws away the main benefit of a database-backed queue.** With JSON, diagnosing a bad job is
   `SELECT task, args FROM jobs WHERE status = 'failed'`. With pickle it is an opaque blob that only
   a Python process with the right modules importable can even read. No `sqlite3` shell, no `grep`,
   no dashboard, no support engineer looking at a customer's database.

3. **It couples the payload to your code, exactly when you can least afford it.** A job pickled by
   code that defines `Invoice` with three fields, unpickled by a worker still running the two-field
   version, raises on load. During a rolling deploy you have mixed worker versions *by definition*,
   that is what rolling means. JSON degrades (an unknown field is ignored); pickle fails closed. The
   same applies across Python upgrades and any refactor that moves a class between modules.

4. **The use case does not need it.** Task arguments should be identifiers, not objects. `enqueue(123)`
   not `enqueue(document)`. This is not merely a serialization preference: a job may sit in the queue
   for minutes, then be retried ten minutes after that. A task carrying a snapshot of an object is
   operating on stale data by the time it runs, which is a correctness bug independent of how the
   snapshot was encoded. Reloading from the source of truth inside the task is the correct pattern,
   and it needs nothing richer than a primary key. **A codec that cannot serialize your arguments is
   usually telling you the arguments are wrong.**

### What the default codec actually handles

Plain JSON is too thin: no `datetime`, no `Decimal`, no `UUID`. So the default codec is JSON plus a
tagged-value convention, roughly 60 lines of standard library:

```python
{"$t": "datetime", "v": "2026-08-31T17:34:00+02:00"}
{"$t": "decimal",  "v": "19.99"}
{"$t": "uuid",     "v": "..."}
{"$t": "set",      "v": [1, 2, 3]}
{"$t": "bytes",    "v": "<base64>"}
```

Covered out of the box: `str`, `int`, `float`, `bool`, `None`, `list`, `dict`, `tuple`, `set`,
`frozenset`, `bytes`, `datetime`, `date`, `time`, `timedelta`, `Decimal`, `UUID`, `Path`, `Enum`, and
`Path`. Anything whose reconstruction needs a class (a `@dataclass`, an `Enum`, a domain object)
must be **registered**, and this is a deliberate refusal rather than a gap:

```python
app.codec.register_dataclass(Money)
app.codec.register_enum(Priority)
app.codec.register_type(MyAdapter())
```

The alternative would be to store a class path in the payload and import it on decode, which hands
anyone able to write a job row an arbitrary-import primitive, and therefore arbitrary module-level
code execution inside every worker. That is the same hole pickle opens, reached by a different route,
so the same answer applies: nothing gets imported by name on the strength of the payload.

Encoding an unregistered type raises `SerializationError` at **enqueue** time, in the caller's own
traceback, naming the type and the three registration helpers. One line per type, once, and it makes
refusing pickle reasonable: an explicit, reviewable, safe conversion instead of an implicit one that
also executes code.

### The pickle escape hatch

Some payloads genuinely resist a clean encoding, and a library that says "no" to them is a library
people work around badly. So pickle stays, gated:

```python
app = TinyJobs("sqlite:///jobs.db", allow_pickle=True)   # off by default

@app.task(codec="pickle")
def train_model(model): ...
```

Enabling it logs a warning naming the security tradeoff. Backends that transport jobs over a network
may refuse it outright via configuration. Because `codec` is stored per job, none of this is a
one-way door: you can migrate task by task, in either direction, with a mixed queue in between.

### Rejected alternatives

- **`marshal`**: fast, stdlib, but the format is explicitly documented as unstable across Python
  versions. Unusable for anything persisted.
- **JSON only, no extensions**: pushes `datetime` handling into every task in the codebase.
- **A bundled MessagePack/CBOR implementation**: better wire size, but no better type coverage than
  extended JSON, and vendoring an encoder contradicts the zero-dependency goal for a gain that is
  irrelevant at this scale. Worth revisiting only as an optional codec if payload size ever becomes a
  measured problem.

---

## The backend protocol

The seam that makes storage swappable. It is narrow on purpose, and synchronous on purpose;
see [Public API](#public-api) for why.

```python
class Backend(Protocol):

    # --- lifecycle -------------------------------------------------------
    def setup(self) -> None: ...              # create schema, run migrations
    def close(self) -> None: ...

    # --- producer side ---------------------------------------------------
    def enqueue(self, job: Job) -> Job: ...
    def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]: ...   # see note
    def transaction(self) -> AbstractContextManager[Transaction]: ...

    # --- consumer side ---------------------------------------------------
    def claim(self, *, worker_id: str, queues: Sequence[str],
              limit: int, lease_seconds: float) -> list[Job]: ...
    def heartbeat(self, job_ids: Sequence[str], lease_seconds: float) -> None: ...
    def complete(self, job_id: str, result: bytes | None) -> None: ...
    def fail(self, job_id: str, error: ExecutionError,
             retry_at: float | None) -> None: ...

    # --- control ---------------------------------------------------------
    def cancel(self, job_id: str) -> bool: ...
    def get(self, job_id: str) -> Job | None: ...
    def list(self, *, status: str | None = None, queue: str | None = None,
             limit: int = 100, cursor: str | None = None) -> Page[Job]: ...
    def stats(self, now: float) -> Stats: ...   # counts per queue, oldest due, expired leases

    # --- schedules --------------------------------------------------------
    def schedules(self) -> list[ScheduleState]: ...
    def register_schedules(self, states: Sequence[ScheduleState]) -> None: ...
    def fire_schedule(self, name: str, *, expected: float, next_run_at: float,
                      jobs: Sequence[Job]) -> list[Job] | None: ...

    # --- housekeeping -----------------------------------------------------
    def reap_expired(self, now: float) -> int: ...
    def purge(self, *, before: float, statuses: Sequence[str]) -> int: ...
```

`enqueue_many` earns its place as a distinct method rather than a loop in the core, because **the
transaction boundary is the optimization**. A thousand separate `enqueue` calls on SQLite means a
thousand fsyncs; the same thousand inside one transaction means one, which is two orders of magnitude.
Redis backends will want pipelining, Postgres a multi-row `INSERT`. None of that is reachable if the
core does the looping. And since the protocol freezes at v1.0, a seam that is not there at v1.0 cannot
be added later, so it goes in now, with a default implementation in the base mixin (`transaction()`
around a loop over `enqueue()`) so that simple backends get the correct behaviour for free.

### The contract a backend must honour

The signatures are the easy part. These invariants are what an implementation is actually promising,
and a conformance test suite ships with the package to check them:

1. **`claim` is atomic.** A job is handed to exactly one worker. Two concurrent claims never return
   the same job. This is the one hard requirement; everything else is comfort.
2. **`claim` respects `run_at`.** Never return a job scheduled for the future.
3. **`claim` respects `status`.** Never return a cancelled or terminal job.
4. **A claimed job carries a lease.** If the holder stops calling `heartbeat`, `reap_expired`
   eventually makes the job claimable again.
5. **`complete` and `fail` are idempotent**, and both are no-ops on a job the caller no longer holds
   the lease for. A worker that stalls past its lease, then wakes up and reports success, must not
   overwrite the result of the replacement attempt.
6. **`enqueue` with a duplicate active `dedup_key` returns the existing job** rather than creating a
   second one.
7. **`fire_schedule` is a compare-and-set.** It advances `next_run_at` only if it still holds
   `expected`, and inserts the jobs in the same transaction, so a firing is either fully recorded or
   not at all. A caller that loses the race gets `None` and creates nothing.
8. **`register_schedules` keeps a stored `next_run_at`** unless the cron or the timezone changed.
   That is what makes a restart pick up where it left off, and what lets the misfire policy see the
   firings it missed.

### Capabilities

Not every store can do everything. A Redis list has no cheap priority ordering, for instance.
Backends declare what they support so the core can degrade predictably instead of failing at runtime:

```python
class Capabilities(NamedTuple):
    priority: bool
    delayed: bool
    dedup: bool
    transactional_enqueue: bool
    retention: bool
    schedules: bool
    max_payload_bytes: int | None
```

Requesting an unsupported feature raises at *enqueue* time with a clear message, not silently later.

### Plugin discovery

Third-party backends register through entry points. `importlib.metadata` is stdlib, so this stays
dependency-free:

```toml
# in tinyjobs-redis's pyproject.toml
[project.entry-points."tinyjobs.backends"]
redis = "tinyjobs_redis:RedisBackend"
```

The user's side of the swap, in full:

```python
app = TinyJobs("sqlite:///jobs.db")     # before
app = TinyJobs("redis://localhost/0")   # after
```

The URL scheme selects the backend. The core package never imports, references or knows the names of
optional backends; it looks up the scheme in the entry point group and raises a helpful error
(`pip install tinyjobs-redis`) if nothing is registered for it. Bundled: `sqlite://`, `memory://`.

---

## The SQLite backend

The default, and the one that has to be right. SQLite is a good queue substrate. It is
transactional, it fsyncs, and in WAL mode readers never block the writer, provided you respect its
one constraint: **a single writer at a time.**

### Connection setup

```sql
PRAGMA journal_mode = WAL;         -- readers don't block the writer
PRAGMA synchronous  = NORMAL;      -- durable enough with WAL; FULL on request
PRAGMA busy_timeout = 5000;        -- wait for the write lock instead of failing
PRAGMA foreign_keys = ON;
```

Every write transaction is opened as `BEGIN IMMEDIATE`. Taking the write lock upfront turns lock
contention into a bounded wait instead of a mid-transaction `SQLITE_BUSY` and a forced retry, the
classic source of "database is locked" in multi-process SQLite.

One connection per thread (`check_same_thread=True`), never shared across threads.

### The claim

The heart of the system. On SQLite >= 3.35 it is one statement, atomic by definition:

```sql
UPDATE jobs
   SET status = 'running',
       worker_id = :worker,
       attempts = attempts + 1,
       lease_expires_at = :now + :lease
 WHERE id IN (
       SELECT id FROM jobs
        WHERE queue IN (:queues)
          AND status = 'queued'
          AND run_at <= :now
        ORDER BY priority DESC, run_at ASC
        LIMIT :batch
 )
RETURNING *;
```

Verified against SQLite 3.51. On older libraries (Debian 11 ships 3.34) the backend falls back to
`SELECT` ids, then `UPDATE ... WHERE id = ? AND status = 'queued'` inside `BEGIN IMMEDIATE`, checking
`rowcount` to detect a lost race. Same guarantees, one extra round trip. The version check happens
once at `setup()`.

### Polling

SQLite has no `LISTEN/NOTIFY`, so workers poll. Naive polling either burns CPU or adds latency, so the
interval adapts: after a claim that returned work, poll again immediately; after an empty claim, back
off geometrically from ~5 ms to a configurable ceiling (default 500 ms), with jitter so that N workers
starting together do not synchronize into a thundering herd.

For the local single-machine case we can do better than polling alone: an optional wakeup file
descriptor lets an in-process `enqueue` nudge a sleeping worker directly, dropping enqueue-to-start
latency to sub-millisecond without changing the polling fallback that covers cross-process enqueues.

### Crash recovery

A claimed job holds a lease. The worker refreshes it (`heartbeat`) at one third of the lease duration
while the job runs. If the worker is killed by SIGKILL, by the OOM killer or by a yanked power cable, the lease
simply expires.

Any worker can run the reaper; it is another single atomic statement:

```sql
UPDATE jobs
   SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
       lease_expires_at = NULL, worker_id = NULL,
       run_at = :now
 WHERE status = 'running' AND lease_expires_at < :now
RETURNING id;
```

The abandoned `Execution` rows are marked `LOST`. This is precisely the at-least-once window: the job
may have completed its side effects before dying. Hence the idempotency contract.

### Housekeeping

Retention is on by default, and it is a disk-space policy, nothing more. The partial claim index
means history never touches the hot path, so "how much do I keep" is decoupled from "how fast is my
queue". That is measurable: claiming a job takes the same 11 microseconds whether the table holds
nothing or half a million finished jobs. Defaults:

| Status      | Kept for | Why                                      |
| ----------- | -------- | ---------------------------------------- |
| `success`   | 7 days   | Enough to answer "did it run last week?" |
| `cancelled` | 7 days   | Same                                     |
| `failed`    | 30 days  | These are the ones you come back to      |

Set any of them to `0` to keep forever, or use `KEEP_EVERYTHING` to disable pruning entirely.

The sweep is claimed the same way a schedule is fired, with one conditional `UPDATE` on a marker row
in `meta`:

```sql
UPDATE meta SET value = :now
 WHERE key = 'last_purge_at'
   AND (CAST(value AS REAL) = 0 OR CAST(value AS REAL) <= :now - :interval)
RETURNING key;
```

Exactly one worker's `UPDATE` matches, so forty workers still produce one sweep per interval with no
leader to elect. The `= 0` arm exists because a fresh database starts the marker at zero, and without
it the first sweep would never be due on a clock whose value is smaller than the interval.

Deletion happens in bounded batches, oldest first, with a cap per sweep so a large backlog is worked
off over several intervals rather than in one transaction that stalls every claim. `executions` rows
follow their job through `ON DELETE CASCADE`, which the connection enables with
`PRAGMA foreign_keys = ON`.

Two indexes make this cheap, and both follow the same rule as the claim index: match the query, or
SQLite silently sorts. `idx_jobs_purge` is `(status, finished_at) WHERE finished_at IS NOT NULL`,
which matches the sweep exactly, and being partial it only holds terminal rows. Since a terminal job
is never updated again, only deleted, that index costs one write per job and nothing on the claim
path. Getting it wrong turns the sweep quadratic: measured on 100,000 expired jobs, with the index
0.22 seconds, without it 4.45 seconds, and the gap widens with size.

Pruning does not shrink the file. SQLite puts the freed pages on a freelist and reuses them, so the
database plateaus rather than shrinking, which is exactly what a soak run shows: flat at 6.9 MB for
as long as you care to watch. Returning space to the filesystem needs `VACUUM`, which wants exclusive
access, so it stays a manual command and never runs on its own.

### Where the limits are

Expect **low thousands of jobs per second** for enqueue and claim on a local SSD, dominated by fsync
rather than by Python. That covers most applications. It will not cover a high-volume ingestion
pipeline, and it degrades on network filesystems. SQLite's locking over NFS is unreliable, and the
backend refuses to run on a detected network mount rather than corrupting data quietly. Both cases are
the reason the backend seam exists.

---

## The scheduler

Cron looks like the easy part and contains most of the subtle bugs. What we commit to:

```python
@app.task(cron="*/15 * * * *", timezone="Europe/Rome", misfire="skip", overlap=False)
async def refresh_cache() -> None:
    ...
```

A cron task is an ordinary task with a rule attached. It can still be enqueued by hand, and the jobs
the rule creates are ordinary jobs: same retries, same history, same cancellation. It is called with
no arguments, so a signature that requires some is rejected when the decorator runs, as is a cron
expression, a timezone or a misfire policy that cannot work. A mistake surfaces at import, not at
midnight.

### Where it runs: inside every worker, by default

`tinyjobs worker` runs the scheduler as one more coroutine on its event loop. Not as a separate
process, and not on a designated leader.

This is the dividend of leader-free firing below. Because the compare-and-set guarantees exactly one
job per tick no matter how many schedulers race for it, running fifty schedulers is not a bug to be
prevented, it is free high availability. There is no leader to elect, no failover to test, and no
deployment where cron silently stops because one special process died. `--no-scheduler` opts out, and
`tinyjobs scheduler` runs one standalone for anyone who wants the isolation.

The cost is a polling loop per worker, which the tick makes negligible: the scheduler sleeps until the
earliest `next_run_at` rather than waking on a fixed interval, capped at 30 seconds so a worker that
slept on stale state rereads it soon. A tick that fires nothing is one `SELECT` over the `schedules`
table.

### Leader-free firing

Running three workers must not create three jobs per tick. The usual answer is leader election; ours
is cheaper, because the store already gives us atomicity:

```sql
BEGIN IMMEDIATE;
UPDATE schedules SET next_run_at = :computed_next
 WHERE name = :name AND next_run_at = :expected;
-- only if that matched one row:
INSERT INTO jobs ...;
COMMIT;
```

Exactly one worker's `UPDATE` matches. That one creates the job, the others get zero rows and move
on. The comparison is against the exact value the scheduler read, not against its own clock, so two
workers whose clocks disagree by a few seconds still cannot both win. And the job goes in inside the
same transaction as the advance, so a worker killed halfway leaves either both or neither: there is no
window where the schedule moved on and the job was never written.

Registration is the other half. Every scheduler upserts its declarations when it starts, and the
upsert keeps the stored `next_run_at` unless the cron or the timezone changed. A restart therefore
resumes where the previous process stopped, which is what lets the misfire policy see what it missed,
while a changed expression starts afresh from now.

### Timezones and DST

Schedules store an IANA timezone name and are evaluated with `zoneinfo` (stdlib). This makes the
awkward cases explicit rather than accidental:

- A fixed time of day, `30 2 * * *`, on the day a DST gap swallows 02:30. Policy: fire at the first
  valid instant after the gap, 03:00.
- The same schedule on the day 02:30 happens twice. Policy: fire once, on the first occurrence.
- A schedule whose hour field is a wildcard (`*/10 * * * *`, `0 * * * *`) is about frequency, not
  about a time of day, so it follows the clock as it actually ticks: the repeated hour runs twice, and
  the hour the gap removes does not run at all. The interval between firings stays what you wrote.
  This is the rule Vixie cron uses.
- The default timezone is UTC, which has none of these problems. Local time is opt-in. `"UTC"` never
  touches the tz database; any other name needs one, which Windows lacks unless `tzdata` is
  installed.

### Misfires

If the scheduler was down from 09:00 to 11:00 and a schedule fires every 15 minutes, that is eight
missed firings. Guessing here is how you get a thundering herd on restart, so it is a per-schedule
policy:

- `skip` (default): forget them, resume from now.
- `run_once`: create a single catch-up job.
- `run_all`: create all eight, each with the `run_at` it should have had, oldest first. For
  accounting-style workloads where every period matters. At most 100 are created per tick, so a long
  outage on a frequent schedule is worked off over consecutive ticks instead of in one transaction.

"Missed" needs a threshold, because a firing a second late is not missed. That threshold is the
**misfire grace**, 60 seconds by default (`--misfire-grace`). A firing at most that late counts as on
time, under every policy. So does a firing that falls inside the grace at the end of an outage, which
is why a restart that lands a few seconds after the minute still runs that minute; under `run_once` that
job doubles as the catch-up.

The clock can also go the other way. If it moves back, by hand or by a VM restored from a snapshot,
the stored `next_run_at` is suddenly far in the future and the schedule would sit silent until the
clock caught up. A stored firing later than the next one the cron allows from `now + grace` is
treated as exactly that, and recomputed from now. The grace is the tolerance, so clocks that merely
disagree by less than it between machines are never mistaken for a jump.

### Overlap

By default a schedule will not create a new job while its previous one is still queued or running,
implemented with the dedup key, so it needs no extra machinery. `overlap=True` opts out. A run that is
waiting out its retry backoff is still queued, so it holds the key too: a failing hourly job does not
pile up a second one behind it. `run_all` requires `overlap=True`, since its catch-up jobs would
otherwise collapse into one.

### Cron syntax

Standard five-field expressions (`minute hour day month weekday`) with `*`, ranges, steps, lists, and
three-letter names; `0` and `7` are both Sunday. When both the day of the month and the weekday are
restricted, either one matching is enough, as in every cron since Vixie. Plus the familiar macros:
`@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly`, `@annually`. An expression that can
never fire, such as `0 0 30 2 *`, is rejected rather than accepted and left waiting.

Seconds-level precision is out of scope. At that frequency you want a long-running task with a sleep,
not a scheduler.

### Where the limits are

Measured on an Apple silicon laptop, every schedule set to `* * * * *`, all of them firing on the
same minute boundary, which is the worst case:

| | |
| --- | --- |
| one scheduler, in process | about 110 µs per firing, one transaction each: 10,000 in 1.05 s |
| four workers racing, 2,000 schedules | 8,000 firings, each exactly once, p95 0.57 s late, max 0.62 s |
| four workers racing, 10,000 schedules | 40,000 firings, each exactly once, p95 6.4 s late, max 7.5 s |
| 56 schedulers racing, one schedule, 12 minutes | 12 jobs, no duplicates, p95 15 ms late |
| 50 `SIGKILL`s in 10 minutes, 4 workers and a scheduler | 10 jobs, no gaps, no duplicates, max 4 ms late |

Racing schedulers share one write lock, and every loser still takes it for an `UPDATE` that matches
nothing, so lateness grows with schedules times workers. Applications have tens of schedules, where
none of this is visible; at ten thousand firing on the same minute the tail is seconds late, and still
exactly once.

The other limits are about time rather than volume:

- **A task that blocks the event loop blocks that worker's scheduler.** An `async def` calling
  `time.sleep()` for 150 seconds on an every-minute schedule made its own worker skip the two firings
  it slept through. The same task with `--no-scheduler` and a standalone `tinyjobs scheduler` fired
  every minute within 36 ms; the jobs queued behind the blocked worker, but none were lost. Other
  workers cover for a blocked one, unless they block the same way.
- **Restarts.** A graceful stop and a `SIGKILL` behave the same, because the state is the row. Down
  for 35 seconds across a firing: it ran 25 seconds late on restart. Down for 85 seconds: the firing
  more than a minute late was skipped and the next one ran 15 seconds late. Down for four and a half
  minutes: `skip` dropped four firings, `run_all` created all five in order.
- **Clock jumps.** Forward by an hour: treated as an outage, missed firings skipped. Back by an hour:
  realigned on the next tick, instead of falling silent for the hour.
- **Rolling deploys that change a cron.** While both versions run, each firing is computed by
  whichever version wins it, and it is still never doubled, since the compare-and-set does not care
  whose expression produced the value. Once only the new version runs, every next time comes from the
  new expression; the `cron` column that `tinyjobs schedules` prints catches up the next time a new
  worker starts.


---

## Execution model

A worker is a loop: claim a batch, dispatch each job to the right executor, record the outcome. All
the interesting decisions are in the dispatch.

### Executors

| Task kind                    | Executor                | Timeout        | Notes                          |
| ---------------------------- | ----------------------- | -------------- | ------------------------------ |
| `async def`                  | worker event loop       | hard           | `asyncio.timeout`, real cancel |
| `def`                        | `ThreadPoolExecutor`    | **soft**       | see below                      |
| `def`, `isolated=True`       | `ProcessPoolExecutor`   | hard           | survives segfaults too         |

A single worker process runs an event loop plus a thread pool, plus a process pool created lazily and
only if some registered task declares `isolated=True`. Async and sync tasks share the process without
either starving the other.

**There is no `--concurrency-model` flag.** The alternative is tempting, so here is the argument
against it. The thing such a flag would select, threads versus processes, is not a property of the
deployment, it is a property of the individual task: a function that holds the GIL for four seconds
needs a process whether you run one worker or forty. `isolated=True` already expresses that at the
declaration site, where the person who knows the answer is standing. A global flag would ask the same
question again at the layer with the least information, and answer it the same way for every task.

Scaling out is therefore "run more worker processes", supervised by systemd, supervisord or your
orchestrator, which you need regardless the moment you have two machines. The worker does not fork or
supervise children itself; that stays out of v1, where adding it later is purely additive and removing
it would not be.

Knobs: `--concurrency N` sets the thread pool size and doubles as the default ceiling for concurrent
async tasks and isolated processes, each overridable (`--async-concurrency`, `--isolated-concurrency`)
when the defaults do not fit.

### Timeouts are not uniform, and pretending otherwise would be a lie

**A running Python thread cannot be killed.** There is no safe interrupt for arbitrary code; the
CPython APIs that come close corrupt interpreter state. So for a sync task in a thread pool, a timeout
can only mark the job `TIMEOUT` and move on. The thread keeps running until the function returns, and
it keeps occupying a pool slot.

This catches people out, so, plainly:

- A sync task stuck in an infinite loop leaks a thread for the life of the process.
- A blocking socket read with no timeout of its own blocks past your `timeout=`.

Mitigations, in order of preference: pass timeouts to the library you are calling (every HTTP client
has one); write the task as `async def`, where cancellation actually works; or mark it
`isolated=True` and pay for a process that can be killed. The documentation says all of this at the
point where `timeout=` is introduced, not in a footnote.

### Graceful shutdown

On `SIGTERM` (or the first `SIGINT`), the worker stops claiming, keeps heartbeating in-flight jobs,
and waits up to `--shutdown-timeout` for them to finish. Anything still running is released back to
`QUEUED` so another worker picks it up immediately rather than waiting for the lease to expire. A
second `SIGINT` exits at once and lets the leases lapse.

---

## Retries

```python
@app.task(
    max_attempts=5,
    retry_on=(ConnectionError, TimeoutError),   # default: all exceptions
    retry_backoff="exponential",                # or "fixed", or a callable
    retry_base=1.0,
    retry_max=600.0,
    retry_jitter=True,
)
async def sync_invoice(invoice_id: int): ...
```

Delay for attempt `n` is `min(retry_base * 2 ** (n - 1), retry_max)`, multiplied by a random factor in
`[0.5, 1.0]` when jitter is on. Jitter is the default and it is not cosmetic: when a downstream service
goes down and comes back, un-jittered exponential backoff makes every failed job retry in the same
instant and knock it over again.

A retry is not a new job. It is the same job with `attempts` incremented and `run_at` pushed into the
future, plus a new `Execution` row. The job id is stable for its whole life, which is what makes it
usable as an idempotency key downstream.

Two escapes from the default policy, raised from inside a task:

- `raise Retry(in_=30)`: retry at a time you choose, ignoring the backoff curve. For honouring a
  `Retry-After` header.
- `raise Abandon("reason")`: fail immediately and permanently, skipping remaining attempts. For
  errors that will never succeed on retry, like a 422.

Jobs that exhaust their attempts stay in the store as `FAILED` with the full traceback of every
attempt. `tinyjobs jobs retry <id>` resets the counter and requeues, giving you a dead letter queue without a
second queue to operate.

---

## Results

Off by default. Most background jobs are run for their side effects, and storing return values that
nobody reads is pure write amplification on the hot path.

```python
@app.task(store_result=True, result_ttl=3600)
async def render_report(report_id: int) -> str:
    return "/tmp/report.pdf"

job = render_report.enqueue(42)
result = await job.result(timeout=30)     # polls; raises on failure or timeout
```

Results are serialized with the same codec as the arguments, capped at a configurable size (default
64 KiB; a result larger than that belongs in object storage, with the job returning a key), and
deleted when the TTL expires.

`job.wait(timeout=...)` and `await job.awaited(timeout=...)` poll with the same adaptive backoff as
the worker loop, and return the job rather than raising on failure: a job that failed still has a
status, an attempt count and a last error worth reading. Only the timeout raises.

They are a convenience for scripts, tests and glue code, not a synchronisation primitive. Blocking a
request thread on a background job means you did not need a background job.

---

## Public API

### Async core, sync facade

The backend protocol is `async`. Every method a backend implements is a coroutine. On top of that
sits a synchronous facade, so enqueuing still works from a script, a Django view or a `__main__`
block with no event loop in sight.

An earlier version of this document argued the opposite, and the argument was half right. The
diagnosis was correct: `sqlite3` is a blocking C library, the standard library ships no async driver
for it, and an `async def enqueue` sitting directly on a blocking `sqlite3` call is not asynchronous.
It is a synchronous call that has taken your event loop hostage.

The conclusion drawn from it was wrong. The problem is not the `async` keyword, it is **where the
thread hop lives**. Put it inside the SQLite backend and `async def claim` becomes honest, because it
really does yield control:

```python
async def claim(self, ...):
    return await self._call(self._claim_sync, ...)
```

Put it in the architecture instead, and every future backend pays for a limitation that belongs to
SQLite alone. A natively async backend would have to expose synchronous methods, which means running
its own event loop on another thread and bridging back to it. Since the whole point of the backend
seam is Redis, Postgres and whatever comes after, that is the wrong trade.

The old shape also leaked in a way that is easy to measure. Because `asyncio.to_thread` spreads calls
across the default executor's thread pool, and SQLite connections are thread-local, a worker doing
nothing unusual ended up holding **ten connections instead of one**. Moving the hop into the backend,
onto a dedicated single-worker executor, brings it back to one and serializes writes inside the
process for free, which is exactly what SQLite's single-writer model wants anyway.

```python
job = generate_pdf.enqueue(123)             # sync facade, from anywhere
job = await generate_pdf.aenqueue(123)      # native path
```

The facade drives a single background event loop, started lazily on the first synchronous call, and
marshals coroutines onto it. The obvious alternative, one `asyncio.run()` per call, was rejected
because connection pools in async network clients are bound to the loop that created them; a fresh
loop per enqueue would break precisely the backends this design exists to support.

Calling a synchronous method from inside a running loop raises `SynchronousCallInsideEventLoopError`
and names the awaitable to use instead, rather than deadlocking or silently blocking the caller's
loop.

Async does not make the queue faster on its own. On SQLite the limit is still `fsync`. It pays when
there are many concurrent I/O waits in flight, which is the network-backend case, and it is why the
protocol has to be async before those backends exist rather than after.

**Task functions are unaffected by any of this.** `async def` runs on the loop, `def` runs in a pool,
and you write whichever fits the work.

### Defining an app

```python
from tinyjobs import TinyJobs

app = TinyJobs("sqlite:///jobs.db")

@app.task
async def generate_pdf(document_id: int) -> str:
    ...

@app.task(queue="slow", max_attempts=5, timeout=300)
def transcode(video_id: int) -> None:
    ...

@app.task(cron="*/5 * * * *", timezone="Europe/Rome")
async def refresh_cache() -> None:
    ...
```

### Enqueuing

```python
job = generate_pdf.enqueue(123)

job = generate_pdf.with_options(
    delay=60,                       # or run_at=<datetime>
    priority=10,
    queue="urgent",
    key=f"pdf:{123}",               # deduplication
).enqueue(123)
```

**Why `with_options` rather than `enqueue(123, delay=60)`.** Options and task arguments cannot share
one keyword namespace without a silent collision: the moment somebody writes a task that takes a
parameter called `queue`, `priority` or `key`, none of which are exotic names, their argument would
be swallowed as an option, or the option would land in their function. The bug would surface as a job
running in the wrong queue, or a `TypeError` far from its cause. Separating them costs seven
characters and removes the failure mode entirely. `with_options` returns a lightweight view and never
mutates the task, so it is safe to build per call site.

`enqueue` performs no work: it serializes the arguments, writes one row, returns. `job.id` and
`job.status` are available immediately.

```
enqueue()
   ↓  validate against the task signature   ← fails here, not in the worker
   ↓  serialize (args, kwargs)
   ↓  backend.enqueue()
   ↓  row committed
   ↓  return Job
```

Arguments are checked against the task's signature with `inspect.signature().bind()` **at enqueue
time**. A typo becomes a `TypeError` in the caller's traceback, right where it was written, instead of
a failed job discovered in a log an hour later.

### Enqueuing without the task's code

That validation is only possible because `enqueue` is a method *on a Task object*, so reaching it means
the function is registered in this process, by construction. But there is a legitimate pattern where
it is not: a producer that dispatches work to workers whose code it deliberately does not import, such
as a small API service feeding a heavy ML worker it shares nothing with.

Rather than weaken `enqueue` with a warning for both cases, that pattern gets its own entry point:

```python
app.send("ml.train_model", args=(dataset_id,), queue="gpu", priority=5)
```

No signature to check, no import attempted, and the caller has said so explicitly by choosing this
method. The two APIs answer the two situations exactly, and neither has to compromise: `task.enqueue()`
is strict because it can afford to be, `app.send()` is permissive because that is the whole point of
reaching for it.

### Transactional enqueue

The dual-write bug is old and common: you commit a row, enqueue a job about it, and the enqueue
succeeds while the commit rolls back, or the worker claims the job a millisecond before the commit
lands and finds nothing there. A broker-backed queue cannot fix this, because the broker and your
database have no transaction in common. A database-backed queue can.

```python
with app.transaction() as tx:
    tx.execute("INSERT INTO documents (id, name) VALUES (?, ?)", (123, "contract.pdf"))
    generate_pdf.enqueue(123, tx=tx)
# both committed, or neither
```

tinyjobs owns the connection and hands it to you, rather than borrowing one of yours. That is what
lets it guarantee the settings this whole design rests on (WAL, `busy_timeout`, `BEGIN IMMEDIATE`)
instead of inheriting whatever an ORM happened to configure. Passing `tx=` to a backend whose
`Capabilities.transactional_enqueue` is `False` raises at enqueue time, not silently degrades.

**The constraint, stated plainly: the `jobs` table must live in the same SQLite file as your data.**
There is no way around it. SQLite documents that transactions spanning `ATTACH`ed databases are not
atomic when `journal_mode` is WAL, and WAL is not negotiable here. It is what lets readers work while
a worker writes. So transactional enqueue means pointing tinyjobs at your application database
(`TinyJobs("sqlite:///app.db")`) and letting its tables sit alongside yours. If you would rather keep
the queue in a file of its own, you keep that separation and give up this one feature; everything else
is unaffected.

Joining a connection your ORM already owns, via something like `app.using(django_connection)`, is the
natural next step and is deliberately **not** in v1. Designing an ORM integration before there is a
real ORM to test it against produces an API shaped by imagination, and v1.0 freezes it. The hook it
would attach to is the same `Backend.transaction()` already in the protocol.

### Task discovery

Workers must import task modules to populate the registry. A job row stores a task *name*, and a
worker that never imported the module cannot resolve it.

```bash
tinyjobs worker --app myproject.tasks:app
tinyjobs worker --app myproject:app --import "myproject.jobs.*"
```

A job whose task is unknown to the worker is left `QUEUED` and logged, never failed: the usual cause
is a misconfigured `--import` or a partially rolled-out deploy, and failing the job would destroy work
that a correctly configured worker could still run.

---

## Operations

### Observability

Structured logging through the stdlib `logging` module, on the `tinyjobs` logger, with `job_id`,
`task`, `queue`, `attempt` and `duration_ms` as `extra` fields. No logging is configured by the
library. A library that calls `basicConfig` is a library that fights your application.

Lifecycle hooks for metrics, tracing or alerting, with no framework assumptions:

```python
@app.on_job_start
def _(job): ...

@app.on_job_success
def _(job, result, duration): ...

@app.on_job_failure
def _(job, exc, will_retry): ...
```

Hooks run in the worker. Success and failure fire after the outcome is recorded, so a hook that reads
the job back sees it. They may be sync or async. One that raises is logged on `tinyjobs.hooks` and
ignored: observability must never change what happens to a job.

### Health

`tinyjobs stats` gives counts by queue and status, how long the oldest due job has waited, and the
number of jobs currently past their lease, which is the metric to alert on, because it is the one
that means workers are dying.

`tinyjobs check` is the same data turned into a verdict, exiting 1 on any problem so it can back a
probe or a cron alert. It fails when the app registers no tasks, when queued jobs name a task the app
does not register (the usual sign of a missing `--import`), when a job is past its lease, when a due
job has waited longer than `--max-wait`, and when a cron schedule is more than a minute overdue, which
means nothing is firing it. Reaching the store and its schema version is checked by connecting at
all. Clock skew is not: on one SQLite file there is no second clock to compare against.

### Schema migrations

A `schema_version` row in `meta`, forward-only migrations applied at `setup()`. Workers refuse to
start against a schema newer than they understand, so a partial rollback cannot corrupt a queue.

### Security notes

- Pickle is off unless explicitly enabled; see [Serialization](#serialization).
- Task names come from the registry only. A job row naming an unregistered task is never imported
  dynamically. That would be an arbitrary-import primitive for anyone able to write a row.
- The SQLite file holds serialized arguments. If tasks carry sensitive data, file permissions are
  the boundary; the library does not encrypt.

---

## Testing

Testing background jobs is normally miserable, so it is a design constraint rather than an afterthought.

```python
# 1. Eager mode: run inline on enqueue, no worker, no store
app = TinyJobs("memory://", eager=True)

# 2. Manual draining: real queue semantics, deterministic ordering
app = TinyJobs("memory://")
generate_pdf.enqueue(1)
app.drain()                      # run everything currently queued, then stop

# 3. Fake clock: test backoff and cron without waiting
with app.freeze_time("2026-01-01T00:00:00Z") as clock:
    refresh_cache.enqueue()
    clock.advance(minutes=5)
    app.drain()
```

The `memory://` backend runs the same conformance suite as SQLite, so behaviour under test matches
behaviour in production.

---

## On writing C extensions

The original plan said "C extension: only after benchmarks". That instinct is right; the conclusion it
leads to is stronger, and writing it down now saves months of work later.

**There is probably nothing here worth writing in C.** A job queue's cost is not CPU:

- The dominant cost of `enqueue` is `fsync`, the kernel and the disk, not the interpreter.
- The dominant cost of `claim` is the SQLite B-tree lookup, already C.
- JSON encoding uses CPython's C accelerator, already C.
- Cron parsing computes a next firing time in microseconds, once per schedule per tick.
- Actual job execution is user code, which is the entire point.

The remaining Python is object construction and a loop. Making it twice as fast would move an
end-to-end number by a fraction of a percent, because the time is spent waiting for a disk.

There is also a direct conflict with the primary goal. **A C extension breaks "pip install, no
dependencies."** It means building wheels for every platform × Python version × architecture, a CI
matrix to maintain, and a source build, with a working compiler, for anyone on a combination you did
not publish. "Pure Python, zero dependencies, works everywhere" is the strongest single advantage over
Celery. Spending it on a speedup that profiling has not yet asked for is a bad trade.

**Policy, if the question returns:** C is permitted only for a hot path a profile shows is worth more
than 20% end-to-end, must ship with a pure-Python fallback selected at import time, and must remain
optional, so that `pip install tinyjobs` never requires a compiler. The first version is pure Python, and it
should be benchmarked honestly before anyone opens a `.c` file.

---

## Roadmap

**v0.1, the core loop**
Task registry, `task.enqueue()` with signature validation, `app.send()` by name, SQLite backend with
the atomic partial-index claim, single worker, sync and async executors, retries with backoff, JSON
codec, `tinyjobs worker`, `tinyjobs jobs`.

**v0.2, time**
Cron schedules, `run_at` / delayed jobs, the embedded leader-free scheduler, misfire and overlap
policies, timezone and DST handling.

**v0.3, production shape**
Leases and the reaper, graceful shutdown, multi-queue routing, priorities, deduplication keys,
lifecycle hooks, retention and automatic purge, `app.transaction()` for transactional enqueue.

**v0.4, comfort**
Result storage, `memory://` backend and the conformance suite, eager and drain test modes,
`tinyjobs stats` / `check`, the process executor and `isolated=True`.

**v1.0, commitment**
Frozen backend protocol, documented plugin API, migration tooling, full documentation. After this the
protocol is a compatibility promise.

**Beyond**
`app.using(conn)` to join an ORM's own transaction, designed against a real Django or SQLAlchemy
application rather than against a guess. `tinyjobs-redis` and `tinyjobs-postgres` as separate
packages. `--processes N`, if running one worker per core under a supervisor turns out to be the
friction people report. A read-only web dashboard, also dependency-free (`http.server` and one HTML
file). Chains and groups only if usage demands them. They are where task queues usually go to become
frameworks.
