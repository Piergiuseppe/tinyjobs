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
- **Transactional enqueue.** On Postgres, `enqueue` joins your application's own transaction: if
  the transaction rolls back, the job is never created. This removes the classic dual-write bug where
  a job starts running against a row that was never committed. It is a genuine advantage of a
  database-backed queue over a broker-backed one; see [Transactional enqueue](#transactional-enqueue)
  for the API, and for why SQLite does not have it yet.

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
    hand_backs      INTEGER NOT NULL DEFAULT 0, -- hand-backs in a row
    claims          INTEGER NOT NULL DEFAULT 0, -- every claim ever; never reset
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
    claim       INTEGER NOT NULL,            -- the jobs.claims value of this run
    status      TEXT NOT NULL,               -- running | success | failed | timeout | lost | cancelled
    worker_id   TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL,
    error_type  TEXT,
    traceback   TEXT
);
CREATE UNIQUE INDEX idx_executions_claim ON executions (job_id, claim);

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

The seam that makes storage swappable. It is narrow on purpose, and async on purpose;
see [Public API](#public-api) for why.

```python
class Backend(Protocol):
    capabilities: Capabilities

    # --- lifecycle -------------------------------------------------------
    async def setup(self) -> None: ...        # create schema, run migrations
    async def close(self) -> None: ...

    # --- producer side ---------------------------------------------------
    async def enqueue(self, job: Job) -> Job: ...
    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]: ...   # see note

    # --- consumer side ---------------------------------------------------
    async def claim(self, *, worker_id: str, queues: Sequence[str],
                    limit: int, lease_seconds: float) -> list[Job]: ...
    async def heartbeat(
        self, job_ids: Sequence[str], lease_seconds: float, worker_id: str | None = None
    ) -> None: ...
    async def complete(self, job: Job, result: bytes | None) -> bool: ...
    async def fail(self, job: Job, error: ExecutionError, retry_at: float | None,
                   execution_status: ExecutionStatus = ExecutionStatus.FAILED) -> bool: ...
    async def release(self, job: Job, delay: float = 0.0) -> bool: ...
    async def hand_back(self, job: Job, delay: float = 0.0) -> bool: ...

    # --- control ---------------------------------------------------------
    async def cancel(self, job_id: str) -> bool: ...
    async def requeue(self, job_id: str) -> bool: ...
    async def get(self, job_id: str) -> Job | None: ...
    async def list(self, *, status: JobStatus | None = None, queue: str | None = None,
                   limit: int = 100, cursor: str | None = None) -> Page: ...
    async def executions(self, job_id: str) -> list[Execution]: ...
    async def counts_by_status(self) -> dict[str, int]: ...
    async def stats(self, now: float) -> Stats: ...

    # --- schedules --------------------------------------------------------
    async def schedules(self) -> list[ScheduleState]: ...
    async def register_schedules(self, states: Sequence[ScheduleState]) -> None: ...
    async def fire_schedule(self, name: str, *, expected: float, next_run_at: float,
                            jobs: Sequence[Job]) -> list[Job] | None: ...

    # --- housekeeping -----------------------------------------------------
    async def reap_expired(self) -> int: ...
    async def purge(self, policy: RetentionPolicy, now: float) -> int | None: ...
```

`enqueue_many` earns its place as a distinct method rather than a loop in the core, because **the
transaction boundary is the optimization**. A thousand separate `enqueue` calls on SQLite means a
thousand fsyncs; the same thousand inside one transaction means one, which is two orders of magnitude.
Redis backends will want pipelining, Postgres a multi-row `INSERT`. None of that is reachable if the
core does the looping. And since the protocol freezes at v1.0, a seam that is not there at v1.0 cannot
be added later, so it goes in now, with a default implementation in `BaseBackend` (a loop over
`enqueue()`) so that simple backends get the correct behaviour for free.

A backend may also declare `transient_errors`, the exception types that mean the store could not be
reached. When `complete` or `fail` raises one, the worker writes again after 0.25 s, doubling up to
4 s, until a second before its lease would run out, where the last try lands. Every outcome write
checks that the worker still holds that claim, so a write repeated after its reply was lost changes
nothing, and the worker then reads the execution of its own claim to learn whether the first one
landed; that read is retried the same way. A job whose task returned but whose outcome is still
being written when the worker shuts down is not handed back to the queue: its lease runs out and the
reaper decides, so `max_attempts` still holds. Redis and Postgres declare theirs; SQLite does not,
and is never retried.

### The contract a backend must honour

The signatures are the easy part. These invariants are what an implementation is actually promising,
and the conformance suite that checks them ships with the package, so a plugin runs the same tests
as the bundled backends:

```python
from tinyjobs.testing import BackendContract

class MyBackendTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    async def make_backend(self, clock):
        return MyBackend("my://test", clock=clock)   # a fresh, empty store per test
```

It drives the backend through a `FrozenClock`, so leases, delays and retention are tested without
sleeping, and it races several clients against one store for the claim, dedup and cron invariants.
Tests for a capability the backend does not declare are skipped. A backend whose clients cannot share
a store, like `memory://`, sets `independent_clients = False` and races one client against itself.

1. **`claim` is atomic.** A job is handed to exactly one worker. Two concurrent claims never return
   the same job. This is the one hard requirement; everything else is comfort.
2. **`claim` respects `run_at`.** Never return a job scheduled for the future.
3. **`claim` respects `status`.** Never return a cancelled or terminal job.
4. **A claimed job carries a lease.** If the holder stops calling `heartbeat`, `reap_expired`
   eventually makes the job claimable again.
5. **`complete`, `fail`, `release` and `hand_back` are idempotent**, and all are no-ops on a job the
   caller no longer holds. The holder is the worker and the claim, a per-job counter that only ever
   grows, so a run that stalls past its lease, or was cancelled and retried, cannot report for the run
   that replaced it, even when the same worker claimed the job again. Executions are keyed by claim
   too, so a retried job keeps the history of its earlier runs, and cancelling a running job marks
   its execution `cancelled`.
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
    loop_bound: bool
```

Requesting an unsupported feature raises at *enqueue* time with a clear message, not silently later.
`loop_bound` is not a feature but a constraint: a backend whose connections belong to the event loop
that opened them, as an asyncio Redis or Postgres client's do, sets it, and the app opens it on the
facade's loop and routes every call there (see [Async core, sync facade](#async-core-sync-facade)).

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
if nothing is registered for it. Bundled: `sqlite://`, `memory://`, `redis://` behind the `redis`
extra and `postgres://` behind the `postgres` extra, each client imported only when its URL is used.

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

Switching a fresh file to WAL needs an exclusive lock, and SQLite answers "database is locked" at
once instead of honouring `busy_timeout`. Two workers starting together on a new file lost that race
in 11 of 60 tries, so the switch is retried until `busy_timeout` runs out.

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
while the job runs, naming itself, so a worker that lost the job cannot extend the lease of the one
that holds it now. A plugin gets `worker_id` only when its `heartbeat` names that parameter; an
older two-argument one, or one hidden behind a `*args, **kwargs` wrapper, is called without it. If
the worker is killed by SIGKILL, by the OOM killer or by a yanked power cable, the lease
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

## The Redis backend

Bundled rather than a separate `tinyjobs-redis` package: until the protocol freezes at v1.0, a change
to it would otherwise need two releases in lockstep. It depends on nothing but the public protocol, so
it can move out later without touching a line. The client library is an extra, `tinyjobs[redis]`, and
is imported only when a `redis://` URL is used, so the core stays dependency-free.

### Atomicity

Every state change is one Lua script: enqueue, claim, complete, fail, release, hand back, cancel, requeue,
heartbeat, reap, a purge batch, a cron firing. Redis runs a script without interleaving any other
command, which gives each transition the guarantee SQLite gets from `BEGIN IMMEDIATE`. The scripts
share one library, and in it a single pair of functions, `leave` and `enter`, knows which indexes
each status lives in. A transition is `leave(old)`, write the hash, `enter(new)`, so no script can
forget an index.

### Data model

All keys sit under `{prefix}`, which by default is `{tinyjobs}`:

| key | type | holds |
| --- | --- | --- |
| `job:<id>` | hash | the job's fields, the same columns as the SQLite table |
| `executions:<id>` | hash | attempt number to a JSON record |
| `ready:<queue>` | sorted set | due jobs, score `-priority`, member `run_at\|id`, so order is priority then age |
| `waiting:<queue>` | sorted set | jobs not due yet, by `run_at`; a claim promotes the due ones first |
| `queued:<queue>` | sorted set | every queued job by `run_at`, for the oldest-due figure |
| `leases` | sorted set | running jobs by lease expiry, for the reaper |
| `finished:<status>` | sorted set | terminal jobs by `finished_at`, for retention |
| `index:all`, `index:status:<s>`, `index:queue:<q>` | sorted sets | ids for listing; UUIDv7 ids sort by creation |
| `dedup:<key>` | string | the active job holding a dedup key |
| `counts`, `queued_tasks` | hashes | counters kept by `enter` and `leave`, so `stats` is O(queues) |
| `schedule:<name>`, `schedules` | hash, set | cron state; firing compares `next_run_at` and swaps it |

Every machine keeps Redis's time. The app's default clock is a `FollowingClock`: once the backend
has read Redis's `TIME`, `now()` is that reading plus the time elapsed since, counted on a clock that
never steps and keeps counting through a suspend (`CLOCK_BOOTTIME` on Linux, `CLOCK_MONOTONIC` on
macOS). A step of the wall clock therefore changes nothing, whether it lands between two calls or
during a reading. The backend reads `TIME` again on its first call after thirty seconds, after a
connection error, and when the wall clock and the elapsed clock disagree by half a second. Only the
`TIME` command itself is timed, on a connection already taken from the pool and opened. A reading
whose round trip took more than half a second only renews the reading held when the two agree
within half that round trip plus a quarter of a second; when they do not, it is taken, and either
way the next try waits a second. An older reading never replaces a newer one. Without this a host an hour ahead reaped every live lease the others held.

A host can still be on a stale reading for a moment: after a failover to a node whose clock differs,
until its next reading, or during a suspend its elapsed clock did not count. Leases do not depend on
it. A following host marks its `now` with a `~`, and for it the scripts read `TIME` themselves: the
reaper takes the earlier of the two, so it never reaps by a time ahead of Redis's, and a claim or a
heartbeat stamps the later of the two lease ends, so no lease is written to end before Redis says it
should. Everything else written in that moment, a delayed `run_at` or a cron slot, is off by the
stale reading until the next one, at most thirty seconds later; the app also reads again before it
builds a job whenever a reading is due. The one thing a following host cannot survive is Redis's own
time jumping ahead, as after a failover to a node whose clock is ahead: every lease stamped before
the jump by more than its length looks expired and is reaped, so its job runs again, or fails when it
has no attempts left. Redis's nodes therefore stay on NTP.

Instants the caller supplies, a `run_at` and `Retry(at=...)`, are read on the local wall clock and
moved by the difference between the two. A clock the caller passes, `FrozenClock` or any object with
`now()` and `monotonic()`, is used as it is and never follows, which is how the contract suite drives
Redis by hand. With the default clock job ids take their time from Redis's too, so listings put the
newest job first whichever host sent it; with any other clock they take the wall clock. Readings due at the same moment share one round trip, and a clock shared by
apps on two servers whose times differ by more than a second raises instead of mixing them.

### Where the limits are

- **Persistence is Redis's job.** On its default snapshots a crash loses everything since the last
  one, which in testing was the whole queue. `appendonly yes` with the default `everysec` lost nothing
  when Redis was killed mid-run. The README says so where people will read it.
- **One hash slot.** The hash tag puts every key in the same slot, which is what lets a script touch
  them all. It runs on a single Redis or a primary with replicas; a cluster would hold the queue on one
  node.
- **Script length bounds latency.** Redis serves nobody else while a script runs. Enqueues go in
  batches of 100 and a claim promotes at most 100 due delayed jobs, which kept the slowest call at
  18 ms when 100,000 delayed jobs fell due at once. Claiming stays at 0.7 ms with 200,000 queued.
- **Polling, not blocking.** Workers poll with the same adaptive interval as on SQLite. A blocking
  pop would save the idle round trips but cannot express priorities across queues.
- **No transactional enqueue, ever.** Redis shares no transaction with your database, so the
  Postgres feature has no Redis counterpart.
- **A bounded pool that waits.** redis-py 8 caps its pool at 100 and raises `MaxConnectionsError` the
  moment a caller finds it full, which turned half of 200 concurrent `aenqueue()` calls into errors.
  The backend uses a blocking pool of the same size instead, so 400 concurrent requests to a FastAPI
  endpoint all succeeded, and a caller only fails after `pool_timeout`.

---

## The Postgres backend

Bundled like Redis, behind the `postgres` extra (`psycopg` and `psycopg-pool`), and imported only
when a `postgres://` or `postgresql://` URL is used. It runs on psycopg 3's async API with an
`AsyncConnectionPool`, so it is `loop_bound` and the app routes every call onto its own loop.

### Tables

Four tables, named after the prefix and optionally inside a `schema=`: `<prefix>_jobs`,
`<prefix>_executions`, `<prefix>_schedules` and `<prefix>_meta`, with the same columns as SQLite.
Before `setup` reads any of them it finds them in the catalog, along the schemas the connection's
`search_path` named when the pool opened, and checks that all four sit in one schema and that the
tables and the schema belong to the connecting role or to a role that can become it (`SET` on
Postgres 16 and later, membership before), and that no role inherits the privileges of without
being able to become it too; a schema owned by `pg_database_owner` counts as the database owner's. Postgres runs a trigger, a policy or a view's function with the privileges of the
role reading or writing the row, so a table owned by anyone else would let that role act as
tinyjobs. For the same reason a table is refused too when it grants `TRIGGER` or `REFERENCES` to
such a role or to `PUBLIC`, since a foreign key from that role's own table would make a purge fire
its triggers, or when code that runs on it belongs to one: a function behind a trigger, constraint,
default, generated column, index or policy, an operator, a domain or type, or an index's operator
class. Setup and every `tx=` call find that code by following, in `pg_depend`, the objects attached
to the tables down through every object a user created, and the same goes for each table that
references them by a foreign key, whose triggers a cascade fires, and each table that inherits from
them, whose rows tinyjobs' statements reach. A table they reference, such as a lookup, runs nothing
of its owner's on tinyjobs' statements and is not counted, and neither is a view, a function or a stored
`regclass` that merely names the tables runs nothing during tinyjobs' statements and is not counted,
so no role can block the tables by creating one. The guard stays near a tenth of a millisecond. Such tables are refused before a row is read, even
when tinyjobs connects as a superuser, was made a member of their owner, or the database belongs to
someone else. With no tables yet, the schema they would be created in is checked the same way, and
created only when it does not exist, so a role that owns its schema needs no `CREATE` on the
database. Setup
then reads the stored schema version and creates nothing when it matches, so a fleet starting at
once costs a few catalog reads and one `SELECT` each. Otherwise it takes a transaction-scoped
advisory lock and checks again under it. Any role that may connect can take an advisory lock, so
one held for five seconds is waited out rather than obeyed: setup creates the tables without it, and
a concurrent creation it then trips over sends it round again, up to three times. Between the
`CREATE` statements and the rows the meta table is seeded with, setup checks the placement and the
owners once more in the same transaction, so a table another role creates in the gap is caught
before tinyjobs writes a row into it, and the whole creation rolls back. A store written by a
newer release is refused and left alone. Every setup then checks that each tinyjobs index exists as
a valid index of the right table, unique where it must be, and that the statistics below exist. One that is
missing, invalid or of the wrong kind is rebuilt with `CREATE INDEX CONCURRENTLY`, on a connection
of its own and under a session advisory lock, so workers keep claiming and enqueueing while it
builds; other setups starting meanwhile wait for that rebuild instead of racing it. The executions
index claims insert against is the exception: claims cannot run without it, so it is built in one
plain step, during which they wait instead of failing. Unique indexes go first. A build waits up to
30 seconds for transactions already holding the table; past that a unique index stops setup with
the reason, and the plain ones left are kept for the next setup with a warning, without waiting
again, since jobs run without them, only more slowly. If active jobs with the same dedup key got in
while that index was missing, setup names those jobs and asks to cancel all but one of each group.
`CREATE INDEX IF NOT EXISTS` skips a name another relation already holds, which would leave claims
failing and dedup keys ignored, so then setup names the index and stops, and the next setup after
that relation is dropped creates it. From then on every statement names the tables with the
schema setup found them in, so a table of the same name created later earlier on a `search_path`
cannot take the jobs, and tinyjobs' own connections run with `search_path` set to `pg_catalog,
pg_temp`, so no function or operator someone created elsewhere is ever called. The statements that
run on a caller's connection for `tx=` name every function, type and operator with `pg_catalog`, so
the caller's `search_path` cannot change them either. The caller may connect as another role, or
switch with `SET ROLE` between two calls, so every `tx=` call runs the ownership check through the
caller's connection, against the role current at that moment, before anything touches the tables;
a role the tables' owner does not answer to never writes through them, and none of the owner's
triggers, defaults or index expressions runs with its privileges. A `tx=` call holds the
connection's lock from the check to its last statement, as psycopg's own `executemany` does, so no
other statement on that connection, such as a `RESET ROLE` from another task or thread, lands
anywhere inside it. The check and the first insert share one pipeline sync, so the check costs no
round trip (a libpq older than 14 has no pipeline mode and spends one); a failing check makes
Postgres skip the insert unparsed. Inside a caller's own pipeline they join it without a sync, so an
autocommit pipeline still commits as one; there the check is read before the insert is sent, so a
refusal sends nothing more and leaves the pipeline as any failed statement does, for the caller to
sync. The insert sent behind the check is never prepared: psycopg counts a statement Postgres skipped
as prepared, and on an autocommit connection no rollback would clear that, so every later call
would fail. A `tx=` call that needs more than one batch on an idle autocommit connection runs in a
transaction of its own, so it stores all its jobs or none, as an enqueue through the pool does. Values are bound by the server, arrays in binary, so neither the caller's `search_path`
nor its `standard_conforming_strings` or client encoding changes what runs or what is stored. This
uses psycopg internals present in 3.2 and 3.3; on a psycopg without them `tx=` raises
`UnsupportedCapabilityError` rather than run unguarded. A connection whose client encoding is
`SQL_ASCII` is refused, since psycopg reads its text as bytes. A guard inside the insert
statement would not do: a statement-level `BEFORE` trigger fires before any row reaches the guard,
and the planner may evaluate an immutable column default before the statement starts. A refused
call leaves the caller's transaction aborted. Setup also reads the store's identity: a
random instance id written into the meta table when the tables are created, the database's oid and,
when the role may read it, the cluster's system identifier. A copy made with `CREATE DATABASE ...
TEMPLATE` or a restored dump keeps the instance id but not the other two. A `tx=` connection whose
identity differs makes tinyjobs read its own again on a fresh connection of its own, with the same
settings and timeouts as the pool's and a connect timeout, before refusing, so an app that rides
through a move of the same store, such as a switchover to a replica, follows it.

`id`, `queue`, `status` and `dedup_key` use the `C` collation, so ids sort as bytes, the way UUIDv7
ids are ordered on SQLite and Redis, whatever the database's locale. A btree entry holds about
2.7 kB, so the unique index covers a dedup key's SHA-256 digest, stored beside it, and the queue
indexes cover a queue's first 500 characters, with the full name compared on top; neither has a
length limit. Queues longer than that which share their first 500 characters share index entries,
so each is claimed by reading its siblings' rows too. Extended statistics tell the planner that the
prefix and the name go together; without them it multiplies the two conditions' selectivities, and
after a few prepared listings by queue and status Postgres picked a generic plan that read the whole
primary key, 14 ms instead of 0.12 at 300,000 jobs. Many jobs go in batches of at most 1,000 jobs and 64 MiB of payloads, keys and errors,
in one transaction, so no array parameter nears Postgres' 1 GB limit on a value. The jobs table has a fillfactor
of 85 and an autovacuum tuned to run often: a job row is updated several times in its life, and the
free space lets most of those updates stay on the same page. There is no index on the lease, so a
heartbeat touches no indexed column and is a HOT update; the reaper scans the running jobs instead,
which are few.

### The claim

A claim is one statement. A recursive query walks the priorities present in the wanted queues from
the highest down, using the partial index on `(left(queue, 500), priority DESC, run_at)` over queued
jobs, and
for each level that holds a due job takes the due jobs with `FOR UPDATE SKIP LOCKED`, oldest first,
until it has enough. Each step reads a span of index entries per queue below the last level, in index
order, and either finds the highest level with a due job or skips every level the spans covered,
since they hold each such level's oldest job; over several queues it skips only down to the highest
of the spans' last entries. The span starts at one entry, doubles up to 128 while steps cross several
levels, and drops back to one when it finds a due level, or when a span of at least 16 entries stays
inside one level. So the common
claim reads one entry per queue, delayed jobs spread one per priority are skipped up to 128 per step,
and a large delayed level costs one index descent instead of a scan.
Over several queues, each level visits the queues in the order of their oldest due job, so a batch
can take a queue's newer jobs before another queue's next one, and the next claim starts from that
other queue. Rows are locked only as the claim takes them, so a concurrent claim never skips a due
job that nobody is taking. The same statement marks them running, stamps the lease and opens their
executions, and it stamps a token of its own. A claim that ends without its answer, from a lost
connection, a server that stopped answering or a cancelled call, is settled before the worker's next
claim and when the backend closes: tinyjobs ends the backend it ran on if that backend has not
changed state since, so one that PgBouncer has handed to another client is left alone, then hands
back exactly the jobs carrying its token, without counting the attempt, so a lease
is never left to expire on jobs a worker never received. Workers never wait on each other's rows. Walking levels rather than sorting matters when
a backlog is not due: with 50,000 delayed jobs at one priority ahead of a due one, the plain `ORDER
BY priority DESC, run_at` read 205 buffers per claim and the level walk 50. Spans matter when the
delayed work spreads over many priorities: with 20,000 delayed jobs each at its own priority above a
due one, a walk of one level per step read 100,000 buffers and took 57 ms per claim, the spans 1,200
buffers and 4 ms.

### Time

As on Redis, every machine follows the server's clock: the backend reads `clock_timestamp()` on
setup, every thirty seconds and after a connection error, and the statements that decide by time
compare against the earlier of the caller's `now` and `statement_timestamp()`. A claim or a heartbeat
stamps the later of the two lease ends. So a host on a stale reading never reaps a live lease and
never writes one that ends too soon.

### Waking workers

Idle workers do not wait for their next poll. After an enqueue that left a job due, the backend sends
`pg_notify` with the queue as payload, cut to its first 1,000 characters so it stays under
Postgres's limit, at most once per queue every 50 ms; a burst sends one notification and a trailing
one. The channel is `tinyjobs_` and a hash of the instance id, which always fits in a channel name.
So every spelling of one store, by `schema=` or by `search_path`, shares one channel, and two stores
never do. Each worker process holds one extra
connection that `LISTEN`s, and a notification for one of its queues ends its idle wait at once,
unless it claimed since. A lost listening connection is reopened within a second, and every reopen
wakes the workers once, since notifications sent meanwhile are gone. A server
`idle_session_timeout` closes an idle listener too, and it reopens the same way; idle pooled
connections it closes are replaced, and `psycopg_pool` logs a warning for each. Polling stays as the fallback, so
`notify=False` or a dropped notification costs latency, never a job.

### Joining the caller's transaction

`tx=` takes the caller's psycopg connection or cursor, sync or async, and writes the job on it:

```python
with psycopg.connect(DATABASE_URL) as connection, connection.transaction():
    connection.execute("INSERT INTO orders (id) VALUES (%s)", (42,))
    send_receipt.with_options(tx=connection).enqueue(42)
```

The insert is written once as a generator of `(statement, parameters)` steps, driven by the pool's
async connections, by the caller's async connection on the caller's loop, and by the caller's sync
connection in the caller's thread. A sync enqueue therefore never crosses to the backend's loop with
a connection that belongs to another thread. The notification goes inside the same transaction, and
Postgres delivers it only at commit, so a worker never wakes for a job it cannot see yet; a rollback
takes both. The tables are named with their schema here too, so the caller's `search_path` or a
temporary table of the same name cannot divert the job. The first use of a connection reads the
store's identity through it and raises `ConfigurationError` when it differs, which catches a
connection to another database, even one holding tinyjobs tables of its own or a copy of this one.

Dedup keys work inside the transaction as outside: a second enqueue with a held key returns the
holder, including one written earlier in the same transaction. Jobs sharing a key inside one call
become one job before anything is written, and the first one given is the one stored. Every call
inserts its jobs in key order, so two bulk enqueues racing on the same keys wait for each other
instead of deadlocking. A holder written by another transaction that has not committed makes the
insert wait for it, as any unique index does. Eager mode refuses `tx=`, because it would run the job
before the transaction could commit, and so does every backend that cannot join one.

In Django, `OPTIONS = {"url": ..., "database": "default"}` enqueues through that database's own
connection, so a task enqueued inside `transaction.atomic()` is written in its transaction, and a
rolled-back savepoint takes its tasks with it. `aenqueue` runs in the thread that holds the
transaction, through `sync_to_async(thread_sensitive=True)`.

### Connections

Each process keeps a pool of 1 to 10 connections (`min_connections=`, `max_connections=`), plus the
listening one. Every pooled connection is set to read committed, a 30 second `statement_timeout` and
a one minute `idle_in_transaction_session_timeout`; every connection,
the listening one too, is named `tinyjobs` in `pg_stat_activity` unless the URL names it and keeps
TCP keepalives and a one minute `tcp_user_timeout` on, and the listening one gives up connecting
after `pool_timeout`, two seconds at least. A pooled statement is never cut short while bytes move
in either direction or while the server is still working on it, waiting on a lock, a synchronous
standby or its disk; a deadline would fail a large claim over a slow link, or report a commit
waiting on a standby as failed while it then applies. After ten silent seconds tinyjobs asks the
server, on a fresh connection, what the statement's backend is doing. The question names the
backend by its process id and start time, so a reused id is never taken for it, runs as the login
role, which sees its own sessions even when the url or `ALTER ROLE ... SET role` switches to
another, and uses only `pg_catalog` operators whatever the url's `search_path`. If the backend is
gone, the call raises `OperationalError`, which the worker treats like a lost connection. If the
backend has been blocked sending to us for those ten seconds, or has waited five minutes for bytes
from us with nothing moving on our side, an answer lost on its way or a request stuck in a proxy,
tinyjobs ends that backend first, so nothing it was doing takes effect afterwards, then raises. A
server that cannot be asked within ten seconds is not taken for gone, since a PgBouncer whose
server slots this pool already holds looks the same: the call waits for those five minutes of
silence, then raises. A claim waits half its lease instead, asking after silences of a third of
that, between one and ten seconds, so its jobs go back to the queue before the lease runs out. Cancelling a call bounds
the cancel request at five seconds on psycopg 3.2 and 3.3 alike, and the call ends in its
cancellation even when the server answers the cancel with another error, so it is never retried. Pooled connections ask for `UTF8` unless the URL names a client encoding, so a `SQL_ASCII`
database works too. A connection beyond the minimum closes after five idle
minutes, and every connection is replaced after half an hour. Deadlocks, serialization
failures and a session the server closed under `idle_session_timeout`, which it only does while
waiting for a command, so before the statement ran, are retried three times with a warning, except inside the caller's transaction, which only
the caller can retry. Before using a pooled connection, tinyjobs reads what the server already sent on
it without a round trip, under a microsecond, and hands a closed one back to be replaced and takes
the next at once. So the connections a restart or `pg_terminate_backend` closed while idle cost
neither an error nor a wait: with eight of them dead, the next three statements took under a
second together. A read that still loses its connection mid-flight, such as `get`, a listing or the
clock, runs once more on a fresh one; writes do not, since the server may have applied them, and
nothing is retried after waiting `pool_timeout` for a connection. Options the backend does not know
and libpq does not either, such as a misspelled `max_conections=`, whether passed as arguments or
in the url's query string, are refused by `TinyJobs(...)`
itself rather than surfacing as a pool that never opens; so is a url whose host or port came out
malformed, which is what a raw `@`, `/`, `?` or `#` in the password does, and an error about the url
never quotes it once it holds a password; `cursor_factory` is not taken either, since
tinyjobs picks its own cursors. On a caller's connection whose cursors bind raw, `tx=` uses a plain
cursor of its own. A fork leaves the parent's connections alone: the
child opens its own.

### Where the limits are

- **Behind PgBouncer, session pooling only.** Each connection is configured once when opened, and
  the listener needs a session of its own. Transaction pooling would lose both.
- **A held snapshot stops the cleanup.** A transaction left open anywhere on the server, or a
  replication slot that stopped advancing, keeps autovacuum from removing finished job rows, and the
  jobs table bloats until it ends.
- **`statement_timeout` is the server's limit, and so is what it counts.** A purge or a claim that
  needs longer than 30 seconds fails, and Postgres counts sending a result, and receiving the
  parameters of a statement not yet prepared, as part of the statement, so a payload slower than
  that to move fails at once with nothing applied. Raise it with `statement_timeout=` or turn it off
  with `None`; the watch on silent connections stays on either way.
- **Supported versions are the ones the community supports**, 14 to 18 today. CI runs the suite
  on 14 and 18.

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

A single worker process runs an event loop plus a thread pool. Async and sync tasks share the process
without either starving the other. A process executor for tasks declared `isolated=True` is planned,
not in 0.7 yet.

**There is no `--concurrency-model` flag.** The alternative is tempting, so here is the argument
against it. The thing such a flag would select, threads versus processes, is not a property of the
deployment, it is a property of the individual task: a function that holds the GIL for four seconds
needs a process whether you run one worker or forty. `isolated=True`, once it lands, expresses that at the
declaration site, where the person who knows the answer is standing. A global flag would ask the same
question again at the layer with the least information, and answer it the same way for every task.

Scaling out is therefore "run more worker processes", supervised by systemd, supervisord or your
orchestrator, which you need regardless the moment you have two machines. The worker does not fork or
supervise children itself; that stays out of v1, where adding it later is purely additive and removing
it would not be.

Knobs: `--concurrency N` sets the thread pool size and doubles as the default ceiling for concurrent
async tasks, overridable with `--async-concurrency` when the default does not fit.

### Timeouts are not uniform, and pretending otherwise would be a lie

**A running Python thread cannot be killed.** There is no safe interrupt for arbitrary code; the
CPython APIs that come close corrupt interpreter state. So for a sync task in a thread pool, a timeout
can only mark the job `TIMEOUT` and move on. The thread keeps running until the function returns, and
it keeps occupying a pool slot.

This catches people out, so, plainly:

- A sync task stuck in an infinite loop leaks a thread for the life of the process.
- A blocking socket read with no timeout of its own blocks past your `timeout=`.

Mitigations, in order of preference: pass timeouts to the library you are calling (every HTTP client
has one); or write the task as `async def`, where cancellation actually works. `isolated=True`, a
process that can be killed, is planned. The documentation says all of this at the
point where `timeout=` is introduced, not in a footnote.

### Graceful shutdown

On `SIGTERM` (or the first `SIGINT`), the worker stops claiming, keeps heartbeating in-flight jobs,
and waits up to `--shutdown-timeout` for them to finish. Anything still running is released back to
`QUEUED` so another worker picks it up immediately rather than waiting for the lease to expire. A
second `SIGINT` exits at once and lets the leases lapse.

A claim whose caller is cancelled while it is in flight still lands in the backend, so tinyjobs lets
it finish and hands its jobs back, as if they were never claimed; otherwise a cancelled drain would
leave its job `running` for the hour its lease lasts. The claim runs on tinyjobs' own loop, not the
caller's, so this holds when the caller's loop ends with the claim in flight (`asyncio.run`
returning, Ctrl-C under it) and when the process exits: that loop finishes such claims before it
stops, including a hand-back sent while that loop is being closed. Closing it cancels every other task
at once and waits five seconds at most, and half a second more for a task that swallowed the cancel,
which is cancelled once again: a task that keeps ignoring cancellation is logged and left behind, so
the close never hangs. At exit SQLite runs only the calls
that return jobs (complete, fail, release, hand back) on the loop thread once the interpreter refuses
new work to its executor; a claim is refused, so a worker still running stops instead of claiming
jobs it cannot start. A job whose job threads were already shut down is handed back without using an
attempt, never recorded as failed. Once the interpreter is exiting, tinyjobs starts no new loop
thread, and the exit closes only the connections no live thread is using, so a worker left running
in a daemon thread cannot crash the process. A job task cancelled before it starts is released the
same way as one cancelled while it runs.

A worker or scheduler installs SIGINT and SIGTERM handlers only on the main thread; elsewhere it
runs without them, since Python delivers signals to the main thread only. Workers on one loop share
one handler, so a signal reaches each of them. When the last one stops it puts back what it found: a
handler the loop already had, or the process's previous handler, including one set outside Python
(read and restored with `sigaction`). A handler replaced meanwhile is kept. On asyncio's own loop the
shared handler is a loop handler. Elsewhere (uvloop, or a loop without `add_signal_handler`) it is a
Python handler that schedules the stop on the loop, so the loop's own handlers stay in place and keep
receiving the signal.

### Inside a web server

`app.serve(worker=True)` runs the same worker as a task on the server's own loop, and
`app.lifespan()` wraps it in the shape FastAPI and Starlette expect. The server owns the process
signals, so an embedded worker installs no handlers: leaving the lifespan is its `SIGTERM`, with the
same drain and release. `handle_signals=False` is accepted there and `True` is refused.

A worker, a drain or a scheduler can be started again after it stops, on the same loop or a new one,
and `stop()` is safe to call from any thread; called before the start, it makes that run return at
once.

Where the limits are:

- **One loop for both.** An `async` task that blocks stalls every request in that process, not only
  other jobs.
- **One GIL for both.** CPU-bound `def` tasks raise request latency; the README has the numbers. Moving
  them to `tinyjobs worker` changes nothing in the task code.
- **No forced stop.** The standalone worker's second signal has no equivalent, because the server
  keeps the signals. Under `docker stop` with the default 30 s timeout, a 15 s job was killed at
  10 s and finished 76 s after the restart, once its lease expired. With `shutdown_timeout=5` the
  container exited cleanly and the released job finished 16 s after the restart.
- **One worker per server process.** `uvicorn --workers 4` means four workers on one queue, which the
  claim and the leader-free scheduler already handle.

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
second queue to operate. A retry is refused while another active job holds the same dedup key. A retried
job is due at once; one cancelled before any worker took it keeps its place from when it was sent.

---

## Results

Off by default. Most background jobs are run for their side effects, and storing return values that
nobody reads is pure write amplification on the hot path.

```python
@app.task(store_result=True)
async def render_report(report_id: int) -> str:
    return "/tmp/report.pdf"

job = render_report.enqueue(42)
result = job.wait(timeout=30).result()     # polls, then decodes the stored value
```

Results are serialized with the same codec as the arguments and stored on the job row, so they live
as long as the job does and go when retention purges it. There is no size cap; a large result belongs
in object storage, with the job returning a key. A cap and a per-task TTL are planned.

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

That loop is also where such a backend lives. Redis declares `Capabilities(loop_bound=True)`, and its
connection is always opened on the facade's loop, whoever connects first: a sync call, `ready()`, a
lifespan or a worker. A call from any other loop hops there and awaits the result, and cancelling it
cancels its work there. One app therefore serves any number of loops at once, a lifespan and a
worker thread with its own `asyncio.run()` included, and a loop that ends or is stopped takes nothing
with it. Before this rule a FastAPI app on Redis without a lifespan answered 421 of 1,000 mixed `def`
and `async def` requests with a 500, and a worker on a second loop failed every claim; now neither
does. SQLite and `memory://` hold nothing bound to a loop and are used in place from any of them.
The hop costs about a tenth of a millisecond, 3,600 sequential enqueues a second from one loop
instead of 5,200 when that loop held the connection; four workers still drain 20,000 jobs in 2.1
seconds.

Closing is ordered. `aclose()` refuses new calls with `AppClosedError`, waits up to five seconds for
the calls already running, closes the backend and stops the facade's loop. A call caught by the
close gets `AppClosedError` too, never a `RuntimeError` from a shut-down executor or a closed pool:
four threads calling `enqueue`, `start` and `close` and three event loops calling `aenqueue`, `ready`
and `aclose` at random for 9 s saw nothing else, on SQLite and on Redis, and asyncio logged no
unretrieved exception. A loop being shut down refuses new work instead of running it beside the
closing connection. A
sync call on a closed app raises without starting the loop thread, and an app dropped without
`close()` stops that thread and closes its connections when it is collected, so `python -X dev` stays
quiet at exit. That cleanup runs on a `threading.Thread` of its own: run from a bare OS thread, it
would leave a dummy thread in `threading` for good on 3.12, and every later fork would
count it as a foreign thread and skip the pause. At exit tinyjobs waits for these cleanups,
including one whose thread is registered but has not started yet: joining it straight away raised
`RuntimeError` and skipped the rest of the exit cleanup. Closing stops the app's workers first and waits for the jobs they are running, up to
their `shutdown_timeout`, so a finished job is never left `running`. A job that closes its own app
does not wait at all: it tells every worker to stop and returns, and the close takes effect once its
runner has recorded the outcome and the other workers have finished, so two jobs in two workers that
each close the app do not wait on each other. A worker or
scheduler whose app is closed stops instead of retrying. After
`os.fork()` the child leaves the parent's loop thread and connections alone, never closing them, and
connects on its first call, so an app started before a preforking server forks works in every child.
A fork waits for SQLite calls already running in other threads, and for the write transactions open
there, up to ten seconds, so the child never inherits a lock held by a thread it does not have, and
new calls wait from the moment the fork starts until it is done. SQLite keeps its file locks in
per-process memory that a fork copies, so a child forked while another thread still holds a write
open, past those ten seconds, cannot write to that database at all. A thread that forks inside a transaction does not wait for itself, and the child
cannot commit or roll back the parent's transaction. When tinyjobs' own threads are the only other
threads, the fork first pauses them, before the wait, and they resume right after, so Python's
multi-threaded `fork()` warning stays quiet and an idle app adds no delay. A SQLite executor thread
that the pause retires leaves its connection to the thread that replaces it, so no thread is left
closing it when the fork happens. A SQLite call queued, or waiting at the fork gate, when the fork
starts steps aside: it runs on the next executor once the fork is done, so that executor's thread
retires too. Cleanups inherited by a child, such as the close of a connection whose thread the child
does not have, do nothing there. The pause gives up rather
than wait for work: a loop that does not stop within 0.1 s (a job blocking it) or a job running in a
worker thread through `asyncio.to_thread` forks with the threads running, and the warning is then
accurate. So does a process with threads Python did not start (a BLAS pool, a gRPC channel), where
Python warns whatever tinyjobs does. A call already waiting on the loop when it pauses keeps waiting
and gets its result once the loop resumes, a call made during the pause is served then, and a close
that lands during a pause still stops the loop and closes it. A closed SQLite backend stays closed
across a fork. A signal handler that raises while a fork waits, Ctrl-C or a `SIGTERM` handler calling
`sys.exit()`, does not cut the wait short: the wait starts again, up to three times, and the exception is raised from the
fork hook afterwards, where CPython reports it and drops it, as it does for any fork hook. Without
tinyjobs the handler would run after `fork()` returns; with it, that exception is lost.

A signal handler may call the synchronous API. The calling thread waits on a lock of its own, which
is released when the work is done, so a handler that interrupts a sync call in progress is served.
Two cases cannot be: the handler interrupted its thread while that thread held one of tinyjobs'
locks, which only async code takes, or while it was inside the fork hooks, where the work it would
wait for needs that same thread to go on. There the call raises `InterruptedCallError` at once
instead of hanging, `close()` included, which then leaves the app open; setting a flag in the handler
and making the call after it returns always works. tinyjobs' locks are plain C `RLock`s it keeps weak
references to, and whether a thread holds one is read from the lock itself, so a thread that is only
waiting for a lock does not count as holding it. A forked child frees every such lock another thread
held at the fork and keeps the ones its own thread holds.

A handler that raises, such as Ctrl-C, cannot leave tinyjobs half-done either, and tinyjobs never
touches signal handlers, signal masks or the wakeup fd to get there. Python runs handlers only on the
main thread, so the main thread keeps no books. A synchronous call, `close()` included, builds its
request, hands it to tinyjobs' control thread through a `SimpleQueue` and waits on the request's
lock. That thread starts and stops the loops and schedules the call, and the coroutine is made on the
loop, so an interrupted call is either cancelled before it starts or finishes without its waiter; a
close keeps going on its own thread and the app still ends closed. The fork hooks follow the same
rule. The hooks that run after a fork are C callables, so no handler can run inside them. The hook
before it only adds entries keyed by its own thread, which those C hooks remove, and asks the control
thread to pause the loops and exit; in the parent a C hook starts the thread that resumes them, and a
child settles itself from the first such thread it starts. Each pause carries its fork's own resume
callable and is dropped once that fork is over, so a pause left behind by an interrupted hook never
stops the loops during a later fork, and a pause that outlasts its fork resumes the loops itself. A handler that raises inside a fork hook
is reported and dropped, as CPython does for every fork hook, and the fork goes on. SQLite's fork gate
and connection pool keep their state in dicts and sets changed one C call at a time. A write nested
inside another joins it through a savepoint, as in 0.6, so a failure inside it undoes only its own
work, whether it sits in a plain block, a coroutine, a `@contextmanager` or `@asynccontextmanager`
helper or an `ExitStack`. A nested write held by a generator that other code drives with `next()` is
different, because the outer code keeps writing while the generator is suspended: closing that
generator keeps what was done, and any other failure rolls back the whole transaction if anything
changed since the nested write opened, schema and `user_version` included. A nested write
interrupted before it finished poisons the outer one, which rolls back instead of committing. A nested
write still suspended when the nested write it joined exits passes to the outer one: its work is
part of that transaction, and a later failure of it, or the exit of the write it left having
failed, rolls the whole transaction back. A root write that ends while such a write is still
suspended commits or rolls back without it and retires its connection: the pool forgets it, closes
it once no call runs on it, and every later statement through it, from any frame, cursor or
thread, fails with `ProgrammingError` instead of writing outside a transaction, as does a new
write or `pool.connection` opened from inside the block of the write cut loose. One that ends while
a write that joined it is still running on the same thread, as when a generator dependency is
closed from inside the endpoint's own write, waits for that write and then commits or rolls back,
without any write that was only suspended; a rolled-back outer makes that write raise instead of
committing, and so does an outer poisoned meanwhile, even when that write failed itself. One that ends while a joined write runs on another thread retires the connection the
same way, stops the statement in flight, then rolls back and raises. A transaction whose
connection another thread used meanwhile, as when `asyncio.to_thread` outlives a timeout, retires
it at the end the same way, so that thread's later statements fail instead of committing on their
own: a committing root waits out a statement still running there, a failing root stops it first,
and a failing nested write rolls the whole transaction back. A write entered by hand inside an open one must
exit before it, or the outer one rolls back, raises and retires the connection. Every connection
the pool closes is retired first, so a close never lands under a statement another thread is
running. A write that would
join a transaction SQLite has already ended, as `INSERT OR ROLLBACK` does, is refused, and the
outer write rolls back. A write entered by hand and dropped without exiting is rolled back when it is collected,
together with any write that joined it meanwhile, and logs a warning on `tinyjobs.sqlite`. A write counts as open while the `with` block that holds it runs: the block where it was
entered, or, for a write opened inside some `__enter__`, the block that entered that; the frame's
exception table and the jumps inside the block tell this exactly. A write entered through an
`ExitStack` counts as open while that stack still holds its exit, wherever the stack moves, and one
opened by a generator outside any block, such as a pytest fixture, counts as open until it exits.
So a later write never joins a dead one, and the fork gate counts a write as held until its exit is
done. A later write joins an open one when it runs inside its block, in the same `contextvars`
context, or in the same asyncio task; otherwise it gets a connection of its own and waits for the
lock, so concurrent FastAPI requests served by one thread never share a transaction; two asyncio
tasks on one thread are the exception described below. Context is
what Python's `contextvars` says it is: `asyncio.to_thread` and FastAPI give each call its own copy,
while `loop.run_in_executor` runs everything in the worker thread's one context, so work sent there
joins a transaction an earlier call left open on that thread. That also
means a write opened by a sync FastAPI endpoint does not join the one its sync dependency holds,
since FastAPI runs each in a separate context; use the connection the dependency yields. A
transaction whose entry or exit was interrupted is rolled back by the next tinyjobs call on that
thread, sync or async, even while something still holds the interrupted write, and at once when
nothing does. A write may exit on another thread than the one that entered it and commits there,
untouched by the first thread meanwhile, and its connection stays open if that first thread ends.
Two asyncio tasks on one thread share its connection, so a task that writes while another task's
transaction is open gets an error instead of joining it; `asyncio.run` called inside a write joins
it, one task at a time. A tinyjobs call made while the thread is
opening or closing a transaction, which is where a signal handler lands, is refused with
`InterruptedCallError`, so the half-open transaction is neither healed nor joined. An interrupted
write that a later one rolled back refuses to commit. All of this relies on the GIL making each of those C calls atomic; free-threaded builds
are not supported. Only the main thread is checked for holding a lock, since a handler cannot run
anywhere else; a fork in progress counts on every thread. Async code on a loop in the main thread is
outside this: `asyncio.run` and uvicorn turn Ctrl-C into a cancellation, which tinyjobs handles at
every `await`, and a handler that raises in the middle of that code is not covered. The tests raise
an exception at every point of a call, a close, a drain, a transaction and a fork where CPython can
run a handler, and check that nothing is left behind.

The main thread keeps no books because CPython itself is not safe against a handler raising at an
arbitrary instruction. On 3.13 and 3.14, the backward jump of a loop whose body ends inside a `try` is
missing from the exception table. An exception raised there skips the enclosing `except` and
`finally`: 200 of 200 trials on 3.13 and 3.14, none on 3.12. On 3.14, `with lock:` checks
for signals after `__enter__` returns but before the block is protected, so the lock stays held. On
3.12, an interrupted `Thread.is_alive()` reports a live thread as dead, so a fork reads
whether the control thread has exited from the thread's handle instead, which 3.14 renamed from
`_handle` to `_os_thread_handle`.

The first connection is elected across loops and threads: one caller connects, and the others wait
on a thread-safe future without blocking their own loop and share its error if it fails, so a server
that refuses the connection is asked once, not once per waiter. If the caller's loop stops first, a
waiter takes over; a Redis connection is made on the facade's loop and finishes either way. A second
backend made in that race is closed, as is one whose `setup()` fails. An `asyncio.Lock` did this
before, and it belongs to one loop: two loops connecting at once deadlocked in every trial, and four
loops racing four threads hung all of them. With the election, 0 of 100 such trials fail on SQLite
and 0 of 50 on Redis.

Calling a synchronous method from inside a running loop raises `SynchronousCallInsideEventLoopError`
and names the awaitable to use instead, rather than deadlocking or silently blocking the caller's
loop.

Every call connects lazily, except after the app has been closed. `close()`, `aclose()` and leaving
`async with app` or `app.serve()` mark it closed, and a later `enqueue()` raises `AppClosedError`
rather than quietly opening a connection nobody will close; so do `drain()` and a scheduler tick.
`start()`, `ready()`, `serve()` and starting a worker open it again. Nested `async with` blocks
share one connection, which closes when the outermost one ends.

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
mutates the task, so it is safe to build per call site. A priority must fit in 32 bits, from -2³¹ to
2³¹ - 1, which every backend stores exactly; anything else raises `ValueError` at enqueue.

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
database have no transaction in common. A database-backed queue can, and on Postgres tinyjobs does:

```python
with psycopg.connect(DATABASE_URL) as connection, connection.transaction():
    connection.execute("INSERT INTO documents (id, name) VALUES (%s, %s)", (123, "contract.pdf"))
    generate_pdf.with_options(tx=connection).enqueue(123)
# both committed, or neither
```

`tx=` goes on `with_options`, `app.send` and `app.asend`, and takes the connection or a cursor the
caller already has, sync or async, which is the one an ORM or a request handler holds. Borrowing it
rather than handing one out is what makes the job part of a transaction tinyjobs did not start; the
settings the claim relies on live on tinyjobs's own pool, and an insert needs none of them. How the
Postgres backend joins it is in
[Joining the caller's transaction](#joining-the-callers-transaction). `tx=` on a backend whose
`Capabilities.transactional_enqueue` is `False` raises `UnsupportedCapabilityError` at enqueue time,
and so does eager mode.

SQLite does not have it yet. The jobs table would have to live in the same file as your data:
SQLite documents that transactions spanning `ATTACH`ed databases are not atomic when `journal_mode`
is WAL, and WAL is not negotiable here, since it is what lets readers work while a worker writes.

Until it does, a tinyjobs call that writes (an enqueue, a send, a cancel, a retry, a purge or a drain)
made while the same context holds an open `backend.transaction()` on the same database file is
refused at once: it would run on another connection and wait for the write lock that transaction
holds. Make the call after the block, or from `transaction.on_commit` in Django. An asyncio task
started inside the block is not refused, since it can wait while the block goes on; a thread handed
the context, as `asyncio.to_thread` does, is. The refusal sees only the context that opened the
transaction; a FastAPI endpoint enqueueing while its dependency holds one runs in another context, so
there the enqueue waits for the lock and fails after `busy_timeout`. Inside the block, `commit()`,
`rollback()`, `executescript()`, `with connection:` and setting `isolation_level` or `autocommit`
are refused because they would end the transaction early, and a
transaction SQLite ends by itself, as `INSERT OR ROLLBACK` does, makes the block raise when it exits,
since every statement after that point committed on its own.

### Task discovery

Workers must import task modules to populate the registry. A job row stores a task *name*, and a
worker that never imported the module cannot resolve it.

```bash
tinyjobs worker --app myproject.tasks:app
tinyjobs worker --app myproject:app --import "myproject.jobs.*"
```

A job whose task is unknown to the worker is left `QUEUED` and logged, never failed: the usual cause
is a misconfigured `--import` or a partially rolled-out deploy, and failing the job would destroy work
that a correctly configured worker could still run. A hand-back gives the attempt back: `attempts`
and the execution history read as if the claim never happened, and only the job's `hand_backs`
counts it, so a task allowed three attempts still gets three once a worker knows it. Any other
outcome resets `hand_backs`, so it counts the hand-backs in a row, including those of a claim
whose caller was cancelled. A backend without `hand_back`
falls back to `release`, which uses the attempt up. Each hand-back
waits twice as long as the one before, from `unknown_task_delay` (60 s) up to
`unknown_task_max_delay` (an hour). A fixed delay wrote an `ERROR` line every minute, forever, for a
job retention never prunes because it never finishes; doubling brings that to 24 a day once the
delay reaches the hour.

### Django tasks

`tinyjobs.contrib.django.TinyJobsBackend` is a `django.tasks` backend. A Django task has no name of
its own, only an import path, so the job stores that path and the registry takes a resolver: a name
it has not seen is looked up once, wrapped as a tinyjobs task and kept.

The resolver is the trust boundary. A job row is data, and whoever can write one should not be able
to make the worker import arbitrary modules, so it only imports modules that belong to an app in
`INSTALLED_APPS`, and only accepts a `django.tasks.Task` whose path is the job's; `enqueue` refuses
any other task with `InvalidTask`, so it cannot be sent. Anything else stays unknown and is handed
back like any unregistered task; an import that raises is logged with its traceback. A task bound to
another alias is rebound to this one, since `Task.using(backend=...)` sends it here without changing
the module attribute. So the alias a task is declared with is a default, not a boundary: whoever can
write a job row can already name any task of the installed apps. Django validated the task when it
was enqueued, so the copies made for a stored job, rebound or carrying the job's queue and priority,
are not validated again: a queue since dropped from `QUEUES`, or a priority outside Django's range
sent through `app.send`, still runs and reads back. `manage.py tinyjobs` also imports each installed
app's `tasks` module first, so `check` counts every task found there, whatever alias it is declared
with, before any job arrives.

Django builds one backend instance per thread. They share one `TinyJobs` per alias and options, so a
WSGI server does not open a connection pool per thread. The async methods go through the sync ones
in a thread: Django under ASGI has no lifespan to connect at startup, and the first connection,
raced from the server's loop and from a sync view's thread at once, turned 33 of 400 requests into
500s. Through one loop, 1,000 out of 1,000 succeed.

The mapping to `TaskResult` follows what tinyjobs already stores. Executions give `worker_ids`,
`started_at` and the errors; the exception path comes from the last line of the stored traceback,
because the execution keeps only the class name. Cancelled reads as `FAILED`, since Django has no
such status, and so does a task whose worker died. Neither raised anything, so each gets a
`JobCancelled` or `WorkerLost` error from `tinyjobs.contrib.django.exceptions`, and `errors[-1]`
always exists on a failed result. Django tasks are not retried, so every job gets one attempt.

`takes_context` needs the running job inside the task. The execution service puts it in a context
variable, and the thread executor copies the context into the thread that runs a `def` task.
Signals fire as Django's own backend fires them; in eager mode `task_enqueued` arrives last, because
the task has already run by the time `enqueue()` returns.

---

## Operations

### Observability

Logging goes through the stdlib `logging` module, on the `tinyjobs` logger and its children, with the
job id in the message. Structured `extra` fields (`job_id`, `task`, `queue`, `attempt`,
`duration_ms`) are planned. No logging is configured by the library. A library that calls `basicConfig` is a library that fights your application.

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
Version 3 adds `jobs.hand_backs` and `jobs.claims`, and keys executions by claim. The migration
numbers each job's existing executions in the order they started, and sets `claims` to the larger of
`attempts` and that count, so a job retried under 0.6 keeps every row. On Redis the store moves to
version 2: a job without `hand_backs` reads as zero, and a job without `claims` gets the same
renumbering the first time a script touches it or it is read, so `get`, `list` and `executions`
already show the adopted order and `claims`.

Upgrading from 0.6 is a stop-the-world step. A 0.6 worker writes executions by attempt, which no
longer matches the layout, so stop every 0.6 process before the first 0.7 one starts. Once the store
is upgraded, a 0.6 process refuses to start against it. 0.6 runs its schema statements before it
checks the version, so 0.7 keeps an empty index under the 0.6 unique index's name: that statement
becomes a no-op, and a refused 0.6 start cannot stop a running 0.7 worker from retrying jobs.

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
# 1. Eager mode: every enqueue runs the job before it returns
app = TinyJobs("memory://", eager=True)

# 2. Manual draining: real queue semantics, deterministic ordering
app = TinyJobs("memory://")
generate_pdf.enqueue(1)
app.drain()                      # run everything due, highest priority first, then stop

# 3. Fake clock: test delays and backoff without waiting
clock = FrozenClock(time.time())
app = TinyJobs("memory://", clock=clock)
flaky.enqueue()
app.drain()                      # first attempt fails, the retry is due in 10s
clock.advance(10)
app.drain()
```

`memory://` is not a second implementation. It is the SQLite backend on a private in-memory database,
so a test exercises the same claim, the same dedup index and the same retry bookkeeping as
production, with nothing to keep in sync. Eager mode is a drain after every enqueue, so a delayed job
or a retry with backoff stays queued until the clock reaches it, exactly as it would under a worker.
`drain()` runs jobs one at a time, fires hooks, and raises `UnknownTaskError` on a job no task in the
app can run, leaving it queued. It does not fire cron schedules; enqueue the task to test it.

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
lifecycle hooks, retention and automatic purge.

**v0.4, comfort**
Result storage, `memory://` backend and the conformance suite, eager and drain test modes,
`tinyjobs stats` / `check`.

**Next**
Transactional enqueue on SQLite, the process executor and `isolated=True`, a result size cap and
TTL, structured log fields.

**v1.0, commitment**
Frozen backend protocol, documented plugin API, migration tooling, full documentation. After this the
protocol is a compatibility promise.

**Beyond**
Joining a SQLAlchemy session's transaction, designed against a real application rather than against a
guess. `--processes N`, if running one worker per core under a supervisor turns out to be the
friction people report. A read-only web dashboard, also dependency-free (`http.server` and one HTML
file). Chains and groups only if usage demands them. They are where task queues usually go to become
frameworks.
