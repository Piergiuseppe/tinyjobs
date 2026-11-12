# Other backends

The core package doesn't know about third-party backends. It looks an unknown URL scheme up in the
`tinyjobs.backends` entry point group, so a package registers itself with:

```toml
[project.entry-points."tinyjobs.backends"]
mysql = "tinyjobs_mysql:MySQLBackend.from_url"
```

The entry point names a factory, called as `factory(url, clock=clock, **options)` with the URL given
to `TinyJobs`, the app's clock and the keyword options the app was built with. A store reads the
time from `clock.now()`, never from `time.time()`, which is what lets tests and
[eager mode](testing-your-tasks.md) move it.

This seam is provisional until 1.0: see [Compatibility](compatibility.md) for what may still change
and how a change is announced.

Backends declare what they support (`priority`, `delayed`, `dedup`, ...) and asking for something
unsupported raises when you enqueue rather than failing quietly later. The protocol is `async`, and
`tinyjobs.testing.BackendContract` is the test suite a backend has to pass, the same one SQLite,
Postgres and Redis run:

```python
class MySQLContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    async def make_backend(self, clock):
        return MySQLBackend.from_url(URL, clock=clock)
```

Subclassing `tinyjobs.backends.BaseBackend` leaves fourteen methods to write: `enqueue`, `claim`,
`heartbeat`, `complete`, `fail`, `release`, `cancel`, `requeue`, `get`, `list`, `executions`,
`reap_expired`, `counts_by_status` and `stats`. `heartbeat` takes `worker_id=` and returns the ids
whose lease it renewed. `claim_job` is optional, and the contract tests that claim by id are skipped
without it, or run without that step. Without it, an eager call drains every due job, as earlier
releases did, and waits for a job it inserted. A store that does not declare `loop_bound` is called
from more than one thread: claims, and the hand-backs of a claim given up, run on the app's own
loop, and every other call on the loop that made it, so state it keeps in the process needs a lock.
The template's lock is a `threading.Lock` held for a whole call, which is safe only because none of
its methods awaits anything; a store that does real I/O under it would stall its loop. The race
tests open several instances of the store; a test class whose store keeps its jobs in the process,
where two instances share nothing, sets `independent_clients = False` and races one instance against
itself. A backend that enforces [limits](limits.md) declares `limits` in its capabilities and takes
`limits=`, a mapping of task name to `tinyjobs.Limit` (`concurrency`, and `interval` in seconds), on
`claim`, and implements `release_held(tasks=, queues=)`, which returns the held jobs of the tasks
named, in the queues named or in every queue, to the queue; the contract tests for them are skipped
without the capability, and a worker or a drain whose app registers a limited task refuses such a
backend at start. A backend that keeps [a job waiting on another](jobs.md#one-job-after-another)
declares `after`: it places a job whose `after` names a parent by that parent's status when it is
inserted, raising `JobNotFoundError` for a parent it does not hold; it queues the waiting children
of a job as it completes it and cancels every waiting descendant of a job that fails for good or is
cancelled; `requeue` asks the parent again; and a waiting job holds its dedup key, which a job
placed cancelled does not, and stays out of the queued figures of `stats`. Without the capability
`after=` raises at enqueue and those contract tests are skipped; one of them cancels a job with
4,000 jobs waiting on it within `wide_cancel_seconds`, one second unless the test class sets
another.

`tests/test_async_backend.py` in the repository holds `RamBackend`, an in-memory backend of about
320 lines that passes the contract with the defaults and `retention`, if you want a template.
