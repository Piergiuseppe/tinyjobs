# Other backends

The core package doesn't know about third-party backends. It looks an unknown URL scheme up in the
`tinyjobs.backends` entry point group, so a package registers itself with:

```toml
[project.entry-points."tinyjobs.backends"]
mysql = "tinyjobs_mysql:MySQLBackend.from_url"
```

Backends declare what they support (`priority`, `delayed`, `dedup`, ...) and asking for something
unsupported raises when you enqueue rather than failing quietly later. The protocol is `async`, and
`tinyjobs.testing.BackendContract` is the test suite a backend has to pass, the same one SQLite,
Postgres and Redis run. `claim_job` is optional, and its contract test is skipped without it. Without
it, an eager call drains every due job, as earlier releases did, and waits for a job it inserted.
A backend that enforces [limits](limits.md) declares `limits` in its capabilities and takes
`limits=`, a mapping of task name to `tinyjobs.Limit` (`concurrency`, and `interval` in seconds),
on `claim`, and implements `release_held(tasks=, queues=)`, which returns the held jobs of the tasks
named, in the queues named or in every queue, to the queue; the contract tests for
them are skipped without the capability, and a worker or a drain whose app registers a limited task
refuses such a backend at start. A backend that keeps [a job waiting on another](jobs.md#one-job-after-another)
declares `after`: it places a job whose `after` names a parent by that parent's status when it is
inserted, raising `JobNotFoundError` for a parent it does not hold; it queues the waiting children
of a job as it completes it and cancels every waiting descendant of a job that fails
for good or is cancelled; `requeue` asks the parent again; and a waiting job holds its dedup key, which a job placed
cancelled does not, and stays out of the queued figures of `stats`. Without the capability `after=`
raises at enqueue and those contract tests are skipped; one of them cancels a job with 4,000 jobs
waiting on it within `wide_cancel_seconds`, one second unless the test class sets another. `tests/test_async_backend.py` implements a minimal in-memory backend in about 130
lines if you want a template.
