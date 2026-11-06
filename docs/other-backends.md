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
`tests/test_async_backend.py` implements a minimal in-memory backend in about 130 lines if
you want a template.
