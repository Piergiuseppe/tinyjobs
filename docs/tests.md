# Tests

```bash
python -m unittest discover -s tests -t .
```

About 2,000 tests, no test dependencies. The one that matters most is
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

One test needs another release. `tests/test_upgrade.py` lets the previous release create a store and
fill it, opens it with this one, and then has both send, run and read each other's jobs, four
processes draining at once at the end, on SQLite and on whichever of Postgres and Redis is set.
Where this release says the previous one cannot work beside it, the test expects the older process
to be refused and the jobs it left to run. It skips unless it is told where the previous release is,
the newest one below the version in `tinyjobs/__init__.py`, `<0.18` here:

```bash
python -m venv .venv-previous
.venv-previous/bin/pip install 'tinyjobs[redis,postgres]<0.18'
TINYJOBS_PREVIOUS_PYTHON=$PWD/.venv-previous/bin/python python -m unittest tests.test_upgrade
```

The workflow runs it on every pull request, push to main and release tag, against the release before
the one in the tree.
