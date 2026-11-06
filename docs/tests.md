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
