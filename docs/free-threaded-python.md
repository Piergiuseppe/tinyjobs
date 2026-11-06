# Free-threaded Python

On a free-threaded build such as 3.14t the GIL stays off: no tinyjobs module turns it back on, so
`def` tasks run in parallel in the worker's thread pool. A worker is still one event loop, and one
loop is one thread; `loops=` gives a worker several, each in its own thread with its own Redis or
Postgres connection, sharing `concurrency` between them:

```bash
tinyjobs worker --app tasks:app --concurrency 64 --loops 3
```

From Python, `app.worker(concurrency=64, loops=3).run()` does the same.

Only the first loop fires cron and prunes. `loops` is for `tinyjobs worker` and `worker.run()`: an
embedded worker lives on the server's loop and refuses it. SQLite refuses `loops` above 1, since its
one writer would make them wait on each other, and with the GIL on extra loops take turns on it, so
run more processes there instead.

Two installs differ. psycopg-binary has no free-threaded wheels: install `psycopg psycopg-pool` and
the system libpq rather than the `postgres` extra. hiredis turns the GIL back on as soon as redis-py
imports it, so leave it out; a worker that finds the GIL on in a free-threaded build says so in its
log.
