# Postgres

```bash
pip install 'tinyjobs[postgres]'
```

```python
app = TinyJobs("postgresql://app@localhost/shop")
app = TinyJobs("postgresql://app@localhost/shop", prefix="billing", schema="jobs")
```

The same tasks, workers, cron, hooks, `stats` and `check` again. The tables (`tinyjobs_jobs`,
`tinyjobs_executions`, `tinyjobs_schedules`, `tinyjobs_meta`) are created on first connect, and any
number of processes can start at once. Every machine keeps the server's time, as on Redis. An idle
worker is woken by `LISTEN`/`NOTIFY` and starts a new job in a few milliseconds instead of at its next
poll; `notify=False` turns that off.

**Enqueue inside your own transaction.** Pass your psycopg connection or cursor, sync or async, as
`tx`. The job commits or rolls back with everything else, and workers only hear of it at commit:

```python
with psycopg.connect(DATABASE_URL) as connection, connection.transaction():
    connection.execute("INSERT INTO orders (id) VALUES (%s)", (42,))
    send_receipt.with_options(tx=connection).enqueue(42)
```

`app.send(name, tx=connection)` and the async calls take it too. The connection must reach the
database tinyjobs uses, not a copy of it, and the tables must belong to its role or to a role that
can become it, the same rule as below, or the enqueue raises `ConfigurationError`; a connection
with `client_encoding` `SQL_ASCII` is refused too. In Django, add
`"database": "default"` to `OPTIONS` and every enqueue joins `transaction.atomic()`.

Each pool holds at most 10 connections (`max_connections=`), whatever its worker runs, plus one
for `LISTEN`, and one more, short-lived and shared, to ask about statements silent for ten
seconds when none is idle. `worker.run()` and `tinyjobs worker` open a pool of their own for
claims, outcomes and cron, and one more for each extra loop; the app's pool opens only when your
code calls it, a task enqueuing another for instance. An embedded worker uses the app's. Size the server's `max_connections` or the PgBouncer pool at 12 per
pool. Connections beyond the minimum go back after ten idle
seconds. Two workers of 32 busy without pause behind a PgBouncer session pool of 25, over a link
adding 300 ms each way, left the app waiting 1.9 seconds for a server as a rule and 2.4 at most,
with none refused and no warning. A smaller pool makes the app queue behind the workers for as long as they stay
busy: at 20, the app got 13 queries through in four and a half minutes, and PgBouncer refused one
after its 120 second `query_wait_timeout`. Every statement times out after 30 seconds
(`statement_timeout=`), a server that stops answering fails the call instead of hanging it, and
connections are named `tinyjobs` in `pg_stat_activity`. Every connection commits with
`synchronous_commit` at least `on`, whatever the server's default, so an acknowledged job survives a
crash of the server; a job enqueued through a connection of your own commits under your setting. With `log_statement` or
`log_min_duration_statement` on, Postgres logs the bound parameters (payloads and results in hex;
error messages, tracebacks and dedup keys as text) unless `log_parameter_max_length` is 0. The
tables and their schema must belong to the connecting role or a role that can become it (`SET` on
Postgres 16 and later), every role that inherits the owner's privileges must be able to become it
too, and no other role may hold `TRIGGER` or `REFERENCES` on them or own code that runs on them,
such as a trigger, a constraint's function or an index's; otherwise pass `schema=` with one it owns.
Behind PgBouncer use session pooling with its default `server_reset_query`. A transaction left open
for hours anywhere on the server stops autovacuum from cleaning the jobs table. Postgres 14 to 18
are supported, and CI tests 14 and 18.
