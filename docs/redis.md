# Redis

```bash
pip install 'tinyjobs[redis]'
```

```python
app = TinyJobs("redis://localhost:6379/0")
app = TinyJobs("redis://localhost:6379/0", prefix="billing")    # several apps on one server
```

Nothing else changes: the same tasks, workers, cron, hooks, `stats` and `check`. Every state change
is one Lua script, so a claim is as atomic as on SQLite, and any number of workers on any number of
machines can share the queue. With the default clock every machine keeps Redis's time, so their own
clocks need not agree; a `run_at` or `Retry(at=...)` you pass is read on the local clock and moved
onto Redis's. Reading that time takes a round trip, so on a host too loaded to answer within half a
second a delayed job can start up to a quarter of a second early. A `clock=` you pass yourself is used as it is, and must then agree across machines.
Leases hold even while a machine still runs on an old reading: the scripts never reap by a time
ahead of Redis's and never stamp a lease from one behind it. Keep the Redis server and its replicas
on NTP: after a failover to a node whose clock is ahead by more than a lease, every running job looks
abandoned and runs again, or fails when it has no attempts left. When upgrading from 0.7.0, which
used each machine's own clock, stop or upgrade every 0.7.0 process, workers and senders alike, before
starting the new workers if their clocks drift from Redis's by more than a lease, and grant `TIME`
to a Redis user restricted by ACLs. It passes the same contract suite as SQLite.

**Turn persistence on.** Redis keeps the queue in memory. Killed with `SIGKILL` mid-run, a Redis on
its default snapshots lost the whole queue; with `appendonly yes` the same crash lost nothing, and
the jobs that were running came back once their leases expired. A restart, graceful or not, is
otherwise harmless: workers log the outage, back off, and carry on.

Worth knowing: a job costs about 1 KB of Redis memory, retention prunes it like on SQLite, and a job
cannot join your database's transaction. All keys share one hash slot (`{prefix}`), so it runs on a
single Redis or a primary with replicas, not spread across a cluster.

Each pool holds at most 100 connections (`max_connections=`). A burst beyond that waits for a free
one rather than failing, and gives up with redis-py's `ConnectionError` after `pool_timeout=` seconds
(20). The app has one pool, opened on first use; `worker.run()` and `tinyjobs worker` open one of
their own for claims, outcomes and cron, plus one for each extra loop.
