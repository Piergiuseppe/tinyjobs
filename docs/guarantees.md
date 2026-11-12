# Guarantees

What tinyjobs promises, on every backend, and how to see that it holds on yours.

- A job that `enqueue` acknowledged runs until it succeeds or uses its attempts: a worker killed
  outright leaves it to its lease, and the reaper returns it to the queue. A job sent to run
  [after another](jobs.md#one-job-after-another) is promised the same from the moment that one
  succeeds, and is otherwise cancelled, on the record, with the reason.
- A job finishing while its worker dies may run again, never be lost. The second run shows as a
  second execution, so an idempotent task makes the whole thing effectively once.
- Cron fires once per due time from any number of workers, a deduplication key refuses a second
  job while one is pending or running, and on Postgres and SQLite a job enqueued in a transaction
  that rolls back never exists.

They hold when the store keeps what it acknowledged, so `tinyjobs check` audits its configuration
and fails on what would break them: a Redis without `appendonly`, whose last append-only write or
snapshot failed, with an `allkeys` eviction policy, nearly full, or a replica; a SQLite file not in
WAL mode, with `synchronous` off, on a network filesystem, on `tmpfs` or a container's overlay
layer, itself a mount point or on a disk with under 64 MB free; a Postgres with `fsync` off,
Neon's excepted, or a standby. Each failure names the setting and its remedy, and a setting the
server will not reveal is reported as not checked.

On Postgres, `synchronous_commit` is not left to the server: every connection tinyjobs opens sets
it to `on` where the server's default is `off`, and keeps a stronger level such as `remote_apply`,
so a job acknowledged to its sender survives a crash of the server whatever the server was
configured for. The audit then names a default of `off`, since a job enqueued through a
connection of your own, inside your transaction, still commits under it.

A development setup breaks the guarantees on purpose, a Redis without `appendonly` in a compose
file, a SQLite file on the container's layer. `tinyjobs check --store-warnings` prints those
findings as `WARN` and leaves the exit status to the rest of the check, so a readiness probe
stays green there; `app.check(store_warnings=True)` is the same in code.

`tinyjobs verify` proves them on your own setup, on a store that holds no other jobs:

```bash
tinyjobs verify redis://localhost:6379/9
```

It starts three workers on a queue of its own, sends 50 jobs a second for 30 seconds, stops a
worker every five seconds, `SIGTERM` and `SIGKILL` in turn, and starts another in its place,
then waits for the store to settle and reports:

```
sent 1,500 jobs: 1,500 succeeded, 0 failed, 0 cancelled, 0 unfinished, 0 lost
workers stopped 5 times, 3 by SIGTERM and 2 by SIGKILL, 0 unexpected deaths
4 jobs ran more than once, after the kills; every job applied its side effect once
end-to-end p50 69 ms, p95 108 ms, kills included
0 enqueue failures; the store is left as it was found
ok
```

The side effect is a row per job in a ledger the workers share, written with `INSERT OR IGNORE`,
which is the idempotency the contract asks of your tasks: a job that ran twice still counts once.
It exits 1 on a job lost, failed, cancelled or unfinished, a side effect missing or a ledger it
cannot read, a worker dying on its own or not stopping cleanly on `SIGTERM`, an enqueue failing,
a store that does not settle or fails mid-run, a run cut short, a run too short to kill anyone,
or a job left in the store unless other jobs appeared, so it can gate a deploy; a store refused
or unreachable, or settings that make no sense, exit 2 before anything runs. Jobs are matched by
number, so a loss cannot hide behind a send that failed after the store had taken it, and a job
enqueued twice is caught. `--duration`, `--rate`, `--workers`, `--slots`, `--kill-every`,
`--lease` and `--settle` set the load, with `--settle` above `--lease` plus the 1 s reap interval
since a killed job comes back when its lease ends, and sized to twice the lease when only
`--lease` is raised, `--prefix` the namespace in the store, and `--keep` leaves the
finished jobs in the store for `tinyjobs jobs` and the dashboard instead of purging them. It
refuses an in-memory store, since the workers could not share it, and a store that holds other
jobs, and purges its own at the end, leaving only the store's schema rows behind, unless other
jobs appeared during the run, in which case it purges nothing and says so. A verifier killed
outright leaves its workers to stop themselves, and its temporary directory and its jobs in the
store behind, which a new file, a flushed Redis database or dropped Postgres tables clear. The
URL is printed with its password masked. What it does not do, stopping the store or skewing
clocks, is in [Measured](measured.md).
