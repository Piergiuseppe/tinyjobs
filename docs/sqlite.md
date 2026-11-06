# SQLite

```python
app = TinyJobs("sqlite:///jobs.db")                   # a path next to where you run
app = TinyJobs("sqlite:////var/lib/shop/jobs.db")     # an absolute one, four slashes
app = TinyJobs("sqlite:///jobs.db", busy_timeout=10)  # seconds to wait for the write lock, 5 by default
```

The default, with nothing to install: `TinyJobs()` alone opens `tinyjobs.db`. The file and its
directory are created on first use, in WAL mode with `synchronous=NORMAL`, so readers never wait
for the writer, a crashed process loses nothing it committed, and only a power cut can roll back
the last commits. Any number of workers and senders on the same machine can share the file. SQLite
allows one writer at a time, so every claim and outcome takes its turn on the write lock; a process
that finds it taken waits up to `busy_timeout` rather than failing. Workers poll, since SQLite
cannot wake them: right after a claim that found work, then backing off from 5 ms to `--poll-max`
(0.5 s). A worker refuses `loops` above 1 here; run more processes instead.

Expect low thousands of jobs per second on a local SSD, bounded by `fsync` rather than by Python:
four processes of eight slots drain 5,200 no-op jobs a second from one file, and three workers held
25 jobs per second for ten hours of restarts and kills without losing one. Keep the file on a local
disk: SQLite's locking over NFS and SMB is unreliable, and `tinyjobs check` fails on one on Linux.
Pruning keeps the file at a plateau instead of shrinking it; see [retention](keeping-the-database-from-growing-forever.md).
A job cannot join your own database's transaction yet. The queue is a plain database, so the
`sqlite3` shell reads it, as [How it works](how-it-works.md) shows, and `memory://` is the same
backend on a private in-memory database for [tests](testing-your-tasks.md).
