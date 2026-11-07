# SQLite

```python
app = TinyJobs("sqlite:///jobs.db")                   # a path next to where you run
app = TinyJobs("sqlite:////var/lib/shop/jobs.db")     # an absolute one, four slashes
app = TinyJobs("sqlite:///jobs.db", busy_timeout=10)  # seconds to wait for the write lock, 5 by default
app = TinyJobs("sqlite:///app.db", prefix="shop")      # the tables are shop_jobs, shop_executions, ...
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
The queue is a plain database, so the `sqlite3` shell reads it, as [How it works](how-it-works.md)
shows, and `memory://` is the same backend on a private in-memory database for
[tests](testing-your-tasks.md).

**Enqueue inside your own transaction.** The tables are `tinyjobs_jobs`, `tinyjobs_executions`,
`tinyjobs_schedules` and `tinyjobs_meta`, plus an empty `tinyjobs_install_lock` that a first
enqueue inside a transaction writes to, so the queue can live in the file your application
already uses, and a job sent there with your `sqlite3` connection or cursor as `tx` commits or
rolls back with your own rows:

```python
app = TinyJobs("sqlite:///app.db")

connection = sqlite3.connect("app.db")
with connection:
    connection.execute("INSERT INTO orders (id) VALUES (?)", (42,))
    send_receipt.with_options(tx=connection).enqueue(42)
connection.close()
```

`app.send(name, tx=connection)` and the async calls take it too. The insert runs on your
connection, in your thread, and tinyjobs neither begins nor commits: the transaction is yours, as
is the write lock from your first write to your commit, so keep the block as short as the one that
writes the order. The enqueue writes before it reads, so it can be the first statement of a plain
`BEGIN`; a transaction that read before any write meets "database is locked" at once when a
worker committed in between, which is SQLite's rule in WAL mode, and the error then says so and
what to do. That only concerns a transaction you opened yourself: with `isolation_level=None`,
open a block that reads first with `BEGIN IMMEDIATE`; with `autocommit=False`, which always
begins deferred, enqueue before any read. The module's default transaction control opens the
transaction at your first write, so a read before it runs outside any transaction and is not
affected. The connection must be open on the file the app uses, or the call raises
`ConfigurationError` naming the file it expected, and an in-memory store refuses `tx`. The first
call of a process may well be this one: the app need not be connected, and it checks the schema
through a short connection of its own, outside your transaction; a file without the tables, or
with an older queue, gets them through your connection, inside your transaction when one is open,
where a short write takes the file's write lock before the install reads anything, and in a
short `BEGIN IMMEDIATE` of its own otherwise, which waits for the write lock up to your
connection's `timeout`. Only that second case switches the file to WAL, so a process that only ever enqueues
inside transactions leaves that to the first worker, or to `tinyjobs check`, which switches it
too. Tables of your own named `jobs` or `meta` are left alone: only a queue from before 0.14,
recognised by its columns, is migrated. `prefix=` renames the tables, for two apps on one file or
a name your application has taken; it follows the Postgres rule, lowercase letters, digits and
underscores, forty characters at most.

**Upgrading to 0.14.** The tables were `jobs`, `executions`, `schedules` and `meta` before. Stop
every tinyjobs process on the file, the workers and the application processes that enqueue, then
start one 0.14 process: it renames the four tables in place, keeps every row, and leaves a bare
`meta` table behind so that any tinyjobs process older than 0.14 refuses the file instead of
starting a second queue in it. A worker left running fails every call against the renamed tables
until it is stopped, and a job it held runs again once its lease expires, even one it had finished.
A file from before 0.14 is migrated under the default prefix only, and its jobs stay there; open
it once that way before giving an app another prefix.
