# Moving to another store

`tinyjobs move` takes the jobs still to run out of one store and puts them in another: from a
SQLite file to Postgres or Redis, between two Redis servers, in any direction between the three,
and back. New jobs never stop being accepted or run. Only the jobs left behind in the old store
wait, from the moment its workers stop until the command ends.

## The steps

1. Deploy the application and its workers with the address of the new store. The restart sends
   `SIGTERM` to the old workers, which finish the jobs they hold; the new ones start on the new
   store, where every job sent from now on goes. Cron moves with this step: the new workers
   register the schedules in the new store.
2. Move what the old store still holds:

```bash
tinyjobs move --from sqlite:///jobs.db --to redis://localhost:6379/0
```

```text
moved 20,000 job(s) from the old store (sqlite) to the new store (redis)
nothing left to run in the old store (sqlite)
```

It exits 0 when the old store has nothing left to run, 1 when it has, so it can be run again
until it does, and 2 when it refused to start or stopped on an error, which running it again
does not cure by itself; whatever ended it, it first prints what it had moved, which after a
second `Ctrl-C` can be a job short. `--app tasks:app`
in place of `--to` moves to the store and the prefix the application now points at, which is the
safest way to name them: the command cannot know whether the store `--to` names is the one the
workers read, and jobs moved to another database or prefix wait there until they are moved
again. `--from-prefix` names a key or table prefix on the old store and `--to-prefix` one on the
store `--to` names. `--dry-run` counts what would move now and moves nothing; it exits 0, or 1
if it was stopped before the end, and names apart the jobs of a worker that has stopped
reporting, which a real run returns to the queue first. A first `Ctrl-C` stops the command after
the jobs it is on, a second at once. A SQLite file has to be reachable from where the command
runs. The command names a store by its kind and does not print its address, in a report or in an
error, since an address can carry a password; an error about a malformed address, or one raised
by a database driver, can still quote a host or an option.

A moved job is deleted from the old store once the new one has taken it, so the new store has
to keep what it takes: run `tinyjobs check` on it first, which a Redis without `appendonly` or a
Postgres with `fsync` off fails.

Run the move when the old store is quiet: no worker on it, and nothing sending to it. With several
instances replaced one at a time, the old ones keep sending to the old store until the last is
gone, so run the command then.

## What moves

Every job that is queued, scheduled for later, held by a limit or waiting for another job, with
its id, arguments, queue, priority, time to run, attempts already used and deduplication key. A
job held by a limit arrives queued, and the workers of the new store hold it again if the limit
says so. A chain of jobs sent with `after=` moves whole, and its jobs run in their order in the
new store. A job that meets its deduplication key already held in the new store is not sent, and
the one there stands for both.

A moved job is deleted from the old store, with the record of its earlier attempts, so nothing
of it is left there to be retried or mistaken for a job still to run. What stays in the old
store:

- Jobs that already ended, with their results and attempt history. A job whose parent already
  succeeded moves without the link to it.
- A job still running. It finishes there. If its worker was killed, the command returns it to the
  queue once its lease has ended and moves it, unless that was its last attempt, which fails it
  as a worker's reaper would; until the lease ends it is left, with the jobs waiting for it, and
  the command exits 1.
- A job that needs what the new store lacks. The three stores of tinyjobs take everything; one
  written by someone else may not keep jobs waiting for another, deduplication keys or jobs
  scheduled for later, or may cap the size of a job. Such jobs stay, the command says how many
  and why, and it exits 1 on every run until they are dealt with.
- Schedules and their pause state. The new store starts from the schedules its workers declare.

## What it guarantees

Every job still to run is in the new store before it leaves the old one, under its id, or as
the job holding its deduplication key when it met one there. So a command that stops anywhere
loses nothing, and the next run finishes it: sending a job the new
store already holds changes nothing there, whatever became of the copy meanwhile, run, failed or
cancelled, and the job is then deleted from the old store. Two runs at once move each job once.

A job runs in both stores in two cases. A worker still running on the old store takes it
between the copy and the delete, which are a few hundred jobs apart: the delete is refused, the
job goes on running there, and the command names it. If that worker fails the job and returns
it to the queue before the delete, the delete goes through and the command has nothing to
name: the copy runs, with the attempts the job had used when it was read.

```text
100 job(s) were started by a worker of the old store (redis) while they moved, and run in both
stores: 019a8d2e-..., ...; stop its workers before moving
```

Or a run stops halfway, and before the next one the new store loses what stood for a job
there: its retention prunes the copies it had run, a week later by default, or the job holding
the deduplication key that a moved job had met ends. Those jobs are sent again. At-least-once
delivery allows all of it.

A job someone cancels in the old store in that same interval keeps its copy in the new store,
and the command names it too.

A job sent to the old store while it is being emptied is never cancelled by the move. Sent
behind a job that is moving, it keeps that job in the old store, and the next run takes the two
together; sent behind one that has left, it is refused with `JobNotFoundError`, as after any id
the store does not hold. On Postgres one case escapes: a job sent behind another in the very
instant that one is deleted is cancelled by the store on its next sweep, as a job whose parent
is gone.

Two stores are two sets of keys and limits while both hold jobs: a deduplication key or a limit
of `concurrency=1` holds inside each, not across them.

## What it refuses

- A store in memory at either end, such as `sqlite:///` with nothing after it: it lives and ends
  with the command, so the jobs moved to it would be gone.
- One store under two addresses, which would otherwise have its jobs deleted for a copy that is
  the job itself. To tell, a real run begins by writing one job to the new store, in the queue
  `tinyjobs-move` that no worker reads, looks for it in the old one, and deletes it. A run
  killed in between leaves that job behind, and a run more than an hour later deletes it.
- An old store that holds no job of any status. That is what a mistyped path or prefix looks
  like, since opening a store that is not there creates it; for a SQLite file that does not
  exist the command says so and creates nothing. It is also what a store looks like once
  everything has left it and its ended jobs were pruned.

## How fast

20,000 jobs, one in ten in a chain, moved from a SQLite file to a Redis 7 beside it in 20
seconds, about 1,000 jobs a second, and back in 25, on two cores of a shared four-core ARM Linux
VPS. The command makes three calls for each job, one after the other: it reads it, sends it and
deletes it. So the round trip to the slower store sets the pace. It keeps the ids of the jobs to
move in memory.
