# Limits

Two caps a task can carry, both counted across every worker on the store:

```python
@app.task(concurrency=2)
async def rebuild_index(tenant_id: int) -> None: ...

@app.task(rate="10/m")
async def call_partner_api(order_id: int) -> None: ...
```

`concurrency=2` means at most two jobs of `rebuild_index` run at once, whether one worker or ten
are up. `rate="10/m"` means two starts of `call_partner_api` are at least six seconds apart, so no
window of a minute holds more than ten. A task can carry both. The units are `s`, `m`, `h` and `d`,
or spelled out as `seconds`, `minutes`, `hours` and `days`, in either case, and a period can be
longer than one unit: `"1/30s"` is one start every thirty seconds.

A worker's own `concurrency` says how many jobs that one process runs at once; a task's says how
many of that task the whole deployment runs. The two are independent: a worker with `concurrency=8`
and a task with `concurrency=2` fills its six other slots with other work.

## What happens to a job the limit stops

A job of a task at its limit is not lost and not skipped. The claim that meets it moves it to the
`held` status, where it waits outside the claim's way, and the next claim with room for the task
takes it, in the order the queue promised: priority first, then the time it became due. Held jobs
show as `held` in `tinyjobs jobs`, in the dashboard and in the counts; `job.wait()` keeps waiting
on one, and `tinyjobs jobs cancel` cancels it like a queued job. A held job keeps its
deduplication key, so a duplicate sent while it waits is still collapsed onto it.

A held job starts as soon as the task has room: when a job of a concurrency-limited task finishes,
the worker that ran it claims again at once. A rate frees room with time, and the worker that
started the last job of a rate-limited task claims again when its interval ends, while every worker
that registers such a task polls at least as often as the shortest interval among its tasks, so a
backlog drains at the rate asked for.

Held jobs count as queued: the oldest due job in `tinyjobs stats`, the per-task figures of the
dashboard and the metrics, and `tinyjobs check --max-wait` count them with the queued ones, so a backlog stuck behind a saturated
task is reported like any other.

## What the limits mean exactly

- The running count is the jobs of the task in the `running` status, so a job whose worker died
  counts until its lease expires and the reaper returns it to the queue, within `--lease-seconds`.
  The other way round, a synchronous job that outlives its `timeout=` is recorded as failed while
  its thread runs on, and the room it took is free again; a task that must never run past its cap
  wants `isolated=True` or an async body, which a timeout does stop.
- A worker that registers a rate-limited task bounds its idle poll at the shortest interval among
  its tasks, with a floor of 50 ms so a very fast rate does not turn an idle worker into a flood of
  claims; an explicit `poll_minimum` above that wins, and the worker that started the last job of
  the task claims again when the interval ends whatever its poll is.
- A rate is spacing, not a bucket: a task idle for an hour starts one job, then the next an interval
  later. Ten jobs sent at once under `"10/m"` start over a minute. This is the one reading of "ten
  a minute" that holds in every window, which is what a quota wants.
- Limits are enforced by the store at the claim, with the values the claiming worker knows from its
  task definitions. Raise a limit in your code and restart the workers, and the next claim admits
  as many held jobs as the new room allows; lower it and the running jobs finish, then the smaller
  room applies; remove it and a worker that registers the task without a limit returns the task's
  held jobs on its queues to the queue, at start and on every reaper pass, so a rolling deployment
  that mixes old and new builds settles as soon as the old ones are gone.
- On a `django.tasks` task the two go on Django's own decorator, `@task(concurrency=2)`, or in
  the settings by import path; [Django](django.md) has both.
- `app.send("module.task")` from a process without the task's code is limited like any other job,
  because the limit belongs to the task name, which the worker knows.
- A worker that does not register a task knows no limit for it; such jobs are handed back as
  unknown, as before, and never run there.
- Eager mode runs the task inline at the call and ignores limits, since no claim happens.
  `app.drain()` claims, so it honours them: a drain returns with jobs held for a rate until the
  clock moves, as [Testing your tasks](testing-your-tasks.md) shows.

## Stores

SQLite, Postgres and Redis all enforce limits. A backend plugin says so with `limits` in its
capabilities; one that does not refuses to start a worker or a drain for an app whose tasks carry a
limit, rather than run them unlimited.

On Postgres a task keeps its row in the limits table after its limit is removed, and every claim
and finish of that task still locks that row; delete the row once no process declares the limit
if the task is hot enough for that to matter, and a process that still declares it creates the
row again from the running jobs.

Upgrading to 0.15 adds the `held` status and a limits table (keys, on Redis) to every store. Stop every
tinyjobs process on a store before starting the first 0.15 one: it upgrades the store on its first
call, and a process older than 0.15 refuses to start against the upgraded store, since it would not
know what a held job is.
