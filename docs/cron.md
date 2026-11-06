# Cron

```python
@app.task(cron="*/15 * * * *")
async def refresh_cache() -> None:
    ...


@app.task(cron="30 2 * * *", timezone="Europe/Rome")
def nightly_report() -> None:
    ...
```

There is no extra process to run. Every `tinyjobs worker` also runs the scheduler, and however many
workers you start, each firing creates exactly one job: the schedule moves on with a single
conditional `UPDATE`, and only the worker whose update matched writes the job, in the same
transaction. Kill any of them, even with `kill -9`, and the rest carry on.

What it creates is an ordinary job, with the task's retries, queue and priority.

```bash
tinyjobs schedules --app tasks:app
```

```
NAME                  CRON          TIMEZONE     NEXT RUN             LAST RUN             LAST JOB
--------------------  ------------  -----------  -------------------  -------------------  ------------------------------------
tasks.nightly_report  30 2 * * *    Europe/Rome  2026-10-11 02:30:00  -                    -
tasks.refresh_cache   */15 * * * *  UTC          2026-10-10 14:15:00  2026-10-10 14:09:00  01a125b7-6b62-7135-b9a0-b7707bf39c1c
```

```bash
tinyjobs worker --app tasks:app --no-scheduler     # runs jobs, fires nothing
tinyjobs scheduler --app tasks:app                 # fires schedules, runs nothing
```

A schedule can be stopped for a while, or run now, without touching code:

```bash
tinyjobs schedules pause tasks.nightly_report --app tasks:app
tinyjobs schedules resume tasks.nightly_report --app tasks:app
tinyjobs schedules run tasks.nightly_report --app tasks:app       # one job, now
```

In code they are `app.pause_schedule(name)`, `app.resume_schedule(name)` and
`app.run_schedule(name)`. A paused schedule shows `paused` as its next run and as an ok line in
`check`, fires nothing until resumed, and survives restarts and deploys; resuming picks the next
time from now, with no catch-up.

Changing the cron expression or the timezone starts the schedule afresh, which also un-pauses it,
and so does any worker or scheduler older than 0.11, which reads a pause as a clock that moved
back, while an older `tinyjobs schedules` fails to list one: upgrade them all before pausing
anything.

`run` enqueues one ordinary job with the schedule's dedup key, so while the previous run is still
queued or running you get that job back instead of a new one, unless `overlap=True`; the next
scheduled firing stays where it was.

When nothing was running at the moment a schedule was due, `misfire` decides what happens on restart:

```python
@app.task(cron="0 * * * *")                                   # skip, the default
@app.task(cron="0 * * * *", misfire="run_once")               # one catch-up run
@app.task(cron="0 0 * * *", misfire="run_all", overlap=True)  # every missed day, oldest first
```

A firing only counts as missed when it is more than a minute late (`--misfire-grace`). A schedule
won't start a run while the previous one is still queued or running, unless `overlap=True`.

A malformed expression, an unknown timezone, `0 0 30 2 *` (which never fires) or a cron task that
needs arguments all raise at import, not at 2 a.m.

Worth knowing:

- **The resolution is one minute.**
- **An `async` task that blocks the event loop also blocks its worker's scheduler**, which then skips
  the firings it slept through. Write blocking work as `def`, or run a standalone `tinyjobs scheduler`.
- **Daylight saving.** A time of day (`30 2 * * *`) fires once, even when 02:30 happens twice or not
  at all. A frequency (`*/10 * * * *`) follows the real clock: the repeated hour runs twice, the
  skipped one not at all. Local timezones on Windows need `pip install tzdata`.
- **Clocks** that disagree by less than the grace are harmless. A jump forward counts as an outage; a
  jump back is noticed on the next tick.
- **One schedule per task name.** For two cadences, register the function twice under two names.
- **Volume.** Ten thousand schedules on the same minute all fire exactly once, the last few seconds
  late. Numbers in [Measured](measured.md).
