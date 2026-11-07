# Operating it

The runbook, in the order things come up.

**Deploy.** There is nothing to run besides `tinyjobs worker --app tasks:app`: add processes to
scale, and `--queues` to keep slow queues from starving fast ones. A new version of your tasks is a
rolling restart, one worker at a time. A change to the store's schema, announced in
[Status](status.md), is the one time to stop every worker first, since a running worker keeps
its old statements and runs them against the changed store. An older worker started later
refuses a schema newer than itself, so a rollback fails loudly instead of corrupting the queue,
except on a SQLite file that 0.14 created, where an older worker would start a queue of its own.
[SQLite](sqlite.md) has the 0.14 steps and what a process left running does; the upgrade order
that pausing needs is under [Cron](cron.md).

**Stop.** `SIGTERM` stops claiming and exits within `--shutdown-timeout` (30 s): running jobs get
most of it, and the async and isolated ones still running at the end are handed back so another
worker takes them at once; what a second Ctrl-C does and why the timeout must stay under the
orchestrator's grace period are in [FastAPI](fastapi.md). A worker killed outright leaves its jobs to
their leases, 60 s by default (`--lease`), before the reaper returns them to the queue.

**Watch.** Alert on `tinyjobs check` exiting 1, which a store configured against the
[guarantees](guarantees.md) does too, with `--store-warnings` where a development store is
volatile on purpose, or on `tinyjobs_store_expired_leases` above zero:
any value means a worker died, and the next worker's reaper returns its jobs within
`--reap-interval` (30 s); no worker left at all shows as the scrape target going down. A rising
`tinyjobs_store_oldest_due_seconds` is a backlog: more workers, or a queue of its own for the slow
task. `tinyjobs_jobs_total` by `outcome` and the `--log-format json` fields tell which task is
failing and why.

**Fix.** `jobs retry` requeues a failed or cancelled job and `jobs cancel` stops a queued one, or
a running one cooperatively. A cron job that keeps failing is `schedules pause`, and `jobs cancel`
on its queued retry, until the fix ships, then `schedules run` for the missed one and
`schedules resume`. Queued jobs for a task the app no longer registers are a missing `--import` on
the worker, or `jobs cancel` when the task is gone for good.
