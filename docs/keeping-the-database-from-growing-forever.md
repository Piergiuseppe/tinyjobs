# Keeping the database from growing forever

Terminal jobs are pruned on a schedule. Successes and cancellations are kept for a week, failures for
thirty days, because failures are the ones you come back to:

```python
app = TinyJobs("sqlite:///jobs.db", retention=RetentionPolicy(failed=90 * 86400))
app = TinyJobs("sqlite:///jobs.db", retention=KEEP_EVERYTHING)   # prune nothing
```

```bash
tinyjobs worker --app tasks:app --retain-success 7d --retain-failed 30d
tinyjobs worker --app tasks:app --no-purge

tinyjobs purge --app tasks:app --before 30d --dry-run
```

Every worker checks whether a sweep is due, and exactly one of them wins, because claiming the sweep
is a single conditional `UPDATE` on a marker row. So running forty workers still means one sweep per
interval, with no leader to elect. Queued and running jobs are never touched at any horizon, and each
sweep deletes in small batches, so it never holds the write lock long enough to stall a claim. A
sweep that reaches its cap of 50,000 jobs goes straight on with another, so pruning keeps up with
any rate a queue finishes jobs at.

Note that pruning does not shrink the file. SQLite reuses the freed pages, so the database
**plateaus** rather than shrinking. Handing space back to the filesystem needs `VACUUM`, which wants
exclusive access, so it stays a manual thing.
