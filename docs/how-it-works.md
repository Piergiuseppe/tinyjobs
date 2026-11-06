# How it works

`jobs.db` holds two tables. `jobs` is the queue, `executions` is the history of attempts.

A worker claims work with a single statement, which is what makes it safe to run as many workers as you like:

```sql
UPDATE jobs SET status = 'running', worker_id = ?, attempts = attempts + 1, lease_expires_at = ?
 WHERE id IN (SELECT id FROM jobs
               WHERE status = 'queued' AND run_at <= ? AND queue IN (?)
               ORDER BY priority DESC, run_at ASC LIMIT ?)
RETURNING *;
```

Claiming a job takes a lease. The worker refreshes it while the job runs, and if the worker dies the
lease expires and another worker picks the job up. On Redis and Postgres, a worker that cannot
record a job's outcome because the server went away keeps trying until a second before its lease
would run out, so a restart or a failover that ends by then does not run the finished job again,
unless the worker is shut down first and its `shutdown_timeout` ends the wait. A claim whose answer
is lost is handed back without the attempt counted; if the server stays away past the lease, the
lease expires first and the attempt counts, so a job with `max_attempts=1` ends `failed` without
having run rather than risk running twice. On SQLite older than
3.35 the same claim runs as two statements inside `BEGIN IMMEDIATE`.

Because it's just SQLite, you can read the queue with anything:

```bash
sqlite3 jobs.db "SELECT task, status, attempts, payload FROM jobs WHERE status = 'failed';"
```

```
sync_invoice|failed|3|{"args":[8842],"kwargs":{}}
```

That readability is most of the reason for preferring JSON over pickle.
