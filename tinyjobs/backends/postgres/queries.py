from __future__ import annotations

from .records import JOB_COLUMNS, JOB_TYPES, job_columns
from .schema import Tables

READING = "SELECT extract(epoch FROM clock_timestamp())::double precision AS now"

SERVER = "extract(epoch FROM statement_timestamp())::double precision"

NOW = "%(now)s::double precision"

NOTICE_CHARACTERS = 1000


class Queries:
    def __init__(self, tables: Tables, following: bool) -> None:
        self.schema = tables.schema
        jobs, executions = tables.jobs, tables.executions
        schedules, meta = tables.schedules, tables.meta
        columns = job_columns()
        horizon = f"least({NOW}, {SERVER})" if following else NOW
        lease_end = (
            f"greatest(%(lease)s::double precision, {SERVER} + %(seconds)s::double precision)"
            if following
            else "%(lease)s::double precision"
        )
        arrays = ", ".join(
            f"%({column})s::{kind}[]" for column, kind in zip(JOB_COLUMNS, JOB_TYPES)
        )

        self.server_time = READING

        self.insert = f"""
            WITH incoming AS (
                SELECT * FROM unnest({arrays}) WITH ORDINALITY
                    AS incoming ({columns}, position)
            ),
            inserted AS (
                INSERT INTO {jobs} ({columns})
                SELECT {columns} FROM incoming ORDER BY position
                ON CONFLICT DO NOTHING
                RETURNING id, queue, run_at <= {horizon} AS due
            )
            SELECT id, queue, due FROM inserted
        """

        self.by_ids = f"SELECT {columns} FROM {jobs} WHERE id = ANY(%(ids)s::text[])"

        self.active_by_keys = f"""
            SELECT {columns} FROM {jobs}
             WHERE dedup_key = ANY(%(keys)s::text[]) AND status IN ('queued', 'running')
        """

        self.claim = f"""
            WITH RECURSIVE levels (priority) AS (
                SELECT max(top.priority)
                  FROM unnest(%(queues)s::text[]) AS wanted (name)
                 CROSS JOIN LATERAL (
                       SELECT priority FROM {jobs}
                        WHERE status = 'queued' AND queue = wanted.name
                        ORDER BY priority DESC
                        LIMIT 1
                 ) AS top
                UNION ALL
                SELECT (
                       SELECT max(lower.priority)
                         FROM unnest(%(queues)s::text[]) AS wanted (name)
                        CROSS JOIN LATERAL (
                              SELECT priority FROM {jobs}
                               WHERE status = 'queued' AND queue = wanted.name
                                 AND priority < levels.priority
                               ORDER BY priority DESC
                               LIMIT 1
                        ) AS lower
                  )
                  FROM levels
                 WHERE levels.priority IS NOT NULL
            ),
            picked AS (
                SELECT due.id
                  FROM levels
                 CROSS JOIN LATERAL (
                       SELECT candidate.id, candidate.run_at
                         FROM unnest(%(queues)s::text[]) AS wanted (name)
                        CROSS JOIN LATERAL (
                              SELECT id, run_at FROM {jobs}
                               WHERE status = 'queued' AND queue = wanted.name
                                 AND priority = levels.priority AND run_at <= {horizon}
                               ORDER BY run_at ASC
                               LIMIT %(limit)s
                               FOR UPDATE SKIP LOCKED
                        ) AS candidate
                        ORDER BY candidate.run_at ASC
                        LIMIT %(limit)s
                 ) AS due
                 WHERE levels.priority IS NOT NULL
                 LIMIT %(limit)s
            ),
            claimed AS (
                UPDATE {jobs} AS job
                   SET status = 'running',
                       worker_id = %(worker)s,
                       attempts = job.attempts + 1,
                       claims = job.claims + 1,
                       lease_expires_at = {lease_end}
                  FROM picked
                 WHERE job.id = picked.id AND job.status = 'queued'
                RETURNING {job_columns("job")}
            ),
            opened AS (
                INSERT INTO {executions}
                       (job_id, attempt, claim, status, worker_id, started_at)
                SELECT id, attempts, claims, 'running', %(worker)s, {NOW} FROM claimed
                ON CONFLICT (job_id, claim) DO UPDATE
                   SET status = 'running', worker_id = excluded.worker_id,
                       started_at = excluded.started_at, finished_at = NULL,
                       error_type = NULL, traceback = NULL
            )
            SELECT {columns} FROM claimed ORDER BY priority DESC, run_at ASC
        """

        self.heartbeat = f"""
            UPDATE {jobs} SET lease_expires_at = {lease_end}
             WHERE status = 'running'
               AND (%(worker)s::text IS NULL OR worker_id = %(worker)s)
               AND id = ANY(%(ids)s::text[])
        """

        held = (
            "id = %(id)s AND status = 'running' AND worker_id = %(worker)s "
            "AND claims = %(claims)s"
        )
        close_execution = f"""
            closed AS (
                UPDATE {executions} AS execution
                   SET status = %(execution_status)s, finished_at = %(now)s,
                       error_type = %(error_type)s, traceback = %(traceback)s
                  FROM changed
                 WHERE execution.job_id = changed.id AND execution.claim = changed.claims
            )
        """

        self.complete = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'success', finished_at = %(now)s, result = %(result)s,
                       lease_expires_at = NULL, last_error = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, claims
            ),
            {close_execution}
            SELECT count(*) AS changed FROM changed
        """

        self.fail = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'failed', finished_at = %(now)s, last_error = %(summary)s,
                       lease_expires_at = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, claims
            ),
            {close_execution}
            SELECT count(*) AS changed FROM changed
        """

        self.reschedule = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, last_error = %(summary)s,
                       lease_expires_at = NULL, worker_id = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, claims
            ),
            {close_execution}
            SELECT count(*) AS changed FROM changed
        """

        self.release = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, lease_expires_at = NULL,
                       worker_id = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id
            ),
            lost AS (
                UPDATE {executions} SET status = 'lost', finished_at = %(now)s
                 WHERE status = 'running' AND job_id IN (SELECT id FROM changed)
            )
            SELECT count(*) AS changed FROM changed
        """

        self.hand_back = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, attempts = attempts - 1,
                       hand_backs = hand_backs + 1, lease_expires_at = NULL, worker_id = NULL
                 WHERE {held}
                RETURNING id, claims
            ),
            forgotten AS (
                DELETE FROM {executions} AS execution USING changed
                 WHERE execution.job_id = changed.id AND execution.claim = changed.claims
            )
            SELECT count(*) AS changed FROM changed
        """

        self.cancel = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'cancelled', finished_at = %(now)s, lease_expires_at = NULL,
                       hand_backs = 0
                 WHERE id = %(id)s AND status IN ('queued', 'running')
                RETURNING id
            ),
            closed AS (
                UPDATE {executions} SET status = 'cancelled', finished_at = %(now)s
                 WHERE status = 'running' AND job_id IN (SELECT id FROM changed)
            )
            SELECT count(*) AS changed FROM changed
        """

        self.requeue = f"""
            UPDATE {jobs} AS target
               SET status = 'queued',
                   run_at = CASE WHEN claims = 0 THEN created_at ELSE %(now)s END,
                   attempts = 0, finished_at = NULL, lease_expires_at = NULL,
                   worker_id = NULL, last_error = NULL
             WHERE id = %(id)s AND status IN ('failed', 'cancelled')
               AND NOT EXISTS (
                   SELECT 1 FROM {jobs} AS holder
                    WHERE holder.dedup_key = target.dedup_key
                      AND holder.status IN ('queued', 'running')
               )
            RETURNING id, queue, run_at <= {horizon} AS due
        """

        self.notify = (
            f"SELECT count(pg_notify(%(channel)s, left(queue, {NOTICE_CHARACTERS}))) AS sent "
            "FROM unnest(%(queues)s::text[]) AS woken (queue)"
        )

        self.reap = f"""
            WITH expired AS (
                SELECT id FROM {jobs}
                 WHERE status = 'running' AND lease_expires_at < {horizon}
                 FOR UPDATE SKIP LOCKED
            ),
            reaped AS (
                UPDATE {jobs} AS job
                   SET status = CASE WHEN job.attempts < job.max_attempts
                                     THEN 'queued' ELSE 'failed' END,
                       run_at = {horizon},
                       finished_at = CASE WHEN job.attempts < job.max_attempts
                                          THEN NULL ELSE {horizon} END,
                       lease_expires_at = NULL, worker_id = NULL, hand_backs = 0,
                       last_error = %(message)s
                  FROM expired
                 WHERE job.id = expired.id AND job.status = 'running'
                RETURNING job.id, job.queue, job.status
            ),
            lost AS (
                UPDATE {executions} AS execution
                   SET status = 'lost', finished_at = {horizon}
                  FROM reaped
                 WHERE execution.job_id = reaped.id AND execution.status = 'running'
            )
            SELECT count(*) AS reaped,
                   coalesce(array_agg(DISTINCT queue) FILTER (WHERE status = 'queued'), '{{}}')
                       AS queues
              FROM reaped
        """

        self.get = f"SELECT {columns} FROM {jobs} WHERE id = %(id)s"

        self.list = (
            f"SELECT {columns} FROM {jobs} {{where}} ORDER BY id DESC LIMIT %(limit)s"
        )

        self.executions = f"""
            SELECT id, job_id, attempt, status, worker_id, started_at,
                   finished_at, error_type, traceback
              FROM {executions} WHERE job_id = %(id)s ORDER BY claim ASC
        """

        self.count_by_status = (
            f"SELECT status, count(*) AS total FROM {jobs} GROUP BY status"
        )

        self.count_by_queue = (
            f"SELECT queue, status, count(*) AS total FROM {jobs} GROUP BY queue, status"
        )

        self.oldest_due_by_queue = f"""
            SELECT queue, min(run_at) AS oldest FROM {jobs}
             WHERE status = 'queued' AND run_at <= %(now)s
             GROUP BY queue
        """

        self.count_expired = f"""
            SELECT count(*) AS total FROM {jobs}
             WHERE status = 'running' AND lease_expires_at < %(now)s
        """

        self.queued_by_task = f"""
            SELECT task, count(*) AS total FROM {jobs}
             WHERE status = 'queued' GROUP BY task
        """

        self.claim_sweep = f"""
            UPDATE {meta} SET value = %(now)s::text
             WHERE key = 'last_purge_at'
               AND (value::double precision = 0 OR value::double precision <= %(cutoff)s)
            RETURNING key
        """

        self.purge_batch = f"""
            WITH gone AS (
                DELETE FROM {jobs}
                 WHERE id IN (
                       SELECT id FROM {jobs}
                        WHERE status = %(status)s AND finished_at IS NOT NULL
                          AND finished_at < %(cutoff)s - ({NOW} - {horizon})
                        ORDER BY finished_at
                        LIMIT %(batch)s
                        FOR UPDATE SKIP LOCKED
                 )
                RETURNING id
            )
            SELECT count(*) AS removed FROM gone
        """

        self.schedules = f"""
            SELECT name, cron, timezone, next_run_at, last_run_at, last_job_id
              FROM {schedules} ORDER BY name
        """

        self.register_schedules = f"""
            INSERT INTO {schedules} AS stored (name, cron, timezone, next_run_at)
            SELECT * FROM unnest(%(names)s::text[], %(crons)s::text[],
                                 %(timezones)s::text[], %(next_runs)s::double precision[])
            ON CONFLICT (name) DO UPDATE
               SET cron = excluded.cron, timezone = excluded.timezone,
                   next_run_at = excluded.next_run_at
             WHERE stored.cron IS DISTINCT FROM excluded.cron
                OR stored.timezone IS DISTINCT FROM excluded.timezone
        """

        self.advance_schedule = f"""
            UPDATE {schedules} SET next_run_at = %(next_run_at)s
             WHERE name = %(name)s AND next_run_at = %(expected)s
        """

        self.record_schedule_run = f"""
            UPDATE {schedules} SET last_run_at = %(now)s, last_job_id = %(job_id)s
             WHERE name = %(name)s
        """

        self.schema_version = (
            f"SELECT value FROM {meta} WHERE key = 'schema_version'"
        )

        self.table_exists = "SELECT to_regclass(%(name)s) IS NOT NULL AS present"

        self.instance = f"SELECT value FROM {meta} WHERE key = 'instance'"

        self.placement = """
            SELECT n.nspname AS schema,
                   bool_and(pg_has_role(current_user, c.relowner, 'MEMBER')) AS owned,
                   count(*) AS tables
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE c.oid = ANY(ARRAY[
                       to_regclass(%(jobs)s), to_regclass(%(executions)s),
                       to_regclass(%(schedules)s), to_regclass(%(meta)s)
                   ]::oid[])
             GROUP BY n.nspname
        """
