from __future__ import annotations

from .records import JOB_COLUMNS, JOB_TYPES, job_columns
from .schema import Tables

READING = "SELECT extract(epoch FROM pg_catalog.clock_timestamp())::double precision AS now"

SERVER = "extract(epoch FROM pg_catalog.statement_timestamp())::double precision"

NOW = "%(now)s::double precision"

NOTICE_CHARACTERS = 1000

SPAN_LIMIT = 128

SPAN_KEPT = 8


def _qualified(kind: str) -> str:
    return kind if " " in kind or kind == "integer" else f"pg_catalog.{kind}"


def trusting(owner: str) -> str:
    return f"""(
        pg_catalog.pg_has_role({owner}, CURRENT_USER, 'USAGE')
        OR ({owner} OPERATOR(pg_catalog.=) pg_catalog.to_regrole('pg_database_owner')
            AND pg_catalog.pg_has_role(
                (SELECT d.datdba FROM pg_catalog.pg_database AS d
                  WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()),
                CURRENT_USER,
                'USAGE'
            ))
    )"""


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
            f"pg_catalog.unnest(%({column})s::{_qualified(kind)}[])"
            for column, kind in zip(JOB_COLUMNS, JOB_TYPES)
        )

        self.server_time = READING

        self.insert = f"""
            WITH incoming AS (
                SELECT * FROM ROWS FROM ({arrays}) WITH ORDINALITY
                    AS incoming ({columns}, position)
            ),
            inserted AS (
                INSERT INTO {jobs} ({columns})
                SELECT {columns} FROM incoming ORDER BY position
                ON CONFLICT DO NOTHING
                RETURNING id, queue, run_at OPERATOR(pg_catalog.<=) {horizon} AS due
            )
            SELECT id, queue, due FROM inserted
        """

        self.by_ids = (
            f"SELECT {columns} FROM {jobs} "
            "WHERE id OPERATOR(pg_catalog.=) ANY(%(ids)s::pg_catalog.text[])"
        )

        self.active_by_keys = f"""
            SELECT {columns} FROM {jobs}
             WHERE dedup_key OPERATOR(pg_catalog.=) ANY(%(keys)s::pg_catalog.text[])
               AND status OPERATOR(pg_catalog.=) ANY(ARRAY['queued', 'running']::pg_catalog.text[])
        """

        def taking(picked: str) -> str:
            return f"""
            WITH RECURSIVE {picked},
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

        def chunk(queue: str) -> str:
            return f"""
                SELECT max(priority) FILTER (WHERE run_at <= {horizon}) AS due,
                       CASE WHEN count(*) = levels.span THEN min(priority) END AS last,
                       max(priority) AS top
                  FROM (
                       SELECT priority, run_at FROM {jobs}
                        WHERE status = 'queued' AND queue = {queue}
                          AND priority < levels.priority
                        ORDER BY priority DESC, run_at ASC
                        LIMIT levels.span
                  ) AS chunk
            """

        def levels(seen: str) -> str:
            return f"""
            levels (priority, due, span) AS (
                SELECT 2147483648::bigint, false, 1
                UNION ALL
                SELECT CASE WHEN seen.due >= seen.last OR seen.last IS NULL
                            THEN seen.due ELSE seen.last END::bigint,
                       coalesce(seen.due >= seen.last, seen.last IS NULL),
                       CASE WHEN seen.due >= seen.last OR seen.last IS NULL THEN 1
                            WHEN seen.top > seen.last OR levels.span < {SPAN_KEPT}
                            THEN least(levels.span * 2, {SPAN_LIMIT}) ELSE 1 END
                  FROM levels
                 CROSS JOIN LATERAL ({seen}) AS seen
                 WHERE levels.priority IS NOT NULL
            )
            """

        one_queue = levels(chunk("%(queue)s"))

        many_queues = levels(f"""
            SELECT max(each.due) AS due, max(each.last) AS last, max(each.top) AS top
              FROM unnest(%(queues)s::text[]) AS wanted (name)
             CROSS JOIN LATERAL ({chunk("wanted.name")}) AS each
        """)

        def oldest_first(queue: str) -> str:
            return f"""
                SELECT id FROM {jobs}
                 WHERE status = 'queued' AND queue = {queue}
                   AND priority = levels.priority AND run_at <= {horizon}
                 ORDER BY run_at ASC
                 LIMIT %(limit)s
                 FOR UPDATE SKIP LOCKED
            """

        self.claim_one_queue = taking(f"""
            {one_queue},
            picked AS (
                SELECT due.id
                  FROM levels
                 CROSS JOIN LATERAL ({oldest_first("%(queue)s")}) AS due
                 WHERE levels.due
                 LIMIT %(limit)s
            )
        """)

        self.claim_many_queues = taking(f"""
            {many_queues},
            picked AS (
                SELECT due.id
                  FROM levels
                 CROSS JOIN LATERAL (
                       SELECT wanted.name
                         FROM unnest(%(queues)s::text[]) WITH ORDINALITY
                              AS wanted (name, position)
                        CROSS JOIN LATERAL (
                              SELECT run_at FROM {jobs}
                               WHERE status = 'queued' AND queue = wanted.name
                                 AND priority = levels.priority AND run_at <= {horizon}
                               ORDER BY run_at ASC
                               LIMIT 1
                        ) AS head
                        ORDER BY head.run_at, wanted.position
                 ) AS ready
                 CROSS JOIN LATERAL ({oldest_first("ready.name")}) AS due
                 WHERE levels.due
                 LIMIT %(limit)s
            )
        """)

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
            "SELECT pg_catalog.count(pg_catalog.pg_notify("
            f"%(channel)s, pg_catalog.left(queue, {NOTICE_CHARACTERS}))) AS sent "
            "FROM pg_catalog.unnest(%(queues)s::pg_catalog.text[]) AS woken (queue)"
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

        self.listed = f"SELECT {columns} FROM {jobs} "

        self.executions = f"""
            SELECT id, job_id, attempt, claim, status, worker_id, started_at,
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

        self.identity = f"""
            SELECT stored.value AS instance, here.oid::pg_catalog.text AS database
              FROM {meta} AS stored, pg_catalog.pg_database AS here
             WHERE stored.key OPERATOR(pg_catalog.=) 'instance'
               AND here.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()
        """

        self.cluster_readable = (
            "SELECT pg_catalog.has_function_privilege("
            "'pg_catalog.pg_control_system()', 'EXECUTE') AS readable"
        )

        self.cluster = (
            "SELECT system_identifier::pg_catalog.text AS cluster "
            "FROM pg_catalog.pg_control_system()"
        )

        self.located = f"""
            SELECT pg_catalog.count(*) AS tables,
                   pg_catalog.bool_or(c.relname OPERATOR(pg_catalog.=) %(meta)s) AS meta,
                   COALESCE(pg_catalog.bool_and(
                       {trusting("c.relowner")} AND {trusting("n.nspowner")}
                   ), true) AS owned
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n
                ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
             WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
               AND c.relname OPERATOR(pg_catalog.=) ANY (%(names)s::pg_catalog.name[])
        """

        self.misplaced_indexes = """
            SELECT wanted.name
              FROM ROWS FROM (
                   pg_catalog.unnest(%(indexes)s::pg_catalog.text[]),
                   pg_catalog.unnest(%(tables)s::pg_catalog.text[]),
                   pg_catalog.unnest(%(uniques)s::pg_catalog.bool[])
              ) AS wanted (name, owner, is_unique)
             WHERE NOT EXISTS (
                   SELECT FROM pg_catalog.pg_index AS i
                     JOIN pg_catalog.pg_class AS ic ON ic.oid OPERATOR(pg_catalog.=) i.indexrelid
                     JOIN pg_catalog.pg_class AS tc ON tc.oid OPERATOR(pg_catalog.=) i.indrelid
                     JOIN pg_catalog.pg_namespace AS n
                       ON n.oid OPERATOR(pg_catalog.=) ic.relnamespace
                    WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                      AND tc.relnamespace OPERATOR(pg_catalog.=) n.oid
                      AND ic.relname OPERATOR(pg_catalog.=) wanted.name
                      AND tc.relname OPERATOR(pg_catalog.=) wanted.owner
                      AND i.indisunique OPERATOR(pg_catalog.=) wanted.is_unique
                      AND i.indisvalid
             )
             ORDER BY wanted.name
        """


        self.placement = f"""
            SELECT DISTINCT ON (c.relname)
                   c.relname::text AS name, n.nspname::text AS schema,
                   {trusting("c.relowner")} AND {trusting("n.nspowner")} AS owned
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE c.relname = ANY(%(names)s::name[])
               AND n.nspname = ANY(%(schemas)s::name[])
             ORDER BY c.relname, array_position(%(schemas)s::name[], n.nspname)
        """

        self.schema_trust = f"""
            SELECT {trusting("n.nspowner")} AS owned
              FROM pg_namespace AS n
             WHERE n.nspname = %(schema)s
        """
