from __future__ import annotations

from .records import JOB_COLUMNS, JOB_TYPES, job_columns
from .schema import QUEUE_KEY, QUEUE_KEY_CHARACTERS, Tables

READING = "SELECT extract(epoch FROM pg_catalog.clock_timestamp())::double precision AS now"

SERVER = "extract(epoch FROM pg_catalog.statement_timestamp())::double precision"

NOW = "%(now)s::double precision"

NOTICE_CHARACTERS = 1000

SPAN_LIMIT = 128

SPAN_KEPT = 16

REFUSAL = "tinyjobs refuses tables that a role lacking the privileges of the current role controls"


def queue_is(queue: str) -> str:
    return f"{QUEUE_KEY} = left({queue}, {QUEUE_KEY_CHARACTERS}) AND queue = {queue}"


def _qualified(kind: str) -> str:
    return kind if " " in kind or kind == "integer" else f"pg_catalog.{kind}"


def becoming(role: str) -> str:
    return f"""(CASE
        WHEN pg_catalog.current_setting('server_version_num')::pg_catalog.int4
             OPERATOR(pg_catalog.>=) 160000
        THEN pg_catalog.pg_has_role({role}, CURRENT_USER, 'SET')
        ELSE pg_catalog.pg_has_role({role}, CURRENT_USER, 'MEMBER')
    END)"""


def answering(role: str) -> str:
    return f"""(
        {becoming(role)}
        AND NOT EXISTS (
            WITH RECURSIVE heirs (id) AS (
                SELECT m.member FROM pg_catalog.pg_auth_members AS m
                 WHERE m.roleid OPERATOR(pg_catalog.=) {role}
                UNION
                SELECT m.member FROM pg_catalog.pg_auth_members AS m
                  JOIN heirs ON m.roleid OPERATOR(pg_catalog.=) heirs.id
            )
            SELECT FROM heirs
             WHERE pg_catalog.pg_has_role(heirs.id, {role}, 'USAGE')
               AND NOT {becoming("heirs.id")}
        )
    )"""


def trusting(owner: str) -> str:
    database_owner = """(SELECT d.datdba FROM pg_catalog.pg_database AS d
                  WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database())"""
    return f"""(
        {answering(owner)}
        OR ({owner} OPERATOR(pg_catalog.=) pg_catalog.to_regrole('pg_database_owner')
            AND {answering(database_owner)})
    )"""


def catalog(name: str) -> str:
    return f"'pg_catalog.{name}'::pg_catalog.regclass::pg_catalog.oid"


def owners_of_code_on(tables: str) -> str:
    def owned(table: str, owner: str) -> str:
        return f"""SELECT {owner} FROM pg_catalog.{table}
                    WHERE reached.classid OPERATOR(pg_catalog.=) {catalog(table)}
                      AND oid OPERATOR(pg_catalog.=) reached.objid"""

    forward = """d.classid OPERATOR(pg_catalog.=) reached.classid
                 AND d.objid OPERATOR(pg_catalog.=) reached.objid"""
    attached = """d.deptype OPERATOR(pg_catalog.=) ANY ('{a,i}'::pg_catalog."char"[])"""
    return f"""
        WITH RECURSIVE rooted (oid) AS (
            {tables}
            UNION
            SELECT CASE WHEN con.confrelid OPERATOR(pg_catalog.=) rooted.oid
                        THEN con.conrelid ELSE con.confrelid END
              FROM rooted
              JOIN pg_catalog.pg_constraint AS con
                ON con.contype OPERATOR(pg_catalog.=) 'f'
               AND (con.confrelid OPERATOR(pg_catalog.=) rooted.oid
                    OR con.conrelid OPERATOR(pg_catalog.=) rooted.oid)
        ),
        reached (classid, objid) AS (
            SELECT {catalog("pg_class")}, rooted.oid FROM rooted
            UNION
            SELECT d.classid, d.objid
              FROM rooted
              JOIN pg_catalog.pg_depend AS d
                ON d.refclassid OPERATOR(pg_catalog.=) {catalog("pg_class")}
               AND d.refobjid OPERATOR(pg_catalog.=) rooted.oid
               AND {attached}
            UNION
            SELECT CASE WHEN {forward} THEN d.refclassid ELSE d.classid END,
                   CASE WHEN {forward} THEN d.refobjid ELSE d.objid END
              FROM reached
              JOIN pg_catalog.pg_depend AS d
                ON ({forward} AND d.refobjid OPERATOR(pg_catalog.>=) 16384::pg_catalog.oid)
                OR (d.refclassid OPERATOR(pg_catalog.=) reached.classid
                    AND d.refobjid OPERATOR(pg_catalog.=) reached.objid
                    AND {attached}
                    AND reached.classid OPERATOR(pg_catalog.=) ANY (ARRAY[
                        {catalog("pg_type")}, {catalog("pg_opclass")}, {catalog("pg_opfamily")}
                    ]))
        )
        SELECT owners.role
          FROM reached
         CROSS JOIN LATERAL (
               {owned("pg_proc", "proowner")}
               UNION ALL {owned("pg_operator", "oprowner")}
               UNION ALL {owned("pg_type", "typowner")}
               UNION ALL {owned("pg_opclass", "opcowner")}
               UNION ALL {owned("pg_opfamily", "opfowner")}
               UNION ALL {owned("pg_class", "relowner")}
               UNION ALL
               SELECT granted.grantee
                 FROM pg_catalog.pg_class AS c
                CROSS JOIN pg_catalog.aclexplode(c.relacl) AS granted
                WHERE reached.classid OPERATOR(pg_catalog.=) {catalog("pg_class")}
                  AND c.oid OPERATOR(pg_catalog.=) reached.objid
                  AND granted.privilege_type OPERATOR(pg_catalog.=) 'TRIGGER'
         ) AS owners (role)
    """


def untouched(table: str) -> str:
    return f"""(
        NOT EXISTS (
            SELECT FROM ({owners_of_code_on(f"SELECT {table}.oid")}) AS coded
             WHERE NOT (coded.role OPERATOR(pg_catalog.<>) 0::pg_catalog.oid
                        AND {trusting("coded.role")})
        )
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
            f"pg_catalog.unnest(%({column})b::{_qualified(kind)}[])"
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
            "WHERE id OPERATOR(pg_catalog.=) ANY(%(ids)b::pg_catalog.text[])"
        )

        self.active_by_keys = f"""
            SELECT {columns} FROM {jobs}
             WHERE dedup_digest OPERATOR(pg_catalog.=) ANY(%(digests)b::pg_catalog.bytea[])
               AND status OPERATOR(pg_catalog.=) ANY(ARRAY['queued', 'running']::pg_catalog.text[])
        """

        def taking(picked: str) -> str:
            return f"""
            WITH RECURSIVE {picked},
            chosen AS (SELECT ARRAY(SELECT id FROM picked) AS ids),
            claimed AS (
                UPDATE {jobs} AS job
                   SET status = 'running',
                       worker_id = %(worker)s,
                       attempts = job.attempts + 1,
                       claims = job.claims + 1,
                       lease_expires_at = {lease_end},
                       claim_token = %(token)s
                 WHERE job.id = ANY((SELECT ids FROM chosen)::text[]) AND job.status = 'queued'
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
            SELECT {job_columns("claimed")} FROM claimed
              JOIN unnest((SELECT ids FROM chosen)) WITH ORDINALITY AS chosen_at (id, place)
                ON chosen_at.id = claimed.id
             ORDER BY claimed.priority DESC, claimed.run_at ASC, chosen_at.place
            """

        def chunk(queue: str) -> str:
            return f"""
                SELECT max(priority) FILTER (WHERE run_at <= {horizon}) AS due,
                       CASE WHEN count(*) = levels.span THEN min(priority) END AS last,
                       max(priority) AS top
                  FROM (
                       SELECT priority, run_at FROM {jobs}
                        WHERE status = 'queued' AND {queue_is(queue)}
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
              FROM unnest(%(queues)b::text[]) AS wanted (name)
             CROSS JOIN LATERAL ({chunk("wanted.name")}) AS each
        """)

        def oldest_first(queue: str) -> str:
            return f"""
                SELECT id FROM {jobs}
                 WHERE status = 'queued' AND {queue_is(queue)}
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
                         FROM unnest(%(queues)b::text[]) WITH ORDINALITY
                              AS wanted (name, position)
                        CROSS JOIN LATERAL (
                              SELECT run_at FROM {jobs}
                               WHERE status = 'queued' AND {queue_is("wanted.name")}
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
               AND id = ANY(%(ids)b::text[])
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

        self.unclaim = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', attempts = attempts - 1, lease_expires_at = NULL,
                       worker_id = NULL, claim_token = NULL
                 WHERE status = 'running' AND worker_id = %(worker)s AND claim_token = %(token)s
                RETURNING id, claims, queue
            ),
            forgotten AS (
                DELETE FROM {executions} AS execution USING changed
                 WHERE execution.job_id = changed.id AND execution.claim = changed.claims
            )
            SELECT queue FROM changed
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
                    WHERE holder.dedup_digest = target.dedup_digest
                      AND holder.status IN ('queued', 'running')
               )
            RETURNING id, queue, run_at <= {horizon} AS due
        """

        self.notify = (
            "SELECT pg_catalog.count(pg_catalog.pg_notify("
            f"%(channel)s, pg_catalog.left(queue, {NOTICE_CHARACTERS}))) AS sent "
            "FROM pg_catalog.unnest(%(queues)b::pg_catalog.text[]) AS woken (queue)"
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
            SELECT * FROM unnest(%(names)b::text[], %(crons)b::text[],
                                 %(timezones)b::text[], %(next_runs)b::double precision[])
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
            WITH found AS (
                SELECT c.oid, c.relname, c.relowner, c.relacl, n.nspowner
                  FROM pg_catalog.pg_class AS c
                  JOIN pg_catalog.pg_namespace AS n
                    ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
                 WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                   AND c.relname OPERATOR(pg_catalog.=) ANY (%(names)b::pg_catalog.name[])
            ),
            controlling (role) AS (
                SELECT relowner FROM found
                UNION
                SELECT nspowner FROM found
                UNION
                SELECT coded.role FROM ({owners_of_code_on("SELECT oid FROM found")}) AS coded
            )
            SELECT (SELECT pg_catalog.count(*) FROM found) AS tables,
                   (SELECT pg_catalog.bool_or(relname OPERATOR(pg_catalog.=) %(meta)s) FROM found)
                   AS meta,
                   CASE WHEN NOT EXISTS (
                       SELECT FROM controlling
                        WHERE controlling.role OPERATOR(pg_catalog.=) 0::pg_catalog.oid
                           OR NOT {trusting("controlling.role")}
                   ) THEN true
                   ELSE pg_catalog.concat('{REFUSAL}')::pg_catalog.bool END AS owned
        """

        self.missing_statistics = """
            SELECT wanted.name, false AS on_its_table
              FROM pg_catalog.unnest(%(statistics)b::pg_catalog.text[]) AS wanted (name)
             WHERE NOT EXISTS (
                   SELECT FROM pg_catalog.pg_statistic_ext AS x
                     JOIN pg_catalog.pg_namespace AS n
                       ON n.oid OPERATOR(pg_catalog.=) x.stxnamespace
                    WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                      AND x.stxname OPERATOR(pg_catalog.=) wanted.name
             )
        """

        self.missing_columns = """
            SELECT wanted.name
              FROM ROWS FROM (
                   pg_catalog.unnest(%(columns)b::pg_catalog.text[]),
                   pg_catalog.unnest(%(tables)b::pg_catalog.text[])
              ) AS wanted (name, owner)
             WHERE NOT EXISTS (
                   SELECT FROM pg_catalog.pg_attribute AS a
                     JOIN pg_catalog.pg_class AS c ON c.oid OPERATOR(pg_catalog.=) a.attrelid
                     JOIN pg_catalog.pg_namespace AS n
                       ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
                    WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                      AND c.relname OPERATOR(pg_catalog.=) wanted.owner
                      AND a.attname OPERATOR(pg_catalog.=) wanted.name
                      AND NOT a.attisdropped
             )
             ORDER BY wanted.name
        """

        self.misplaced_indexes = """
            SELECT wanted.name, EXISTS (
                   SELECT FROM pg_catalog.pg_index AS i
                     JOIN pg_catalog.pg_class AS ic ON ic.oid OPERATOR(pg_catalog.=) i.indexrelid
                     JOIN pg_catalog.pg_class AS tc ON tc.oid OPERATOR(pg_catalog.=) i.indrelid
                     JOIN pg_catalog.pg_namespace AS n
                       ON n.oid OPERATOR(pg_catalog.=) ic.relnamespace
                    WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                      AND tc.relnamespace OPERATOR(pg_catalog.=) n.oid
                      AND ic.relname OPERATOR(pg_catalog.=) wanted.name
                      AND tc.relname OPERATOR(pg_catalog.=) wanted.owner
                   ) AS on_its_table
              FROM ROWS FROM (
                   pg_catalog.unnest(%(indexes)b::pg_catalog.text[]),
                   pg_catalog.unnest(%(tables)b::pg_catalog.text[]),
                   pg_catalog.unnest(%(uniques)b::pg_catalog.bool[])
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
                   {trusting("c.relowner")} AND {trusting("n.nspowner")} AND {untouched("c")}
                   AS owned
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE c.relname = ANY(%(names)b::name[])
               AND n.nspname = ANY(%(schemas)b::name[])
             ORDER BY c.relname, array_position(%(schemas)b::name[], n.nspname)
        """

        self.schema_trust = f"""
            SELECT {trusting("n.nspowner")} AS owned
              FROM pg_namespace AS n
             WHERE n.nspname = %(schema)s
        """
