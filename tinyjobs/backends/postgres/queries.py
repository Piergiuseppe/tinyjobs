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
            SELECT tied.oid
              FROM rooted
             CROSS JOIN LATERAL (
                   SELECT con.conrelid
                     FROM pg_catalog.pg_depend AS d
                     JOIN pg_catalog.pg_constraint AS con
                       ON con.oid OPERATOR(pg_catalog.=) d.objid
                    WHERE d.refclassid OPERATOR(pg_catalog.=) {catalog("pg_class")}
                      AND d.refobjid OPERATOR(pg_catalog.=) rooted.oid
                      AND d.classid OPERATOR(pg_catalog.=) {catalog("pg_constraint")}
                      AND con.contype OPERATOR(pg_catalog.=) 'f'
                      AND con.confrelid OPERATOR(pg_catalog.=) rooted.oid
                   UNION ALL
                   SELECT inherited.inhrelid FROM pg_catalog.pg_inherits AS inherited
                    WHERE inherited.inhparent OPERATOR(pg_catalog.=) rooted.oid
             ) AS tied (oid)
        ),
        reached (classid, objid) AS (
            SELECT {catalog("pg_class")}, rooted.oid FROM rooted
            UNION
            SELECT attachment.classid, attachment.objid
              FROM rooted
             CROSS JOIN LATERAL (
                   SELECT d.classid, d.objid FROM pg_catalog.pg_depend AS d
                    WHERE d.refclassid OPERATOR(pg_catalog.=) {catalog("pg_class")}
                      AND d.refobjid OPERATOR(pg_catalog.=) rooted.oid
                      AND {attached}
                   OFFSET 0
             ) AS attachment
            UNION
            SELECT next.classid, next.objid
              FROM reached
             CROSS JOIN LATERAL (
                   SELECT d.refclassid, d.refobjid FROM pg_catalog.pg_depend AS d
                    WHERE {forward}
                      AND d.refobjid OPERATOR(pg_catalog.>=) 16384::pg_catalog.oid
                      AND NOT (d.classid OPERATOR(pg_catalog.=) {catalog("pg_constraint")}
                               AND d.refclassid OPERATOR(pg_catalog.=) {catalog("pg_class")}
                               AND d.deptype OPERATOR(pg_catalog.=) 'n')
                   UNION ALL
                   SELECT d.classid, d.objid FROM pg_catalog.pg_depend AS d
                    WHERE d.refclassid OPERATOR(pg_catalog.=) reached.classid
                      AND d.refobjid OPERATOR(pg_catalog.=) reached.objid
                      AND {attached}
                      AND reached.classid OPERATOR(pg_catalog.=) ANY (ARRAY[
                          {catalog("pg_type")}, {catalog("pg_opclass")}, {catalog("pg_opfamily")}
                      ])
             ) AS next (classid, objid)
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
                  AND granted.privilege_type OPERATOR(pg_catalog.=) ANY (
                      '{{TRIGGER,REFERENCES}}'::pg_catalog.text[])
               UNION ALL
               SELECT granted.grantee
                 FROM pg_catalog.pg_attribute AS a
                CROSS JOIN pg_catalog.aclexplode(a.attacl) AS granted
                WHERE reached.classid OPERATOR(pg_catalog.=) {catalog("pg_class")}
                  AND a.attrelid OPERATOR(pg_catalog.=) reached.objid
                  AND a.attnum OPERATOR(pg_catalog.>) 0
                  AND NOT a.attisdropped
                  AND a.attacl IS NOT NULL
                  AND granted.privilege_type OPERATOR(pg_catalog.=) 'REFERENCES'
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
        schedules, meta, limits = tables.schedules, tables.meta, tables.limits
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
               AND status OPERATOR(pg_catalog.=) ANY(
                   ARRAY['queued', 'held', 'running']::pg_catalog.text[])
        """

        def lowering(source: str) -> str:
            return f"""
            freed AS (
                SELECT task, count(*) AS total FROM {source} GROUP BY task
            ),
            counted AS (
                SELECT allowance.task, freed.total
                  FROM {limits} AS allowance
                  JOIN freed ON allowance.task = freed.task
                 ORDER BY allowance.task
                 FOR UPDATE OF allowance
            ),
            lowered AS (
                UPDATE {limits} AS allowance
                   SET running = greatest(allowance.running - counted.total, 0)
                  FROM counted
                 WHERE allowance.task = counted.task
            )
            """

        raising = f"""
            started AS (
                SELECT task, count(*) AS total FROM claimed GROUP BY task
            ),
            counting AS (
                SELECT allowance.task, started.total
                  FROM {limits} AS allowance
                  JOIN started ON allowance.task = started.task
                 ORDER BY allowance.task
                 FOR UPDATE OF allowance
            ),
            raised AS (
                UPDATE {limits} AS allowance
                   SET running = allowance.running + counting.total
                  FROM counting
                 WHERE allowance.task = counting.task
            ),"""

        def taking(
            picked: str,
            chosen: str = "SELECT id FROM picked",
            taken: str = "job.status = 'queued'",
            extra: str = "",
            answer: str = f"""SELECT {job_columns("claimed")} FROM claimed
              JOIN unnest((SELECT ids FROM chosen)) WITH ORDINALITY AS chosen_at (id, place)
                ON chosen_at.id = claimed.id
             ORDER BY claimed.priority DESC, claimed.run_at ASC, chosen_at.place""",
        ) -> str:
            return f"""
            WITH RECURSIVE {picked},
            chosen AS (
                SELECT ARRAY({chosen}) AS ids
                 WHERE pg_catalog.pg_try_advisory_xact_lock(%(key)s::bigint)
            ),
            claimed AS (
                UPDATE {jobs} AS job
                   SET status = 'running',
                       worker_id = %(worker)s,
                       attempts = job.attempts + 1,
                       claims = job.claims + 1,
                       lease_expires_at = {lease_end},
                       claim_token = %(token)s
                 WHERE job.id = ANY((SELECT ids FROM chosen)::text[]) AND {taken}
                RETURNING {job_columns("job")}
            ),{extra}
            opened AS (
                INSERT INTO {executions}
                       (job_id, attempt, claim, status, worker_id, started_at)
                SELECT id, attempts, claims, 'running', %(worker)s, {NOW} FROM claimed
                ON CONFLICT (job_id, claim) DO UPDATE
                   SET status = 'running', worker_id = excluded.worker_id,
                       started_at = excluded.started_at, finished_at = NULL,
                       error_type = NULL, traceback = NULL
            )
            {answer}
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

        def oldest_first(queue: str, columns: str = "id") -> str:
            return f"""
                SELECT {columns} FROM {jobs}
                 WHERE status = 'queued' AND {queue_is(queue)}
                   AND priority = levels.priority AND run_at <= {horizon}
                 ORDER BY run_at ASC
                 LIMIT %(limit)s
                 FOR UPDATE SKIP LOCKED
            """

        self.claim_by_id = taking(
            f"""
            picked AS (
                SELECT id FROM {jobs}
                 WHERE id OPERATOR(pg_catalog.=) %(id)s::pg_catalog.text
                   AND status IN ('queued', 'held') AND run_at <= {horizon}
                 FOR UPDATE SKIP LOCKED
            )
        """,
            taken="job.status IN ('queued', 'held')",
            extra=raising,
        )

        def one_queue_picked(columns: str = "id") -> str:
            return f"""
            {one_queue},
            picked AS (
                SELECT {", ".join(f"due.{column}" for column in columns.split(", "))}
                  FROM levels
                 CROSS JOIN LATERAL ({oldest_first("%(queue)s", columns)}) AS due
                 WHERE levels.due
                 LIMIT %(limit)s
            )
        """

        def many_queues_picked(columns: str = "id") -> str:
            return f"""
            {many_queues},
            picked AS (
                SELECT {", ".join(f"due.{column}" for column in columns.split(", "))}
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
                 CROSS JOIN LATERAL ({oldest_first("ready.name", columns)}) AS due
                 WHERE levels.due
                 LIMIT %(limit)s
            )
        """

        self.claim_one_queue = taking(one_queue_picked(), extra=raising)

        self.claim_many_queues = taking(many_queues_picked(), extra=raising)

        candidate = "id, task, priority, run_at"
        in_line = "priority DESC, run_at ASC, held DESC, place, id"

        last_read = """
            frontier AS (
                SELECT priority, run_at FROM fresh
                 WHERE (SELECT count(*) FROM fresh) = %(limit)s
                 ORDER BY priority ASC, run_at DESC
                 LIMIT 1
            ),"""

        first_unread = f"""
            frontier AS (
                SELECT cut.level AS priority, coalesce(min(unread.run_at), 'infinity') AS run_at
                  FROM (
                       SELECT min(priority) AS level FROM fresh
                        WHERE (SELECT count(*) FROM fresh) = %(limit)s
                  ) AS cut
                 CROSS JOIN unnest(%(queues)b::text[]) AS wanted (name)
                  LEFT JOIN LATERAL (
                       SELECT run_at FROM {jobs}
                        WHERE status = 'queued' AND {queue_is("wanted.name")}
                          AND priority = cut.level AND run_at <= {horizon}
                          AND id <> ALL(ARRAY(SELECT id FROM fresh))
                        ORDER BY run_at ASC
                        LIMIT 1
                  ) AS unread ON true
                 WHERE cut.level IS NOT NULL
                 GROUP BY cut.level
            ),"""

        def limiting(picked: str, frontier: str) -> str:
            return taking(
                f"""{picked},
            rules AS (
                SELECT rule.task, rule.concurrency, rule.spacing
                  FROM ROWS FROM (
                           unnest(%(tasks)s::text[]),
                           unnest(%(concurrencies)s::integer[]),
                           unnest(%(intervals)s::double precision[])
                       ) AS rule (task, concurrency, spacing)
            ),
            fresh AS (
                SELECT {candidate}, false AS held, row_number() OVER () AS place FROM picked
            ),{frontier}
            involved AS (
                SELECT task FROM fresh
                 UNION
                SELECT rules.task FROM rules
                 WHERE EXISTS (
                       SELECT FROM unnest(%(queues)b::text[]) AS wanted (name)
                        CROSS JOIN LATERAL (
                              SELECT FROM {jobs}
                               WHERE status = 'held' AND task = rules.task
                                 AND {queue_is("wanted.name")}
                               ORDER BY priority DESC, run_at ASC
                               LIMIT 1
                        ) AS head
                 )
            ),
            locked AS (
                SELECT allowance.task, allowance.running, allowance.last_at
                  FROM {limits} AS allowance
                 WHERE allowance.task IN (SELECT task FROM involved)
                 ORDER BY allowance.task
                 FOR UPDATE
            ),
            rooms AS (
                SELECT rules.task,
                       CASE WHEN locked.task IS NULL THEN 0
                            WHEN rules.spacing IS NOT NULL
                                 AND {horizon} < locked.last_at + rules.spacing THEN 0
                            ELSE least(
                                greatest(coalesce(rules.concurrency - locked.running,
                                                  %(limit)s::integer), 0),
                                %(limit)s::integer,
                                CASE WHEN rules.spacing IS NULL THEN %(limit)s::integer
                                     ELSE 1 END)
                       END AS room
                  FROM rules
                  LEFT JOIN locked ON locked.task = rules.task
            ),
            line AS (
                SELECT waiting.{candidate.replace(", ", ", waiting.")}, true AS held,
                       0::bigint AS place
                  FROM rooms
                 CROSS JOIN unnest(%(queues)b::text[]) AS wanted (name)
                 CROSS JOIN LATERAL (
                       SELECT {candidate} FROM {jobs}
                        WHERE status = 'held' AND task = rooms.task
                          AND {queue_is("wanted.name")}
                        ORDER BY priority DESC, run_at ASC
                        LIMIT rooms.room
                        FOR UPDATE SKIP LOCKED
                 ) AS waiting
                 WHERE rooms.room > 0
            ),
            ranked AS (
                SELECT candidates.*, rooms.task IS NOT NULL AS limited,
                       coalesce(rooms.room, 0) AS room,
                       row_number() OVER (
                           PARTITION BY candidates.task ORDER BY {in_line}
                       ) AS rank
                  FROM (
                       SELECT * FROM line
                        WHERE NOT EXISTS (
                              SELECT FROM frontier
                               WHERE line.priority < frontier.priority
                                  OR (line.priority = frontier.priority
                                      AND line.run_at > frontier.run_at)
                        )
                       UNION ALL
                       SELECT * FROM fresh
                  ) AS candidates
                  LEFT JOIN rooms ON rooms.task = candidates.task
            ),
            admitted AS (
                SELECT id, priority, run_at, held, place FROM ranked
                 WHERE NOT limited OR rank <= room
                 ORDER BY {in_line}
                 LIMIT %(limit)s
            )""",
                chosen=f"SELECT id FROM admitted ORDER BY {in_line}",
                taken="job.status IN ('queued', 'held')",
                extra=f"""
            holding AS (
                UPDATE {jobs} AS job SET status = 'held'
                 WHERE job.id = ANY(ARRAY(
                       SELECT id FROM ranked WHERE limited AND NOT held AND rank > room
                   ))
                   AND job.status = 'queued'
                RETURNING job.id
            ),
            raised AS (
                UPDATE {limits} AS allowance
                   SET running = allowance.running + started.total,
                       last_at = CASE WHEN started.spaced THEN {horizon} ELSE allowance.last_at END
                  FROM (
                       SELECT claimed.task, count(*) AS total,
                              bool_or(rules.spacing IS NOT NULL) AS spaced
                         FROM claimed
                         JOIN locked ON locked.task = claimed.task
                         LEFT JOIN rules ON rules.task = claimed.task
                        GROUP BY claimed.task
                  ) AS started
                 WHERE allowance.task = started.task
            ),""",
                answer=f"""SELECT holds.total AS held_total, holds.missing,
                   (SELECT count(*) FROM fresh) AS fresh_total, {job_columns("claimed")}
              FROM (
                   SELECT count(*) AS total, ARRAY(
                          SELECT rules.task FROM rules
                            JOIN involved ON involved.task = rules.task
                           WHERE NOT EXISTS (SELECT FROM locked WHERE locked.task = rules.task)
                          ) AS missing
                     FROM holding
              ) AS holds
              LEFT JOIN (
                   claimed
                   JOIN unnest((SELECT ids FROM chosen)) WITH ORDINALITY AS chosen_at (id, place)
                     ON chosen_at.id = claimed.id
              ) ON true
             ORDER BY claimed.priority DESC, claimed.run_at ASC, chosen_at.place""",
            )

        self.claim_one_queue_limited = limiting(one_queue_picked(candidate), last_read)

        self.claim_many_queues_limited = limiting(many_queues_picked(candidate), first_unread)

        self.ensure_limits = f"""
            INSERT INTO {limits} (task, running)
            SELECT wanted.task, (
                   SELECT count(*) FROM {jobs}
                    WHERE status = 'running' AND task = wanted.task
            )
              FROM unnest(%(tasks)s::text[]) AS wanted (task)
             ORDER BY wanted.task
            ON CONFLICT (task) DO NOTHING
            RETURNING task
        """

        self.lock_tasks = f"""
            SELECT task FROM {limits} WHERE task = ANY(%(tasks)s::text[]) ORDER BY task FOR UPDATE
        """

        self.settle_running = f"""
            SELECT id FROM {jobs}
             WHERE status = 'running' AND task = ANY(%(tasks)s::text[])
             ORDER BY id
             FOR SHARE
        """

        self.recount_tasks = f"""
            UPDATE {limits} AS allowance
               SET running = (
                   SELECT count(*) FROM {jobs}
                    WHERE status = 'running' AND task = allowance.task
               )
             WHERE allowance.task = ANY(%(tasks)s::text[])
        """

        self.lock_limits = f"SELECT task FROM {limits} ORDER BY task FOR UPDATE"

        self.recount_limits = f"""
            UPDATE {limits} AS allowance SET running = coalesce(truth.total, 0)
              FROM {limits} AS mine
              LEFT JOIN (
                   SELECT task, count(*) AS total FROM {jobs}
                    WHERE status = 'running'
                    GROUP BY task
              ) AS truth ON truth.task = mine.task
             WHERE allowance.task = mine.task
               AND allowance.running <> coalesce(truth.total, 0)
        """

        self.claim_recount = f"""
            INSERT INTO {meta} AS stored (key, value) VALUES ('last_recount_at', %(now)s::text)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
             WHERE stored.value::double precision <= %(cutoff)s
                OR stored.value::double precision > %(ahead)s
            RETURNING key
        """

        self.heartbeat = f"""
            UPDATE {jobs} SET lease_expires_at = {lease_end}
             WHERE status = 'running'
               AND (%(worker)s::text IS NULL OR worker_id = %(worker)s)
               AND id = ANY(%(ids)b::text[])
            RETURNING id
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
                RETURNING id, claims, task
            ),
            {close_execution},
            {lowering("changed")}
            SELECT count(*) AS changed FROM changed
        """

        self.fail = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'failed', finished_at = %(now)s, last_error = %(summary)s,
                       lease_expires_at = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, claims, task
            ),
            {close_execution},
            {lowering("changed")}
            SELECT count(*) AS changed FROM changed
        """

        self.reschedule = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, last_error = %(summary)s,
                       lease_expires_at = NULL, worker_id = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, claims, task
            ),
            {close_execution},
            {lowering("changed")}
            SELECT count(*) AS changed FROM changed
        """

        self.release = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, lease_expires_at = NULL,
                       worker_id = NULL, hand_backs = 0
                 WHERE {held}
                RETURNING id, task
            ),
            lost AS (
                UPDATE {executions} SET status = 'lost', finished_at = %(now)s
                 WHERE status = 'running' AND job_id IN (SELECT id FROM changed)
            ),
            {lowering("changed")}
            SELECT count(*) AS changed FROM changed
        """

        self.release_many = f"""
            WITH changed AS (
                UPDATE {jobs} AS job
                   SET status = 'queued', run_at = %(now)s, lease_expires_at = NULL,
                       worker_id = NULL, hand_backs = 0
                  FROM ROWS FROM (
                           pg_catalog.unnest(%(ids)s::pg_catalog.text[]),
                           pg_catalog.unnest(%(workers)s::pg_catalog.text[]),
                           pg_catalog.unnest(%(claims)s::pg_catalog.int4[])
                       ) AS released (id, worker, claims)
                 WHERE job.id = released.id AND job.status = 'running'
                   AND job.worker_id = released.worker AND job.claims = released.claims
                RETURNING job.id, job.queue, job.task
            ),
            lost AS (
                UPDATE {executions} SET status = 'lost', finished_at = %(now)s
                 WHERE status = 'running' AND job_id IN (SELECT id FROM changed)
            ),
            {lowering("changed")}
            SELECT queue FROM changed
        """

        self.unclaim = f"""
            WITH changed AS (
                UPDATE {jobs} AS job
                   SET status = 'queued', attempts = job.attempts - 1, lease_expires_at = NULL,
                       worker_id = NULL, claim_token = NULL
                  FROM pg_catalog.unnest(%(tokens)s::text[]) AS ghost (token)
                 WHERE job.status = 'running' AND job.worker_id = ANY(%(workers)s::text[])
                   AND job.claim_token = ghost.token
                RETURNING job.id, job.claims, job.queue, job.task, ghost.token
            ),
            forgotten AS (
                DELETE FROM {executions} AS execution USING changed
                 WHERE execution.job_id = changed.id AND execution.claim = changed.claims
            ),
            {lowering("changed")}
            SELECT queue, token FROM changed
        """

        self.hand_back = f"""
            WITH changed AS (
                UPDATE {jobs}
                   SET status = 'queued', run_at = %(run_at)s, attempts = attempts - 1,
                       hand_backs = hand_backs + 1, lease_expires_at = NULL, worker_id = NULL
                 WHERE {held}
                RETURNING id, claims, task
            ),
            forgotten AS (
                DELETE FROM {executions} AS execution USING changed
                 WHERE execution.job_id = changed.id AND execution.claim = changed.claims
            ),
            {lowering("changed")}
            SELECT count(*) AS changed FROM changed
        """

        self.cancel = f"""
            WITH target AS (
                SELECT id, status FROM {jobs}
                 WHERE id = %(id)s AND status IN ('queued', 'held', 'running')
                 FOR UPDATE
            ),
            changed AS (
                UPDATE {jobs} AS job
                   SET status = 'cancelled', finished_at = %(now)s, lease_expires_at = NULL,
                       hand_backs = 0
                  FROM target
                 WHERE job.id = target.id
                RETURNING job.id, job.task, target.status AS was
            ),
            closed AS (
                UPDATE {executions} SET status = 'cancelled', finished_at = %(now)s
                 WHERE status = 'running' AND job_id IN (SELECT id FROM changed)
            ),
            {lowering("changed WHERE was = 'running'")}
            SELECT count(*) AS changed FROM changed
        """

        def releasing(within: str) -> str:
            return f"""
            WITH batch AS (
                SELECT id FROM {jobs}
                 WHERE status = 'held' AND task = ANY(%(tasks)s::text[]){within}
                 LIMIT %(batch)s
                 FOR UPDATE SKIP LOCKED
            ),
            released AS (
                UPDATE {jobs} AS job SET status = 'queued'
                  FROM batch
                 WHERE job.id = batch.id AND job.status = 'held'
                RETURNING job.queue
            )
            SELECT queue, count(*) AS total FROM released GROUP BY queue
            """

        self.release_held = releasing("")

        self.release_held_in = releasing(f"""
                   AND {QUEUE_KEY} = ANY(ARRAY(
                       SELECT left(name, {QUEUE_KEY_CHARACTERS})
                         FROM unnest(%(queues)s::text[]) AS name
                   ))
                   AND queue = ANY(%(queues)s::text[])""")

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
                      AND holder.status IN ('queued', 'held', 'running')
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
                RETURNING job.id, job.queue, job.status, job.task
            ),
            lost AS (
                UPDATE {executions} AS execution
                   SET status = 'lost', finished_at = {horizon}
                  FROM reaped
                 WHERE execution.job_id = reaped.id AND execution.status = 'running'
            ),
            {lowering("reaped")}
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
             WHERE status IN ('queued', 'held') AND run_at <= %(now)s
             GROUP BY queue
        """

        self.count_expired = f"""
            SELECT count(*) AS total FROM {jobs}
             WHERE status = 'running' AND lease_expires_at < %(now)s
        """

        self.queued_by_task = f"""
            SELECT task, count(*) AS total FROM {jobs}
             WHERE status IN ('queued', 'held') GROUP BY task
        """

        self.claim_sweep = f"""
            UPDATE {meta} SET value = %(now)s::text
             WHERE key = 'last_purge_at'
               AND (value::double precision = 0 OR value::double precision <= %(cutoff)s)
            RETURNING key
        """

        self.expire_results = f"""
            WITH expired AS (
                UPDATE {jobs} SET result = NULL
                 WHERE id IN (
                       SELECT id FROM {jobs}
                        WHERE status = 'success'
                          AND finished_at >= %(since)s - ({NOW} - {horizon})
                          AND finished_at < %(cutoff)s - ({NOW} - {horizon})
                          AND task = %(task)s AND result IS NOT NULL
                        ORDER BY finished_at
                        LIMIT %(batch)s
                        FOR UPDATE SKIP LOCKED
                 )
                RETURNING id
            )
            SELECT count(*) AS expired FROM expired
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
              FROM ROWS FROM (
                   pg_catalog.unnest(%(statistics)b::pg_catalog.text[]),
                   pg_catalog.unnest(%(described)b::pg_catalog.text[])
              ) AS wanted (name, owner)
             WHERE NOT EXISTS (
                   SELECT FROM pg_catalog.pg_statistic_ext AS x
                     JOIN pg_catalog.pg_namespace AS n
                       ON n.oid OPERATOR(pg_catalog.=) x.stxnamespace
                     JOIN pg_catalog.pg_class AS tc ON tc.oid OPERATOR(pg_catalog.=) x.stxrelid
                    WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
                      AND tc.relnamespace OPERATOR(pg_catalog.=) n.oid
                      AND x.stxname OPERATOR(pg_catalog.=) wanted.name
                      AND tc.relname OPERATOR(pg_catalog.=) wanted.owner
             )
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
            SELECT c.relname::text AS name, n.nspname::text AS schema,
                   {trusting("c.relowner")} AND {trusting("n.nspowner")} AND {untouched("c")}
                   AS owned
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE c.relname = ANY(%(names)b::name[])
               AND n.nspname = ANY(%(schemas)b::name[])
             ORDER BY c.relname, array_position(%(schemas)b::name[], n.nspname)
        """

        self.status_checks = """
            SELECT con.conname::pg_catalog.text AS name
              FROM pg_catalog.pg_constraint AS con
              JOIN pg_catalog.pg_class AS c ON c.oid OPERATOR(pg_catalog.=) con.conrelid
              JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
              JOIN pg_catalog.pg_attribute AS a
                ON a.attrelid OPERATOR(pg_catalog.=) c.oid
               AND a.attname OPERATOR(pg_catalog.=) 'status'
             WHERE n.nspname OPERATOR(pg_catalog.=) %(schema)s
               AND c.relname OPERATOR(pg_catalog.=) %(table)s
               AND con.contype OPERATOR(pg_catalog.=) 'c'
               AND con.conkey OPERATOR(pg_catalog.=) ARRAY[a.attnum]
             ORDER BY con.conname
        """

        self.schema_trust = f"""
            SELECT {trusting("n.nspowner")} AS owned
              FROM pg_namespace AS n
             WHERE n.nspname = %(schema)s
        """
