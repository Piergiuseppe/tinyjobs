from __future__ import annotations

from .records import JOB_COLUMN_LIST, JOB_PLACEHOLDERS
from .schema import Tables

TEMPLATES = {
    "insert_job": f"INSERT INTO {{jobs}} ({JOB_COLUMN_LIST}) VALUES ({JOB_PLACEHOLDERS})",
    "select_job": f"SELECT {JOB_COLUMN_LIST} FROM {{jobs}} WHERE id = ?",
    "select_active_by_dedup": f"""
        SELECT {JOB_COLUMN_LIST} FROM {{jobs}}
         WHERE dedup_key = ? AND status IN ('queued', 'running')
    """,
    "select_claimable_ids": """
        SELECT id FROM {jobs}
         WHERE status = 'queued' AND run_at <= ? AND queue IN ({{queues}})
         ORDER BY priority DESC, run_at ASC
         LIMIT ?
    """,
    "claim_returning": f"""
        UPDATE {{jobs}}
           SET status = 'running',
               worker_id = ?,
               attempts = attempts + 1,
               claims = claims + 1,
               lease_expires_at = ?
         WHERE id IN (
               SELECT id FROM {{jobs}}
                WHERE status = 'queued' AND run_at <= ? AND queue IN ({{{{queues}}}})
                ORDER BY priority DESC, run_at ASC
                LIMIT ?
         )
        RETURNING {JOB_COLUMN_LIST}
    """,
    "claim_one": """
        UPDATE {jobs}
           SET status = 'running',
               worker_id = ?,
               attempts = attempts + 1,
               claims = claims + 1,
               lease_expires_at = ?
         WHERE id = ? AND status = 'queued'
    """,
    "claim_due_one": """
        UPDATE {jobs}
           SET status = 'running',
               worker_id = ?,
               attempts = attempts + 1,
               claims = claims + 1,
               lease_expires_at = ?
         WHERE id = ? AND status = 'queued' AND run_at <= ?
    """,
    "heartbeat": """
        UPDATE {jobs} SET lease_expires_at = ?
         WHERE status = 'running' AND (? IS NULL OR worker_id = ?) AND id IN ({{ids}})
    """,
    "renewed": """
        SELECT id FROM {jobs}
         WHERE lease_expires_at = ? AND status = 'running' AND (? IS NULL OR worker_id = ?)
           AND id IN ({{ids}})
    """,
    "complete_job": """
        UPDATE {jobs}
           SET status = 'success', finished_at = ?, result = ?,
               lease_expires_at = NULL, last_error = NULL, hand_backs = 0
         WHERE id = ? AND status = 'running' AND worker_id = ? AND claims = ?
    """,
    "fail_job": """
        UPDATE {jobs}
           SET status = 'failed', finished_at = ?, last_error = ?,
               lease_expires_at = NULL, hand_backs = 0
         WHERE id = ? AND status = 'running' AND worker_id = ? AND claims = ?
    """,
    "reschedule_job": """
        UPDATE {jobs}
           SET status = 'queued', run_at = ?, last_error = ?,
               lease_expires_at = NULL, worker_id = NULL, hand_backs = 0
         WHERE id = ? AND status = 'running' AND worker_id = ? AND claims = ?
    """,
    "release_job": """
        UPDATE {jobs}
           SET status = 'queued', run_at = ?, lease_expires_at = NULL, worker_id = NULL,
               hand_backs = 0
         WHERE id = ? AND status = 'running' AND worker_id = ? AND claims = ?
    """,
    "hand_back_job": """
        UPDATE {jobs}
           SET status = 'queued', run_at = ?, attempts = attempts - 1,
               hand_backs = hand_backs + 1, lease_expires_at = NULL, worker_id = NULL
         WHERE id = ? AND status = 'running' AND worker_id = ? AND claims = ?
    """,
    "delete_execution": """
        DELETE FROM {executions} WHERE job_id = ? AND claim = ?
    """,
    "cancel_executions": """
        UPDATE {executions}
           SET status = 'cancelled', finished_at = ?
         WHERE job_id = ? AND status = 'running'
    """,
    "cancel_job": """
        UPDATE {jobs}
           SET status = 'cancelled', finished_at = ?, lease_expires_at = NULL,
               hand_backs = 0
         WHERE id = ? AND status IN ('queued', 'running')
    """,
    "requeue_job": """
        UPDATE {jobs}
           SET status = 'queued', run_at = CASE WHEN claims = 0 THEN created_at ELSE ? END,
               attempts = 0,
               finished_at = NULL,
               lease_expires_at = NULL, worker_id = NULL, last_error = NULL
         WHERE id = ? AND status IN ('failed', 'cancelled')
           AND NOT EXISTS (
               SELECT 1 FROM {jobs} AS holder
                WHERE holder.dedup_key = {jobs}.dedup_key
                  AND holder.status IN ('queued', 'running')
           )
    """,
    "reap_expired": """
        UPDATE {jobs}
           SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
               run_at = ?,
               finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE ? END,
               lease_expires_at = NULL,
               worker_id = NULL,
               hand_backs = 0,
               last_error = 'lease expired: the worker holding this job stopped reporting'
         WHERE status = 'running' AND lease_expires_at < ?
        RETURNING id, attempts
    """,
    "select_expired_ids": """
        SELECT id, attempts, max_attempts FROM {jobs}
         WHERE status = 'running' AND lease_expires_at < ?
    """,
    "expire_one": """
        UPDATE {jobs} SET status = ?, run_at = ?, finished_at = ?,
               lease_expires_at = NULL, worker_id = NULL, hand_backs = 0,
               last_error = ?
         WHERE id = ?
    """,
    "insert_execution": """
        INSERT INTO {executions} (job_id, attempt, claim, status, worker_id, started_at)
        VALUES (?, ?, ?, 'running', ?, ?)
        ON CONFLICT (job_id, claim) DO UPDATE
           SET status = 'running', worker_id = excluded.worker_id,
               started_at = excluded.started_at, finished_at = NULL,
               error_type = NULL, traceback = NULL
    """,
    "finish_execution": """
        UPDATE {executions}
           SET status = ?, finished_at = ?, error_type = ?, traceback = ?
         WHERE job_id = ? AND claim = ?
    """,
    "mark_executions_lost": """
        UPDATE {executions}
           SET status = 'lost', finished_at = ?
         WHERE status = 'running' AND job_id IN ({{ids}})
    """,
    "select_executions": """
        SELECT id, job_id, attempt, claim, status, worker_id, started_at,
               finished_at, error_type, traceback
          FROM {executions} WHERE job_id = ? ORDER BY claim ASC
    """,
    "count_by_status": "SELECT status, count(*) AS total FROM {jobs} GROUP BY status",
    "count_by_queue": """
        SELECT queue, status, count(*) AS total FROM {jobs} GROUP BY queue, status
    """,
    "oldest_due_by_queue": """
        SELECT queue, min(run_at) AS oldest FROM {jobs}
         WHERE status = 'queued' AND run_at <= ?
         GROUP BY queue
    """,
    "count_expired_leases": """
        SELECT count(*) AS total FROM {jobs}
         WHERE status = 'running' AND lease_expires_at < ?
    """,
    "queued_by_task": """
        SELECT task, count(*) AS total FROM {jobs} WHERE status = 'queued' GROUP BY task
    """,
    "claim_housekeeping": """
        UPDATE {meta} SET value = ?
         WHERE key = 'last_purge_at'
           AND (CAST(value AS REAL) = 0 OR CAST(value AS REAL) <= ?)
        RETURNING key
    """,
    "schema_version": "SELECT value FROM {meta} WHERE key = 'schema_version'",
    "write_schema_version": "UPDATE {meta} SET value = ? WHERE key = 'schema_version'",
    "record_schema_version": (
        "INSERT INTO {meta} (key, value) VALUES ('schema_version', ?) "
        "ON CONFLICT (key) DO NOTHING"
    ),
    "record_last_purge": (
        "INSERT INTO {meta} (key, value) VALUES ('last_purge_at', '0') "
        "ON CONFLICT (key) DO NOTHING"
    ),
    "select_schedules": """
        SELECT name, cron, timezone, next_run_at, last_run_at, last_job_id
          FROM {schedules} ORDER BY name
    """,
    "register_schedule": """
        INSERT INTO {schedules} (name, cron, timezone, next_run_at) VALUES (?, ?, ?, ?)
        ON CONFLICT (name) DO UPDATE
           SET cron = excluded.cron, timezone = excluded.timezone,
               next_run_at = excluded.next_run_at
         WHERE {schedules}.cron != excluded.cron OR {schedules}.timezone != excluded.timezone
    """,
    "advance_schedule": """
        UPDATE {schedules} SET next_run_at = ?
         WHERE name = ? AND next_run_at = ?
    """,
    "record_schedule_run": """
        UPDATE {schedules} SET last_run_at = ?, last_job_id = ? WHERE name = ?
    """,
    "expire_results": """
        UPDATE {jobs} SET result = NULL
         WHERE id IN (SELECT id FROM {jobs}
                       WHERE status = 'success' AND finished_at >= ? AND finished_at < ?
                         AND task = ? AND result IS NOT NULL
                       ORDER BY finished_at
                       LIMIT ?)
    """,
    "purge_batch": """
        DELETE FROM {jobs}
         WHERE id IN (SELECT id FROM {jobs}
                       WHERE status = ? AND finished_at IS NOT NULL AND finished_at < ?
                       ORDER BY finished_at
                       LIMIT ?)
        RETURNING id
    """,
}


class Queries:
    def __init__(self, tables: Tables) -> None:
        self.tables = tables
        names = tables.names()
        for name, template in TEMPLATES.items():
            setattr(self, name, template.format_map(names))

    def listing(self, where: str) -> str:
        return f"SELECT {JOB_COLUMN_LIST} FROM {self.tables.jobs} {where} ORDER BY id DESC LIMIT ?"
