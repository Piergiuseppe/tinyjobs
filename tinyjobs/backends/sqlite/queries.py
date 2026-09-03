from __future__ import annotations

from .records import JOB_COLUMN_LIST, JOB_PLACEHOLDERS

INSERT_JOB = f"INSERT INTO jobs ({JOB_COLUMN_LIST}) VALUES ({JOB_PLACEHOLDERS})"

SELECT_JOB = f"SELECT {JOB_COLUMN_LIST} FROM jobs WHERE id = ?"

SELECT_ACTIVE_BY_DEDUP = f"""
    SELECT {JOB_COLUMN_LIST} FROM jobs
     WHERE dedup_key = ? AND status IN ('queued', 'running')
"""

SELECT_CLAIMABLE_IDS = """
    SELECT id FROM jobs
     WHERE status = 'queued' AND run_at <= ? AND queue IN ({queues})
     ORDER BY priority DESC, run_at ASC
     LIMIT ?
"""

CLAIM_RETURNING = f"""
    UPDATE jobs
       SET status = 'running',
           worker_id = ?,
           attempts = attempts + 1,
           lease_expires_at = ?
     WHERE id IN (
           SELECT id FROM jobs
            WHERE status = 'queued' AND run_at <= ? AND queue IN ({{queues}})
            ORDER BY priority DESC, run_at ASC
            LIMIT ?
     )
    RETURNING {JOB_COLUMN_LIST}
"""

CLAIM_ONE = """
    UPDATE jobs
       SET status = 'running',
           worker_id = ?,
           attempts = attempts + 1,
           lease_expires_at = ?
     WHERE id = ? AND status = 'queued'
"""

HEARTBEAT = """
    UPDATE jobs SET lease_expires_at = ?
     WHERE status = 'running' AND id IN ({ids})
"""

COMPLETE_JOB = """
    UPDATE jobs
       SET status = 'success', finished_at = ?, result = ?,
           lease_expires_at = NULL, last_error = NULL
     WHERE id = ? AND status = 'running' AND worker_id = ?
"""

FAIL_JOB = """
    UPDATE jobs
       SET status = 'failed', finished_at = ?, last_error = ?,
           lease_expires_at = NULL
     WHERE id = ? AND status = 'running' AND worker_id = ?
"""

RESCHEDULE_JOB = """
    UPDATE jobs
       SET status = 'queued', run_at = ?, last_error = ?,
           lease_expires_at = NULL, worker_id = NULL
     WHERE id = ? AND status = 'running' AND worker_id = ?
"""

RELEASE_JOB = """
    UPDATE jobs
       SET status = 'queued', run_at = ?, lease_expires_at = NULL, worker_id = NULL
     WHERE id = ? AND status = 'running' AND worker_id = ?
"""

CANCEL_JOB = """
    UPDATE jobs
       SET status = 'cancelled', finished_at = ?, lease_expires_at = NULL
     WHERE id = ? AND status IN ('queued', 'running')
"""

REQUEUE_JOB = """
    UPDATE jobs
       SET status = 'queued', run_at = ?, attempts = 0, finished_at = NULL,
           lease_expires_at = NULL, worker_id = NULL, last_error = NULL
     WHERE id = ? AND status IN ('failed', 'cancelled')
"""

REAP_EXPIRED = """
    UPDATE jobs
       SET status = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
           run_at = ?,
           finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE ? END,
           lease_expires_at = NULL,
           worker_id = NULL,
           last_error = 'lease expired: the worker holding this job stopped reporting'
     WHERE status = 'running' AND lease_expires_at < ?
    RETURNING id, attempts
"""

SELECT_EXPIRED_IDS = """
    SELECT id, attempts, max_attempts FROM jobs
     WHERE status = 'running' AND lease_expires_at < ?
"""

INSERT_EXECUTION = """
    INSERT INTO executions (job_id, attempt, status, worker_id, started_at)
    VALUES (?, ?, 'running', ?, ?)
    ON CONFLICT (job_id, attempt) DO UPDATE
       SET status = 'running', worker_id = excluded.worker_id,
           started_at = excluded.started_at, finished_at = NULL,
           error_type = NULL, traceback = NULL
"""

FINISH_EXECUTION = """
    UPDATE executions
       SET status = ?, finished_at = ?, error_type = ?, traceback = ?
     WHERE job_id = ? AND attempt = ?
"""

MARK_EXECUTIONS_LOST = """
    UPDATE executions
       SET status = 'lost', finished_at = ?
     WHERE status = 'running' AND job_id IN ({ids})
"""

SELECT_EXECUTIONS = """
    SELECT id, job_id, attempt, status, worker_id, started_at,
           finished_at, error_type, traceback
      FROM executions WHERE job_id = ? ORDER BY attempt ASC
"""

COUNT_BY_STATUS = "SELECT status, count(*) AS total FROM jobs GROUP BY status"

CLAIM_HOUSEKEEPING = """
    UPDATE meta SET value = ?
     WHERE key = 'last_purge_at'
       AND (CAST(value AS REAL) = 0 OR CAST(value AS REAL) <= ?)
    RETURNING key
"""

PURGE_BATCH = """
    DELETE FROM jobs
     WHERE id IN (SELECT id FROM jobs
                   WHERE status = ? AND finished_at IS NOT NULL AND finished_at < ?
                   ORDER BY finished_at
                   LIMIT ?)
    RETURNING id
"""
