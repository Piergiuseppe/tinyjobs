from __future__ import annotations

SCHEMA_VERSION = 1

DDL = (
    """
    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS jobs (
        id               TEXT PRIMARY KEY,
        task             TEXT NOT NULL,
        payload          BLOB NOT NULL,
        codec            TEXT NOT NULL,
        queue            TEXT NOT NULL DEFAULT 'default',
        status           TEXT NOT NULL,
        priority         INTEGER NOT NULL DEFAULT 0,
        attempts         INTEGER NOT NULL DEFAULT 0,
        max_attempts     INTEGER NOT NULL DEFAULT 1,
        run_at           REAL NOT NULL,
        created_at       REAL NOT NULL,
        finished_at      REAL,
        lease_expires_at REAL,
        worker_id        TEXT,
        dedup_key        TEXT,
        result           BLOB,
        last_error       TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_jobs_claim
        ON jobs (queue, priority DESC, run_at ASC)
        WHERE status = 'queued'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_jobs_lease
        ON jobs (lease_expires_at)
        WHERE status = 'running'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_jobs_listing
        ON jobs (status, created_at DESC)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedup
        ON jobs (dedup_key)
        WHERE dedup_key IS NOT NULL AND status IN ('queued', 'running')
    """,
    """
    CREATE TABLE IF NOT EXISTS executions (
        id          INTEGER PRIMARY KEY,
        job_id      TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        attempt     INTEGER NOT NULL,
        status      TEXT NOT NULL,
        worker_id   TEXT NOT NULL,
        started_at  REAL NOT NULL,
        finished_at REAL,
        error_type  TEXT,
        traceback   TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_attempt
        ON executions (job_id, attempt)
    """,
)
