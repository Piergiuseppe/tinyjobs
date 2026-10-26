from __future__ import annotations

SCHEMA_VERSION = 3

DROP_LEGACY_INDEX = "DROP INDEX IF EXISTS idx_executions_attempt"

LEGACY_INDEX_IS_UNIQUE = (
    "SELECT 1 FROM sqlite_master WHERE type = 'index' "
    "AND name = 'idx_executions_attempt' AND sql LIKE 'CREATE UNIQUE%'"
)

RESERVE_LEGACY_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_executions_attempt ON executions (job_id) WHERE 0"
)

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
        hand_backs       INTEGER NOT NULL DEFAULT 0,
        claims           INTEGER NOT NULL DEFAULT 0,
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
    CREATE INDEX IF NOT EXISTS idx_jobs_by_status
        ON jobs (status, id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_jobs_by_queue
        ON jobs (queue, id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_jobs_purge
        ON jobs (status, finished_at)
        WHERE finished_at IS NOT NULL
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
        claim       INTEGER NOT NULL,
        status      TEXT NOT NULL,
        worker_id   TEXT NOT NULL,
        started_at  REAL NOT NULL,
        finished_at REAL,
        error_type  TEXT,
        traceback   TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_claim
        ON executions (job_id, claim)
    """,
    RESERVE_LEGACY_INDEX,
    """
    CREATE TABLE IF NOT EXISTS schedules (
        name        TEXT PRIMARY KEY,
        cron        TEXT NOT NULL,
        timezone    TEXT NOT NULL,
        next_run_at REAL NOT NULL,
        last_run_at REAL,
        last_job_id TEXT
    )
    """,
)


MIGRATIONS: dict[int, tuple[str, ...]] = {
    2: (
        "DROP INDEX IF EXISTS idx_jobs_listing",
        "CREATE INDEX IF NOT EXISTS idx_jobs_by_status ON jobs (status, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_by_queue ON jobs (queue, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_purge ON jobs (status, finished_at) "
        "WHERE finished_at IS NOT NULL",
        "INSERT INTO meta (key, value) VALUES ('last_purge_at', '0') "
        "ON CONFLICT (key) DO NOTHING",
    ),
    3: (
        "ALTER TABLE jobs ADD COLUMN hand_backs INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN claims INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE executions ADD COLUMN claim INTEGER NOT NULL DEFAULT 0",
        "UPDATE executions SET claim = ("
        "SELECT count(*) FROM executions AS earlier "
        "WHERE earlier.job_id = executions.job_id "
        "AND (earlier.started_at < executions.started_at "
        "OR (earlier.started_at = executions.started_at "
        "AND earlier.id <= executions.id)))",
        "UPDATE jobs SET claims = max(attempts, ("
        "SELECT count(*) FROM executions WHERE executions.job_id = jobs.id))",
    ),
}
