from __future__ import annotations

import re

from ...exceptions import ConfigurationError

SCHEMA_VERSION = 6

COMPATIBLE_SINCE = 6

ADDED_COLUMNS = {6: (("runs_after", "TEXT"), ("waiting_on", "TEXT"))}

PREFIXED_SINCE = 4

DEFAULT_PREFIX = "tinyjobs"

NAME = re.compile(r"^[a-z_][a-z0-9_]{0,39}$")

LEGACY_KINDS = ("jobs", "executions", "schedules", "meta")

KINDS = (*LEGACY_KINDS, "limits")

LEGACY_INDEXES = (
    "idx_jobs_claim",
    "idx_jobs_lease",
    "idx_jobs_by_status",
    "idx_jobs_by_queue",
    "idx_jobs_purge",
    "idx_jobs_dedup",
    "idx_jobs_listing",
    "idx_executions_claim",
    "idx_executions_attempt",
)

LEGACY_STUB = (
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    f"INSERT INTO meta (key, value) VALUES ('schema_version', '{SCHEMA_VERSION}')",
)

TABLES_OF = "SELECT name FROM sqlite_master WHERE type = 'table'"


class Tables:
    def __init__(self, prefix: str = DEFAULT_PREFIX) -> None:
        if not isinstance(prefix, str) or not NAME.fullmatch(prefix):
            raise ConfigurationError(
                f"prefix must be lowercase letters, digits and underscores, starting "
                f"with a letter or an underscore, forty characters at most, not {prefix!r}"
            )
        self.prefix = prefix
        self.jobs = f"{prefix}_jobs"
        self.executions = f"{prefix}_executions"
        self.schedules = f"{prefix}_schedules"
        self.meta = f"{prefix}_meta"
        self.limits = f"{prefix}_limits"
        self.lock = f"{prefix}_install_lock"

    def names(self) -> dict[str, str]:
        return {kind: getattr(self, kind) for kind in KINDS}

    def index(self, name: str) -> str:
        return f"{self.prefix}_{name}"


def ddl(tables: Tables) -> tuple[str, ...]:
    return (
        f"""
        CREATE TABLE IF NOT EXISTS {tables.lock} (
            taken INTEGER
        )
        """,
        meta_table(tables),
        f"""
        CREATE TABLE IF NOT EXISTS {tables.jobs} (
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
            last_error       TEXT,
            runs_after       TEXT,
            waiting_on       TEXT
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_claim")}
            ON {tables.jobs} (queue, priority DESC, run_at ASC)
            WHERE status = 'queued'
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_lease")}
            ON {tables.jobs} (lease_expires_at)
            WHERE status = 'running'
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_by_status")}
            ON {tables.jobs} (status, id DESC)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_by_queue")}
            ON {tables.jobs} (queue, id DESC)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_purge")}
            ON {tables.jobs} (status, finished_at)
            WHERE finished_at IS NOT NULL
        """,
        dedup_index(tables),
        f"""
        CREATE TABLE IF NOT EXISTS {tables.executions} (
            id          INTEGER PRIMARY KEY,
            job_id      TEXT NOT NULL REFERENCES {tables.jobs}(id) ON DELETE CASCADE,
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
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {tables.index("executions_claim")}
            ON {tables.executions} (job_id, claim)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {tables.schedules} (
            name        TEXT PRIMARY KEY,
            cron        TEXT NOT NULL,
            timezone    TEXT NOT NULL,
            next_run_at REAL NOT NULL,
            last_run_at REAL,
            last_job_id TEXT
        )
        """,
        *limit_statements(tables),
        waiting_index(tables),
    )


def meta_table(tables: Tables) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {tables.meta} (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """


def waiting_index(tables: Tables) -> str:
    return f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_waiting")}
            ON {tables.jobs} (waiting_on)
            WHERE waiting_on IS NOT NULL
        """


def dedup_index(tables: Tables) -> str:
    return f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {tables.index("jobs_dedup")}
            ON {tables.jobs} (dedup_key)
            WHERE dedup_key IS NOT NULL AND status IN ('queued', 'held', 'waiting', 'running')
        """


def limit_statements(tables: Tables) -> tuple[str, ...]:
    return (
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_held")}
            ON {tables.jobs} (task, queue, priority DESC, run_at ASC)
            WHERE status = 'held'
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {tables.index("jobs_running_by_task")}
            ON {tables.jobs} (task)
            WHERE status = 'running'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {tables.limits} (
            task    TEXT PRIMARY KEY,
            last_at REAL NOT NULL DEFAULT 0
        )
        """,
    )


def migrations(tables: Tables) -> dict[int, tuple[str, ...]]:
    return {
        2: (
            f"CREATE INDEX IF NOT EXISTS {tables.index('jobs_by_status')} "
            f"ON {tables.jobs} (status, id DESC)",
            f"CREATE INDEX IF NOT EXISTS {tables.index('jobs_by_queue')} "
            f"ON {tables.jobs} (queue, id DESC)",
            f"CREATE INDEX IF NOT EXISTS {tables.index('jobs_purge')} "
            f"ON {tables.jobs} (status, finished_at) WHERE finished_at IS NOT NULL",
            f"INSERT INTO {tables.meta} (key, value) VALUES ('last_purge_at', '0') "
            "ON CONFLICT (key) DO NOTHING",
        ),
        3: (
            f"ALTER TABLE {tables.jobs} ADD COLUMN hand_backs INTEGER NOT NULL DEFAULT 0",
            f"ALTER TABLE {tables.jobs} ADD COLUMN claims INTEGER NOT NULL DEFAULT 0",
            f"ALTER TABLE {tables.executions} ADD COLUMN claim INTEGER NOT NULL DEFAULT 0",
            f"UPDATE {tables.executions} SET claim = ("
            f"SELECT count(*) FROM {tables.executions} AS earlier "
            f"WHERE earlier.job_id = {tables.executions}.job_id "
            f"AND (earlier.started_at < {tables.executions}.started_at "
            f"OR (earlier.started_at = {tables.executions}.started_at "
            f"AND earlier.id <= {tables.executions}.id)))",
            f"UPDATE {tables.jobs} SET claims = max(attempts, ("
            f"SELECT count(*) FROM {tables.executions} "
            f"WHERE {tables.executions}.job_id = {tables.jobs}.id))",
        ),
        4: tuple(f"DROP INDEX IF EXISTS {name}" for name in LEGACY_INDEXES),
        5: (
            *limit_statements(tables),
            f"DROP INDEX IF EXISTS {tables.index('jobs_dedup')}",
            dedup_index(tables),
        ),
        6: (
            waiting_index(tables),
            f"DROP INDEX IF EXISTS {tables.index('jobs_dedup')}",
            dedup_index(tables),
        ),
    }


def renames(tables: Tables, kinds: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        f"ALTER TABLE {kind} RENAME TO {getattr(tables, kind)}" for kind in kinds
    )
