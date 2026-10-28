from __future__ import annotations

import re
import zlib

from ...exceptions import ConfigurationError

SCHEMA_VERSION = 1

QUEUE_KEY_CHARACTERS = 500

QUEUE_KEY = f"left(queue, {QUEUE_KEY_CHARACTERS})"

NAME = re.compile(r"^[a-z_][a-z0-9_]{0,39}$")

INDEXES = (
    ("jobs_claim", "jobs", False),
    ("jobs_by_status", "jobs", False),
    ("jobs_by_queue", "jobs", False),
    ("jobs_purge", "jobs", False),
    ("jobs_dedup", "jobs", True),
    ("executions_claim", "executions", True),
)

STATISTICS = (("jobs_queue", "jobs"),)

COLUMNS = (("claim_token", "jobs"),)


class Tables:
    def __init__(self, prefix: str, schema: str | None = None) -> None:
        for kind, value in (("prefix", prefix), ("schema", schema)):
            if value is not None and not NAME.fullmatch(value):
                raise ConfigurationError(
                    f"the postgres {kind} {value!r} must be lowercase letters, digits and "
                    f"underscores, start with a letter or underscore, and be at most 40 "
                    f"characters"
                )
        self.prefix = prefix
        self.schema = schema
        self.requested = schema
        qualify = f'"{schema}".' if schema else ""
        self.jobs = f'{qualify}"{prefix}_jobs"'
        self.executions = f'{qualify}"{prefix}_executions"'
        self.schedules = f'{qualify}"{prefix}_schedules"'
        self.meta = f'{qualify}"{prefix}_meta"'
        self.lock = zlib.crc32(f"tinyjobs:{schema or ''}:{prefix}".encode())

    def within(self, schema: str) -> "Tables":
        placed = Tables.__new__(Tables)
        placed.__dict__.update(self.__dict__)
        qualify = ('"' + schema.replace('"', '""') + '".').replace("%", "%%")
        placed.schema = schema
        placed.jobs = f'{qualify}"{self.prefix}_jobs"'
        placed.executions = f'{qualify}"{self.prefix}_executions"'
        placed.schedules = f'{qualify}"{self.prefix}_schedules"'
        placed.meta = f'{qualify}"{self.prefix}_meta"'
        return placed

    def instance(self) -> str:
        return f"""
            INSERT INTO {self.meta} (key, value) VALUES ('instance', gen_random_uuid()::text)
            ON CONFLICT (key) DO NOTHING
        """

    def index(self, name: str) -> str:
        return f'"{self.prefix}_{name}"'

    def indexes(self, *, concurrently: bool = False) -> dict[str, str]:
        statements = self._indexes()
        if not concurrently:
            return statements
        return {
            name: statement.replace(" INDEX IF NOT EXISTS ", " INDEX CONCURRENTLY IF NOT EXISTS ", 1)
            for name, statement in statements.items()
        }

    def _indexes(self) -> dict[str, str]:
        jobs, executions = self.jobs, self.executions
        return {
            "jobs_claim": f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_claim")}
                ON {jobs} (({QUEUE_KEY}), priority DESC, run_at ASC)
                WHERE status = 'queued'
            """,
            "jobs_by_status": f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_by_status")}
                ON {jobs} (status, id DESC)
            """,
            "jobs_by_queue": f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_by_queue")}
                ON {jobs} (({QUEUE_KEY}), id DESC)
            """,
            "jobs_purge": f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_purge")}
                ON {jobs} (status, finished_at)
                WHERE finished_at IS NOT NULL
            """,
            "jobs_dedup": f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {self.index("jobs_dedup")}
                ON {jobs} (dedup_digest)
                WHERE dedup_digest IS NOT NULL AND status IN ('queued', 'running')
            """,
            "executions_claim": f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {self.index("executions_claim")}
                ON {executions} (job_id, claim)
            """,
        }

    def columns(self) -> dict[str, str]:
        return {
            "claim_token": f"ALTER TABLE {self.jobs} ADD COLUMN IF NOT EXISTS claim_token text",
        }

    def statistics(self) -> dict[str, str]:
        qualify = (
            ('"' + self.schema.replace('"', '""') + '".').replace("%", "%%") if self.schema else ""
        )
        return {
            "jobs_queue": f"""
            CREATE STATISTICS IF NOT EXISTS {qualify}"{self.prefix}_jobs_queue" (dependencies)
                ON queue, ({QUEUE_KEY}) FROM {self.jobs}
            """,
        }

    def ddl(self, *, schema_missing: bool) -> tuple[str, ...]:
        jobs, executions = self.jobs, self.executions
        indexes = self.indexes()
        statements = []
        if self.requested and schema_missing:
            statements.append(f'CREATE SCHEMA IF NOT EXISTS "{self.requested}"')
        statements += [
            f"""
            CREATE TABLE IF NOT EXISTS {self.meta} (
                key   text PRIMARY KEY,
                value text NOT NULL
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {jobs} (
                id               text COLLATE "C" PRIMARY KEY,
                task             text NOT NULL,
                payload          bytea NOT NULL,
                codec            text NOT NULL,
                queue            text COLLATE "C" NOT NULL DEFAULT 'default',
                status           text COLLATE "C" NOT NULL CHECK (
                                     status IN ('queued', 'running', 'success', 'failed', 'cancelled')
                                 ),
                priority         integer NOT NULL DEFAULT 0,
                attempts         integer NOT NULL DEFAULT 0,
                max_attempts     integer NOT NULL DEFAULT 1,
                hand_backs       integer NOT NULL DEFAULT 0,
                claims           integer NOT NULL DEFAULT 0,
                run_at           double precision NOT NULL,
                created_at       double precision NOT NULL,
                finished_at      double precision,
                lease_expires_at double precision,
                worker_id        text,
                dedup_key        text COLLATE "C",
                dedup_digest     bytea,
                result           bytea,
                last_error       text,
                claim_token      text
            ) WITH (
                fillfactor = 85,
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_vacuum_threshold = 1000,
                autovacuum_vacuum_insert_scale_factor = 0.05,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_vacuum_cost_delay = 1
            )
            """,
            self.columns()["claim_token"],
            indexes["jobs_claim"],
            indexes["jobs_by_status"],
            indexes["jobs_by_queue"],
            indexes["jobs_purge"],
            indexes["jobs_dedup"],
            self.statistics()["jobs_queue"],
            f"""
            CREATE TABLE IF NOT EXISTS {executions} (
                id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                job_id      text COLLATE "C" NOT NULL REFERENCES {jobs} (id) ON DELETE CASCADE,
                attempt     integer NOT NULL,
                claim       integer NOT NULL,
                status      text NOT NULL,
                worker_id   text NOT NULL,
                started_at  double precision NOT NULL,
                finished_at double precision,
                error_type  text,
                traceback   text
            ) WITH (fillfactor = 90)
            """,
            indexes["executions_claim"],
            f"""
            CREATE TABLE IF NOT EXISTS {self.schedules} (
                name        text PRIMARY KEY,
                cron        text NOT NULL,
                timezone    text NOT NULL,
                next_run_at double precision NOT NULL,
                last_run_at double precision,
                last_job_id text
            )
            """,
        ]
        return tuple(statements)

    def seeds(self) -> tuple[str, ...]:
        return (
            f"""
            INSERT INTO {self.meta} (key, value) VALUES ('schema_version', '{SCHEMA_VERSION}')
            ON CONFLICT (key) DO NOTHING
            """,
            f"""
            INSERT INTO {self.meta} (key, value) VALUES ('last_purge_at', '0')
            ON CONFLICT (key) DO NOTHING
            """,
            self.instance(),
        )
