from __future__ import annotations

import re
import zlib

from ...exceptions import ConfigurationError

SCHEMA_VERSION = 1

NAME = re.compile(r"^[a-z_][a-z0-9_]{0,39}$")


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

    def ddl(self) -> tuple[str, ...]:
        jobs, executions = self.jobs, self.executions
        statements = []
        if self.requested:
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
                result           bytea,
                last_error       text
            ) WITH (
                fillfactor = 85,
                autovacuum_vacuum_scale_factor = 0.01,
                autovacuum_vacuum_threshold = 1000,
                autovacuum_vacuum_insert_scale_factor = 0.05,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_vacuum_cost_delay = 1
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_claim")}
                ON {jobs} (queue, priority DESC, run_at ASC)
                WHERE status = 'queued'
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_by_status")}
                ON {jobs} (status, id DESC)
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_by_queue")}
                ON {jobs} (queue, id DESC)
            """,
            f"""
            CREATE INDEX IF NOT EXISTS {self.index("jobs_purge")}
                ON {jobs} (status, finished_at)
                WHERE finished_at IS NOT NULL
            """,
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {self.index("jobs_dedup")}
                ON {jobs} (dedup_key)
                WHERE dedup_key IS NOT NULL AND status IN ('queued', 'running')
            """,
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
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {self.index("executions_claim")}
                ON {executions} (job_id, claim)
            """,
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
            f"""
            INSERT INTO {self.meta} (key, value) VALUES ('schema_version', '{SCHEMA_VERSION}')
            ON CONFLICT (key) DO NOTHING
            """,
            f"""
            INSERT INTO {self.meta} (key, value) VALUES ('last_purge_at', '0')
            ON CONFLICT (key) DO NOTHING
            """,
            self.instance(),
        ]
        return tuple(statements)
