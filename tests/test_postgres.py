from __future__ import annotations

import os
import unittest
import uuid

from tinyjobs.testing import BackendContract

POSTGRES_URL = os.environ.get("TINYJOBS_POSTGRES_URL")

try:
    import psycopg

    from tinyjobs.backends.postgres import PostgresBackend
except ImportError:
    PostgresBackend = None


def new_prefix() -> str:
    return f"tinyjobs_test_{uuid.uuid4().hex[:16]}"


async def drop(prefix: str, schema: str | None = None) -> None:
    qualify = f'"{schema}".' if schema else ""
    async with await psycopg.AsyncConnection.connect(POSTGRES_URL, autocommit=True) as connection:
        for table in ("executions", "jobs", "schedules", "meta"):
            await connection.execute(f'DROP TABLE IF EXISTS {qualify}"{prefix}_{table}" CASCADE')
        if schema:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@unittest.skipUnless(POSTGRES_URL, "set TINYJOBS_POSTGRES_URL to run the postgres tests")
@unittest.skipIf(PostgresBackend is None, "pip install 'tinyjobs[postgres]' to run the postgres tests")
class PostgresContractTests(BackendContract, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.prefix = new_prefix()

    async def make_backend(self, clock):
        return PostgresBackend.from_url(POSTGRES_URL, prefix=self.prefix, clock=clock)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await drop(self.prefix)


if __name__ == "__main__":
    unittest.main()
