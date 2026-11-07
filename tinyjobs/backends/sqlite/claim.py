from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from typing import Sequence

from .queries import Queries

RETURNING_MINIMUM_VERSION = (3, 35, 0)
SUPPORTS_RETURNING = sqlite3.sqlite_version_info >= RETURNING_MINIMUM_VERSION


class ClaimStrategy(ABC):
    def __init__(self, queries: Queries) -> None:
        self._queries = queries

    def ordered(self, rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
        return sorted(rows, key=lambda row: (-row["priority"], row["run_at"]))

    @abstractmethod
    def claim(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_expires_at: float,
        now: float,
    ) -> list[sqlite3.Row]: ...


class ReturningClaim(ClaimStrategy):
    def claim(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_expires_at: float,
        now: float,
    ) -> list[sqlite3.Row]:
        sql = self._queries.claim_returning.format(queues=_placeholders(queues))
        rows = connection.execute(
            sql, (worker_id, lease_expires_at, now, *queues, limit)
        ).fetchall()
        return self.ordered(rows)


class SelectThenUpdateClaim(ClaimStrategy):
    def claim(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_expires_at: float,
        now: float,
    ) -> list[sqlite3.Row]:
        queries = self._queries
        sql = queries.select_claimable_ids.format(queues=_placeholders(queues))
        candidates = connection.execute(sql, (now, *queues, limit)).fetchall()
        claimed: list[sqlite3.Row] = []
        for candidate in candidates:
            cursor = connection.execute(
                queries.claim_one, (worker_id, lease_expires_at, candidate["id"])
            )
            if cursor.rowcount != 1:
                continue
            row = connection.execute(queries.select_job, (candidate["id"],)).fetchone()
            claimed.append(row)
        return self.ordered(claimed)


def build_claim_strategy(queries: Queries) -> ClaimStrategy:
    if SUPPORTS_RETURNING:
        return ReturningClaim(queries)
    return SelectThenUpdateClaim(queries)


def _placeholders(values: Sequence[str]) -> str:
    return ", ".join("?" for _ in values)
