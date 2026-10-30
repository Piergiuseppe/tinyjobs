from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from typing import Sequence

from ...models import Job
from .queries import CLAIM_ONE, CLAIM_RETURNING, SELECT_CLAIMABLE_IDS, SELECT_JOB
from .records import job_from_row

RETURNING_MINIMUM_VERSION = (3, 35, 0)
SUPPORTS_RETURNING = sqlite3.sqlite_version_info >= RETURNING_MINIMUM_VERSION


class ClaimStrategy(ABC):
    def ordered(self, jobs: list[Job]) -> list[Job]:
        return sorted(jobs, key=lambda job: (-job.priority, job.run_at))

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
        tasks: Sequence[str] | None = None,
    ) -> list[Job]: ...


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
        tasks: Sequence[str] | None = None,
    ) -> list[Job]:
        sql = CLAIM_RETURNING.format(queues=_placeholders(queues), tasks=_among(tasks))
        rows = connection.execute(
            sql, (worker_id, lease_expires_at, now, *queues, *(tasks or ()), limit)
        ).fetchall()
        return self.ordered([job_from_row(row) for row in rows])


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
        tasks: Sequence[str] | None = None,
    ) -> list[Job]:
        sql = SELECT_CLAIMABLE_IDS.format(queues=_placeholders(queues), tasks=_among(tasks))
        candidates = connection.execute(sql, (now, *queues, *(tasks or ()), limit)).fetchall()
        claimed: list[Job] = []
        for candidate in candidates:
            cursor = connection.execute(
                CLAIM_ONE, (worker_id, lease_expires_at, candidate["id"])
            )
            if cursor.rowcount != 1:
                continue
            row = connection.execute(SELECT_JOB, (candidate["id"],)).fetchone()
            claimed.append(job_from_row(row))
        return self.ordered(claimed)


def build_claim_strategy() -> ClaimStrategy:
    if SUPPORTS_RETURNING:
        return ReturningClaim()
    return SelectThenUpdateClaim()


def _placeholders(values: Sequence[str]) -> str:
    return ", ".join("?" for _ in values)


def _among(tasks: Sequence[str] | None) -> str:
    return "" if tasks is None else f" AND task IN ({_placeholders(tasks)})"
