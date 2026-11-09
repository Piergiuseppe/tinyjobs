from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from typing import Mapping, Sequence

from ...limits import Limit
from .queries import Queries

RETURNING_MINIMUM_VERSION = (3, 35, 0)
SUPPORTS_RETURNING = sqlite3.sqlite_version_info >= RETURNING_MINIMUM_VERSION

SLICES = 8


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


class LimitedClaim(ClaimStrategy):
    def claim(
        self,
        connection: sqlite3.Connection,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_expires_at: float,
        now: float,
        limits: Mapping[str, Limit],
    ) -> list[sqlite3.Row]:
        rooms = self._rooms(connection, limits, limit, now)
        claimed: list[sqlite3.Row] = []
        for _ in range(SLICES):
            wanted = limit - len(claimed)
            fresh = self._due(connection, queues, wanted, now)
            held = self._held(connection, queues, rooms, wanted)
            holding: list[tuple[str]] = []
            for candidate in _merged(fresh, held, wanted):
                if len(claimed) == limit:
                    break
                task = candidate["task"]
                if rooms.get(task, 1) > 0:
                    admitted = self._admit(connection, candidate, worker_id, lease_expires_at)
                    if admitted is not None:
                        claimed.append(admitted)
                        self._take_room(connection, rooms, limits, task, now)
                elif candidate["status"] == "queued":
                    holding.append((candidate["id"],))
            connection.executemany(self._queries.hold_one, holding)
            if len(claimed) == limit or len(fresh) < wanted:
                break
        return self.ordered(claimed)

    def _rooms(
        self,
        connection: sqlite3.Connection,
        limits: Mapping[str, Limit],
        limit: int,
        now: float,
    ) -> dict[str, int]:
        tasks = tuple(limits)
        marks = _placeholders(tasks)
        running = {
            row["task"]: row["total"]
            for row in connection.execute(self._queries.running_by_task.format(tasks=marks), tasks)
        }
        starts: dict[str, float] = {}
        if any(value.interval is not None for value in limits.values()):
            starts = {
                row["task"]: row["last_at"]
                for row in connection.execute(
                    self._queries.select_last_starts.format(tasks=marks), tasks
                )
            }
        return {
            task: _room(limits[task], running.get(task, 0), starts.get(task, 0.0), limit, now)
            for task in tasks
        }

    def _due(
        self, connection: sqlite3.Connection, queues: Sequence[str], wanted: int, now: float
    ) -> list[sqlite3.Row]:
        sql = self._queries.select_due_candidates.format(queues=_placeholders(queues))
        return connection.execute(sql, (now, *queues, wanted)).fetchall()

    def _held(
        self,
        connection: sqlite3.Connection,
        queues: Sequence[str],
        rooms: Mapping[str, int],
        wanted: int,
    ) -> list[sqlite3.Row]:
        sql = self._queries.select_held_line.format(queues=_placeholders(queues))
        held: list[sqlite3.Row] = []
        for task, room in rooms.items():
            if room > 0:
                held += connection.execute(sql, (task, *queues, min(room, wanted))).fetchall()
        return held

    def _admit(
        self,
        connection: sqlite3.Connection,
        candidate: sqlite3.Row,
        worker_id: str,
        lease_expires_at: float,
    ) -> sqlite3.Row | None:
        cursor = connection.execute(
            self._queries.admit_one,
            (worker_id, lease_expires_at, candidate["id"], candidate["status"]),
        )
        if cursor.rowcount != 1:
            return None
        return connection.execute(self._queries.select_job, (candidate["id"],)).fetchone()

    def _take_room(
        self,
        connection: sqlite3.Connection,
        rooms: dict[str, int],
        limits: Mapping[str, Limit],
        task: str,
        now: float,
    ) -> None:
        if task not in rooms:
            return
        rooms[task] -= 1
        if limits[task].interval is not None:
            connection.execute(self._queries.record_start, (task, now))


def build_claim_strategy(queries: Queries) -> ClaimStrategy:
    if SUPPORTS_RETURNING:
        return ReturningClaim(queries)
    return SelectThenUpdateClaim(queries)


def _placeholders(values: Sequence[str]) -> str:
    return ", ".join("?" for _ in values)


def _room(limit: Limit, running: int, last_at: float, batch: int, now: float) -> int:
    room = batch if limit.concurrency is None else max(limit.concurrency - running, 0)
    if limit.interval is None:
        return room
    return min(room, 1) if now >= last_at + limit.interval else 0


def _merged(
    fresh: list[sqlite3.Row], held: list[sqlite3.Row], wanted: int
) -> list[sqlite3.Row]:
    if len(fresh) == wanted and fresh:
        last = _place(fresh[-1])
        held = [row for row in held if _place(row) <= last]
    return sorted([*held, *fresh], key=_place)


def _place(row: sqlite3.Row) -> tuple[int, float, bool]:
    return (-row["priority"], row["run_at"], row["status"] == "queued")
