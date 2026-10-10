from __future__ import annotations

from typing import Any, Sequence

from redis.asyncio import Redis

from ...clock import Clock
from ...models import (
    Execution,
    ExecutionError,
    Job,
    Page,
    QueueStats,
    ScheduleState,
    Stats,
)
from ...retention import RetentionPolicy
from ...status import ExecutionStatus, JobStatus
from ..base import LEASE_EXPIRED_MESSAGE, BaseBackend, Capabilities
from .keys import Keys
from .records import (
    execution_from,
    job_from_fields,
    job_from_flat,
    jobs_to_arguments,
    number,
    schedule_from,
)
from .scripts import Scripts

DEFAULT_PREFIX = "tinyjobs"
SCHEMA_VERSION = 1
ENQUEUE_BATCH = 100
LIST_CHUNK = 200


class RedisBackend(BaseBackend):
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=False,
        retention=True,
        schedules=True,
        max_payload_bytes=None,
    )

    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = DEFAULT_PREFIX,
        clock: Clock | None = None,
    ) -> None:
        self._client = client
        self._clock = clock or Clock()
        self._keys = Keys(prefix)
        self._scripts = Scripts(client, self._keys.root)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        clock: Clock | None = None,
        **options: Any,
    ) -> "RedisBackend":
        return cls(Redis.from_url(url, **options), prefix=prefix, clock=clock)

    async def setup(self) -> None:
        version = self._keys("schema_version")
        await self._client.set(version, SCHEMA_VERSION, nx=True)
        stored = int(await self._client.get(version))
        if stored > SCHEMA_VERSION:
            raise RuntimeError(
                f"redis schema version {stored} is newer than this tinyjobs "
                f"build understands ({SCHEMA_VERSION}); upgrade the package"
            )

    async def close(self) -> None:
        await self._client.aclose()

    async def enqueue(self, job: Job) -> Job:
        return (await self.enqueue_many([job]))[0]

    async def enqueue_many(self, jobs: Sequence[Job]) -> list[Job]:
        stored: list[Job] = []
        for start in range(0, len(jobs), ENQUEUE_BATCH):
            batch = jobs[start : start + ENQUEUE_BATCH]
            ids = await self._run("enqueue", *jobs_to_arguments(batch))
            stored.extend(await self._resolve(batch, ids))
        return stored

    async def claim(
        self,
        *,
        worker_id: str,
        queues: Sequence[str],
        limit: int,
        lease_seconds: float,
    ) -> list[Job]:
        if limit <= 0 or not queues:
            return []
        now = self._clock.now()
        claimed = await self._scripts.run(
            "claim", now, worker_id, number(now + lease_seconds), limit, *queues
        )
        return [job_from_flat(flat) for flat in claimed]

    async def heartbeat(self, job_ids: Sequence[str], lease_seconds: float) -> None:
        if job_ids:
            now = self._clock.now()
            await self._scripts.run(
                "heartbeat", now, number(now + lease_seconds), *job_ids
            )

    async def complete(self, job: Job, result: bytes | None) -> bool:
        return bool(
            await self._run(
                "complete",
                job.id,
                job.worker_id or "",
                "0" if result is None else "1",
                result or b"",
            )
        )

    async def fail(
        self,
        job: Job,
        error: ExecutionError,
        retry_at: float | None,
        execution_status: ExecutionStatus = ExecutionStatus.FAILED,
    ) -> bool:
        return bool(
            await self._run(
                "fail",
                job.id,
                job.worker_id or "",
                error.summary,
                str(execution_status),
                error.type,
                error.traceback,
                "" if retry_at is None else number(retry_at),
            )
        )

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        run_at = number(self._clock.now() + delay)
        return bool(
            await self._run("release", job.id, job.worker_id or "", run_at)
        )

    async def cancel(self, job_id: str) -> bool:
        return bool(await self._run("cancel", job_id))

    async def requeue(self, job_id: str) -> bool:
        return bool(await self._run("requeue", job_id))

    async def get(self, job_id: str) -> Job | None:
        raw = await self._client.hgetall(self._keys("job", job_id))
        return job_from_fields(raw) if raw else None

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page:
        index = self._index(status, queue)
        upper = f"({cursor}" if cursor else "+"
        found: list[Job] = []
        while len(found) <= limit:
            wanted = LIST_CHUNK if queue and status else limit + 1 - len(found)
            ids = await self._client.zrevrangebylex(
                index, upper, "-", start=0, num=wanted
            )
            if not ids:
                break
            for job in await self._load(ids):
                if job is not None and (queue is None or job.queue == queue):
                    found.append(job)
            upper = f"({ids[-1].decode()}"
        items = found[:limit]
        return Page(items=items, cursor=items[-1].id if len(found) > limit else None)

    async def executions(self, job_id: str) -> list[Execution]:
        raw = await self._client.hgetall(self._keys("executions", job_id))
        executions = [
            execution_from(job_id, attempt, encoded) for attempt, encoded in raw.items()
        ]
        return sorted(executions, key=lambda execution: execution.attempt)

    async def reap_expired(self) -> int:
        return await self._run("reap", LEASE_EXPIRED_MESSAGE)

    async def counts_by_status(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        for (_, status), count in (await self._counts()).items():
            totals[status] = totals.get(status, 0) + count
        return totals

    async def stats(self, now: float) -> Stats:
        counts: dict[str, dict[str, int]] = {}
        for (queue, status), count in (await self._counts()).items():
            counts.setdefault(queue, {})[status] = count
        queues = sorted(counts)
        pipeline = self._client.pipeline(transaction=False)
        for queue in queues:
            pipeline.zrange(self._keys("queued", queue), 0, 0, withscores=True)
        pipeline.zcount(self._keys("leases"), "-inf", f"({number(now)}")
        pipeline.hgetall(self._keys("queued_tasks"))
        *oldest, expired, tasks = await pipeline.execute()
        return Stats(
            queues=[
                QueueStats(queue, counts[queue], _due(first, now))
                for queue, first in zip(queues, oldest)
            ],
            expired_leases=expired,
            queued_tasks={task.decode(): int(count) for task, count in tasks.items()},
        )

    async def purge(self, policy: RetentionPolicy, now: float) -> int | None:
        if not policy.enabled:
            return None
        if not await self._scripts.run(
            "claim_sweep", now, number(now - policy.interval)
        ):
            return None
        removed = 0
        for status, cutoff in policy.horizons(now).items():
            while removed < policy.maximum_per_sweep:
                batch = min(policy.batch, policy.maximum_per_sweep - removed)
                gone = await self._run(
                    "purge_batch", str(status), number(cutoff), batch
                )
                removed += gone
                if gone < batch:
                    break
        return removed

    async def schedules(self) -> list[ScheduleState]:
        names = sorted(await self._client.smembers(self._keys("schedules")))
        pipeline = self._client.pipeline(transaction=False)
        for name in names:
            pipeline.hgetall(self._keys("schedule", name.decode()))
        return [schedule_from(raw) for raw in await pipeline.execute() if raw]

    async def register_schedules(self, states: Sequence[ScheduleState]) -> None:
        arguments: list[str] = []
        for state in states:
            arguments.extend(
                (state.name, state.cron, state.timezone, number(state.next_run_at))
            )
        if arguments:
            await self._run("register_schedules", *arguments)

    async def fire_schedule(
        self,
        name: str,
        *,
        expected: float,
        next_run_at: float,
        jobs: Sequence[Job],
    ) -> list[Job] | None:
        ids = await self._run(
            "fire_schedule",
            name,
            number(expected),
            number(next_run_at),
            *jobs_to_arguments(jobs),
        )
        if ids is None:
            return None
        return await self._resolve(jobs, ids)

    async def _run(self, name: str, *arguments: Any) -> Any:
        return await self._scripts.run(name, self._clock.now(), *arguments)

    async def _resolve(self, jobs: Sequence[Job], ids: Sequence[bytes]) -> list[Job]:
        resolved = []
        for job, stored in zip(jobs, ids):
            stored_id = stored.decode()
            resolved.append(job if stored_id == job.id else await self.get(stored_id))
        return resolved

    async def _load(self, ids: Sequence[bytes]) -> list[Job | None]:
        pipeline = self._client.pipeline(transaction=False)
        for job_id in ids:
            pipeline.hgetall(self._keys("job", job_id.decode()))
        return [job_from_fields(raw) if raw else None for raw in await pipeline.execute()]

    async def _counts(self) -> dict[tuple[str, str], int]:
        raw = await self._client.hgetall(self._keys("counts"))
        counts = {}
        for field, count in raw.items():
            queue, status = field.decode().rsplit("\t", 1)
            counts[(queue, status)] = int(count)
        return counts

    def _index(self, status: JobStatus | None, queue: str | None) -> str:
        if status is not None:
            return self._keys("index", "status", str(status))
        if queue is not None:
            return self._keys("index", "queue", queue)
        return self._keys("index", "all")


def _due(first: list[tuple[bytes, float]], now: float) -> float | None:
    if not first or first[0][1] > now:
        return None
    return first[0][1]
