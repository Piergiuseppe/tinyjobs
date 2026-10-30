from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
import math
import threading
from contextlib import suppress
from typing import Any, Awaitable, Callable, Sequence, TypeVar
from urllib.parse import unquote, urlsplit

from redis.asyncio import BlockingConnectionPool, ConnectionPool, Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoScriptError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ...clock import Clock, FollowingClock, elapsed
from ...exceptions import AppClosedError, ConfigurationError
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
from ..batching import AGAIN, Batches, Entry
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
DEFAULT_MAX_CONNECTIONS = 100
DEFAULT_POOL_TIMEOUT = 20.0
SCHEMA_VERSION = 2
ENQUEUE_BATCH = 100
LIST_CHUNK = 200
CLOCK_SECONDS = 5.0
SPARE_CONNECTIONS = 2
SETTLE_BATCH = 100
CLOSED_MESSAGE = "the redis backend was closed"
CLOSE_SECONDS = 0.5

logger = logging.getLogger("tinyjobs.redis")

T = TypeVar("T")


class RedisBackend(BaseBackend):
    transient_errors = (RedisConnectionError, RedisTimeoutError)
    capabilities = Capabilities(
        priority=True,
        delayed=True,
        dedup=True,
        transactional_enqueue=False,
        retention=True,
        schedules=True,
        max_payload_bytes=None,
        loop_bound=True,
    )

    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = DEFAULT_PREFIX,
        clock: Clock | None = None,
    ) -> None:
        _mend(client.connection_pool)
        self._client = client
        self._clock = FollowingClock() if clock is None else clock
        self._reading = asyncio.Lock()
        self._read = False
        self._refreshing: asyncio.Task | None = None
        self._reserving = threading.Lock()
        self._reserved: dict[object, int] = {}
        self._batches = Batches(self._settled_batch, SETTLE_BATCH)
        self._closed = False
        self._alone = 0
        self._keys = Keys(prefix)
        self._scripts = Scripts(client, self._keys.root)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        clock: Clock | None = None,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        pool_timeout: float = DEFAULT_POOL_TIMEOUT,
        **options: Any,
    ) -> "RedisBackend":
        single_connection_client = options.pop("single_connection_client", False)
        reason = None
        try:
            pool = BlockingConnectionPool.from_url(
                url, max_connections=max_connections, timeout=pool_timeout, **options
            )
        except ValueError as error:
            reason = _url_reason(url, error)
        else:
            reason = _refused(url, pool)
        if reason is not None:
            raise ConfigurationError(f"the redis url is not valid: {reason}")
        client = Redis(
            connection_pool=pool, single_connection_client=single_connection_client
        )
        client.auto_close_connection_pool = True
        return cls(client, prefix=prefix, clock=clock)

    async def setup(self) -> None:
        stored = int(await self._run("schema", SCHEMA_VERSION))
        if stored > SCHEMA_VERSION:
            raise RuntimeError(
                f"redis schema version {stored} is newer than this tinyjobs "
                f"build understands ({SCHEMA_VERSION}); upgrade the package"
            )

    async def close(self) -> None:
        self._closed = True
        await self._batches.close(AppClosedError(CLOSED_MESSAGE), CLOSE_SECONDS)
        refreshing, self._refreshing = self._refreshing, None
        if refreshing is not None and not refreshing.done():
            refreshing.cancel()
            await asyncio.wait({refreshing})
        await self._client.aclose()

    def reserve(self, owner: object, jobs: int) -> int:
        with self._reserving:
            if jobs < 0:
                self._reserved.pop(owner, None)
            else:
                self._reserved[owner] = jobs + SPARE_CONNECTIONS
            reserved = sum(self._reserved.values())
        return max(1, math.ceil(reserved / self._client.connection_pool.max_connections))

    async def spare(self) -> None:
        client = self._client
        await client.initialize()
        if client.connection is not None:
            return
        pool = client.connection_pool
        with self._reserving:
            wanted = sum(self._reserved.values()) or SPARE_CONNECTIONS
        opening = {
            asyncio.ensure_future(connection.connect()): connection
            for connection in _unopened(pool, wanted)
        }
        try:
            if opening:
                await asyncio.gather(*opening)
        finally:
            for task in opening:
                task.cancel()
            await asyncio.shield(_opened(pool, opening))

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
        tasks: Sequence[str] | None = None,
    ) -> list[Job]:
        if limit <= 0 or not queues or (tasks is not None and not tasks):
            return []
        now = await self._now()
        named = [] if tasks is None else list(tasks)
        claimed = await self._script(
            "claim",
            now,
            worker_id,
            number(now + lease_seconds),
            limit,
            -1 if tasks is None else len(named),
            *named,
            *queues,
        )
        return [job_from_flat(flat) for flat in claimed]

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        lease_seconds: float,
        worker_id: str | None = None,
    ) -> list[str]:
        if not job_ids:
            return []
        now = await self._now()
        renewed = await self._script(
            "heartbeat",
            now,
            number(now + lease_seconds),
            worker_id or "",
            *job_ids,
        )
        return [item.decode() if isinstance(item, bytes) else str(item) for item in renewed]

    async def complete(self, job: Job, result: bytes | None) -> bool:
        return bool(
            await self._outcome(
                "complete",
                job.id,
                job.worker_id or "",
                job.claims,
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
            await self._outcome(
                "fail",
                job.id,
                job.worker_id or "",
                job.claims,
                _storable(error.summary),
                str(execution_status),
                _storable(error.type),
                _storable(error.traceback),
                "" if retry_at is None else number(retry_at),
            )
        )

    async def release(self, job: Job, delay: float = 0.0) -> bool:
        run_at = number(await self._now() + delay)
        return bool(
            await self._run(
                "release", job.id, job.worker_id or "", job.claims, run_at
            )
        )

    async def release_many(self, jobs: Sequence[Job]) -> int:
        if not jobs:
            return 0
        clock = self._clock
        now = clock.now() if self._read or not isinstance(clock, FollowingClock) else await self._now()
        held = [
            part
            for job in jobs
            for part in (job.id, job.worker_id or "", job.claims, number(now))
        ]
        return int(
            await self._forgetting(
                self._scripts.run_once(
                    "release_many", now, *held, following=isinstance(clock, FollowingClock)
                )
            )
        )

    async def hand_back(self, job: Job, delay: float = 0.0, *, in_place: bool = False) -> bool:
        now = await self._now()
        run_at = number(min(job.run_at, now) if in_place else now + delay)
        return bool(
            await self._run(
                "hand_back", job.id, job.worker_id or "", job.claims, run_at
            )
        )

    async def cancel(self, job_id: str) -> bool:
        return bool(await self._run("cancel", job_id))

    async def requeue(self, job_id: str) -> bool:
        return bool(await self._run("requeue", job_id))

    async def get(self, job_id: str) -> Job | None:
        return (await self._load([job_id.encode()]))[0]

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
        flat = await self._run("history", job_id)
        raw = dict(zip(flat[::2], flat[1::2]))
        return [
            execution_from(job_id, claim, raw[claim])
            for claim in sorted(raw, key=int)
        ]

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
        if not await self._script(
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

    async def follow_clock(self) -> None:
        await self._now()

    async def schedules(self) -> list[ScheduleState]:
        await self._now()
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
        return await self._script(name, await self._now(), *arguments)

    async def _outcome(self, name: str, *arguments: Any) -> Any:
        now = await self._now()
        alone = self._client.connection is not None or self._closed
        if alone or (not self._batches.waiting and self._spare_connections() > self._alone):
            self._alone += 1
            try:
                return await self._script(name, now, *arguments)
            finally:
                self._alone -= 1
        return await self._batches.submit((name, now, arguments))

    def _spare_connections(self) -> int:
        pool = self._client.connection_pool
        return sum(1 for connection in pool._available_connections if connection.is_connected)

    async def _borrowed(self) -> Any:
        pool = self._client.connection_pool
        for connection in reversed(pool._available_connections):
            if connection.is_connected:
                pool._available_connections.remove(connection)
                pool._in_use_connections.add(connection)
                return connection
        return await _acquire(pool)

    async def _settled_batch(self, take: Callable[[], list[Entry]]) -> list[Any]:
        following = isinstance(self._clock, FollowingClock)
        encoder = self._client.connection_pool.get_encoder()
        pipeline = self._client.pipeline(transaction=False)
        try:
            pipeline.connection = await self._borrowed()
            batch = take()
            answers: list[Any] = [None] * len(batch)
            queued: list[int] = []
            for index, entry in enumerate(batch):
                name, now, arguments = entry.payload
                try:
                    for argument in arguments:
                        encoder.encode(argument)
                except Exception as error:
                    answers[index] = error
                    continue
                self._scripts.queue(pipeline, name, now, *arguments, following=following)
                queued.append(index)
            if not queued:
                return answers
            results = await self._forgetting(pipeline.execute(raise_on_error=False))
        finally:
            await pipeline.reset()
        missing: dict[str, list[int]] = {}
        for index, result in zip(queued, results):
            entry = batch[index]
            if isinstance(result, NoScriptError) and entry.attempts == 1:
                missing.setdefault(entry.payload[0], []).append(index)
            else:
                answers[index] = result
                entry.settle(result)
        for name, indexes in missing.items():
            try:
                await self._scripts.load(name)
            except Exception as error:
                for index in indexes:
                    answers[index] = error
            else:
                for index in indexes:
                    answers[index] = AGAIN
        return answers

    async def _script(self, name: str, now: float, *arguments: Any) -> Any:
        if self._closed:
            raise AppClosedError(CLOSED_MESSAGE)
        following = isinstance(self._clock, FollowingClock)
        return await self._forgetting(
            self._scripts.run(name, now, *arguments, following=following)
        )

    async def _now(self) -> float:
        clock = self._clock
        if not isinstance(clock, FollowingClock):
            return clock.now()
        if self._read and clock.trusted and not clock.overdue():
            if clock.due() and not self._reading.locked():
                self._refresh()
            return clock.now()
        if not self._read or not clock.trusted or clock.due():
            async with self._reading:
                if not self._read or not clock.trusted or clock.due():
                    await self._follow(clock)
        return clock.now()

    async def _follow(self, clock: FollowingClock) -> None:
        clock.follow(*await self._forgetting(self._timed()), self._server())
        self._read = True

    def _refresh(self) -> None:
        if self._refreshing is None or self._refreshing.done():
            self._refreshing = asyncio.create_task(
                self._refreshed(), name="tinyjobs-redis-clock", context=contextvars.Context()
            )

    async def _refreshed(self) -> None:
        clock = self._clock
        if not isinstance(clock, FollowingClock):
            return
        async with self._reading:
            if not clock.due():
                return
            try:
                async with asyncio.timeout(CLOCK_SECONDS):
                    await self._follow(clock)
            except (RedisConnectionError, RedisTimeoutError, TimeoutError) as error:
                logger.debug("could not read the server clock: %s", error)

    async def _timed(self) -> tuple[float, float, float]:
        client = self._client
        await client.initialize()
        if client.connection is not None:
            async with client._single_conn_lock:
                connection = client.connection
                if connection is None:
                    raise RedisConnectionError("the Redis client was closed")
                await connection.connect()
                return await self._timed_on(connection)
        pool = client.connection_pool
        connection = await _acquire(pool)
        try:
            return await self._timed_on(connection)
        finally:
            await pool.release(connection)

    async def _timed_on(self, connection: Any) -> tuple[float, float, float]:
        try:
            await connection.check_health()
            started = elapsed()
            seconds, microseconds = await self._time(connection)
            return seconds + microseconds / 1_000_000, started, elapsed()
        except BaseException:
            await connection.disconnect()
            raise

    async def _time(self, connection: Any) -> tuple[int, int]:
        await connection.send_command("TIME", check_health=False)
        seconds, microseconds = await connection.read_response()
        return int(seconds), int(microseconds)

    async def _forgetting(self, call: Awaitable[T]) -> T:
        try:
            return await call
        except (RedisConnectionError, RedisTimeoutError):
            if isinstance(self._clock, FollowingClock):
                self._clock.forget()
            raise

    def _server(self) -> str:
        pool = self._client.connection_pool
        settings = pool.connection_kwargs
        service = getattr(pool, "service_name", None)
        place = (
            f"sentinel {service}"
            if service
            else settings.get("path") or f"{settings.get('host')}:{settings.get('port')}"
        )
        return f"redis {place} db {settings.get('db', 0)}"

    async def _resolve(self, jobs: Sequence[Job], ids: Sequence[bytes]) -> list[Job]:
        resolved = []
        for job, stored in zip(jobs, ids):
            stored_id = stored.decode()
            resolved.append(job if stored_id == job.id else await self.get(stored_id))
        return resolved

    async def _load(self, ids: Sequence[bytes]) -> list[Job | None]:
        found = await self._fetch(ids)
        legacy = [job_id for job_id, raw in zip(ids, found) if raw and b"claims" not in raw]
        if legacy:
            await self._run("adopt", *legacy)
            adopted = dict(zip(legacy, await self._fetch(legacy)))
            found = [adopted.get(job_id, raw) for job_id, raw in zip(ids, found)]
        return [job_from_fields(raw) if raw else None for raw in found]

    async def _fetch(self, ids: Sequence[bytes]) -> list[dict[bytes, bytes]]:
        pipeline = self._client.pipeline(transaction=False)
        for job_id in ids:
            pipeline.hgetall(self._keys("job", job_id.decode()))
        return await pipeline.execute()

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


class _Unwedged:
    async def get_connection(self, command_name: Any = None, *keys: Any, **options: Any) -> Any:
        pool: Any = self
        try:
            async with asyncio.timeout(pool.timeout):
                async with pool._condition:
                    await pool._condition.wait_for(pool.can_get_connection)
                    connection = _take(pool)
        except TimeoutError as error:
            raise RedisConnectionError("No connection available.") from error
        try:
            await pool.ensure_connection(connection)
        except BaseException:
            await pool.release(connection)
            raise
        return connection


def _take(pool: Any) -> Any:
    try:
        connection = pool._available_connections.pop()
    except IndexError:
        connection = pool.make_connection()
    pool._in_use_connections.add(connection)
    return connection


@functools.cache
def _unwedged(kind: type) -> type:
    return type(kind.__name__, (_Unwedged, kind), {})


def _mend(pool: Any) -> None:
    if "get_available_connection" in vars(ConnectionPool):
        return
    if not isinstance(pool, BlockingConnectionPool):
        return
    if type(pool).get_connection is BlockingConnectionPool.get_connection:
        pool.__class__ = _unwedged(type(pool))


def _storable(text: str) -> str:
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def _secret(url: str) -> bool:
    plain = unquote(url).lower()
    return "@" in plain or "password" in plain


def _url_reason(url: str, error: ValueError) -> str:
    if _secret(url):
        return (
            "its error is hidden, since the url holds a password; check the port and "
            "percent-encode the password's reserved characters"
        )
    return str(error)


def _refused(url: str, pool: Any) -> str | None:
    options = pool.connection_kwargs
    split = urlsplit(url)
    if split.scheme == "unix" and split.netloc and "@" in split.path:
        return (
            "its socket path is hidden, since the url holds a password; percent-encode the "
            "password's reserved characters"
        )
    unknown = _unknown_options(pool)
    if unknown:
        if _secret(url):
            return (
                "it holds a connection option the redis client does not take, its name "
                "hidden since the url holds a password; percent-encode the password's "
                "reserved characters"
            )
        return f"the redis client takes no connection option named {', '.join(unknown)}"
    if options.get("decode_responses"):
        return "tinyjobs reads raw bytes from redis, so decode_responses cannot be set"
    if isinstance(pool.timeout, str):
        try:
            pool.timeout = float(pool.timeout)
        except ValueError:
            return "its timeout is not a number of seconds"
    return None


def _unknown_options(pool: Any) -> list[str]:
    accepted: set[str] = set()
    for kind in pool.connection_class.__mro__:
        if kind is object:
            return []
        if "__init__" not in vars(kind):
            continue
        parameters = inspect.signature(kind.__init__).parameters.values()
        accepted.update(parameter.name for parameter in parameters)
        if not any(parameter.kind is parameter.VAR_KEYWORD for parameter in parameters):
            break
    return sorted(set(pool.connection_kwargs) - accepted)


def _unopened(pool: Any, wanted: int) -> list[Any]:
    available = pool._available_connections
    ready = len(pool._in_use_connections) + sum(1 for c in available if c.is_connected)
    taken: list[Any] = []
    for _ in range(max(0, min(wanted, pool.max_connections) - ready)):
        closed = next((c for c in available if not c.is_connected), None)
        if closed is not None:
            available.remove(closed)
        elif len(available) + len(pool._in_use_connections) < pool.max_connections:
            closed = pool.make_connection()
        else:
            break
        pool._in_use_connections.add(closed)
        taken.append(closed)
    return taken


async def _opened(pool: Any, opening: dict[asyncio.Future, Any]) -> None:
    if not opening:
        return
    await asyncio.wait(opening)
    for task, connection in opening.items():
        if task.cancelled() or task.exception() is not None:
            with suppress(Exception):
                await connection.disconnect()
        await pool.release(connection)


async def _acquire(pool: Any) -> Any:
    named = inspect.signature(pool.get_connection).parameters.get("command_name")
    if named is not None and named.default is inspect.Parameter.empty:
        return await pool.get_connection("TIME")
    return await pool.get_connection()


def _due(first: list[tuple[bytes, float]], now: float) -> float | None:
    if not first or first[0][1] > now:
        return None
    return first[0][1]
