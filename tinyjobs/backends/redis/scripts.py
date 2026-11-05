from __future__ import annotations

import inspect
from importlib.resources import files
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import NoScriptError, ResponseError

NAMES = (
    "enqueue",
    "claim",
    "claim_job",
    "unclaim",
    "complete",
    "fail",
    "release",
    "release_many",
    "hand_back",
    "cancel",
    "requeue",
    "heartbeat",
    "reap",
    "claim_sweep",
    "purge_batch",
    "expire_results",
    "register_schedules",
    "fire_schedule",
    "schema",
    "adopt",
    "history",
)


class Scripts:
    def __init__(self, client: Redis, root: str) -> None:
        source = files(__package__).joinpath("lua")
        library = source.joinpath("library.lua").read_text()
        self._root = root
        self._client = client
        self._sources = {
            name: library + "\n" + source.joinpath(f"{name}.lua").read_text()
            for name in NAMES
        }
        self._scripts = {
            name: client.register_script(text) for name, text in self._sources.items()
        }

    async def run(
        self, name: str, now: float, *arguments: Any, following: bool = False
    ) -> Any:
        stamp = ("~" if following else "") + repr(float(now))
        client = self._client
        if client.single_connection_client or client.connection is not None:
            return await self._scripts[name](keys=[self._root], args=[stamp, *arguments])
        pool = client.connection_pool
        connection = await borrowed(pool)
        try:
            try:
                return await self._sent(connection, name, stamp, arguments)
            except NoScriptError:
                await self._loaded_on(connection, name)
                return await self._sent(connection, name, stamp, arguments)
        except ResponseError:
            raise
        except BaseException:
            await connection.disconnect(nowait=True)
            raise
        finally:
            await pool.release(connection)

    async def _sent(
        self, connection: Any, name: str, stamp: str, arguments: tuple[Any, ...]
    ) -> Any:
        await connection.send_command(
            "EVALSHA", self._scripts[name].sha, 1, self._root, stamp, *arguments
        )
        return await connection.read_response()

    async def _loaded_on(self, connection: Any, name: str) -> None:
        await connection.send_command("SCRIPT", "LOAD", self._sources[name])
        sha = await connection.read_response()
        self._scripts[name].sha = sha.decode() if isinstance(sha, bytes) else sha

    def command(
        self, name: str, now: float, arguments: tuple[Any, ...], following: bool = False
    ) -> tuple[Any, ...]:
        stamp = ("~" if following else "") + repr(float(now))
        return ("EVALSHA", self._scripts[name].sha, 1, self._root, stamp, *arguments)

    async def run_many(self, connection: Any, commands: list[tuple[Any, ...]]) -> list[Any]:
        try:
            await connection.send_packed_command(connection.pack_commands(commands))
            answers: list[Any] = []
            for _ in commands:
                try:
                    answers.append(await connection.read_response())
                except ResponseError as error:
                    answers.append(error)
            return answers
        except BaseException:
            await connection.disconnect(nowait=True)
            raise

    async def load(self, name: str) -> None:
        self._scripts[name].sha = await self._client.script_load(self._sources[name])

    async def run_once(
        self, name: str, now: float, *arguments: Any, following: bool = False
    ) -> Any:
        stamp = ("~" if following else "") + repr(float(now))
        return await self._client.eval(self._sources[name], 1, self._root, stamp, *arguments)


async def borrowed(pool: Any) -> Any:
    for connection in reversed(pool._available_connections):
        if connection.is_connected:
            pool._available_connections.remove(connection)
            pool._in_use_connections.add(connection)
            return connection
    return await acquired(pool)


async def acquired(pool: Any) -> Any:
    named = inspect.signature(pool.get_connection).parameters.get("command_name")
    if named is not None and named.default is inspect.Parameter.empty:
        return await pool.get_connection("TIME")
    return await pool.get_connection()
