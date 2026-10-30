from __future__ import annotations

from importlib.resources import files
from typing import Any

from redis.asyncio import Redis

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
        return await self._scripts[name](keys=[self._root], args=[stamp, *arguments])

    def queue(
        self, pipeline: Any, name: str, now: float, *arguments: Any, following: bool = False
    ) -> None:
        stamp = ("~" if following else "") + repr(float(now))
        pipeline.execute_command(
            "EVALSHA", self._scripts[name].sha, 1, self._root, stamp, *arguments
        )

    async def load(self, name: str) -> None:
        await self._client.script_load(self._sources[name])

    async def run_once(
        self, name: str, now: float, *arguments: Any, following: bool = False
    ) -> Any:
        stamp = ("~" if following else "") + repr(float(now))
        return await self._client.eval(self._sources[name], 1, self._root, stamp, *arguments)
