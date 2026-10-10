from __future__ import annotations

from importlib.resources import files
from typing import Any

from redis.asyncio import Redis

NAMES = (
    "enqueue",
    "claim",
    "complete",
    "fail",
    "release",
    "cancel",
    "requeue",
    "heartbeat",
    "reap",
    "claim_sweep",
    "purge_batch",
    "register_schedules",
    "fire_schedule",
)


class Scripts:
    def __init__(self, client: Redis, root: str) -> None:
        source = files(__package__).joinpath("lua")
        library = source.joinpath("library.lua").read_text()
        self._root = root
        self._scripts = {
            name: client.register_script(
                library + "\n" + source.joinpath(f"{name}.lua").read_text()
            )
            for name in NAMES
        }

    async def run(self, name: str, now: float, *arguments: Any) -> Any:
        return await self._scripts[name](
            keys=[self._root], args=[repr(float(now)), *arguments]
        )
