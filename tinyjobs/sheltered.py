from __future__ import annotations

import asyncio
from typing import Awaitable, TypeVar

T = TypeVar("T")


async def sheltered(awaitable: Awaitable[T]) -> T:
    inner = asyncio.ensure_future(awaitable)
    inner.add_done_callback(_retrieved)
    await asyncio.wait({inner})
    return inner.result()


def _retrieved(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()
