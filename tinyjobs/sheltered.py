from __future__ import annotations

import asyncio
import concurrent.futures
from contextlib import suppress
from functools import partial
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


def landing(running: concurrent.futures.Future) -> asyncio.Future:
    loop = asyncio.get_running_loop()
    waiter = loop.create_future()
    running.add_done_callback(partial(_landed, loop, waiter))
    return waiter


def _landed(
    loop: asyncio.AbstractEventLoop, waiter: asyncio.Future, running: concurrent.futures.Future
) -> None:
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(_copied, waiter, running)


def _copied(waiter: asyncio.Future, running: concurrent.futures.Future) -> None:
    if waiter.done():
        return
    if running.cancelled():
        waiter.cancel()
        return
    error = running.exception()
    if error is None:
        waiter.set_result(running.result())
    else:
        waiter.set_exception(error)
