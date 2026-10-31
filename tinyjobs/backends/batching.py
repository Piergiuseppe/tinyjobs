from __future__ import annotations

import asyncio
import contextvars
import math
import time
from typing import Any, Awaitable, Callable, Hashable, Sequence

from ..deadlines import GRACE_SECONDS, Deadline, current, under
from ..runtime import spared_until_close

AGAIN = object()
PRESSED_SECONDS = 5.0
_SHARED = object()
_EXPIRED = object()


class Entry:
    __slots__ = ("payload", "future", "sent", "attempts", "deadline")

    def __init__(self, payload: Any, deadline: Deadline | None) -> None:
        self.payload = payload
        self.deadline = deadline
        self.future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.sent = False
        self.attempts = 0

    def settle(self, answer: Any) -> None:
        if self.future.done():
            return
        if isinstance(answer, asyncio.CancelledError):
            self.future.cancel()
        elif isinstance(answer, BaseException):
            self.future.set_exception(answer)
        else:
            self.future.set_result(answer)


class _Group:
    __slots__ = ("entries", "task")

    def __init__(self) -> None:
        self.entries: list[Entry] = []
        self.task: asyncio.Task | None = None


class Batches:
    def __init__(
        self,
        send: Callable[[Callable[[], list[Entry]]], Awaitable[Sequence[Any]]],
        size: int,
    ) -> None:
        self._send = send
        self._size = size
        self._groups: dict[Hashable, _Group] = {}
        self._closed: BaseException | None = None

    @property
    def waiting(self) -> bool:
        return any(group.entries for group in self._groups.values())

    async def submit(self, payload: Any) -> Any:
        entry = Entry(payload, _binding(current.get()))
        self._place(entry, _tier(entry))
        try:
            return await asyncio.shield(entry.future)
        except BaseException:
            if not entry.sent:
                for group in self._groups.values():
                    if entry in group.entries:
                        group.entries.remove(entry)
                        entry.future.cancel()
            entry.future.add_done_callback(_retrieved)
            raise

    def _place(self, entry: Entry, key: Hashable) -> None:
        group = self._groups.get(key)
        if group is None:
            group = self._groups[key] = _Group()
        group.entries.append(entry)
        if group.task is None or group.task.done():
            with spared_until_close():
                context = contextvars.copy_context()
            group.task = asyncio.get_running_loop().create_task(
                self._flush(key, group), context=context
            )

    async def close(self, error: BaseException, patience: float) -> None:
        self._closed = error
        groups = list(self._groups.values())
        for group in groups:
            unsent, group.entries[:] = list(group.entries), []
            for entry in unsent:
                entry.settle(error)
        running = {group.task for group in groups if group.task is not None}
        if running:
            _, late = await asyncio.wait(running, timeout=patience)
            for task in late:
                task.cancel()
            if late:
                await asyncio.wait(late)

    async def _flush(self, key: Hashable, group: _Group) -> None:
        try:
            while group.entries and self._closed is None:
                await self._sent(key, group)
        finally:
            if self._groups.get(key) is group and not group.entries:
                del self._groups[key]

    async def _sent(self, key: Hashable, group: _Group) -> None:
        taken: list[Entry] = []
        deadline = Deadline(_earliest(group.entries))

        def take() -> list[Entry]:
            batch: list[Entry] = []
            while group.entries and len(batch) < self._size:
                entry = group.entries.pop(0)
                tier = _tier(entry)
                if tier != key:
                    self._place(entry, tier)
                else:
                    batch.append(entry)
            for entry in batch:
                entry.sent = True
                entry.attempts += 1
            taken.extend(batch)
            deadline.at = _earliest(batch)
            timer.reschedule(_expiry(deadline))
            return batch

        try:
            with under(deadline):
                async with asyncio.timeout_at(_expiry(deadline)) as timer:
                    answers = await self._send(take)
        except BaseException as error:
            if not taken:
                taken, group.entries[:] = list(group.entries), []
            failure = self._closed if isinstance(error, asyncio.CancelledError) else None
            for entry in taken:
                entry.settle(error if failure is None else failure)
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(error, asyncio.CancelledError) and self._closed is None:
                raise
            return
        again = []
        for entry, answer in zip(taken, answers):
            if answer is AGAIN and self._closed is None:
                entry.sent = False
                again.append(entry)
            else:
                entry.settle(self._closed if answer is AGAIN else answer)
        group.entries[:0] = again


def _binding(deadline: Deadline | None) -> Deadline | None:
    binding, root, at = None, None, math.inf
    link = deadline
    while link is not None:
        if link.at < at:
            binding, at = link, link.at
        root, link = link, link.outer
    return binding if binding is not None else root


def _tier(entry: Entry) -> Hashable:
    if entry.deadline is None:
        return _SHARED
    left = entry.deadline.left()
    if left >= PRESSED_SECONDS:
        return _SHARED
    if left <= 0:
        return _EXPIRED
    return math.floor(math.log2(left))


def _earliest(entries: Sequence[Entry]) -> float:
    now = time.monotonic()
    return min(
        (now + entry.deadline.left() for entry in entries if entry.deadline is not None),
        default=math.inf,
    )


def _expiry(deadline: Deadline) -> float | None:
    left = deadline.left()
    if left == math.inf:
        return None
    return asyncio.get_running_loop().time() + max(0.0, left) + GRACE_SECONDS


def _retrieved(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()
