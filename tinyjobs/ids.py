from __future__ import annotations

import os
import secrets
import socket
import threading
import time
import uuid

COUNTER_BITS = 12
COUNTER_MAX = (1 << COUNTER_BITS) - 1
COUNTER_SEED_MAX = COUNTER_MAX >> 1
VERSION_NIBBLE = 0x70
VARIANT_BITS = 0x80


class JobIdGenerator:
    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._last_milliseconds = -1
        self._counter = 0

    def __call__(self) -> str:
        milliseconds, counter = self._next_position()
        return str(uuid.UUID(bytes=self._compose(milliseconds, counter)))

    def _next_position(self) -> tuple[int, int]:
        with self._guard:
            now = int(time.time() * 1000)
            if now > self._last_milliseconds:
                self._last_milliseconds = now
                self._counter = secrets.randbits(COUNTER_BITS) & COUNTER_SEED_MAX
            elif self._counter >= COUNTER_MAX:
                self._last_milliseconds += 1
                self._counter = 0
            else:
                self._counter += 1
            return self._last_milliseconds, self._counter

    def _compose(self, milliseconds: int, counter: int) -> bytes:
        raw = bytearray(milliseconds.to_bytes(6, "big") + os.urandom(10))
        raw[6] = VERSION_NIBBLE | (counter >> 8)
        raw[7] = counter & 0xFF
        raw[8] = (raw[8] & 0x3F) | VARIANT_BITS
        return bytes(raw)


new_job_id = JobIdGenerator()


def new_worker_id() -> str:
    return f"{socket.gethostname()}/{os.getpid()}/{uuid.uuid4().hex[:8]}"
