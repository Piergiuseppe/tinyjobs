import _thread
import os
import signal
import sys
from typing import Any

STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)

EXIT_FLUSH_SECONDS = 0.5

_exiting: list[int] = []


def exit_cleanly(number: int, frame: Any) -> None:
    if not _exiting:
        _exiting.append(number)
        _flush_within(EXIT_FLUSH_SECONDS)
    os._exit(0)


def _flush_within(seconds: float) -> None:
    flushed = _thread.allocate_lock()
    flushed.acquire()
    try:
        _thread.start_new_thread(_flush_then_release, (flushed,))
    except RuntimeError:
        _flush_streams()
        return
    flushed.acquire(timeout=seconds)


def _flush_then_release(flushed: Any) -> None:
    try:
        _flush_streams()
    finally:
        flushed.release()


def _flush_streams() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
