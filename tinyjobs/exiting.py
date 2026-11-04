import _thread
import os
import signal
import sys
from contextlib import contextmanager, suppress
from typing import Any, Iterator

STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)


@contextmanager
def stop_signals_blocked() -> Iterator[None]:
    before = signal.pthread_sigmask(signal.SIG_BLOCK, STOP_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, before)


def ignore_stop_signals() -> None:
    for number in STOP_SIGNALS:
        signal.signal(number, _ignore)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, STOP_SIGNALS)


def _ignore(number: int, frame: Any) -> None:
    return None


def own_group() -> None:
    with suppress(OSError):
        os.setpgid(0, 0)


def kill_group(pid: int) -> None:
    with suppress(OSError):
        os.killpg(pid, signal.SIGKILL)


def kill_own_group() -> None:
    if os.getpgrp() == os.getpid():
        with suppress(OSError):
            os.killpg(os.getpgrp(), signal.SIGKILL)


def signal_name(number: int) -> str:
    try:
        return f"{signal.Signals(number).name} ({number})"
    except ValueError:
        return f"signal {number}"

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
