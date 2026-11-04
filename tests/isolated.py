from __future__ import annotations

import os
import signal
import time
from typing import Any

from tinyjobs import Abandon, Retry, TinyJobs

from tests import isolated_lib

app = TinyJobs("memory://")


class Picky(Exception):
    def __init__(self, code: int, detail: str) -> None:
        super().__init__(f"{code}: {detail}")


@app.task(isolated=True, store_result=True)
def pid() -> int:
    return os.getpid()


@app.task(isolated=True, store_result=True)
def echo(value: Any) -> Any:
    return value


@app.task(isolated=True, store_result=True)
def sleep_then_pid(seconds: float) -> int:
    time.sleep(seconds)
    return os.getpid()


@app.task(isolated=True)
def exit_with(code: int) -> None:
    os._exit(code)


@app.task(isolated=True)
def segfault() -> None:
    os.kill(os.getpid(), signal.SIGSEGV)


@app.task(isolated=True, timeout=0.3)
def hang() -> None:
    time.sleep(30)


@app.task(isolated=True)
def retry_now() -> None:
    raise Retry(in_=0)


@app.task(isolated=True)
def abandon() -> None:
    raise Abandon("not worth it")


@app.task(isolated=True, retry_on=(KeyError,))
def key_error() -> None:
    raise KeyError("missing")


@app.task(isolated=True)
def picky() -> None:
    raise Picky(7, "bad")


@app.task(isolated=True, store_result=True)
def unencodable_result() -> object:
    return object()


@app.task(isolated=True)
def unencodable_result_dropped() -> object:
    return object()


@app.task(isolated=True, retry_on=(Picky,))
def picky_retried() -> None:
    raise Picky(7, "bad")


@app.task(isolated=True, retry_on=(ValueError,))
def unicode_error() -> None:
    raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad byte")


@app.task(isolated=True, store_result=True)
def grandchild_pid() -> int:
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(1) as pool:
        return pool.submit(os.getpid).result()


@app.task(isolated=True, store_result=True)
def terminate_subprocess() -> int | None:
    import subprocess
    import sys

    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    time.sleep(0.3)
    child.terminate()
    try:
        return child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
        return None


@app.task(isolated=True, timeout=0.5)
def hang_with_subprocess(path: str) -> None:
    import subprocess
    import sys

    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    with open(path, "w") as handle:
        handle.write(str(child.pid))
    time.sleep(30)


def plain_pid() -> int:
    return os.getpid()


def other_marker() -> str:
    return "other"


other = TinyJobs("memory://")
other_pid = other.task(name="registered_pid", isolated=True, store_result=True)(other_marker)
registered_pid = app.task(isolated=True, store_result=True, name="registered_pid")(plain_pid)
via_helper = isolated_lib.register(app, store_result=True, name="via_helper")(plain_pid)
aliased_pid = registered_pid
imported_pid = app.task(isolated=True, store_result=True, name="imported_pid")(
    isolated_lib.imported_pid
)


class Holder:
    def method(self) -> None:
        return None

    @classmethod
    def classy(cls) -> None:
        prepare()

    @staticmethod
    def static() -> None:
        prepare()


holder = Holder()

prepared = False


def prepare() -> None:
    global prepared
    prepared = True


def prepare_slowly() -> None:
    time.sleep(2.0)
    prepare()


def prepare_forever() -> None:
    time.sleep(60)


def die() -> None:
    os._exit(5)


def fail_setup() -> None:
    raise RuntimeError("no settings")


@app.task(isolated=True, store_result=True)
def was_prepared() -> bool:
    return prepared


TASKS = [
    hang_with_subprocess,
    registered_pid,
    via_helper,
    imported_pid,
    terminate_subprocess,
    picky_retried,
    unicode_error,
    grandchild_pid,
    was_prepared,
    pid,
    echo,
    sleep_then_pid,
    exit_with,
    segfault,
    hang,
    retry_now,
    abandon,
    key_error,
    picky,
    unencodable_result,
    unencodable_result_dropped,
]
