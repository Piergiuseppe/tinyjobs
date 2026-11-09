from __future__ import annotations

import asyncio

from django.tasks import task

from tinyjobs.contrib.django import task as options


class Refused(Exception):
    pass


@task
def double(value):
    return value * 2


@task(queue_name="emails", priority=10)
async def greet(name):
    await asyncio.sleep(0)
    return f"hello {name}"


@task(takes_context=True)
def introspect(context):
    return {"attempt": context.attempt, "id": context.task_result.id}


@task
def refuse():
    raise Refused("no")


@task
def echo(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}


@task
def unserialisable():
    return object()


twice = double


CALLS = {"flaky": 0}


@options(max_attempts=3, retry_base=0, retry_jitter=False)
def flaky(succeed_on):
    CALLS["flaky"] += 1
    if CALLS["flaky"] < succeed_on:
        raise Refused(f"attempt {CALLS['flaky']}")
    return CALLS["flaky"]


@options(max_attempts=2, retry_base=3600, retry_jitter=False, takes_context=True)
def later(context):
    raise Refused(f"attempt {context.attempt}")


@options(max_attempts=3, retry_on=(KeyError,), retry_base=0)
def picky():
    raise Refused("not a KeyError")


@options(rate="1/h")
def hourly():
    return "ran"


@options(concurrency=2, timeout=5, result_ttl=60)
def capped():
    return "ran"


@task
def plain():
    return "ran"


@options(timeout=0.2)
def slow(seconds):
    import time

    time.sleep(seconds)
    return "late"


@options(timeout=0.2, max_attempts=2, retry_base=0, retry_jitter=False)
def slow_twice(seconds):
    import time

    time.sleep(seconds)
    return "late"


@options(timeout=0.2)
async def slow_async(seconds):
    await asyncio.sleep(seconds)
    return "late"
