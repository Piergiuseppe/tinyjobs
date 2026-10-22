from __future__ import annotations

import asyncio

from django.tasks import task


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
