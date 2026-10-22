from __future__ import annotations

import asyncio

from django.tasks import task


@task
def stray():
    return "should not run"


@task
async def halted():
    raise asyncio.CancelledError
