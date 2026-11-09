from __future__ import annotations

from tinyjobs.contrib.django import task


@task(rate="1/h")
def spaced():
    return "ran"
