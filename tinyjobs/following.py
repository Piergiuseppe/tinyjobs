from __future__ import annotations

from typing import Sequence

from .exceptions import JobNotFoundError
from .models import Job

AFTER_MESSAGE = (
    "after is the job this one runs after, a Job or its id, not {value!r}"
)
PARENT_MISSING_MESSAGE = (
    "job {after} is not in the store, so nothing can run after it; it may have been purged"
)
PARENT_SENT_TOGETHER_MESSAGE = (
    "job {after} is sent in the same call as the job that runs after it; a job can only run "
    "after one already in the store"
)
PARENT_ENDED_MESSAGE = "the job this one was to run after did not succeed"
PARENT_GONE_MESSAGE = (
    "the job this one was to run after is no longer in the store, so how it ended is not known"
)


def refuse_parents_among(jobs: Sequence[Job]) -> None:
    sent = {job.id for job in jobs}
    for job in jobs:
        if job.after in sent:
            raise JobNotFoundError(PARENT_SENT_TOGETHER_MESSAGE.format(after=job.after))


def parent_id(after: object) -> str | None:
    if after is None:
        return None
    if isinstance(after, Job):
        return after.id
    if isinstance(after, str) and after:
        return after
    raise TypeError(AFTER_MESSAGE.format(value=after))
