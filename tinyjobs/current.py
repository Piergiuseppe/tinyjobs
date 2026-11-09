from __future__ import annotations

from contextvars import ContextVar

from .models import Job

current_job: ContextVar[Job] = ContextVar("tinyjobs_current_job")
