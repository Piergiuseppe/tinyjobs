from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

from ..models import Job, JobPayload

if TYPE_CHECKING:
    from ..task import Task


class Executor(ABC):
    @abstractmethod
    async def execute(
        self, task: "Task", payload: JobPayload, *, job: Job | None = None
    ) -> Any: ...

    def shutdown(self) -> None:
        return None
