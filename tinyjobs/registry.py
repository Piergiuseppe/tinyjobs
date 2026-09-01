from __future__ import annotations

from typing import Iterator, TYPE_CHECKING

from .exceptions import TaskAlreadyRegisteredError, UnknownTaskError

if TYPE_CHECKING:
    from .task import Task


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, "Task"] = {}

    def register(self, task: "Task") -> None:
        existing = self._tasks.get(task.name)
        if existing is not None and existing.function is not task.function:
            raise TaskAlreadyRegisteredError(
                f"task name {task.name!r} is already bound to "
                f"{existing.function.__module__}.{existing.function.__qualname__}; "
                f"pass an explicit name= to one of them"
            )
        self._tasks[task.name] = task

    def get(self, name: str) -> "Task":
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTaskError(
                f"no task registered under {name!r}; import the module that "
                f"defines it, or pass --import to the worker"
            ) from None

    def names(self) -> list[str]:
        return sorted(self._tasks)

    def __contains__(self, name: object) -> bool:
        return name in self._tasks

    def __iter__(self) -> Iterator["Task"]:
        return iter(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)
