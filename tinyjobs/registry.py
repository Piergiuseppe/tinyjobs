from __future__ import annotations

from typing import Callable, Iterator, TYPE_CHECKING

from .exceptions import TaskAlreadyRegisteredError, UnknownTaskError

if TYPE_CHECKING:
    from .task import Task

Resolver = Callable[[str], "Task | None"]


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, "Task"] = {}
        self._resolvers: list[Resolver] = []

    def register(self, task: "Task") -> None:
        existing = self._tasks.get(task.name)
        if existing is not None and existing.function is not task.function:
            raise TaskAlreadyRegisteredError(
                f"task name {task.name!r} is already bound to "
                f"{existing.function.__module__}.{existing.function.__qualname__}; "
                f"pass an explicit name= to one of them"
            )
        self._tasks[task.name] = task

    def add_resolver(self, resolver: Resolver) -> Resolver:
        self._resolvers.append(resolver)
        return resolver

    def get(self, name: str) -> "Task":
        task = self._tasks.get(name) or self._resolve(name)
        if task is None:
            raise UnknownTaskError(
                f"no task registered under {name!r}; import the module that "
                f"defines it, or pass --import to the worker"
            )
        return task

    def names(self) -> list[str]:
        return sorted(self._tasks)

    def _resolve(self, name: str) -> "Task | None":
        for resolver in self._resolvers:
            task = resolver(name)
            if task is not None:
                self.register(task)
                return task
        return None

    def __contains__(self, name: object) -> bool:
        return name in self._tasks or (
            isinstance(name, str) and self._resolve(name) is not None
        )

    def __iter__(self) -> Iterator["Task"]:
        return iter(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)
