from __future__ import annotations

import inspect
from typing import Any, Callable, TYPE_CHECKING

from .models import Job, JobPayload
from .options import EnqueueOptions
from .retry import RetryPolicy

if TYPE_CHECKING:
    from .app import TinyJobs


class Task:
    def __init__(
        self,
        app: "TinyJobs",
        function: Callable[..., Any],
        *,
        name: str,
        options: EnqueueOptions,
        retry_policy: RetryPolicy,
        timeout: float | None = None,
        store_result: bool = False,
    ) -> None:
        self._app = app
        self._function = function
        self._name = name
        self._options = options
        self._retry_policy = retry_policy
        self._timeout = timeout
        self._store_result = store_result
        self._signature = inspect.signature(function)
        self._is_coroutine = inspect.iscoroutinefunction(function)

    @property
    def app(self) -> "TinyJobs":
        return self._app

    @property
    def name(self) -> str:
        return self._name

    @property
    def function(self) -> Callable[..., Any]:
        return self._function

    @property
    def options(self) -> EnqueueOptions:
        return self._options

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retry_policy

    @property
    def timeout(self) -> float | None:
        return self._timeout

    @property
    def store_result(self) -> bool:
        return self._store_result

    @property
    def is_coroutine(self) -> bool:
        return self._is_coroutine

    def bind(self, args: tuple, kwargs: dict) -> JobPayload:
        bound = self._signature.bind(*args, **kwargs)
        return JobPayload(args=tuple(bound.args), kwargs=dict(bound.kwargs))

    def with_options(self, **overrides: Any) -> "BoundTask":
        return BoundTask(self, self._options.merged_with(**overrides))

    async def aenqueue_with(
        self, options: EnqueueOptions, args: tuple, kwargs: dict
    ) -> Job:
        payload = self.bind(args, kwargs)
        await self._app.ready()
        return self._app.bind(
            await self._app.dispatch.dispatch_by_name(self._name, payload, options)
        )

    async def aenqueue(self, *args: Any, **kwargs: Any) -> Job:
        return await self.aenqueue_with(self._options, args, kwargs)

    def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        return self._app.run_sync(
            self.aenqueue_with(self._options, args, kwargs), name="enqueue"
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._function(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<Task {self._name}>"


class BoundTask:
    def __init__(self, task: Task, options: EnqueueOptions) -> None:
        self._task = task
        self._options = options

    def with_options(self, **overrides: Any) -> "BoundTask":
        return BoundTask(self._task, self._options.merged_with(**overrides))

    async def aenqueue(self, *args: Any, **kwargs: Any) -> Job:
        return await self._task.aenqueue_with(self._options, args, kwargs)

    def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        return self._task.app.run_sync(
            self._task.aenqueue_with(self._options, args, kwargs), name="enqueue"
        )
