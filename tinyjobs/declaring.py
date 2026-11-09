from __future__ import annotations

from typing import Any, Callable, TYPE_CHECKING

from .duration import is_finite_seconds
from .exceptions import ConfigurationError, described
from .executors.process import MAIN_MODULE, main_spawn_problem
from .limits import declare_limit
from .options import EnqueueOptions
from .retry import RetryPolicy
from .schedules import Schedule
from .task import Locator, Task

if TYPE_CHECKING:
    from .app import TinyJobs

BACKOFFS = ("exponential", "fixed")

ISOLATED_ASYNC_MESSAGE = (
    "isolated=True runs a task in a job process of its own, which takes a def function; "
    "{name} is async def"
)
ISOLATED_MAIN_MESSAGE = (
    "isolated=True runs {name} in a job process that imports the module it was registered in, "
    "and a job process cannot import __main__ when the worker runs from {how}; register the task "
    "in a module of its own"
)
RESULT_TTL_MESSAGE = (
    "{name}: result_ttl only applies to a stored result; pass store_result=True as well"
)
RESULT_TTL_POSITIVE_MESSAGE = (
    "{name}: result_ttl must be a finite number of seconds above zero, got {value}"
)
TIMEOUT_MESSAGE = "{name}: timeout must be a finite number of seconds above zero, got {value}"
MAX_ATTEMPTS_MESSAGE = "{name}: max_attempts must be a whole number of at least 1, got {value}"
RETRY_ON_MESSAGE = (
    "{name}: retry_on must be an exception class or a tuple of them, got {value}"
)
BACKOFF_MESSAGE = (
    '{name}: retry_backoff must be "exponential", "fixed" or a function of the attempt, '
    "got {value}"
)
RETRY_SECONDS_MESSAGE = (
    "{name}: {option} must be a finite number of seconds, zero or more, got {value}"
)
RETRY_JITTER_MESSAGE = "{name}: retry_jitter must be True or False, got {value}"


def declared_task(
    app: "TinyJobs",
    target: Callable[..., Any],
    *,
    name: str,
    registered_in: str | None = None,
    queue: str | None = None,
    priority: int | None = None,
    codec: str | None = None,
    timeout: float | None = None,
    store_result: bool = False,
    result_ttl: float | None = None,
    isolated: bool = False,
    concurrency: int | None = None,
    rate: str | None = None,
    max_attempts: int = 1,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    retry_backoff: Any = "exponential",
    retry_base: float = 1.0,
    retry_max: float = 600.0,
    retry_jitter: bool = True,
    schedule: Schedule | None = None,
    locator: Locator | None = None,
) -> Task:
    assert_timeout(name, timeout)
    assert_result_ttl(name, store_result, result_ttl)
    task = Task(
        app,
        target,
        name=name,
        options=EnqueueOptions(
            queue=queue, priority=priority, max_attempts=max_attempts, codec=codec
        ),
        retry_policy=declare_retries(
            name,
            max_attempts=max_attempts,
            retry_on=retry_on,
            retry_backoff=retry_backoff,
            retry_base=retry_base,
            retry_max=retry_max,
            retry_jitter=retry_jitter,
        ),
        timeout=timeout,
        store_result=store_result,
        result_ttl=result_ttl,
        schedule=schedule,
        isolated=isolated,
        registered_in=registered_in,
        limit=declare_limit(name, concurrency, rate),
        locator=locator,
    )
    if isolated:
        assert_isolatable(task)
    return task


def declare_retries(
    name: str,
    *,
    max_attempts: int,
    retry_on: tuple[type[BaseException], ...],
    retry_backoff: Any,
    retry_base: float,
    retry_max: float,
    retry_jitter: bool,
) -> RetryPolicy:
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ConfigurationError(
            MAX_ATTEMPTS_MESSAGE.format(name=name, value=described(max_attempts))
        )
    kinds = tuple(retry_on) if isinstance(retry_on, (tuple, list)) else (retry_on,)
    if not all(isinstance(kind, type) and issubclass(kind, BaseException) for kind in kinds):
        raise ConfigurationError(RETRY_ON_MESSAGE.format(name=name, value=described(retry_on)))
    if not callable(retry_backoff) and retry_backoff not in BACKOFFS:
        raise ConfigurationError(
            BACKOFF_MESSAGE.format(name=name, value=described(retry_backoff))
        )
    for option, value in (("retry_base", retry_base), ("retry_max", retry_max)):
        if not is_finite_seconds(value) or value < 0:
            raise ConfigurationError(
                RETRY_SECONDS_MESSAGE.format(name=name, option=option, value=described(value))
            )
    if not isinstance(retry_jitter, bool):
        raise ConfigurationError(
            RETRY_JITTER_MESSAGE.format(name=name, value=described(retry_jitter))
        )
    return RetryPolicy(
        max_attempts=max_attempts,
        retry_on=kinds,
        backoff=retry_backoff,
        base=retry_base,
        maximum=retry_max,
        jitter=retry_jitter,
    )


def assert_timeout(name: str, timeout: float | None) -> None:
    if timeout is None:
        return
    if not is_finite_seconds(timeout) or timeout <= 0:
        raise ConfigurationError(TIMEOUT_MESSAGE.format(name=name, value=described(timeout)))


def assert_result_ttl(name: str, store_result: bool, ttl: float | None) -> None:
    if ttl is None:
        return
    if not store_result:
        raise ConfigurationError(RESULT_TTL_MESSAGE.format(name=name))
    if not is_finite_seconds(ttl) or ttl <= 0:
        raise ConfigurationError(
            RESULT_TTL_POSITIVE_MESSAGE.format(name=name, value=described(ttl))
        )


def assert_isolatable(task: Task) -> None:
    if task.is_coroutine:
        raise ConfigurationError(ISOLATED_ASYNC_MESSAGE.format(name=task.name))
    if task.registered_in == MAIN_MODULE:
        how = main_spawn_problem()
        if how is not None:
            raise ConfigurationError(ISOLATED_MAIN_MESSAGE.format(name=task.name, how=how))
