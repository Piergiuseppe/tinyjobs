from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Sequence

from .exceptions import AppClosedError, ConfigurationError
from .limits import Limit
from .polling import AdaptiveInterval

if TYPE_CHECKING:
    from .backends.base import Backend
    from .registry import TaskRegistry

logger = logging.getLogger("tinyjobs.worker")

RATE_POLL_FLOOR = 0.05

LIMITS_UNSUPPORTED_MESSAGE = (
    "{names} declare concurrency or rate, which this store cannot enforce, so running them here "
    "would ignore the limit; drop the limit, or use a store whose capabilities declare limits"
)


def limits_for(backend: "Backend", registry: "TaskRegistry") -> dict[str, dict[str, Limit]]:
    if not backend.capabilities.limits:
        return {}
    limits = registry.limits()
    return {"limits": limits} if limits else {}


def assert_limits_supported(backend: "Backend", registry: "TaskRegistry") -> None:
    if backend.capabilities.limits:
        return
    limited = sorted(registry.limits())
    if limited:
        raise ConfigurationError(LIMITS_UNSUPPORTED_MESSAGE.format(names=", ".join(limited)))


async def release_held_elsewhere(
    backend: "Backend", registry: "TaskRegistry", queues: Sequence[str] | None = None
) -> None:
    if not backend.capabilities.limits:
        return
    limits = registry.limits()
    unlimited = [name for name in registry.names() if name not in limits]
    if not unlimited:
        return
    try:
        released = await backend.release_held(tasks=unlimited, queues=queues)
    except AppClosedError:
        raise
    except Exception as error:
        logger.warning(
            "held jobs of the tasks without a limit here stay held for now (%s); the next pass tries again",
            str(error) or type(error).__name__,
        )
        return
    if released:
        logger.info("returned %d held job(s) of tasks without a limit here to the queue", released)


def polling_under(
    backend: "Backend", registry: "TaskRegistry", minimum: float, maximum: float
) -> AdaptiveInterval:
    assert_limits_supported(backend, registry)
    intervals = [
        limit.interval for limit in registry.limits().values() if limit.interval is not None
    ]
    if intervals:
        maximum = max(min(maximum, max(min(intervals), RATE_POLL_FLOOR)), minimum)
    return AdaptiveInterval(minimum=minimum, maximum=maximum)


def flattened(limits: Any) -> list[tuple[str, int | None, float | None]]:
    return [
        (name, limit.concurrency, limit.interval) for name, limit in sorted(limits.items())
    ]
