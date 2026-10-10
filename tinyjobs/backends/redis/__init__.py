from ...exceptions import ConfigurationError

try:
    import redis.asyncio  # noqa: F401
except ImportError as error:
    raise ConfigurationError(
        "the redis backend needs the redis package; install it with "
        "pip install 'tinyjobs[redis]'"
    ) from error

from .backend import RedisBackend

__all__ = ["RedisBackend"]
