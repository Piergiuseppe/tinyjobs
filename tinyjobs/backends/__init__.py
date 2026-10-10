from .base import BaseBackend, Backend, Capabilities
from .loader import available_schemes, load_backend, register_builtin
from .sqlite import SQLiteBackend


def _redis(url: str, **options):
    from .redis import RedisBackend

    return RedisBackend.from_url(url, **options)


register_builtin("sqlite", SQLiteBackend.from_url)
register_builtin("memory", SQLiteBackend.from_url)
register_builtin("redis", _redis)
register_builtin("rediss", _redis)

__all__ = [
    "Backend",
    "BaseBackend",
    "Capabilities",
    "SQLiteBackend",
    "available_schemes",
    "load_backend",
    "register_builtin",
]
