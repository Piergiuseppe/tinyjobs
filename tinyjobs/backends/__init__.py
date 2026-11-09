from .base import BaseBackend, Backend, Capabilities
from ..limits import Limit
from .loader import available_schemes, load_backend, register_builtin
from .sqlite import SQLiteBackend


def _redis(url: str, **options):
    from .redis import RedisBackend

    return RedisBackend.from_url(url, **options)


def _postgres(url: str, **options):
    from .postgres import PostgresBackend

    return PostgresBackend.from_url(url, **options)


def _postgres_options(url: str, **options):
    try:
        from .postgres import PostgresBackend
    except ImportError:
        return
    PostgresBackend.check_options(url, **options)


register_builtin(
    "sqlite",
    SQLiteBackend.from_url,
    SQLiteBackend.check_options,
    joins_cold=SQLiteBackend.joins_cold,
)
register_builtin("memory", SQLiteBackend.from_url, SQLiteBackend.check_options)
register_builtin("redis", _redis, loop_bound=True)
register_builtin("rediss", _redis, loop_bound=True)
register_builtin("postgres", _postgres, _postgres_options, loop_bound=True)
register_builtin("postgresql", _postgres, _postgres_options, loop_bound=True)

__all__ = [
    "Backend",
    "BaseBackend",
    "Capabilities",
    "Limit",
    "SQLiteBackend",
    "available_schemes",
    "load_backend",
    "register_builtin",
]
