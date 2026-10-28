from .base import BaseBackend, Backend, Capabilities
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


register_builtin("sqlite", SQLiteBackend.from_url)
register_builtin("memory", SQLiteBackend.from_url)
register_builtin("redis", _redis)
register_builtin("rediss", _redis)
register_builtin("postgres", _postgres, _postgres_options)
register_builtin("postgresql", _postgres, _postgres_options)

__all__ = [
    "Backend",
    "BaseBackend",
    "Capabilities",
    "SQLiteBackend",
    "available_schemes",
    "load_backend",
    "register_builtin",
]
