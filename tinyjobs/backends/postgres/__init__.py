from ...exceptions import ConfigurationError

try:
    import psycopg  # noqa: F401
    import psycopg_pool  # noqa: F401
except ImportError as error:
    raise ConfigurationError(
        "the postgres backend needs psycopg 3 and psycopg_pool; install them with "
        "pip install 'tinyjobs[postgres]'"
    ) from error

from .backend import PostgresBackend

__all__ = ["PostgresBackend"]
