from .base import BaseBackend, Backend, Capabilities
from .loader import available_schemes, load_backend, register_builtin
from .sqlite import SQLiteBackend

register_builtin("sqlite", SQLiteBackend.from_url)

__all__ = [
    "Backend",
    "BaseBackend",
    "Capabilities",
    "SQLiteBackend",
    "available_schemes",
    "load_backend",
    "register_builtin",
]
