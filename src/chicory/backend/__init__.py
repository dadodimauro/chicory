from __future__ import annotations

from .base import Backend

__all__ = ["Backend"]

try:
    from .redis import RedisBackend

    __all__.append("RedisBackend")
except ImportError:
    pass


try:
    from .database import DatabaseBackend

    __all__.append("DatabaseBackend")
except ImportError:
    pass
