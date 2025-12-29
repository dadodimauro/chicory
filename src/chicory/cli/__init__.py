from __future__ import annotations

__all__ = []

try:
    from .cli import app

    __all__.extend(["app"])
except ImportError:
    raise RuntimeError(
        "Chicory CLI is not installed. "
        "Please install it using 'pip install chicory[cli]' "
        "or with 'pip install chicory[all]'."
    ) from None
