from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def validate_module_candidate(module_name: str, fn: Callable[..., Any]) -> bool:
    """
    Validate if a function belongs to the specified module.

    Args:
        module_name: The fully qualified module name to check
        fn: The function object to validate

    Returns:
        True if the function is found in the module and matches, False otherwise
    """
    try:
        module = importlib.import_module(module_name)
    except (ImportError, ModuleNotFoundError, ValueError):
        # Module doesn't exist or invalid name
        return False
    except Exception:
        # Catch other import errors (circular imports, syntax errors, etc.)
        return False

    # Check if function has __name__ attribute
    if not hasattr(fn, "__name__"):
        return False

    attr = getattr(module, str(fn.__name__), None)
    return attr is fn


def resolve_module_name(fn: Callable[..., Any]) -> str | None:
    """
    Resolve the importable module name for a function defined in __main__.

    This is useful for task serialization when tasks are defined in __main__,
    as we need to convert them to importable module paths for workers.

    Args:
        fn: The function to resolve the module name for

    Returns:
        The importable module name, or None if it cannot be resolved

    Examples:
        >>> def my_task():
        ...     pass
        >>> resolve_module_name(my_task)
        'myapp.tasks'
    """
    # Only resolve __main__
    if not hasattr(fn, "__module__") or fn.__module__ != "__main__":
        return fn.__module__ if hasattr(fn, "__module__") else None

    code = getattr(fn, "__code__", None)
    if not code or not hasattr(code, "co_filename") or not code.co_filename:
        return None

    try:
        filename = Path(code.co_filename).resolve()
    except (OSError, RuntimeError):
        # Handle cases where path resolution fails
        return None

    # Ignore interactive sessions
    if filename.name in ("<stdin>", "<string>", "<console>"):
        return None

    for base_path in sys.path:
        try:
            base = Path(base_path).resolve()
        except (OSError, RuntimeError, ValueError):
            # Skip invalid paths in sys.path
            continue

        # File must be under a sys.path entry
        try:
            # For Python 3.9+
            if not filename.is_relative_to(base):
                continue
        except AttributeError:
            # Fallback for Python 3.8
            try:
                filename.relative_to(base)
            except ValueError:
                continue

        try:
            rel = filename.relative_to(base)
        except ValueError:
            continue

        # Drop suffix (.py)
        if rel.suffix != ".py":
            continue

        parts = list(rel.with_suffix("").parts)

        # Handle __init__.py
        if parts and parts[-1] == "__init__":
            parts.pop()

        # Skip if no parts left (e.g., just __init__.py in root)
        if not parts:
            continue

        module_name = ".".join(parts)

        # Validate the resolved module name
        if validate_module_candidate(module_name, fn):
            return module_name

    return None
