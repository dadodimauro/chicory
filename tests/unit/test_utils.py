from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from chicory.utils import resolve_module_name, validate_module_candidate

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


class TestValidateModuleCandidate:
    def test_validates_existing_function(self) -> None:
        """Test validation of a function that exists in the module."""
        # Use an actual function from the importlib module
        import importlib

        result = validate_module_candidate("importlib", importlib.import_module)
        assert result is True

    def test_rejects_non_existent_module(self) -> None:
        """Test that non-existent modules return False."""

        def dummy_fn() -> None:
            pass

        result = validate_module_candidate("non.existent.module", dummy_fn)
        assert result is False

    def test_rejects_function_not_in_module(self) -> None:
        """Test that functions not in the specified module return False."""

        def my_function() -> None:
            pass

        # importlib exists but doesn't have 'my_function'
        result = validate_module_candidate("importlib", my_function)
        assert result is False

    def test_rejects_function_with_same_name_but_different_object(self) -> None:
        """Test that same-named but different function objects return False."""

        # Create a different function with the same name
        def import_module() -> None:  # Same name as importlib.import_module
            pass

        result = validate_module_candidate("importlib", import_module)
        assert result is False

    def test_handles_invalid_module_name(self) -> None:
        """Test handling of invalid module names."""

        def dummy_fn() -> None:
            pass

        # Test with various invalid module names
        assert validate_module_candidate("", dummy_fn) is False
        assert validate_module_candidate("invalid..module", dummy_fn) is False

    def test_handles_function_without_name_attribute(self) -> None:
        """Test handling of objects without __name__ attribute."""
        # Create an object without __name__
        callable_without_name = lambda: None  # noqa: E731

        result = validate_module_candidate("importlib", callable_without_name)
        assert result is False

    def test_handles_circular_import_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling of circular import errors."""

        def dummy_fn() -> None:
            pass

        def mock_import_module(name: str) -> Any:
            raise ImportError("Circular import detected")

        monkeypatch.setattr("importlib.import_module", mock_import_module)

        result = validate_module_candidate("some.module", dummy_fn)
        assert result is False

    def test_handles_syntax_error_in_module(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling of syntax errors when importing module."""

        def dummy_fn() -> None:
            pass

        def mock_import_module(name: str) -> Any:
            raise SyntaxError("Invalid syntax in module")

        monkeypatch.setattr("importlib.import_module", mock_import_module)

        result = validate_module_candidate("broken.module", dummy_fn)
        assert result is False


class TestResolveModuleName:
    def test_returns_module_for_non_main_function(self) -> None:
        """Test that functions not in __main__ return their module name."""
        import importlib

        result = resolve_module_name(importlib.import_module)
        assert result == "importlib"

    def test_returns_none_for_stdin(self) -> None:
        """Test that functions from stdin return None."""
        # Create a mock function with __main__ module and <stdin> filename
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        mock_code.co_filename = "<stdin>"
        mock_fn.__code__ = mock_code

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_returns_none_for_string(self) -> None:
        """Test that functions from <string> return None."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        mock_code.co_filename = "<string>"
        mock_fn.__code__ = mock_code

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_returns_none_for_console(self) -> None:
        """Test that functions from <console> return None."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        mock_code.co_filename = "<console>"
        mock_fn.__code__ = mock_code

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_returns_none_for_function_without_code(self) -> None:
        """Test that functions without __code__ return None."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"
        del mock_fn.__code__

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_resolves_module_from_sys_path(self, tmp_path: Path) -> None:
        """Test resolving a function to its module name using sys.path."""
        # Create a temporary module file
        module_dir = tmp_path / "mypackage"
        module_dir.mkdir()
        module_file = module_dir / "tasks.py"
        module_file.write_text("def my_task(): pass\n")

        # Add to sys.path
        original_sys_path = sys.path.copy()
        sys.path.insert(0, str(tmp_path))

        try:
            # Import the module
            import importlib

            mypackage_tasks = importlib.import_module("mypackage.tasks")

            # Test resolution
            result = resolve_module_name(mypackage_tasks.my_task)
            assert result == "mypackage.tasks"
        finally:
            # Cleanup
            sys.path[:] = original_sys_path
            if "mypackage.tasks" in sys.modules:
                del sys.modules["mypackage.tasks"]
            if "mypackage" in sys.modules:
                del sys.modules["mypackage"]

    def test_handles_non_py_files(self) -> None:
        """Test that non-.py files return None."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        mock_code.co_filename = "/path/to/file.txt"
        mock_fn.__code__ = mock_code

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_handles_init_py_files(self, tmp_path: Path) -> None:
        """Test handling of __init__.py files."""
        # Create a package with __init__.py
        package_dir = tmp_path / "mypackage"
        package_dir.mkdir()
        init_file = package_dir / "__init__.py"
        init_file.write_text("def package_task(): pass\n")

        original_sys_path = sys.path.copy()
        sys.path.insert(0, str(tmp_path))

        try:
            import importlib

            mypackage = importlib.import_module("mypackage")

            result = resolve_module_name(mypackage.package_task)
            assert result == "mypackage"
        finally:
            sys.path[:] = original_sys_path
            if "mypackage" in sys.modules:
                del sys.modules["mypackage"]

    def test_handles_path_resolution_errors(self) -> None:
        """Test handling of path resolution errors."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        # Use an invalid path that will cause resolution to fail
        mock_code.co_filename = "/nonexistent/path/that/does/not/exist.py"
        mock_fn.__code__ = mock_code

        result = resolve_module_name(mock_fn)
        assert result is None

    def test_validates_resolved_module(self, tmp_path: Path) -> None:
        """Test that resolved module is validated before returning."""
        # Create a module file
        module_file = tmp_path / "mymodule.py"
        module_file.write_text("def original_task(): pass\n")

        original_sys_path = sys.path.copy()
        sys.path.insert(0, str(tmp_path))

        try:
            # Create a function with same name but different object
            mock_fn = MagicMock()
            mock_fn.__module__ = "__main__"
            mock_fn.__name__ = "different_task"

            mock_code = MagicMock()
            mock_code.co_filename = str(module_file)
            mock_fn.__code__ = mock_code

            # Should return None because validation fails
            result = resolve_module_name(mock_fn)
            assert result is None
        finally:
            sys.path[:] = original_sys_path
            if "mymodule" in sys.modules:
                del sys.modules["mymodule"]

    def test_nested_package_resolution(self, tmp_path: Path) -> None:
        """Test resolving functions in nested packages."""
        # Create nested package structure
        pkg_dir = tmp_path / "company"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").touch()

        subpkg_dir = pkg_dir / "project"
        subpkg_dir.mkdir()
        (subpkg_dir / "__init__.py").touch()

        module_file = subpkg_dir / "tasks.py"
        module_file.write_text("def nested_task(): pass\n")

        original_sys_path = sys.path.copy()
        sys.path.insert(0, str(tmp_path))

        try:
            import importlib

            nested_module = importlib.import_module("company.project.tasks")

            result = resolve_module_name(nested_module.nested_task)
            assert result == "company.project.tasks"
        finally:
            sys.path[:] = original_sys_path
            for mod in ["company.project.tasks", "company.project", "company"]:
                if mod in sys.modules:
                    del sys.modules[mod]


class TestEdgeCases:
    def test_resolve_with_special_characters_in_path(self) -> None:
        """Test handling paths with special characters."""
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "test_fn"

        mock_code = MagicMock()
        mock_code.co_filename = "/path/with spaces/and-dashes/file.py"
        mock_fn.__code__ = mock_code

        # Should handle gracefully
        result = resolve_module_name(mock_fn)
        assert result is None or isinstance(result, str)

    def test_validate_with_builtin_function(self) -> None:
        """Test validation with built-in functions."""
        result = validate_module_candidate("builtins", len)
        assert result is True

    def test_resolve_builtin_function(self) -> None:
        """Test resolving built-in functions."""
        result = resolve_module_name(len)
        assert result == "builtins"
