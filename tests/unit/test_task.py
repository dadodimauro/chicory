from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from chicory.app import Chicory
from chicory.context import TaskContext  # noqa: TC001
from chicory.exceptions import ValidationError
from chicory.task import Task
from chicory.types import BrokerType, TaskOptions, ValidationMode


def create_mock_function(
    name: str = "test_fn",
    module: str = "test_module",
    annotations: dict | None = None,
) -> MagicMock:
    """Create a mock function that works with inspect and get_type_hints."""
    mock_fn = MagicMock()
    mock_fn.__module__ = module
    mock_fn.__name__ = name
    mock_fn.__annotations__ = annotations or {}
    mock_fn.__globals__ = {}
    mock_fn.__code__ = MagicMock(co_filename=f"/{module.replace('.', '/')}.py")
    return mock_fn


class TestTaskBuildInputModel:
    def test_build_input_model_with_context(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(ctx: TaskContext, x: int, y: str = "default") -> None:
            pass

        task = Task(task_fn, app, options)
        input_model = task._input_model

        assert "x" in input_model.model_fields
        assert input_model.model_fields["x"].annotation is int
        assert input_model.model_fields["x"].is_required() is True
        assert "y" in input_model.model_fields
        assert input_model.model_fields["y"].annotation is str
        assert input_model.model_fields["y"].is_required() is False
        assert "ctx" not in input_model.model_fields

    def test_build_input_model_without_context(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: float, b: bool = True) -> None:
            pass

        task = Task(task_fn, app, options)
        input_model = task._input_model

        assert "a" in input_model.model_fields
        assert input_model.model_fields["a"].annotation is float
        assert input_model.model_fields["a"].is_required() is True
        assert "b" in input_model.model_fields
        assert input_model.model_fields["b"].annotation is bool
        assert input_model.model_fields["b"].is_required() is False

    def test_build_input_model_no_params(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn() -> None:
            pass

        task = Task(task_fn, app, options)
        input_model = task._input_model

        assert len(input_model.model_fields) == 0


class TestTaskValidateInputs:
    def test_validate_inputs_success(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: int, b: str = "default") -> None:
            pass

        task = Task(task_fn, app, options)
        args = [42]
        kwargs = {"b": "hello"}

        validated = task._validate_inputs(args[0], **kwargs)

        assert validated["a"] == 42
        assert validated["b"] == "hello"

    def test_validate_inputs_missing_required(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(x: int, y: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = []
        kwargs = {"y": "value"}

        try:
            task._validate_inputs(*args, **kwargs)  # type: ignore
            assert False, (
                "Expected validation to fail due to missing required parameter"
            )
        except Exception as e:
            assert "x" in str(e)

    def test_validate_inputs_type_conversion(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: int, b: float) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = ["123"]
        kwargs: dict[str, Any] = {"b": "45.67"}

        validated = task._validate_inputs(*args, **kwargs)

        assert validated["a"] == 123
        assert validated["b"] == 45.67

    def test_validate_inputs_extra_parameters(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: int) -> None:
            pass

        task = Task(task_fn, app, options)
        args = [10]
        kwargs = {"b": "extra"}

        with pytest.raises(ValidationError):
            task._validate_inputs(*args, **kwargs)  # type: ignore


@pytest.mark.asyncio
class TestTaskDelay:
    async def test_delay_success(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.STRICT
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = None

        async def task_fn(a: int, b: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = [1, "test"]
        kwargs: dict[str, Any] = {}

        result = await task.delay(*args, **kwargs)

        assert result.task_id is not None
        app.broker.publish.assert_awaited_once()

    async def test_delay_validation_error(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.STRICT
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = None

        async def task_fn(a: int, b: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = ["not_an_int", "test"]
        kwargs: dict[str, Any] = {}

        with pytest.raises(ValidationError):
            await task.delay(*args, **kwargs)

    async def test_delay_non_async_task(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: int) -> None:
            pass

        task = Task(task_fn, app, options)

        with pytest.raises(TypeError):
            await task.delay(1)

    async def test_delay_with_input_validation_modes(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.NONE
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = ValidationMode.NONE

        async def task_fn(x: int, y: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = ["not_an_int", "test"]
        kwargs: dict[str, Any] = {}

        # This should pass validation because validation mode is NONE
        await task.delay(*args, **kwargs)

        app.broker.publish.assert_awaited_once()


@pytest.mark.asyncio
class TestTaskSend:
    async def test_send_success(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.STRICT
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = None

        async def task_fn(a: int, b: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = [1, "test"]
        kwargs: dict[str, Any] = {}

        task_id = await task.send(*args, **kwargs)

        assert task_id is not None
        app.broker.publish.assert_awaited_once()

    async def test_send_with_input_validation(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.INPUTS
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = None

        async def task_fn(x: int, y: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = ["not_an_int", "test"]
        kwargs: dict[str, Any] = {}

        with pytest.raises(ValidationError):
            await task.send(*args, **kwargs)

    async def test_send_non_async_task(self) -> None:
        app = Mock(spec=Chicory)
        options = Mock(spec=TaskOptions)
        options.name = "test_task"

        def task_fn(a: int) -> None:
            pass

        task = Task(task_fn, app, options)

        with pytest.raises(TypeError):
            await task.send(1)

    async def test_send_without_input_validation(self) -> None:
        app = Mock(spec=Chicory)
        app.broker = Mock()
        app.broker.publish = AsyncMock()
        app.config = Mock()
        app.config.validation_mode = ValidationMode.NONE
        options = Mock(spec=TaskOptions)
        options.name = "test_task"
        options.validation_mode = None

        async def task_fn(x: int, y: str) -> None:
            pass

        task = Task(task_fn, app, options)
        args: list[Any] = ["not_an_int", "test"]
        kwargs: dict[str, Any] = {}

        # This should pass validation because validation mode is NONE
        task_id = await task.send(*args, **kwargs)

        assert task_id is not None
        app.broker.publish.assert_awaited_once()


class TestTaskResolveTaskName:
    def test_resolve_task_name_with_explicit_name(self) -> None:
        """Test that explicit task names override resolution."""
        app = Chicory(broker=BrokerType.REDIS)

        async def my_task() -> None:
            pass

        options = TaskOptions(name="custom.task.name")
        task = Task(fn=my_task, app=app, options=options)

        assert task.name == "custom.task.name"

    def test_resolve_task_name_from_main_with_successful_resolution(self) -> None:
        """Test that __main__ tasks can be resolved if path is valid."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        # Create a mock function from __main__ that can be resolved
        mock_fn = create_mock_function(
            name="my_task",
            module="__main__",
        )
        mock_fn.__code__ = MagicMock(co_filename="/path/to/myapp/tasks.py")

        # Mock resolve_module_name to return a valid module
        with patch("chicory.task.resolve_module_name", return_value="myapp.tasks"):
            task = Task(fn=mock_fn, app=app, options=options)
            assert task.name == "myapp.tasks.my_task"

    def test_resolve_task_name_from_main_raises_error_when_unresolvable(self) -> None:
        """Test that __main__ tasks raise RuntimeError when unresolvable."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        # Create a mock function from __main__
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "unresolvable_task"
        mock_fn.__code__ = MagicMock(co_filename="<stdin>")

        # Mock resolve_module_name to return None (unresolvable)
        with patch("chicory.task.resolve_module_name", return_value=None):
            with pytest.raises(RuntimeError) as exc_info:
                Task(fn=mock_fn, app=app, options=options)

            assert "unresolvable_task" in str(exc_info.value)
            assert "__main__" in str(exc_info.value)
            assert "python -m module" in str(exc_info.value)

    def test_resolve_task_name_error_message_includes_function_name(self) -> None:
        """Test that error message includes the function name."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "my_specific_task"
        mock_fn.__code__ = MagicMock(co_filename="<string>")

        with patch("chicory.task.resolve_module_name", return_value=None):
            with pytest.raises(RuntimeError) as exc_info:
                Task(fn=mock_fn, app=app, options=options)

            assert "my_specific_task" in str(exc_info.value)

    def test_resolve_task_name_error_message_includes_solutions(self) -> None:
        """Test that error message includes helpful solutions."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "problematic_task"
        mock_fn.__code__ = MagicMock(co_filename="<console>")

        with patch("chicory.task.resolve_module_name", return_value=None):
            with pytest.raises(RuntimeError) as exc_info:
                Task(fn=mock_fn, app=app, options=options)

            error_msg = str(exc_info.value)
            # Check for solution suggestions
            assert "python -m module" in error_msg
            assert "task(name=" in error_msg or "setting task(name=..." in error_msg

    def test_resolve_task_name_handles_nested_module(self) -> None:
        """Test resolution of tasks from nested modules."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        mock_fn = create_mock_function(
            name="nested_task",
            module="myapp.submodule.tasks",
        )

        task = Task(fn=mock_fn, app=app, options=options)
        assert task.name == "myapp.submodule.tasks.nested_task"

    def test_resolve_task_name_with_async_function(self) -> None:
        """Test that async functions are resolved correctly."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        async def my_async_task() -> None:
            pass

        task = Task(fn=my_async_task, app=app, options=options)
        assert "my_async_task" in task.name
        assert task.is_async is True

    def test_resolve_task_name_with_sync_function(self) -> None:
        """Test that sync functions are resolved correctly."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        def my_sync_task() -> None:
            pass

        task = Task(fn=my_sync_task, app=app, options=options)
        assert "my_sync_task" in task.name
        assert task.is_async is False

    def test_resolve_task_name_main_with_valid_sys_path(self, tmp_path) -> None:
        """Test __main__ resolution when file is in sys.path."""
        import sys

        # Create a temporary module
        module_file = tmp_path / "test_module.py"
        module_file.write_text("def test_task(): pass\n")

        # Add to sys.path
        original_sys_path = sys.path.copy()
        sys.path.insert(0, str(tmp_path))

        try:
            # Import the module
            import importlib

            test_module = importlib.import_module("test_module")

            app = Chicory(broker=BrokerType.REDIS)
            options = TaskOptions()

            # Create task from the imported function
            task = Task(fn=test_module.test_task, app=app, options=options)
            assert task.name == "test_module.test_task"
        finally:
            sys.path[:] = original_sys_path
            if "test_module" in sys.modules:
                del sys.modules["test_module"]

    def test_resolve_task_name_preserves_function_attributes(self) -> None:
        """Test that task resolution doesn't modify function attributes."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        def my_task() -> str:
            """Task docstring."""
            return "result"

        original_name = my_task.__name__
        original_doc = my_task.__doc__
        original_module = my_task.__module__

        task = Task(fn=my_task, app=app, options=options)

        assert my_task.__name__ == original_name
        assert my_task.__doc__ == original_doc
        assert my_task.__module__ == original_module

    def test_resolve_task_name_with_lambda(self) -> None:
        """Test that lambda functions can be used as tasks."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        lambda_task = lambda x: x + 1  # noqa: E731

        task = Task(fn=lambda_task, app=app, options=options)
        assert "<lambda>" in task.name

    def test_resolve_task_name_from_main_interactive_session(self) -> None:
        """Test that interactive session tasks are rejected."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        # Simulate interactive session
        mock_fn = MagicMock()
        mock_fn.__module__ = "__main__"
        mock_fn.__name__ = "interactive_task"
        mock_fn.__code__ = MagicMock(co_filename="<stdin>")

        with patch("chicory.task.resolve_module_name", return_value=None):  # noqa: SIM117
            with pytest.raises(RuntimeError):
                Task(fn=mock_fn, app=app, options=options)

    def test_resolve_task_name_explicit_overrides_main(self) -> None:
        """Test that explicit name works even for __main__ functions."""
        app = Chicory(broker=BrokerType.REDIS)

        mock_fn = create_mock_function(
            name="main_task",
            module="__main__",
        )
        mock_fn.__code__ = MagicMock(co_filename="<stdin>")

        # Explicit name should bypass __main__ check
        options = TaskOptions(name="myapp.tasks.main_task")

        with patch("chicory.task.resolve_module_name", return_value=None):
            task = Task(fn=mock_fn, app=app, options=options)
            assert task.name == "myapp.tasks.main_task"


class TestTaskResolveTaskNameErrorHandling:
    """Test error handling in _resolve_task_name."""

    def test_function_without_module_attribute(self) -> None:
        """Test handling of callables without __module__."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        # Create a callable without __module__
        class CallableWithoutModule:
            def __call__(self):
                pass

        callable_obj = CallableWithoutModule()

        # This should work but might use __main__ or raise
        # Depending on how Python handles missing __module__
        try:
            task = Task(fn=callable_obj, app=app, options=options)
            # If it works, verify the name is set
            assert task.name is not None
        except (AttributeError, RuntimeError):
            # Expected if __module__ is missing
            pass

    def test_function_without_name_attribute(self) -> None:
        """Test handling of callables without __name__."""
        app = Chicory(broker=BrokerType.REDIS)
        options = TaskOptions()

        mock_fn = MagicMock()
        mock_fn.__module__ = "test_module"
        # Delete __name__ attribute
        delattr(mock_fn, "__name__")

        # Should raise an error when trying to access __name__
        with pytest.raises((AttributeError, KeyError)):
            Task(fn=mock_fn, app=app, options=options)


class TestTaskResolveTaskNameIntegration:
    """Integration tests for _resolve_task_name with real decorator usage."""

    def test_decorator_usage_with_explicit_name(self) -> None:
        """Test @app.task decorator with explicit name."""
        app = Chicory(broker=BrokerType.REDIS)

        @app.task(name="myapp.custom_name")
        async def my_decorated_task() -> int:
            return 42

        assert my_decorated_task.name == "myapp.custom_name"

    def test_decorator_usage_with_auto_name(self) -> None:
        """Test @app.task decorator with automatic name resolution."""
        app = Chicory(broker=BrokerType.REDIS)

        @app.task()
        async def auto_named_task() -> int:
            return 42

        assert "auto_named_task" in auto_named_task.name
