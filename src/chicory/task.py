from __future__ import annotations

import inspect
import uuid
from typing import TYPE_CHECKING, Any, Generic, get_type_hints

from pydantic import BaseModel, ConfigDict, create_model
from pydantic import ValidationError as PydanticValidationError

from chicory.context import TaskContext
from chicory.exceptions import ValidationError
from chicory.result import AsyncResult
from chicory.types import P, R, TaskMessage, TaskOptions, ValidationMode
from chicory.utils import resolve_module_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from chicory.app import Chicory


class Task(Generic[P, R]):  # noqa: UP046
    """Wrapper around a task function providing invocation APIs."""

    def __init__(self, fn: Callable[P, R], app: Chicory, options: TaskOptions) -> None:
        self.fn = fn
        self.app = app
        self.options = options
        self.is_async = inspect.iscoroutinefunction(fn)

        self.name = self.options.name or self._resolve_task_name()

        # Check if first param is TaskContext
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        self.has_context = (
            len(params) > 0 and get_type_hints(fn).get(params[0].name) is TaskContext
        )

        # Build Pydantic model for validation
        self._input_model = self._build_input_model()
        self.output_model = get_type_hints(fn).get("return", None)

    def _resolve_task_name(self) -> str:
        """Determine the task name from the function."""
        module = self.fn.__module__
        resolved = resolve_module_name(self.fn)

        if module == "__main__" and resolved:
            module = resolved
        elif module == "__main__":
            raise RuntimeError(
                "Task %s defined in __main__ could not be resolved to an importable "
                "module. Workers may not be able to execute this task. "
                "Consider running via `python -m module` or setting task(name=...).",
                self.fn.__name__,  # ty:ignore[possibly-missing-attribute]
            )

        return f"{module}.{self.fn.__name__}"  # ty:ignore[possibly-missing-attribute]

    def _build_input_model(self) -> type[BaseModel]:
        """Create a Pydantic model for input validation."""
        hints = get_type_hints(self.fn)
        sig = inspect.signature(self.fn)

        fields: dict[str, tuple[type, Any]] = {}
        for name, param in sig.parameters.items():
            # Skip TaskContext parameter using resolved type hints
            annotation = hints.get(name, Any)
            if annotation is TaskContext:
                continue

            annotation = hints.get(name, Any)
            if param.default is inspect.Parameter.empty:
                fields[name] = (annotation, ...)
            else:
                fields[name] = (annotation, param.default)

        return create_model(
            f"{self.name}Input",
            **fields,  # type: ignore
            __config__=ConfigDict(extra="forbid"),
        )  # ty:ignore[no-matching-overload]

    def _validate_inputs(self, *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
        """Validate and convert inputs using Pydantic."""
        hints = get_type_hints(self.fn)
        sig = inspect.signature(self.fn)
        params = [
            p for p in sig.parameters.values() if hints.get(p.name) is not TaskContext
        ]

        # Bind args to parameter names
        bound = {}
        for i, arg in enumerate(args):
            if i < len(params):
                bound[params[i].name] = arg
        bound.update(kwargs)

        try:
            model_instance = self._input_model.model_validate(bound)
            return model_instance.model_dump()
        except PydanticValidationError as e:
            raise ValidationError(
                f"Input validation error for task {self.name!r}: {e}"
            ) from e

    async def delay(self, *args: P.args, **kwargs: P.kwargs) -> AsyncResult[R]:
        if not self.is_async:
            raise TypeError("delay can only be called on async tasks")

        validation_mode = (
            self.options.validation_mode or self.app.config.validation_mode
        )

        if validation_mode in (ValidationMode.INPUTS, ValidationMode.STRICT):
            _ = self._validate_inputs(*args, **kwargs)

        task_id = str(uuid.uuid4())
        message = TaskMessage(
            id=task_id,
            name=self.name,
            args=list(args),
            kwargs=kwargs,
            retries=0,
            eta=None,
        )

        await self.app.broker.publish(message)
        return AsyncResult[R](task_id, self.app.backend)

    async def send(self, *args: P.args, **kwargs: P.kwargs) -> str:
        """
        Fire-and-forget task invocation.

        Returns task_id for logging/tracing.
        """
        result = await self.delay(*args, **kwargs)
        return result.task_id
