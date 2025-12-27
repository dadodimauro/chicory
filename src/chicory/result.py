from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Generic, cast

from chicory.exceptions import BackendNotConfiguredError
from chicory.types import T, TaskState

if TYPE_CHECKING:
    from chicory.backend.base import Backend


class AsyncResult(Generic[T]):  # noqa: UP046
    """Asynchronous result handler for task results."""

    def __init__(self, task_id: str, backend: Backend | None = None) -> None:
        self.task_id = task_id
        self._backend = backend

    def _ensure_backend(self) -> Backend:
        if not self._backend:
            raise BackendNotConfiguredError(
                "Cannot access result: no backend configured. "
                "Configure a backend or use .send() for fire-and-forget."
            )
        return self._backend

    async def get(
        self, timeout: float | None = None, poll_interval: float = 0.1
    ) -> T | None:
        """Wait for and return the task result."""
        backend = self._ensure_backend()

        elapsed = 0.0
        while True:
            result = await backend.get_result(self.task_id)

            if result:
                match result.state:
                    case TaskState.SUCCESS:
                        return cast("T", result.result)
                    case TaskState.FAILURE:
                        raise Exception(result.error or "Task failed")
                    case _:
                        pass

            if timeout is not None and elapsed >= timeout:
                raise TimeoutError(
                    f"Task {self.task_id} did not complete in {timeout}s"
                )

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

    async def state(self) -> TaskState:
        """Get current task state."""
        backend = self._ensure_backend()
        result = await backend.get_result(self.task_id)
        if result:
            return result.state
        return TaskState.PENDING

    async def ready(self) -> bool:
        """Check if the task is complete (success or failure)."""
        state = await self.state()
        return state in {TaskState.SUCCESS, TaskState.FAILURE}

    async def failed(self) -> bool:
        """Check if the task has failed."""
        state = await self.state()
        return state == TaskState.FAILURE
