from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from chicory.types import BackendStatus, TaskResult, TaskState, WorkerStats


class Backend(ABC):
    """Protocol for result backends."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the backend."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the backend."""
        ...

    @abstractmethod
    async def set_state(self, task_id: str, state: TaskState) -> None:
        """Update task state."""
        ...

    @abstractmethod
    async def store_result(self, task_id: str, result: TaskResult[Any]) -> None:
        """Store task result."""
        ...

    @abstractmethod
    async def get_result(self, task_id: str) -> TaskResult[Any] | None:
        """Retrieve task result."""
        ...

    @abstractmethod
    async def delete_result(self, task_id: str) -> None:
        """Delete task result."""
        ...

    @abstractmethod
    async def store_heartbeat(
        self, worker_id: str, heartbeat: WorkerStats, ttl: int = 30
    ) -> None:
        """Store worker heartbeat with TTL."""
        ...

    @abstractmethod
    async def get_heartbeat(self, worker_id: str) -> WorkerStats | None:
        """Retrieve worker heartbeat."""
        ...

    @abstractmethod
    async def get_active_workers(self) -> list[WorkerStats]:
        """Get all active workers (with recent heartbeats)."""
        ...

    @abstractmethod
    async def cleanup_stale_workers(self, stale_seconds: int = 60) -> int:
        """Remove worker heartbeats older than stale_seconds. Returns count removed."""
        ...

    @abstractmethod
    async def healthcheck(self) -> BackendStatus:
        """Check the health of the backend connection."""
        ...
