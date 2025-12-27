from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from chicory.types import BackendStatus, TaskResult, TaskState, WorkerStats


@runtime_checkable
class Backend(Protocol):
    """Protocol for result backends."""

    async def connect(self) -> None:
        """Establish connection to the backend."""
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Close connection to the backend."""
        raise NotImplementedError

    async def set_state(self, task_id: str, state: TaskState) -> None:
        """Update task state."""
        raise NotImplementedError

    async def store_result(self, task_id: str, result: TaskResult[Any]) -> None:
        """Store task result."""
        raise NotImplementedError

    async def get_result(self, task_id: str) -> TaskResult[Any] | None:
        """Retrieve task result."""
        raise NotImplementedError

    async def delete_result(self, task_id: str) -> None:
        """Delete task result."""
        raise NotImplementedError

    async def store_heartbeat(
        self, worker_id: str, heartbeat: WorkerStats, ttl: int = 30
    ) -> None:
        """Store worker heartbeat with TTL."""
        raise NotImplementedError

    async def get_heartbeat(self, worker_id: str) -> WorkerStats | None:
        """Retrieve worker heartbeat."""
        raise NotImplementedError

    async def get_active_workers(self) -> list[WorkerStats]:
        """Get all active workers (with recent heartbeats)."""
        raise NotImplementedError

    async def cleanup_stale_workers(self, stale_seconds: int = 60) -> int:
        """Remove worker heartbeats older than stale_seconds. Returns count removed."""
        raise NotImplementedError

    async def healthcheck(self) -> BackendStatus:
        """Check the health of the backend connection."""
        raise NotImplementedError
