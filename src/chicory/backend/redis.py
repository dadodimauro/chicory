from __future__ import annotations

from typing import Any

from redis import asyncio as redis

from chicory.config import RedisBackendConfig  # noqa: TC001
from chicory.types import BackendStatus, TaskResult, TaskState, WorkerStats

from .base import Backend


class RedisBackend(Backend):
    """Redis-based result backend."""

    def __init__(self, config: RedisBackendConfig) -> None:
        self.dsn = config.dsn
        self.result_ttl = config.result_ttl
        self._pool: redis.ConnectionPool | None = None
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        self._pool = redis.ConnectionPool.from_url(self.dsn)
        self._client = redis.Redis(
            connection_pool=self._pool, auto_close_connection_pool=False
        )
        await self._client.ping()  # ty:ignore[invalid-await]

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None

    def _state_key(self, task_id: str) -> str:
        return f"chicory:state:{task_id}"

    def _result_key(self, task_id: str) -> str:
        return f"chicory:result:{task_id}"

    def _heartbeat_key(self, worker_id: str) -> str:
        return f"chicory:heartbeat:{worker_id}"

    def _workers_set_key(self) -> str:
        """Key for tracking all worker IDs."""
        return "chicory:workers"

    async def set_state(self, task_id: str, state: TaskState) -> None:
        if not self._client:
            raise RuntimeError("Backend not connected")

        await self._client.setex(
            self._state_key(task_id),
            self.result_ttl,
            state.value,
        )

    async def store_result(self, task_id: str, result: TaskResult[Any]) -> None:
        if not self._client:
            raise RuntimeError("Backend not connected")

        pipe = self._client.pipeline()
        pipe.setex(
            self._state_key(task_id),
            self.result_ttl,
            result.state.value,
        )
        pipe.setex(
            self._result_key(task_id),
            self.result_ttl,
            result.model_dump_json(),
        )
        await pipe.execute()

    async def get_result(self, task_id: str) -> TaskResult[Any] | None:
        if not self._client:
            raise RuntimeError("Backend not connected")

        data = await self._client.get(self._result_key(task_id))
        if data:
            return TaskResult.model_validate_json(data)

        # Check if we have just state
        state: bytes | None = await self._client.get(self._state_key(task_id))
        if state:
            return TaskResult(
                task_id=task_id,
                state=TaskState(state.decode()),
            )

        return None

    async def delete_result(self, task_id: str) -> None:
        if not self._client:
            raise RuntimeError("Backend not connected")

        pipe = self._client.pipeline()
        pipe.delete(self._state_key(task_id))
        pipe.delete(self._result_key(task_id))
        await pipe.execute()

    async def store_heartbeat(
        self, worker_id: str, heartbeat: WorkerStats, ttl: int = 30
    ) -> None:
        """Store worker heartbeat with TTL."""
        if not self._client:
            raise RuntimeError("Backend not connected")

        pipe = self._client.pipeline()

        pipe.setex(
            self._heartbeat_key(worker_id),
            ttl,
            heartbeat.model_dump_json(),
        )

        # Add to workers set (for discovery)
        pipe.sadd(self._workers_set_key(), worker_id)

        await pipe.execute()

    async def get_heartbeat(self, worker_id: str) -> WorkerStats | None:
        """Retrieve worker heartbeat."""
        if not self._client:
            raise RuntimeError("Backend not connected")

        data = await self._client.get(self._heartbeat_key(worker_id))
        if data:
            return WorkerStats.model_validate_json(data)
        return None

    async def get_active_workers(self) -> list[WorkerStats]:
        """Get all active workers with recent heartbeats."""
        if not self._client:
            raise RuntimeError("Backend not connected")

        worker_ids: set[bytes] = await self._client.smembers(self._workers_set_key())  # ty:ignore[invalid-await]

        active_workers = []
        for worker_id_bytes in worker_ids:
            worker_id = (
                worker_id_bytes.decode()
                if isinstance(worker_id_bytes, bytes)
                else worker_id_bytes
            )

            heartbeat = await self.get_heartbeat(worker_id)
            if heartbeat:
                active_workers.append(heartbeat)

        return active_workers

    async def cleanup_stale_workers(self, stale_seconds: int = 60) -> int:
        """Remove stale worker IDs from the tracking set."""
        if not self._client:
            return 0

        worker_ids: set[bytes] = await self._client.smembers(self._workers_set_key())  # ty:ignore[invalid-await]
        removed = 0

        for worker_id_bytes in worker_ids:
            worker_id = (
                worker_id_bytes.decode()
                if isinstance(worker_id_bytes, bytes)
                else worker_id_bytes
            )

            # Check if heartbeat key still exists
            exists = await self._client.exists(self._heartbeat_key(worker_id))
            if not exists:
                await self._client.srem(self._workers_set_key(), worker_id)  # ty:ignore[invalid-await]
                removed += 1

        return removed

    async def healthcheck(self) -> BackendStatus:
        """Check the health of the backend connection."""
        if not self._client:
            return BackendStatus(connected=False, error="Not connected")

        try:
            await self._client.ping()  # ty:ignore[invalid-await]
            return BackendStatus(connected=True)
        except Exception as e:
            return BackendStatus(connected=False, error=str(e))
