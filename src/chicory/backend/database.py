from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeAlias

from sqlalchemy import delete, exc, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from chicory.config import (
    MSSQLBackendConfig,
    MySQLBackendConfig,
    PostgresBackendConfig,
    SQLiteBackendConfig,
)
from chicory.exceptions import DbPoolExhaustedException
from chicory.types import BackendStatus, TaskResult, TaskState, WorkerStats

from .base import Backend
from .models import Base, TaskResultModel, WorkerHeartbeatModel

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

AsyncSessionMaker: TypeAlias = async_sessionmaker[AsyncSession]
DatabaseBackendConfig: TypeAlias = (
    MSSQLBackendConfig
    | SQLiteBackendConfig
    | MySQLBackendConfig
    | PostgresBackendConfig
)


class DatabaseBackend(Backend):
    """
    Database-based result backend using SQLAlchemy ORM.

    Supports PostgreSQL, MySQL, SQLite, and MSSQL databases.
    Uses standard SQLAlchemy ORM operations for cross-database compatibility.
    """

    def __init__(
        self,
        config: DatabaseBackendConfig,
        consumer_name: str | None = None,
    ) -> None:
        self.config = config
        self.dsn = config.dsn
        self.echo = config.echo
        self.pool_size = config.pool_size
        self.max_overflow = config.max_overflow
        self.pool_recycle = config.pool_recycle
        self.pool_timeout = config.pool_timeout

        self.consumer_name = consumer_name or f"worker-{uuid.uuid4().hex[:8]}"

        self._engine: AsyncEngine | None = None
        self._sessionmaker: AsyncSessionMaker | None = None

    @asynccontextmanager
    async def _get_db_session_from_pool(
        self,
    ) -> AsyncGenerator[AsyncSession]:
        """
        Context manager aware db session. Will throw DbPoolExhaustedException
        if db pool is exhausted.

        Yields:
            AsyncSession: An async database session.

        Raises:
            DbPoolExhaustedException: If the database connection pool is exhausted.
        """
        if not self._sessionmaker:
            raise RuntimeError("Database backend not connected")

        async with self._sessionmaker() as session:
            try:
                yield session
            except exc.TimeoutError as ex:
                await session.rollback()
                raise DbPoolExhaustedException("db pool exhaustion") from ex
            except:
                await session.rollback()
                raise
            else:
                await session.commit()

    def _get_engine_kwargs(self) -> dict[str, Any]:
        """Get engine kwargs based on database type."""
        base_kwargs: dict[str, Any] = {
            "url": self.dsn,
            "echo": self.echo,
            "pool_pre_ping": True,
        }

        # SQLite doesn't support connection pooling the same way
        if isinstance(self.config, SQLiteBackendConfig):
            # For SQLite, special handling for in-memory or file-based
            if ":memory:" in self.dsn or self.config.file_path == ":memory:":
                from sqlalchemy.pool import StaticPool

                base_kwargs["poolclass"] = StaticPool
                base_kwargs["connect_args"] = {"check_same_thread": False}
            else:
                base_kwargs["connect_args"] = {"check_same_thread": False}
                base_kwargs["pool_size"] = self.pool_size
                base_kwargs["max_overflow"] = self.max_overflow
                base_kwargs["pool_recycle"] = self.pool_recycle
                base_kwargs["pool_timeout"] = self.pool_timeout
        elif isinstance(self.config, PostgresBackendConfig):
            base_kwargs["connect_args"] = {
                "server_settings": {"application_name": self.consumer_name}
            }
            base_kwargs["pool_size"] = self.pool_size
            base_kwargs["max_overflow"] = self.max_overflow
            base_kwargs["pool_recycle"] = self.pool_recycle
            base_kwargs["pool_timeout"] = self.pool_timeout
        else:
            # MySQL and MSSQL
            base_kwargs["pool_size"] = self.pool_size
            base_kwargs["max_overflow"] = self.max_overflow
            base_kwargs["pool_recycle"] = self.pool_recycle
            base_kwargs["pool_timeout"] = self.pool_timeout

        return base_kwargs

    def _create_async_engine(self) -> None:
        """Create an async engine from the configuration."""
        engine_kwargs = self._get_engine_kwargs()
        self._engine = create_async_engine(**engine_kwargs)

    def _create_async_sessionmaker(self) -> None:
        """Create an async sessionmaker from the given engine."""
        if not self._engine:
            raise RuntimeError("Database engine not initialized")

        self._sessionmaker = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

    async def connect(self) -> None:
        """Establish connection to the backend and create tables if needed."""
        self._create_async_engine()
        self._create_async_sessionmaker()

        if not self._engine:
            raise RuntimeError("Failed to create database engine")

        # Create tables if they don't exist
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Test connection
        async with self._get_db_session_from_pool() as session:
            pass

    async def disconnect(self) -> None:
        """Close connection to the backend."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None

    # TODO @anvouk: https://github.com/dadodimauro/chicory/pull/17
    # Check if this methdod should run only if record exists
    async def set_state(self, task_id: str, state: TaskState) -> None:
        """Update task state without storing full result."""
        async with self._get_db_session_from_pool() as session:
            stmt = select(TaskResultModel).where(TaskResultModel.task_id == task_id)
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                existing.state = state.value
                existing.updated_at = datetime.now(UTC)
            else:
                # Create a minimal result record
                task_result = TaskResult(
                    task_id=task_id,
                    state=state,
                    result=None,
                    error=None,
                    traceback=None,
                )
                new_record = TaskResultModel(
                    task_id=task_id,
                    state=state.value,
                    result_json=task_result.model_dump_json(),
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
                session.add(new_record)

    async def store_result(self, task_id: str, result: TaskResult[Any]) -> None:
        """Store task result."""
        async with self._get_db_session_from_pool() as session:
            now = datetime.now(UTC)
            result_json = result.model_dump_json()

            stmt = select(TaskResultModel).where(TaskResultModel.task_id == task_id)
            db_result = await session.execute(stmt)
            existing = db_result.scalar_one_or_none()

            if existing:
                existing.state = result.state.value
                existing.result_json = result_json
                existing.updated_at = now
            else:
                new_record = TaskResultModel(
                    task_id=task_id,
                    state=result.state.value,
                    result_json=result_json,
                    created_at=now,
                    updated_at=now,
                )
                session.add(new_record)

    async def get_result(self, task_id: str) -> TaskResult[Any] | None:
        """Retrieve task result by task ID."""
        async with self._get_db_session_from_pool() as session:
            stmt = select(TaskResultModel).where(TaskResultModel.task_id == task_id)
            result = await session.execute(stmt)
            db_result = result.scalar_one_or_none()

            if db_result and db_result.result_json:
                return TaskResult.model_validate_json(db_result.result_json)

            return None

    async def delete_result(self, task_id: str) -> None:
        """Delete task result by task ID."""
        async with self._get_db_session_from_pool() as session:
            stmt = delete(TaskResultModel).where(TaskResultModel.task_id == task_id)
            await session.execute(stmt)

    async def store_heartbeat(
        self, worker_id: str, heartbeat: WorkerStats, ttl: int = 30
    ) -> None:
        """Store worker heartbeat with TTL emulation via expires_at column."""
        async with self._get_db_session_from_pool() as session:
            now = datetime.now(UTC)
            expires_at = now + timedelta(seconds=ttl)
            heartbeat_json = heartbeat.model_dump_json()

            stmt = select(WorkerHeartbeatModel).where(
                WorkerHeartbeatModel.worker_id == worker_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                existing.heartbeat_json = heartbeat_json
                existing.expires_at = expires_at
                existing.updated_at = now
            else:
                new_record = WorkerHeartbeatModel(
                    worker_id=worker_id,
                    heartbeat_json=heartbeat_json,
                    expires_at=expires_at,
                    updated_at=now,
                )
                session.add(new_record)

    async def get_heartbeat(self, worker_id: str) -> WorkerStats | None:
        """Retrieve worker heartbeat if not expired."""
        async with self._get_db_session_from_pool() as session:
            now = datetime.now(UTC)
            stmt = select(WorkerHeartbeatModel).where(
                WorkerHeartbeatModel.worker_id == worker_id,
                WorkerHeartbeatModel.expires_at > now,
            )
            result = await session.execute(stmt)
            heartbeat = result.scalar_one_or_none()

            if heartbeat:
                return WorkerStats.model_validate_json(heartbeat.heartbeat_json)

            return None

    async def get_active_workers(self) -> list[WorkerStats]:
        """Get all active workers with non-expired heartbeats."""
        async with self._get_db_session_from_pool() as session:
            now = datetime.now(UTC)
            stmt = select(WorkerHeartbeatModel).where(
                WorkerHeartbeatModel.expires_at > now
            )
            result = await session.execute(stmt)
            heartbeats = result.scalars().all()

            return [
                WorkerStats.model_validate_json(hb.heartbeat_json) for hb in heartbeats
            ]

    async def cleanup_stale_workers(self, stale_seconds: int = 60) -> int:
        """Remove expired worker heartbeats. Returns count of removed records."""
        async with self._get_db_session_from_pool() as session:
            now = datetime.now(UTC)
            stmt = (
                delete(WorkerHeartbeatModel)
                .where(WorkerHeartbeatModel.expires_at <= now)
                .returning(WorkerHeartbeatModel.worker_id)
            )
            result = await session.execute(stmt)
            return len(result.fetchall())

    async def healthcheck(self) -> BackendStatus:
        """Check the health of the backend connection."""
        if not self._engine:
            return BackendStatus(connected=False, error="Not connected")

        try:
            async with self._get_db_session_from_pool() as session:
                await session.execute(select(1))
            return BackendStatus(connected=True)
        except Exception as e:
            return BackendStatus(connected=False, error=str(e))

    async def cleanup_old_results(self, older_than_seconds: int = 86400) -> int:
        """
        Clean up task results older than specified seconds.

        This is an additional utility method for maintenance.
        Default is 24 hours (86400 seconds).

        Returns the count of deleted records.
        """
        async with self._get_db_session_from_pool() as session:
            cutoff = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
            stmt = (
                delete(TaskResultModel)
                .where(TaskResultModel.updated_at < cutoff)
                .returning(TaskResultModel.task_id)
            )
            result = await session.execute(stmt)
            return len(result.fetchall())
