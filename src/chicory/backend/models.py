from __future__ import annotations

from datetime import UTC, datetime
from typing import TypeAlias

from sqlalchemy import DateTime, MetaData, String, Text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from chicory.config import (
    MSSQLBackendConfig,
    MySQLBackendConfig,
    PostgresBackendConfig,
    SQLiteBackendConfig,
)

AsyncSessionMaker: TypeAlias = async_sessionmaker[AsyncSession]
DatabaseBackendConfig: TypeAlias = (
    MSSQLBackendConfig
    | SQLiteBackendConfig
    | MySQLBackendConfig
    | PostgresBackendConfig
)


my_metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    },
)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    __abstract__ = True
    metadata = my_metadata


class TaskResultModel(Base):
    """SQLAlchemy model for storing task results."""

    __tablename__ = "chicory_task_results"

    task_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    state: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        index=True,
    )


class WorkerHeartbeatModel(Base):
    """SQLAlchemy model for storing worker heartbeats."""

    __tablename__ = "chicory_worker_heartbeats"

    worker_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    heartbeat_json: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
        index=True,
    )
