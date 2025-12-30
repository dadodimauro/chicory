from __future__ import annotations

import random
from datetime import UTC, datetime  # noqa: TC003
from enum import StrEnum
from typing import Any, Generic, ParamSpec, TypeVar

from pydantic import BaseModel, Field, computed_field

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")


class TaskState(StrEnum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    DEAD_LETTERED = "DEAD_LETTERED"  # New state for DLQ


class DeliveryMode(StrEnum):
    AT_LEAST_ONCE = "at_least_once"
    AT_MOST_ONCE = "at_most_once"


class ValidationMode(StrEnum):
    NONE = "none"
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    STRICT = "strict"


class RetryBackoff(StrEnum):
    """Backoff strategy for retries."""

    FIXED = "fixed"  # Fixed delay between retries
    LINEAR = "linear"  # Delay increases linearly: delay * attempt
    EXPONENTIAL = "exponential"  # Delay doubles each attempt: delay * 2^attempt


class RetryPolicy(BaseModel):
    """Configuration for task retry behavior."""

    max_retries: int = Field(
        default=3, ge=0, description="Maximum number of retry attempts"
    )
    retry_delay: float = Field(
        default=1.0, ge=0, description="Base delay between retries in seconds"
    )
    backoff: RetryBackoff = Field(
        default=RetryBackoff.EXPONENTIAL, description="Backoff strategy"
    )
    max_delay: float = Field(
        default=3600.0, ge=0, description="Maximum delay between retries in seconds"
    )
    jitter: bool = Field(
        default=True,
        description="Add random jitter to delay to prevent thundering herd",
    )
    retry_on: list[str] | None = Field(
        default=None,
        description=(
            "List of exception class names to retry on. "
            "None means retry on all exceptions."
        ),
    )
    ignore_on: list[str] | None = Field(
        default=None,
        description=(
            "List of exception class names to NOT retry on "
            "(takes precedence over retry_on)."
        ),
    )

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt (1-indexed)."""

        match self.backoff:
            case RetryBackoff.FIXED:
                delay = self.retry_delay
            case RetryBackoff.LINEAR:
                delay = self.retry_delay * attempt
            case RetryBackoff.EXPONENTIAL:
                delay = self.retry_delay * (2 ** (attempt - 1))
            case _:  # pragma: no cover
                delay = self.retry_delay  # Fallback to fixed

        # Apply max delay cap
        delay = min(delay, self.max_delay)

        # Apply jitter (±25%)
        if self.jitter:
            jitter_range = delay * 0.25
            delay = delay + random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    def should_retry(self, exception: Exception) -> bool:
        """Determine if the given exception should trigger a retry."""
        exc_name = type(exception).__name__
        exc_full_name = f"{type(exception).__module__}.{exc_name}"

        # Check ignore list first (takes precedence)
        if self.ignore_on:
            for ignored in self.ignore_on:
                if exc_name == ignored or exc_full_name == ignored:
                    return False

        # Check retry list
        if self.retry_on is None:
            return True  # Retry on all exceptions

        for allowed in self.retry_on:
            if exc_name == allowed or exc_full_name == allowed:
                return True

        return False


class TaskMessage(BaseModel):
    """Message schema for task serialization."""

    id: str = Field(..., description="Unique task identifier")
    name: str = Field(..., description="Name of the task to execute")
    args: list[Any] = Field(..., description="Positional arguments for the task")
    kwargs: dict[str, Any] = Field(..., description="Keyword arguments for the task")
    retries: int = Field(default=0, description="Number of retries attempted")
    eta: datetime | None = Field(
        default=None, description="Earliest time at which the task can be executed"
    )
    # Priority queues (RabbitMQ)
    priority: int | None = Field(
        default=None,
        ge=0,
        le=255,
        description="Message priority (0-255, higher = more priority)",
    )
    # DLQ tracking
    first_failure_at: datetime | None = Field(
        default=None, description="Timestamp of the first failure"
    )
    last_error: str | None = Field(
        default=None, description="Last error message for debugging"
    )


class TaskResult(BaseModel, Generic[T]):  # noqa: UP046
    """Result payload stored in backend"""

    task_id: str = Field(..., description="Unique task identifier")
    state: TaskState = Field(..., description="Final state of the task")
    result: T | None = Field(
        default=None, description="Result of the task if successful"
    )
    error: str | None = Field(
        default=None, description="Error message if the task failed"
    )
    traceback: str | None = Field(
        default=None, description="Traceback information if the task failed"
    )


class TaskOptions(BaseModel):
    """Configuration options for a task"""

    name: str | None = Field(default=None, description="Optional name for the task")
    retry_policy: RetryPolicy | None = Field(
        default=None, description="Retry policy configuration"
    )
    delivery_mode: DeliveryMode = Field(
        DeliveryMode.AT_MOST_ONCE,
        description="Delivery mode for task execution",
    )
    ignore_result: bool = Field(
        default=False, description="Whether to ignore the task result"
    )
    validation_mode: ValidationMode = Field(
        default=ValidationMode.INPUTS, description="Validation mode for the task"
    )

    def get_retry_policy(self) -> RetryPolicy:
        """Get the retry policy, returning a default (no retries) if not configured."""
        if self.retry_policy:
            return self.retry_policy

        return RetryPolicy(max_retries=0)


class BaseStatus(BaseModel):
    connected: bool = Field(..., description="Connection status")
    error: str | None = Field(
        default=None, description="Error message if not connected"
    )


class BrokerStatus(BaseStatus): ...


class BackendStatus(BaseStatus): ...


class WorkerStats(BaseModel):
    worker_id: str = Field(..., description="Unique worker identifier")
    hostname: str = Field(..., description="Hostname where worker is running")
    pid: int = Field(..., description="Process ID")
    queue: str = Field(..., description="Queue being consumed")
    concurrency: int = Field(..., description="Configured concurrency level")
    tasks_processed: int = Field(default=0, description="Total tasks processed")
    tasks_failed: int = Field(default=0, description="Total tasks failed")
    active_tasks: int = Field(default=0, description="Currently processing tasks")
    started_at: datetime = Field(..., description="Worker start time")
    last_heartbeat: datetime | None = Field(
        default=None, description="Last heartbeat timestamp"
    )
    is_running: bool = Field(default=True, description="Worker running status")

    broker: BrokerStatus | None = Field(
        default=None, description="Broker connection status"
    )
    backend: BackendStatus | None = Field(
        default=None, description="Backend connection status"
    )

    @computed_field
    @property
    def uptime_seconds(self) -> float:  # pragma: no cover
        """Worker uptime in seconds."""
        return (datetime.now(UTC) - self.started_at).total_seconds()


class BrokerType(StrEnum):
    REDIS = "redis"
    RABBITMQ = "rabbitmq"


class BackendType(StrEnum):
    REDIS = "redis"
    MSSQL = "mssql"
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"
