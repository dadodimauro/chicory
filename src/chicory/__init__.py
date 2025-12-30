from __future__ import annotations

from chicory.app import Chicory
from chicory.config import (
    ChicoryConfig,
    RedisBackendConfig,
    RedisBrokerConfig,
    WorkerConfig,
)
from chicory.context import TaskContext
from chicory.exceptions import (
    BackendNotConfiguredError,
    ChicoryError,
    MaxRetriesExceededError,
    RetryError,
    TaskNotFoundError,
    ValidationError,
)
from chicory.result import AsyncResult
from chicory.task import Task
from chicory.types import (
    BackendType,
    BrokerType,
    DeliveryMode,
    RetryBackoff,
    RetryPolicy,
    TaskMessage,
    TaskOptions,
    TaskResult,
    TaskState,
    ValidationMode,
    WorkerStats,
)
from chicory.worker import Worker

__all__ = [
    "Chicory",
    "Task",
    "TaskContext",
    "AsyncResult",
    "TaskState",
    "TaskMessage",
    "TaskOptions",
    "TaskResult",
    "BrokerType",
    "BackendType",
    "ValidationMode",
    "DeliveryMode",
    "RetryBackoff",
    "RetryPolicy",
    "WorkerStats",
    "ChicoryError",
    "TaskNotFoundError",
    "ValidationError",
    "RetryError",
    "MaxRetriesExceededError",
    "BackendNotConfiguredError",
    "Worker",
    "ChicoryConfig",
    "WorkerConfig",
    "RedisBackendConfig",
    "RedisBrokerConfig",
]

try:
    from chicory.broker.rabbitmq import RabbitMQBroker

    __all__.append("RabbitMQBroker")
except ImportError:
    pass

try:
    from chicory.backend.redis import RedisBackend
    from chicory.broker.redis import RedisBroker

    __all__.extend(
        [
            "RedisBroker",
            "RedisBackend",
        ]
    )
except ImportError:
    pass

try:
    from chicory.backend.database import DatabaseBackend

    __all__.append("DatabaseBackend")
except ImportError:
    pass
