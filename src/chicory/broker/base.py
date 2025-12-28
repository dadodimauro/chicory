from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from chicory.types import TaskMessage  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from chicory.types import BrokerStatus

DEFAULT_QUEUE = "default"


@dataclass
class TaskEnvelope:
    """Broker-specific metadata plus decoded message."""

    message: TaskMessage
    delivery_tag: str  # broker-specific identifier for ack/nack
    raw_data: bytes | None = None
    _raw_message: object | None = None  # Broker-specific message object


@dataclass
class DLQMessage:
    """A message in the Dead Letter Queue."""

    message_id: str
    original_message: TaskMessage
    failed_at: str  # ISO timestamp
    error: str | None
    retry_count: int


class Broker(ABC):
    """Protocol for message brokers."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the broker."""
        ...

    @abstractmethod
    async def publish(self, message: TaskMessage, queue: str = DEFAULT_QUEUE) -> None:
        """Publish a task message to the specified queue."""
        ...

    @abstractmethod
    def consume(self, queue: str = DEFAULT_QUEUE) -> AsyncGenerator[TaskEnvelope]:
        """Consume task messages from the specified queue."""
        ...

    @abstractmethod
    async def ack(self, envelope: TaskEnvelope, queue: str = DEFAULT_QUEUE) -> None:
        """Acknowledge successful processing of a message."""
        ...

    @abstractmethod
    async def nack(
        self, envelope: TaskEnvelope, requeue: bool = True, queue: str = DEFAULT_QUEUE
    ) -> None:
        """Negatively acknowledge a message, optionally requeuing it."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop consuming messages."""
        ...

    @abstractmethod
    async def move_to_dlq(
        self,
        envelope: TaskEnvelope,
        error: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        """Move a failed message to the Dead Letter Queue."""
        ...

    @abstractmethod
    async def get_dlq_messages(
        self,
        queue: str = DEFAULT_QUEUE,
        count: int = 100,
    ) -> list[DLQMessage]:
        """Retrieve messages from the Dead Letter Queue."""
        ...

    @abstractmethod
    async def replay_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
        reset_retries: bool = True,
    ) -> bool:
        """Move a message from DLQ back to the main queue for reprocessing."""
        ...

    @abstractmethod
    async def delete_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
    ) -> bool:
        """Permanently delete a message from the DLQ."""
        ...

    @abstractmethod
    async def get_dlq_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        ...

    @abstractmethod
    async def get_pending_count(self, queue: str = DEFAULT_QUEUE) -> int: ...
    @abstractmethod
    async def get_queue_size(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of ready messages in queue."""
        ...

    @abstractmethod
    async def get_consumer_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of active consumers."""
        ...

    @abstractmethod
    async def purge_queue(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from queue. Returns count deleted."""
        ...

    @abstractmethod
    async def healthcheck(self) -> BrokerStatus:
        """Check the health of the broker connection."""
        ...
