from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

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


@dataclass
class DLQMessage:
    """A message in the Dead Letter Queue."""

    message_id: str
    original_message: TaskMessage
    failed_at: str  # ISO timestamp
    error: str | None
    retry_count: int


@runtime_checkable
class Broker(Protocol):
    """Protocol for message brokers."""

    async def connect(self) -> None:
        """Establish connection to the broker."""
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Close connection to the broker."""
        raise NotImplementedError

    async def publish(self, message: TaskMessage, queue: str = DEFAULT_QUEUE) -> None:
        """Publish a task message to the specified queue."""
        raise NotImplementedError

    def consume(self, queue: str = DEFAULT_QUEUE) -> AsyncGenerator[TaskEnvelope]:
        """Consume task messages from the specified queue."""
        raise NotImplementedError

    async def ack(self, envelope: TaskEnvelope, queue: str = DEFAULT_QUEUE) -> None:
        """Acknowledge successful processing of a message."""
        raise NotImplementedError

    async def nack(
        self, envelope: TaskEnvelope, requeue: bool = True, queue: str = DEFAULT_QUEUE
    ) -> None:
        """Negatively acknowledge a message, optionally requeuing it."""
        raise NotImplementedError

    def stop(self) -> None:
        """Stop consuming messages."""
        raise NotImplementedError

    async def move_to_dlq(
        self,
        envelope: TaskEnvelope,
        error: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        """Move a failed message to the Dead Letter Queue."""
        raise NotImplementedError

    async def get_dlq_messages(
        self,
        queue: str = DEFAULT_QUEUE,
        count: int = 100,
    ) -> list[DLQMessage]:
        """Retrieve messages from the Dead Letter Queue."""
        raise NotImplementedError

    async def replay_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
    ) -> bool:
        """Move a message from DLQ back to the main queue for reprocessing."""
        raise NotImplementedError

    async def delete_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
    ) -> bool:
        """Permanently delete a message from the DLQ."""
        raise NotImplementedError

    async def get_dlq_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        raise NotImplementedError

    async def get_queue_size(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of ready messages in queue."""
        raise NotImplementedError

    async def get_consumer_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of active consumers."""
        raise NotImplementedError

    async def purge_queue(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from queue. Returns count deleted."""
        raise NotImplementedError

    async def healthcheck(self) -> BrokerStatus:
        """Check the health of the broker connection."""
        raise NotImplementedError
