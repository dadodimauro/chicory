from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from aio_pika.pool import Pool
from pydantic import BaseModel

from chicory.logging import logger
from chicory.types import BrokerStatus, DeliveryMode, TaskMessage

from .base import DEFAULT_QUEUE, Broker, DLQMessage, TaskEnvelope

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from chicory.config import RabbitMQBrokerConfig


class DLQData(BaseModel):
    data: str
    failed_at: str  # ISO timestamp
    error: str | None
    retry_count: int
    task_id: str
    task_name: str
    original_queue: str


class RabbitMQBroker(Broker):
    """RabbitMQ-based message broker with DLQ and configurable delivery semantics."""

    def __init__(
        self,
        config: RabbitMQBrokerConfig,
        delivery_mode: DeliveryMode = DeliveryMode.AT_MOST_ONCE,
        consumer_name: str | None = None,
    ) -> None:
        self.dsn = config.dsn
        self.connection_pool_size = config.connection_pool_size
        self.channel_pool_size = config.channel_pool_size
        self.prefetch_count = config.prefetch_count
        self.queue_max_length = config.queue_max_length
        self.queue_max_length_bytes = config.queue_max_length_bytes
        self.dlq_max_length = config.dlq_max_length
        self.message_ttl = config.message_ttl
        self.dlq_message_ttl = config.dlq_message_ttl
        self.durable_queues = config.durable_queues
        self.queue_mode = config.queue_mode

        self.delivery_mode = delivery_mode
        self.consumer_name = consumer_name

        self._connection_pool: Pool[AbstractRobustConnection] | None = None
        self._channel_pool: Pool[AbstractChannel] | None = None
        self._running = False
        self._consumer_tag: str | None = None

    async def _get_connection(self) -> AbstractRobustConnection:
        """Create a new RobustConnection to RabbitMQ."""
        return await aio_pika.connect_robust(self.dsn)

    async def _get_channel(self) -> AbstractChannel:
        """Get a channel from the connection pool."""
        if not self._connection_pool:
            raise RuntimeError("Connection pool is not initialized")

        async with self._connection_pool.acquire() as connection:
            return await connection.channel()

    def _queue_name(self, queue: str) -> str:
        """Get the main queue name."""
        return f"chicory.queue.{queue}"

    def _dlq_name(self, queue: str) -> str:
        """Get the DLQ name for a queue."""
        return f"chicory.dlq.{queue}"

    def _delayed_exchange_name(self, queue: str) -> str:
        """Get the delayed exchange name for a queue."""
        return f"chicory.delayed.{queue}"

    def _delayed_queue_name(self, queue: str) -> str:
        """Get the delayed queue name."""
        return f"chicory.delayed-queue.{queue}"

    async def _declare_queue(
        self,
        channel: AbstractChannel,
        queue: str,
        dlq_routing: bool = False,
    ) -> AbstractQueue:
        """Declare a queue with appropriate settings."""
        queue_name = self._queue_name(queue)

        arguments: dict[str, Any] = {}
        if self.queue_mode:
            arguments["x-queue-mode"] = self.queue_mode
        if self.queue_max_length:
            arguments["x-max-length"] = self.queue_max_length
        if self.queue_max_length_bytes:
            arguments["x-max-length-bytes"] = self.queue_max_length_bytes
        if self.message_ttl:
            arguments["x-message-ttl"] = self.message_ttl
        if dlq_routing:
            dlq_name = self._dlq_name(queue)
            arguments["x-dead-letter-exchange"] = ""  # Default exchange
            arguments["x-dead-letter-routing-key"] = dlq_name

        return await channel.declare_queue(
            queue_name,
            durable=self.durable_queues,
            arguments=arguments if arguments else None,
        )

    async def _declare_dlq(
        self,
        channel: AbstractChannel,
        queue: str,
    ) -> AbstractQueue:
        """Declare the Dead Letter Queue."""
        dlq_name = self._dlq_name(queue)

        arguments: dict[str, Any] = {}
        if self.dlq_max_length:
            arguments["x-max-length"] = self.dlq_max_length
        if self.dlq_message_ttl:
            arguments["x-message-ttl"] = self.dlq_message_ttl

        return await channel.declare_queue(
            dlq_name,
            durable=self.durable_queues,
            arguments=arguments if arguments else None,
        )

    async def _declare_delayed_infrastructure(
        self,
        channel: AbstractChannel,
        queue: str,
    ) -> tuple[AbstractQueue, str]:
        """
        Declare infrastructure for delayed/scheduled tasks.

        Uses a pattern where delayed messages are sent to a temporary queue
        with TTL, which then routes to the main queue via DLX.
        """
        main_queue_name = self._queue_name(queue)
        delayed_queue_name = self._delayed_queue_name(queue)

        # Delayed queue routes to main queue after TTL expires
        delayed_queue = await channel.declare_queue(
            delayed_queue_name,
            durable=self.durable_queues,
            arguments={
                "x-dead-letter-exchange": "",  # Default exchange
                "x-dead-letter-routing-key": main_queue_name,
            },
        )

        return delayed_queue, delayed_queue_name

    async def _ping(self) -> None:
        if not self._channel_pool:
            raise RuntimeError("Channel pool is not initialized")

        try:
            async with self._channel_pool.acquire() as channel:
                pass
        except Exception as exc:
            raise RuntimeError(f"Failed to connect to RabbitMQ: {exc}") from exc

    async def _publish_delayed(
        self,
        channel: AbstractChannel,
        message: TaskMessage,
        data: bytes,
        queue: str,
    ) -> None:
        """Publish a delayed task using TTL + DLX pattern."""
        if not message.eta:
            return

        delay_seconds = (message.eta - datetime.now(UTC)).total_seconds()

        await self._declare_delayed_infrastructure(channel, queue)

        # Publish with per-message TTL
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=data,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                if self.durable_queues
                else aio_pika.DeliveryMode.NOT_PERSISTENT,
                message_id=message.id,
                timestamp=datetime.now(UTC),
                expiration=delay_seconds,  # TTL for this specific message
                headers={
                    "task_id": message.id,
                    "task_name": message.name,
                    "scheduled_for": message.eta.isoformat(),
                },
            ),
            routing_key=self._delayed_queue_name(queue),
        )

    async def connect(self) -> None:
        """Establish connection to the broker."""
        self._connection_pool = Pool(
            self._get_connection, max_size=self.connection_pool_size
        )
        self._channel_pool = Pool(self._get_channel, max_size=self.channel_pool_size)

        await self._ping()

    async def disconnect(self) -> None:
        """Close connection to the broker."""
        self.stop()

        if self._channel_pool:
            await self._channel_pool.close()
            self._channel_pool = None

        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None

    async def publish(self, message: TaskMessage, queue: str = DEFAULT_QUEUE) -> None:
        """Publish a task message to the specified queue."""
        if not self._channel_pool:
            raise RuntimeError("Broker not connected")

        async with self._channel_pool.acquire() as channel:
            data = message.model_dump_json().encode()

            # Handle delayed/scheduled tasks
            if message.eta and message.eta > datetime.now(UTC):
                await self._publish_delayed(channel, message, data, queue)
                return

            # Declare queue with DLQ routing and ensure it exists
            await self._declare_queue(channel, queue, dlq_routing=True)
            await self._declare_dlq(channel, queue)

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=data,
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    if self.durable_queues
                    else aio_pika.DeliveryMode.NOT_PERSISTENT,
                    message_id=message.id,
                    timestamp=datetime.now(UTC),
                    headers={
                        "task_id": message.id,
                        "task_name": message.name,
                        "retries": message.retries,
                    },
                ),
                routing_key=self._queue_name(queue),
            )

    async def consume(self, queue: str = DEFAULT_QUEUE) -> AsyncGenerator[TaskEnvelope]:
        """Consume task messages from the specified queue."""
        if not self._channel_pool:
            raise RuntimeError("Broker not connected")

        async with self._channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=self.prefetch_count)

            rabbit_queue = await self._declare_queue(channel, queue, dlq_routing=True)
            await self._declare_dlq(channel, queue)

            self._running = True

            # Start consuming
            async with rabbit_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._running:
                        break

                    try:
                        task_message = TaskMessage.model_validate_json(
                            message.body.decode()
                        )

                        envelope = TaskEnvelope(
                            message=task_message,
                            delivery_tag=str(message.delivery_tag),
                            raw_data=message.body,
                        )

                        # AT_MOST_ONCE: acknowledge immediately before processing
                        if self.delivery_mode == DeliveryMode.AT_MOST_ONCE:
                            await message.ack()

                        # Store reference for potential nack/ack later
                        envelope._raw_message = message

                        yield envelope

                    except Exception:
                        # Malformed message - acknowledge and skip
                        await message.ack()
                        continue

    async def ack(self, envelope: TaskEnvelope, queue: str = DEFAULT_QUEUE) -> None:
        """Acknowledge successful processing of a message."""
        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            raw_message = envelope._raw_message
            if not isinstance(raw_message, AbstractIncomingMessage):
                raise RuntimeError("Invalid raw message for acknowledgment")
            if raw_message and not raw_message.processed:
                await raw_message.ack()

    async def nack(
        self, envelope: TaskEnvelope, requeue: bool = True, queue: str = DEFAULT_QUEUE
    ) -> None:
        """Negatively acknowledge a message, optionally requeuing it."""
        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            raw_message = envelope._raw_message
            if not isinstance(raw_message, AbstractIncomingMessage):
                raise RuntimeError("Invalid raw message for negative acknowledgment")
            if raw_message and not raw_message.processed:
                await raw_message.nack(requeue=requeue)

    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False

    async def move_to_dlq(
        self,
        envelope: TaskEnvelope,
        error: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        """Move a failed message to the Dead Letter Queue."""
        if not self._channel_pool:
            raise RuntimeError("Broker not connected")

        async with self._channel_pool.acquire() as channel:
            dlq = await self._declare_dlq(channel, queue)

            dlq_data = DLQData(
                data=envelope.message.model_dump_json(),
                failed_at=datetime.now(UTC).isoformat(),
                error=error,
                retry_count=envelope.message.retries,
                task_id=envelope.message.id,
                task_name=envelope.message.name,
                original_queue=queue,
            )

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=dlq_data.model_dump_json().encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    if self.durable_queues
                    else aio_pika.DeliveryMode.NOT_PERSISTENT,
                    timestamp=datetime.now(UTC),
                    headers={
                        "task_id": envelope.message.id,
                        "task_name": envelope.message.name,
                        "original_queue": queue,
                        "failed_at": datetime.now(UTC).isoformat(),
                    },
                ),
                routing_key=dlq.name,
            )

            # Acknowledge the original message to remove it from the main queue
            raw_message: AbstractIncomingMessage | None = getattr(
                envelope, "_raw_message", None
            )
            if raw_message and not raw_message.processed:
                await raw_message.ack()

    async def get_dlq_messages(
        self,
        queue: str = DEFAULT_QUEUE,
        count: int = 100,
    ) -> list[DLQMessage]:
        """Retrieve messages from the Dead Letter Queue."""
        if not self._channel_pool:
            return []

        async with self._channel_pool.acquire() as channel:
            dlq = await self._declare_dlq(channel, queue)

            messages: list[DLQMessage] = []

            # Use basic_get to peek at messages without consuming them
            for _ in range(count):
                message = await dlq.get(fail=False)
                if not message:
                    break

                try:
                    dlq_data = DLQData.model_validate_json(message.body)

                    original_message = TaskMessage.model_validate_json(dlq_data.data)

                    messages.append(
                        DLQMessage(
                            message_id=str(message.delivery_tag),
                            original_message=original_message,
                            failed_at=dlq_data.failed_at,
                            error=dlq_data.error,
                            retry_count=dlq_data.retry_count,
                        )
                    )

                    # Requeue the message so we don't consume it
                    await message.nack(requeue=True)

                except Exception:
                    # Skip malformed messages and acknowledge them
                    logger.warning(
                        "Malformed DLQ message, acknowledging and skipping",
                        exc_info=True,
                    )
                    await message.ack()
                    continue

            return messages

    async def replay_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
        reset_retries: bool = True,
    ) -> bool:
        """Move a message from DLQ back to the main queue for reprocessing."""
        if not self._channel_pool:
            return False

        async with self._channel_pool.acquire() as channel:
            dlq = await self._declare_dlq(channel, queue)

            # Search for the specific message
            # Note: RabbitMQ doesn't have native message ID lookup,
            # so we need to iterate through messages
            target_message: AbstractIncomingMessage | None = None
            messages_to_requeue: list[AbstractIncomingMessage] = []

            try:
                for _ in range(10000):  # Safety limit
                    message = await dlq.get(fail=False)
                    if not message:
                        break

                    if str(message.delivery_tag) == message_id:
                        target_message = message
                        break
                    messages_to_requeue.append(message)

                # Requeue messages we didn't want
                for msg in messages_to_requeue:
                    await msg.nack(requeue=True)

                if not target_message:
                    return False

                dlq_data = DLQData.model_validate_json(target_message.body)
                task_message = TaskMessage.model_validate_json(dlq_data.data)

                # Reset retry count if requested
                if reset_retries:
                    task_message = TaskMessage(
                        id=task_message.id,
                        name=task_message.name,
                        args=task_message.args,
                        kwargs=task_message.kwargs,
                        retries=0,
                        eta=None,
                        first_failure_at=None,
                        last_error=None,
                    )

                # Publish back to main queue
                await self.publish(task_message, queue)

                # Acknowledge the DLQ message
                await target_message.ack()

                return True

            except Exception:
                logger.warning(
                    "Error replaying message from DLQ, requeuing messages",
                    exc_info=True,
                )
                if target_message:
                    await target_message.nack(requeue=True)
                for msg in messages_to_requeue:
                    await msg.nack(requeue=True)
                return False

    async def delete_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
    ) -> bool:
        """Permanently delete a message from the DLQ."""
        if not self._channel_pool:
            return False

        async with self._channel_pool.acquire() as channel:
            dlq = await self._declare_dlq(channel, queue)

            # Search for and delete the specific message
            messages_to_requeue: list[AbstractIncomingMessage] = []
            found = False

            try:
                for _ in range(10000):  # Safety limit
                    message = await dlq.get(fail=False)
                    if not message:
                        break

                    if str(message.delivery_tag) == message_id:
                        await message.ack()  # Delete it
                        found = True
                        break
                    messages_to_requeue.append(message)

                # Requeue messages we didn't want to delete
                for msg in messages_to_requeue:
                    await msg.nack(requeue=True)

                return found

            except Exception:
                # On error, requeue all messages
                logger.warning(
                    "Error deleting message from DLQ, requeuing messages",
                    exc_info=True,
                )
                for msg in messages_to_requeue:
                    await msg.nack(requeue=True)
                return False

    async def get_dlq_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        if not self._channel_pool:
            return 0

        async with self._channel_pool.acquire() as channel:
            dlq = await self._declare_dlq(channel, queue)

            try:
                return dlq.declaration_result.message_count or 0
            except Exception:
                return 0

    async def get_pending_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """
        Get the number of pending (unacknowledged) messages.

        Note: In RabbitMQ, this represents messages that are unacked by consumers.
        """
        if not self._channel_pool:
            return 0

        async with self._channel_pool.acquire() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                # TODO @dadodimauro: https://github.com/dadodimauro/chicory/issues/3
                # This gives us messages being processed but not yet acked
                # Note: RabbitMQ doesn't expose this directly via AMQP,
                # would need management API for exact unacked count.
                return 0
            except Exception:
                logger.exception("Error getting pending count for queue %s", queue)
                return 0

    async def get_queue_size(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of ready messages in queue."""
        if not self._channel_pool:
            return 0

        async with self._channel_pool.acquire() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                return rabbit_queue.declaration_result.message_count or 0
            except Exception:
                logger.exception("Error getting queue size for queue %s", queue)
                return 0

    async def get_consumer_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of active consumers."""
        if not self._channel_pool:
            return 0

        async with self._channel_pool.acquire() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                return rabbit_queue.declaration_result.consumer_count or 0
            except Exception:
                logger.exception("Error getting consumer count for queue %s", queue)
                return 0

    async def purge_queue(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from queue. Returns count deleted."""
        if not self._channel_pool:
            return 0

        async with self._channel_pool.acquire() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                result = await rabbit_queue.purge()
                return result.message_count or 0
            except Exception:
                logger.exception("Error purging queue %s", queue)
                return 0

    async def healthcheck(self) -> BrokerStatus:
        """Check the health of the broker connection."""
        if not self._channel_pool:
            return BrokerStatus(connected=False, error="Not connected")

        try:
            async with self._channel_pool.acquire() as channel:
                pass  # If this succeeds, we're healthy
            return BrokerStatus(connected=True)
        except Exception as e:
            return BrokerStatus(connected=False, error=str(e))
