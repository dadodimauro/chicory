from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
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
    moved_by: str


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
        self.max_priority = config.max_priority
        self.channel_acquire_timeout = config.channel_acquire_timeout
        self.reconnect_delay_base = config.reconnect_delay_base
        self.reconnect_delay_max = config.reconnect_delay_max
        self.max_dlq_scan_limit = config.max_dlq_scan_limit

        self.delivery_mode = delivery_mode
        self.consumer_tag = consumer_name or f"worker-{uuid.uuid4().hex[:8]}"

        # Connection pool for publishing operations
        self._connection_pool: Pool[AbstractRobustConnection] | None = None
        self._channel_pool: Pool[AbstractChannel] | None = None

        # Dedicated connection/channel for consuming
        self._consumer_connection: AbstractRobustConnection | None = None
        self._consumer_channel: AbstractChannel | None = None

        self._running = False
        self._declared_queues: set[str] = set()
        self._declaration_lock = asyncio.Lock()

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
        force: bool = False,
    ) -> AbstractQueue:
        """
        Declare a queue with appropriate settings.

        Args:
            channel: The channel to use for declaration
            queue: The queue name
            dlq_routing: Whether to set up DLQ routing
            force: Force re-declaration even if already declared
        """
        queue_name = self._queue_name(queue)

        if not force and queue_name in self._declared_queues:
            return await channel.get_queue(queue_name, ensure=False)

        async with self._declaration_lock:
            # Check again after acquiring lock
            if not force and queue_name in self._declared_queues:
                return await channel.get_queue(queue_name, ensure=False)

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
            if self.max_priority is not None:
                arguments["x-max-priority"] = self.max_priority

            result = await channel.declare_queue(
                queue_name,
                durable=self.durable_queues,
                arguments=arguments if arguments else None,
            )

            self._declared_queues.add(queue_name)
            return result

    async def _declare_dlq(
        self,
        channel: AbstractChannel,
        queue: str,
        force: bool = False,
    ) -> AbstractQueue:
        """Declare the Dead Letter Queue."""
        dlq_name = self._dlq_name(queue)

        if not force and dlq_name in self._declared_queues:
            return await channel.get_queue(dlq_name, ensure=False)

        async with self._declaration_lock:
            if not force and dlq_name in self._declared_queues:
                return await channel.get_queue(dlq_name, ensure=False)

            arguments: dict[str, Any] = {}
            if self.dlq_max_length:
                arguments["x-max-length"] = self.dlq_max_length
            if self.dlq_message_ttl:
                arguments["x-message-ttl"] = self.dlq_message_ttl

            result = await channel.declare_queue(
                dlq_name,
                durable=self.durable_queues,
                arguments=arguments if arguments else None,
            )

            self._declared_queues.add(dlq_name)
            return result

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
        """Verify broker connectivity."""
        if not self._channel_pool:
            raise RuntimeError("Channel pool is not initialized")

        try:
            async with asyncio.timeout(5.0):
                async with self._channel_pool.acquire() as channel:
                    pass
        except TimeoutError as exc:
            raise TimeoutError("Timeout while connecting to RabbitMQ") from exc
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
                # TODO @dadodimauro: Verify if expiration should be in ms  # noqa: TD003
                expiration=delay_seconds,  # TTL for this specific message
                priority=message.priority,
                headers={
                    "task_id": message.id,
                    "task_name": message.name,
                    "scheduled_for": message.eta.isoformat(),
                    "published_by": self.consumer_tag,
                },
            ),
            routing_key=self._delayed_queue_name(queue),
        )

    async def _setup_consumer_channel(self, queue: str) -> AbstractQueue:
        """Set up a dedicated consumer connection and channel."""
        if self._consumer_connection is None:
            self._consumer_connection = await aio_pika.connect_robust(self.dsn)

        if self._consumer_channel is None or self._consumer_channel.is_closed:
            self._consumer_channel = await self._consumer_connection.channel()
            await self._consumer_channel.set_qos(prefetch_count=self.prefetch_count)

        rabbit_queue = await self._declare_queue(
            self._consumer_channel, queue, dlq_routing=True
        )
        await self._declare_dlq(self._consumer_channel, queue)

        return rabbit_queue

    async def _close_consumer_channel(self) -> None:
        """Close the dedicated consumer channel and connection."""
        if self._consumer_channel and not self._consumer_channel.is_closed:
            await self._consumer_channel.close()
            self._consumer_channel = None

        if self._consumer_connection and not self._consumer_connection.is_closed:
            await self._consumer_connection.close()
            self._consumer_connection = None

    @asynccontextmanager
    async def _acquire_channel(self) -> AsyncGenerator[AbstractChannel]:
        """
        Acquire a channel with proper error handling.
        """
        if not self._channel_pool:
            raise RuntimeError("Broker not connected")

        try:
            async with asyncio.timeout(
                self.channel_acquire_timeout
            ):  # Prevent deadlock
                async with self._channel_pool.acquire() as channel:
                    yield channel
        except TimeoutError:
            logger.error("Timeout acquiring channel from pool")
            raise RuntimeError("Channel pool exhausted or deadlocked")

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

        await self._close_consumer_channel()

        if self._channel_pool:
            await self._channel_pool.close()
            self._channel_pool = None

        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None

        self._declared_queues.clear()
        logger.info("RabbitMQ broker disconnected")

    async def publish(self, message: TaskMessage, queue: str = DEFAULT_QUEUE) -> None:
        """Publish a task message to the specified queue."""
        async with self._acquire_channel() as channel:
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
                    priority=message.priority,
                    headers={
                        "task_id": message.id,
                        "task_name": message.name,
                        "retries": message.retries,
                        "published_by": self.consumer_tag,
                    },
                ),
                routing_key=self._queue_name(queue),
            )

    async def consume(self, queue: str = DEFAULT_QUEUE) -> AsyncGenerator[TaskEnvelope]:
        """Consume task messages from the specified queue."""
        self._running = True
        reconnect_delay = self.reconnect_delay_base

        while self._running:
            try:
                rabbit_queue = await self._setup_consumer_channel(queue)
                reconnect_delay = self.reconnect_delay_base

                # Start consuming
                async with rabbit_queue.iterator(
                    consumer_tag=self.consumer_tag
                ) as queue_iter:
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

                            # Store reference for potential nack/ack later
                            envelope._raw_message = message

                            yield envelope

                        except Exception:
                            # Malformed message - acknowledge and skip
                            logger.warning(
                                "Malformed message received, "
                                "acknowledging and skipping",
                                exc_info=True,
                            )
                            await message.ack()
                            continue
            except asyncio.CancelledError:
                logger.debug("Consumer cancelled")
                break
            except Exception:
                if not self._running:
                    break

                logger.exception(
                    f"Consumer error, reconnecting in {reconnect_delay:.1f}s"
                )
                await self._close_consumer_channel()
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.reconnect_delay_max)

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
        raw_message: AbstractIncomingMessage | None = getattr(
            envelope, "_raw_message", None
        )

        # Use native RabbitMQ DLX by rejecting with requeue=False
        # This leverages the DLX we configured on the queue
        if raw_message and not raw_message.processed:
            try:
                # Reject without requeue - RabbitMQ will route to DLX
                await raw_message.reject(requeue=False)
                logger.debug(f"Message {envelope.message.id} rejected to DLQ via DLX")
                return
            except Exception:
                logger.warning(
                    "Failed to reject message via DLX, falling back to manual move",
                    exc_info=True,
                )

        # Fallback: Manual move to DLQ (for cases where DLX isn't available)
        await self._manual_move_to_dlq(envelope, error, queue)

    async def _manual_move_to_dlq(
        self,
        envelope: TaskEnvelope,
        error: str | None,
        queue: str,
    ) -> None:
        """Manually move a message to DLQ."""
        async with self._acquire_channel() as channel:
            dlq = await self._declare_dlq(channel, queue)

            dlq_data = DLQData(
                data=envelope.message.model_dump_json(),
                failed_at=datetime.now(UTC).isoformat(),
                error=error,
                retry_count=envelope.message.retries,
                task_id=envelope.message.id,
                task_name=envelope.message.name,
                original_queue=queue,
                moved_by=self.consumer_tag,
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
                        "moved_by": self.consumer_tag,
                    },
                ),
                routing_key=dlq.name,
            )

            # acknowledge the original message
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
        if count > 1000:
            logger.warning(
                f"Requesting {count} DLQ messages may be slow. "
                "Consider using pagination or the RabbitMQ Management API."
            )

        async with self._acquire_channel() as channel:
            dlq = await self._declare_dlq(channel, queue)

            messages: list[DLQMessage] = []

            # Use basic_get to peek at messages without consuming them
            for _ in range(min(count, self.max_dlq_scan_limit)):
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
        async with self._acquire_channel() as channel:
            dlq = await self._declare_dlq(channel, queue)

            # Search for the specific message
            # Note: RabbitMQ doesn't have native message ID lookup,
            # so we need to iterate through messages
            target_message: AbstractIncomingMessage | None = None
            messages_to_requeue: list[AbstractIncomingMessage] = []
            scanned = 0

            try:
                for _ in range(self.max_dlq_scan_limit):  # Safety limit
                    scanned += 1
                    message = await dlq.get(fail=False)
                    if not message:
                        break

                    if str(message.delivery_tag) == message_id:
                        target_message = message
                        break
                    messages_to_requeue.append(message)

                if scanned >= self.max_dlq_scan_limit and not target_message:
                    logger.warning(
                        f"Scanned {self.max_dlq_scan_limit} messages without "
                        f"finding message_id={message_id}. Message may not exist "
                        "or DLQ is too large. Consider using RabbitMQ Management API."
                    )

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
        async with self._acquire_channel() as channel:
            dlq = await self._declare_dlq(channel, queue)

            # Search for and delete the specific message
            messages_to_requeue: list[AbstractIncomingMessage] = []
            found = False
            scanned = 0

            try:
                for _ in range(self.max_dlq_scan_limit):  # Safety limit
                    scanned += 1
                    message = await dlq.get(fail=False)
                    if not message:
                        break

                    if str(message.delivery_tag) == message_id:
                        await message.ack()  # Delete it
                        found = True
                        break
                    messages_to_requeue.append(message)

                if scanned >= self.max_dlq_scan_limit and not found:
                    logger.warning(
                        f"Scanned {self.max_dlq_scan_limit} messages without "
                        f"finding message_id={message_id}."
                    )

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
        async with self._acquire_channel() as channel:
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
        async with self._acquire_channel() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                # TODO @dadodimauro:  # noqa: TD003
                # RabbitMQ doesn't expose unacked count via AMQP directly.
                # This returns consumer count as a proxy - real implementation
                # would need the Management API.
                # For now, we return 0 and log that Management API is needed.
                logger.debug(
                    "get_pending_count requires RabbitMQ Management API for accuracy"
                )
                return 0
            except Exception:
                logger.exception("Error getting pending count for queue %s", queue)
                return 0

    async def get_queue_size(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of ready messages in queue."""
        async with self._acquire_channel() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                return rabbit_queue.declaration_result.message_count or 0
            except Exception:
                logger.exception("Error getting queue size for queue %s", queue)
                return 0

    async def get_consumer_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of active consumers."""
        async with self._acquire_channel() as channel:
            try:
                rabbit_queue = await self._declare_queue(channel, queue)
                return rabbit_queue.declaration_result.consumer_count or 0
            except Exception:
                logger.exception("Error getting consumer count for queue %s", queue)
                return 0

    async def purge_queue(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from queue. Returns count deleted."""
        async with self._acquire_channel() as channel:
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
            await self._ping()
            return BrokerStatus(connected=True)
        except TimeoutError:
            return BrokerStatus(connected=False, error="Health check timed out")
        except Exception as e:
            return BrokerStatus(connected=False, error=str(e))
