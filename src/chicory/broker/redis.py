from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import redis.asyncio as redis

from chicory.config import RedisBrokerConfig  # noqa: TC001
from chicory.types import BrokerStatus, DeliveryMode, TaskMessage

from .base import DEFAULT_QUEUE, Broker, DLQMessage, TaskEnvelope

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class RedisBroker(Broker):
    """
    Redis Streams-based message broker with DLQ and configurable delivery semantics.
    """

    def __init__(
        self,
        config: RedisBrokerConfig,
        delivery_mode: DeliveryMode = DeliveryMode.AT_MOST_ONCE,
        consumer_name: str | None = None,
    ) -> None:
        self.dsn = config.dsn
        self.consumer_group = config.consumer_group
        self.block_ms = config.block_ms
        self.claim_min_idle_ms = config.claim_min_idle_ms
        self.max_stream_length = config.max_stream_length
        self.dlq_max_length = config.dlq_max_length

        self.delivery_mode = delivery_mode
        self.consumer_name = consumer_name or f"worker-{uuid.uuid4().hex[:8]}"

        self._pool: redis.ConnectionPool | None = None
        self._client: redis.Redis | None = None
        self._running = False

    async def connect(self) -> None:
        self._pool = redis.ConnectionPool.from_url(self.dsn)
        self._client = redis.Redis(
            connection_pool=self._pool,
            auto_close_connection_pool=False,
            decode_responses=False,  # Keep as bytes for consistency
        )
        await self._client.ping()  # type: ignore[unused-awaitable]

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None

    def _stream_key(self, queue: str) -> str:
        return f"chicory:stream:{queue}"

    def _delayed_key(self, queue: str) -> str:
        return f"chicory:delayed:{queue}"

    def _dlq_key(self, queue: str) -> str:
        return f"chicory:dlq:{queue}"

    async def _ensure_consumer_group(self, queue: str) -> None:
        """Create consumer group if it doesn't exist."""
        if not self._client:
            return

        stream_key = self._stream_key(queue)
        try:
            await self._client.xgroup_create(
                stream_key,
                self.consumer_group,
                id="0",
                mkstream=True,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):  # Group already exists
                raise

    async def publish(self, message: TaskMessage, queue: str = DEFAULT_QUEUE) -> None:
        """Publish a message to the stream."""
        if not self._client:
            raise RuntimeError("Broker not connected")

        data = message.model_dump_json()

        if message.eta and message.eta > datetime.now(UTC):
            # Use sorted set for delayed tasks
            score = message.eta.timestamp()
            await self._client.zadd(self._delayed_key(queue), {data: score})
            return

        stream_key = self._stream_key(queue)

        # XADD with optional MAXLEN for stream trimming
        kwargs: dict[str, Any] = {"fields": {"data": data, "task_id": message.id}}
        if self.max_stream_length:
            kwargs["maxlen"] = self.max_stream_length
            kwargs["approximate"] = True

        await self._client.xadd(stream_key, **kwargs)

    async def _move_ready_delayed(self, queue: str) -> None:
        """Move delayed tasks that are ready to the stream."""
        if not self._client:
            return

        now = datetime.now(UTC).timestamp()
        delayed_key = self._delayed_key(queue)
        stream_key = self._stream_key(queue)

        # Get all tasks with score <= now
        ready = await self._client.zrangebyscore(delayed_key, 0, now)

        if ready:
            pipe = self._client.pipeline()
            for task_data in ready:
                pipe.xadd(stream_key, {"data": task_data})
                pipe.zrem(delayed_key, task_data)
            await pipe.execute()

    async def _claim_stale_messages(self, queue: str) -> list[TaskEnvelope]:
        """Claim messages that have been pending too long (dead consumer recovery)."""
        if not self._client:
            return []

        stream_key = self._stream_key(queue)
        envelopes = []

        try:
            # XAUTOCLAIM: automatically claim messages idle for too long
            result = await self._client.xautoclaim(
                stream_key,
                self.consumer_group,
                self.consumer_name,
                min_idle_time=self.claim_min_idle_ms,
                start_id="0-0",
                count=10,
            )

            if result and len(result) >= 2:
                messages = result[1]  # Format: [cursor, messages, ...]
                for msg_id, fields in messages:
                    if fields and b"data" in fields:
                        data = fields[b"data"]
                        message = TaskMessage.model_validate_json(data)
                        envelopes.append(
                            TaskEnvelope(
                                message=message,
                                delivery_tag=msg_id.decode()
                                if isinstance(msg_id, bytes)
                                else msg_id,
                                raw_data=data,
                            )
                        )
        except redis.ResponseError:
            pass  # Group might not exist yet

        return envelopes

    async def consume(self, queue: str = DEFAULT_QUEUE) -> AsyncGenerator[TaskEnvelope]:
        if not self._client:
            raise RuntimeError("Broker not connected")

        await self._ensure_consumer_group(queue)

        self._running = True
        stream_key = self._stream_key(queue)
        last_claim_check = 0.0

        while self._running:
            # Periodically move ready delayed tasks
            await self._move_ready_delayed(queue)

            # Periodically check for stale messages to reclaim (at-least-once)
            now = asyncio.get_event_loop().time()
            if (
                self.delivery_mode == DeliveryMode.AT_LEAST_ONCE
                and now - last_claim_check > 10
            ):
                stale = await self._claim_stale_messages(queue)
                for envelope in stale:
                    yield envelope
                last_claim_check = now

            try:
                # XREADGROUP: read new messages for this consumer
                # Use ">" to read only new messages not yet delivered to any consumer
                result = await self._client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams={stream_key: ">"},
                    count=1,
                    block=self.block_ms,
                )

                if result:
                    for stream_name, messages in result:
                        for msg_id, fields in messages:
                            if fields and b"data" in fields:
                                data = fields[b"data"]
                                message = TaskMessage.model_validate_json(data)

                                delivery_tag = (
                                    msg_id.decode()
                                    if isinstance(msg_id, bytes)
                                    else msg_id
                                )

                                # AT_MOST_ONCE: ack immediately before processing
                                if self.delivery_mode == DeliveryMode.AT_MOST_ONCE:
                                    await self._client.xack(
                                        stream_key, self.consumer_group, msg_id
                                    )

                                yield TaskEnvelope(
                                    message=message,
                                    delivery_tag=delivery_tag,
                                    raw_data=data,
                                )

            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    await self._ensure_consumer_group(queue)
                else:
                    raise

    async def ack(self, envelope: TaskEnvelope, queue: str = DEFAULT_QUEUE) -> None:
        """Acknowledge successful processing (only meaningful for at-least-once)."""
        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE and self._client:
            await self._client.xack(
                self._stream_key(queue),
                self.consumer_group,
                envelope.delivery_tag,
            )

    async def nack(
        self, envelope: TaskEnvelope, requeue: bool = True, queue: str = DEFAULT_QUEUE
    ) -> None:
        """Negative acknowledgment - handle failed processing."""
        if not self._client:
            return

        stream_key = self._stream_key(queue)

        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            if requeue:
                # Don't ack - message stays in PEL and will be reclaimed
                pass
            else:
                # Acknowledge to remove from PEL
                await self._client.xack(
                    stream_key, self.consumer_group, envelope.delivery_tag
                )

    async def move_to_dlq(
        self,
        envelope: TaskEnvelope,
        error: str | None = None,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        """Move a failed message to the Dead Letter Queue."""
        if not self._client:
            raise RuntimeError("Broker not connected")

        dlq_key = self._dlq_key(queue)
        stream_key = self._stream_key(queue)

        # Build DLQ entry with metadata
        dlq_entry = {
            "data": envelope.message.model_dump_json(),
            "failed_at": datetime.now(UTC).isoformat(),
            "error": error or "",
            "retry_count": str(envelope.message.retries),
            "original_stream_id": envelope.delivery_tag,
            "task_id": envelope.message.id,
            "task_name": envelope.message.name,
        }

        # Use pipeline for atomicity
        pipe = self._client.pipeline()

        # Add to DLQ stream
        kwargs: dict[str, Any] = {"fields": dlq_entry}
        if self.dlq_max_length:
            kwargs["maxlen"] = self.dlq_max_length
            kwargs["approximate"] = True

        pipe.xadd(dlq_key, **kwargs)

        # ACK from main stream to remove from PEL
        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            pipe.xack(stream_key, self.consumer_group, envelope.delivery_tag)

        await pipe.execute()

    async def get_dlq_messages(
        self,
        queue: str = DEFAULT_QUEUE,
        count: int = 100,
        start_id: str = "-",
        end_id: str = "+",
    ) -> list[DLQMessage]:
        """Retrieve messages from the Dead Letter Queue."""
        if not self._client:
            return []

        dlq_key = self._dlq_key(queue)

        try:
            messages = await self._client.xrange(dlq_key, start_id, end_id, count=count)
        except redis.ResponseError:
            return []

        result = []
        for msg_id, fields in messages:
            stream_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

            # Parse message data
            data = fields.get(b"data", b"{}")
            if isinstance(data, bytes):
                data = data.decode()

            try:
                original_message = TaskMessage.model_validate_json(data)
            except Exception:
                continue  # Skip malformed messages

            error = fields.get(b"error", b"")
            if isinstance(error, bytes):
                error = error.decode()

            failed_at = fields.get(b"failed_at", b"")
            if isinstance(failed_at, bytes):
                failed_at = failed_at.decode()

            retry_count_raw = fields.get(b"retry_count", b"0")
            if isinstance(retry_count_raw, bytes):
                retry_count_raw = retry_count_raw.decode()
            retry_count = int(retry_count_raw)

            result.append(
                DLQMessage(
                    original_message=original_message,
                    failed_at=failed_at,
                    error=error if error else None,
                    retry_count=retry_count,
                    message_id=stream_id,
                )
            )

        return result

    async def replay_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
        reset_retries: bool = True,
    ) -> bool:
        """Move a message from DLQ back to the main queue for reprocessing."""
        if not self._client:
            return False

        dlq_key = self._dlq_key(queue)

        # Get the message from DLQ
        try:
            messages = await self._client.xrange(
                dlq_key, message_id, message_id, count=1
            )
        except redis.ResponseError:
            return False

        if not messages:
            return False

        _, fields = messages[0]
        data = fields.get(b"data", b"{}")
        if isinstance(data, bytes):
            data = data.decode()

        try:
            message = TaskMessage.model_validate_json(data)
        except Exception:
            return False

        # Reset retry count if requested
        if reset_retries:
            message = TaskMessage(
                id=message.id,
                name=message.name,
                args=message.args,
                kwargs=message.kwargs,
                retries=0,
                eta=None,
                first_failure_at=None,
                last_error=None,
            )

        # Use pipeline for atomicity
        pipe = self._client.pipeline()

        # Publish back to main queue
        stream_key = self._stream_key(queue)
        publish_kwargs: dict[str, Any] = {
            "fields": {"data": message.model_dump_json(), "task_id": message.id}
        }
        if self.max_stream_length:
            publish_kwargs["maxlen"] = self.max_stream_length
            publish_kwargs["approximate"] = True

        pipe.xadd(stream_key, **publish_kwargs)

        # Remove from DLQ
        pipe.xdel(dlq_key, message_id)

        await pipe.execute()
        return True

    async def delete_from_dlq(
        self,
        message_id: str,
        queue: str = DEFAULT_QUEUE,
    ) -> bool:
        """Permanently delete a message from the DLQ."""
        if not self._client:
            return False

        dlq_key = self._dlq_key(queue)

        try:
            deleted = await self._client.xdel(dlq_key, message_id)
            return deleted > 0
        except redis.ResponseError:
            return False

    async def purge_dlq(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from the DLQ. Returns count of deleted messages."""
        if not self._client:
            return 0

        dlq_key = self._dlq_key(queue)

        try:
            # Get count before deleting
            info = await self._client.xlen(dlq_key)
            await self._client.delete(dlq_key)
            return info
        except redis.ResponseError:
            return 0

    async def get_dlq_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        if not self._client:
            return 0

        dlq_key = self._dlq_key(queue)

        try:
            return await self._client.xlen(dlq_key)
        except redis.ResponseError:
            return 0

    async def get_pending_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get the number of pending (unacknowledged) messages."""
        if not self._client:
            return 0

        try:
            info = await self._client.xpending(
                self._stream_key(queue), self.consumer_group
            )
            return (
                info.get("pending", 0)
                if isinstance(info, dict)
                else (info[0] if info else 0)
            )
        except redis.ResponseError:
            return 0

    def stop(self) -> None:
        self._running = False

    async def get_queue_size(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of ready messages in queue."""
        if not self._client:
            return 0

        stream_key = self._stream_key(queue)

        try:
            return await self._client.xlen(stream_key)
        except redis.ResponseError:
            return 0

    async def get_consumer_count(self, queue: str = DEFAULT_QUEUE) -> int:
        """Get number of active consumers."""
        if not self._client:
            return 0

        try:
            info = await self._client.xinfo_consumers(
                self._stream_key(queue), self.consumer_group
            )
            return len(info)
        except redis.ResponseError:
            return 0

    async def purge_queue(self, queue: str = DEFAULT_QUEUE) -> int:
        """Delete all messages from queue. Returns count deleted."""
        if not self._client:
            return 0

        stream_key = self._stream_key(queue)

        try:
            count = await self._client.xlen(stream_key)
            await self._client.delete(stream_key)
            return count
        except redis.ResponseError:
            return 0

    async def healthcheck(self) -> BrokerStatus:
        """Check the health of the broker connection."""
        if not self._client:
            return BrokerStatus(connected=False, error="Not connected")

        try:
            await self._client.ping()  # type: ignore[unused-awaitable]
            return BrokerStatus(connected=True)
        except Exception as e:
            return BrokerStatus(connected=False, error=str(e))
