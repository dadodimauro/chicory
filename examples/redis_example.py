"""
Chicory Task Queue - Comprehensive Example
===========================================

This example demonstrates all major features of Chicory:
- Task definition and execution
- Retry policies with different backoff strategies
- Delivery modes (at-least-once vs at-most-once)
- Task context for advanced control
- Dead Letter Queue (DLQ) management
- Delayed/scheduled tasks
- Batch task processing
- Error handling and monitoring

Prerequisites:
--------------
1. Redis server running on localhost:6379
2. Install chicory: pip install chicory

Usage:
------
Terminal 1 - Start worker with DLQ enabled:
    chicory worker examples.redis:app --dlq

Terminal 2 - Run this example:
    python examples/redis.py
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from chicory import (
    AsyncResult,
    BackendType,
    BrokerType,
    Chicory,
    DeliveryMode,
    RetryBackoff,
    RetryPolicy,
    TaskContext,
    ValidationMode,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# APPLICATION SETUP
# ============================================================================

app = Chicory(
    broker=BrokerType.REDIS,
    backend=BackendType.REDIS,
    validation_mode=ValidationMode.INPUTS,
)


# ============================================================================
# BASIC TASKS
# ============================================================================


@app.task(name="examples.add")
async def add(x: int, y: int) -> int:
    """Simple addition task - demonstrates basic async task."""
    logger.info(f"Computing {x} + {y}")
    await asyncio.sleep(0.1)  # Simulate work
    return x + y


@app.task(
    name="examples.multiply",
    delivery_mode=DeliveryMode.AT_MOST_ONCE,
)
async def multiply(x: int, y: int) -> int:
    """At-most-once delivery - fire and forget, no retry guarantees."""
    return x * y


# ============================================================================
# RETRY POLICIES
# ============================================================================


@app.task(
    name="examples.flaky_api_call",
    retry_policy=RetryPolicy(
        max_retries=5,
        retry_delay=1.0,
        backoff=RetryBackoff.EXPONENTIAL,  # 1s, 2s, 4s, 8s, 16s
        max_delay=30.0,
        jitter=True,  # Add randomness to prevent thundering herd
    ),
)
async def flaky_api_call(url: str, attempt: int = 1) -> dict[str, Any]:
    """
    Simulates an unreliable API call with exponential backoff.
    Will retry up to 5 times with increasing delays.
    """
    logger.info(f"Calling API: {url} (attempt {attempt})")

    # Simulate failure on first 2 attempts
    if attempt <= 2:
        logger.warning(f"API call failed on attempt {attempt}")
        raise ConnectionError(f"API unavailable (attempt {attempt})")

    return {"status": "success", "data": f"Response from {url}", "attempt": attempt}


@app.task(
    name="examples.retry_with_context",
    retry_policy=RetryPolicy(
        max_retries=3,
        retry_delay=2.0,
        backoff=RetryBackoff.LINEAR,  # 2s, 4s, 6s
    ),
)
async def retry_with_context(ctx: TaskContext, value: int) -> int:
    """
    Demonstrates manual retry control using TaskContext.
    Check remaining retries and customize retry behavior.
    """
    logger.info(
        f"Processing value={value}, "
        f"retry {ctx.retries}/{ctx.retry_policy.max_retries if ctx.retry_policy else 0}"
    )

    # Conditional retry based on business logic
    if value < 0:
        if ctx.remaining_retries > 0:
            logger.warning(
                f"Negative value, retrying... ({ctx.remaining_retries} left)"
            )
            await ctx.retry(countdown=1.0)  # Custom retry delay
        else:
            logger.error("Max retries reached, failing task")
            await ctx.fail(ValueError("Value must be positive"))

    return value * 2


@app.task(
    name="examples.selective_retry",
    retry_policy=RetryPolicy(
        max_retries=3,
        retry_delay=1.0,
        backoff=RetryBackoff.FIXED,
        retry_on=["ConnectionError", "TimeoutError"],  # Only retry these
        ignore_on=["ValueError"],  # Never retry these - go straight to DLQ
    ),
)
async def selective_retry(operation: str) -> str:
    """
    Demonstrates selective retry based on exception types.
    - ConnectionError/TimeoutError: retried up to 3 times
    - ValueError: immediately sent to DLQ
    """
    logger.info(f"Executing operation: {operation}")

    if operation == "network_fail":
        raise ConnectionError("Network unavailable")  # Will be retried
    if operation == "invalid_input":
        raise ValueError("Invalid operation")  # Goes to DLQ immediately

    return f"Completed: {operation}"


# ============================================================================
# DELAYED/SCHEDULED TASKS
# ============================================================================


@app.task(name="examples.scheduled_reminder")
async def scheduled_reminder(message: str, scheduled_time: str) -> str:
    """Task that can be scheduled for future execution."""
    logger.info(f"Reminder triggered at {datetime.now(UTC)}: {message}")
    return f"Reminder sent: {message}"


# ============================================================================
# LONG-RUNNING TASKS
# ============================================================================


@app.task(
    name="examples.process_large_dataset",
    retry_policy=RetryPolicy(max_retries=2, retry_delay=5.0),
    delivery_mode=DeliveryMode.AT_LEAST_ONCE,
)
async def process_large_dataset(
    dataset_id: str, chunk_size: int = 100
) -> dict[str, Any]:
    """
    Simulates processing a large dataset in chunks.
    Demonstrates long-running task with progress tracking.
    """
    logger.info(f"Processing dataset {dataset_id} with chunk_size={chunk_size}")

    total_items = 1000
    processed = 0

    for chunk in range(0, total_items, chunk_size):
        await asyncio.sleep(0.1)  # Simulate processing
        processed += min(chunk_size, total_items - chunk)
        logger.info(f"Progress: {processed}/{total_items} items processed")

    return {
        "dataset_id": dataset_id,
        "total_items": total_items,
        "status": "completed",
        "processed_at": datetime.now(UTC).isoformat(),
    }


# ============================================================================
# FIRE-AND-FORGET TASKS
# ============================================================================


@app.task(
    name="examples.log_analytics_event",
    ignore_result=True,  # Don't store result in backend
    delivery_mode=DeliveryMode.AT_MOST_ONCE,
)
async def log_analytics_event(
    event_type: str, user_id: int, metadata: dict[str, Any]
) -> None:
    """
    Fire-and-forget analytics logging.
    Result not stored, best-effort delivery.
    """
    logger.info(f"Analytics: {event_type} for user {user_id}")
    # In production: send to analytics service, logging system, etc.
    await asyncio.sleep(0.05)


# ============================================================================
# ERROR-PRONE TASKS (for DLQ demonstration)
# ============================================================================


@app.task(
    name="examples.always_fails",
    retry_policy=RetryPolicy(
        max_retries=2, retry_delay=0.5, backoff=RetryBackoff.FIXED
    ),
)
async def always_fails(should_fail: bool = True) -> str:
    """
    Task that always fails - ends up in Dead Letter Queue.
    Useful for testing DLQ functionality.
    """
    if should_fail:
        raise RuntimeError("This task is designed to fail")
    return "Success!"


# ============================================================================
# CLIENT CODE - DEMONSTRATES USAGE
# ============================================================================


async def demo_basic_tasks():
    """Demonstrate basic task invocation and result retrieval."""
    logger.info("\n" + "=" * 60)
    logger.info("1. BASIC TASKS")
    logger.info("=" * 60)

    # Simple task with result
    result = await add.delay(10, 20)
    logger.info(f"✓ Sent 'add' task: {result.task_id}")
    value = await result.get(timeout=5)
    logger.info(f"✓ Result: {value}")

    # Fire-and-forget
    task_id = await log_analytics_event.send("page_view", 12345, {"page": "/home"})
    logger.info(f"✓ Sent fire-and-forget analytics event: {task_id}")


async def demo_retry_policies():
    """Demonstrate different retry strategies."""
    logger.info("\n" + "=" * 60)
    logger.info("2. RETRY POLICIES")
    logger.info("=" * 60)

    # Exponential backoff - simulates recovering API
    logger.info("\n→ Testing exponential backoff (will retry and succeed)...")
    result = await flaky_api_call.delay("https://api.example.com/data", attempt=1)
    logger.info(f"  Task ID: {result.task_id}")

    # Manual retry with context
    logger.info("\n→ Testing manual retry with context...")
    result = await retry_with_context.delay(-5)  # Negative value triggers retry
    logger.info(f"  Task ID: {result.task_id}")

    # Selective retry
    logger.info("\n→ Testing selective retry (ConnectionError - will retry)...")
    result = await selective_retry.delay("network_fail")
    logger.info(f"  Task ID: {result.task_id}")


async def demo_scheduled_tasks():
    """Demonstrate task scheduling with ETA."""
    logger.info("\n" + "=" * 60)
    logger.info("3. SCHEDULED/DELAYED TASKS")
    logger.info("=" * 60)

    # Schedule task for 5 seconds in the future
    eta = datetime.now(UTC) + timedelta(seconds=5)

    # Note: We need to manually create and publish the message with ETA
    import uuid

    from chicory.types import TaskMessage

    message = TaskMessage(
        id=str(uuid.uuid4()),
        name="examples.scheduled_reminder",
        args=["Meeting in 5 seconds!", eta.isoformat()],
        kwargs={},
        retries=0,
        eta=eta,
    )

    await app.broker.publish(message)
    logger.info(f"✓ Scheduled reminder for {eta.strftime('%H:%M:%S')}")
    logger.info(f"  Current time: {datetime.now(UTC).strftime('%H:%M:%S')}")


async def demo_batch_processing():
    """Demonstrate processing multiple tasks in parallel."""
    logger.info("\n" + "=" * 60)
    logger.info("4. BATCH PROCESSING")
    logger.info("=" * 60)

    # Send batch of tasks
    logger.info("\n→ Sending batch of 10 tasks...")
    results: list[AsyncResult[int]] = []
    for i in range(10):
        result = await add.delay(i, i * 2)
        results.append(result)

    logger.info(f"✓ Sent {len(results)} tasks")

    # Collect results
    logger.info("\n→ Collecting results...")
    for i, result in enumerate(results):
        value = await result.get(timeout=10)
        logger.info(f"  Task {i + 1}/10: {value}")


async def demo_dlq_operations():
    """Demonstrate Dead Letter Queue functionality."""
    logger.info("\n" + "=" * 60)
    logger.info("5. DEAD LETTER QUEUE (DLQ)")
    logger.info("=" * 60)

    # Send a task that will fail and end up in DLQ
    logger.info("\n→ Sending task that will fail (goes to DLQ)...")
    result = await always_fails.delay(should_fail=True)
    logger.info(f"  Task ID: {result.task_id}")

    # Wait a bit for retries to exhaust
    await asyncio.sleep(3)

    # Check DLQ (requires broker with DLQ support)
    if hasattr(app.broker, "get_dlq_count"):
        dlq_count = await app.broker.get_dlq_count()
        logger.info(f"\n✓ DLQ contains {dlq_count} message(s)")

        if dlq_count > 0:
            # List DLQ messages
            dlq_messages = await app.broker.get_dlq_messages(count=5)
            logger.info("\n→ DLQ Messages:")
            for msg in dlq_messages[:3]:  # Show first 3
                logger.info(f"  • Task: {msg.original_message.name}")
                logger.info(f"    Failed at: {msg.failed_at}")
                logger.info(f"    Error: {msg.error}")
                logger.info(f"    Retry count: {msg.retry_count}")

            # Demonstrate DLQ replay (optional)
            # logger.info("\n→ Replaying first message from DLQ...")
            # await app.broker.replay_from_dlq(dlq_messages[0].stream_id)
            # logger.info("✓ Message replayed to queue")


async def demo_monitoring():
    """Demonstrate monitoring and introspection."""
    logger.info("\n" + "=" * 60)
    logger.info("6. MONITORING & INTROSPECTION")
    logger.info("=" * 60)

    # Check pending messages
    if hasattr(app.broker, "get_pending_count"):
        pending = await app.broker.get_pending_count()
        logger.info(f"✓ Pending messages in queue: {pending}")

    # List registered tasks
    logger.info(f"\n✓ Registered tasks ({len(app._tasks)}):")
    for task_name in sorted(app._tasks.keys()):
        task = app._tasks[task_name]
        retry_policy = task.options.get_retry_policy()
        logger.info(f"  • {task_name}")
        logger.info(f"    - Retries: {retry_policy.max_retries}")
        logger.info(f"    - Backoff: {retry_policy.backoff}")
        logger.info(f"    - Delivery: {task.options.delivery_mode}")


async def main():
    """Run all demonstrations."""
    logger.info("=" * 60)
    logger.info("CHICORY TASK QUEUE - COMPREHENSIVE DEMO")
    logger.info("=" * 60)
    logger.info("\nMake sure you have:")
    logger.info("  1. Redis running on localhost:6379")
    logger.info(
        "  2. Worker running: chicory worker examples.base:app --use-dead-letter-queue"
    )
    logger.info("\n" + "=" * 60)

    await app.connect()

    try:
        # Run all demonstrations
        await demo_basic_tasks()
        await asyncio.sleep(1)

        await demo_retry_policies()
        await asyncio.sleep(2)

        await demo_scheduled_tasks()
        await asyncio.sleep(1)

        await demo_batch_processing()
        await asyncio.sleep(1)

        await demo_dlq_operations()
        await asyncio.sleep(1)

        await demo_monitoring()

        logger.info("\n" + "=" * 60)
        logger.info("DEMO COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\n❌ Demo failed: {e}", exc_info=True)
    finally:
        await app.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
