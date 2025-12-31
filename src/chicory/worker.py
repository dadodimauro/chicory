from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import signal
import socket
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from chicory.context import TaskContext
from chicory.exceptions import MaxRetriesExceededError, RetryError, ValidationError
from chicory.types import (
    DeliveryMode,
    RetryPolicy,
    TaskMessage,
    TaskResult,
    TaskState,
    ValidationMode,
    WorkerStats,
)

if TYPE_CHECKING:
    from chicory.app import Chicory
    from chicory.broker.base import TaskEnvelope
    from chicory.config import WorkerConfig
    from chicory.task import Task


class Worker:
    """
    Async task worker.

    Consumes messages from broker, executes tasks, and stores results.
    """

    def __init__(
        self,
        app: Chicory,
        config: WorkerConfig | None = None,
    ) -> None:
        """
        Initialize a worker.

        Args:
            app: The Chicory application instance
            config: WorkerConfig instance. If not provided, uses app.config.worker
        """
        self.app = app
        self.config = config or app.config.worker

        # Set attributes from config
        self.concurrency = self.config.concurrency
        self.queue = self.config.queue
        self.use_dead_letter_queue = self.config.use_dead_letter_queue
        self.heartbeat_interval = self.config.heartbeat_interval
        self.heartbeat_ttl = self.config.heartbeat_ttl

        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._consume_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

        self.worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.started_at = datetime.now(UTC)
        self.last_heartbeat: datetime | None = None

        self._tasks_processed = 0
        self._tasks_failed = 0

        self._logger = logging.getLogger(f"chicory.worker.{self.worker_id}")

        self._logger.info(
            f"Initialized worker {self.worker_id}",
            extra={"worker_id": self.worker_id, "queue": self.queue},
        )

    async def start(self) -> None:
        """
        Start the worker (non-blocking).

        The worker will run in the background. Call stop() to gracefully shutdown.
        """
        if self._running:
            self._logger.warning("Worker is already running")
            return

        self._logger.info(
            f"Starting worker with concurrency={self.concurrency}",
            extra={"worker_id": id(self)},
        )

        await self.app.connect()
        self._running = True

        # Start consume loop as a background task
        self._consume_task = asyncio.create_task(self._consume_loop())
        # Start heartbeat loop
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self, timeout: float | None = None) -> None:
        """
        Stop the worker gracefully.

        Args:
            timeout: Maximum time to wait for tasks to complete (seconds).
                    If None, uses config.shutdown_timeout
        """
        if not self._running:
            self._logger.warning("Worker is not running")
            return

        if timeout is None:
            timeout = self.config.shutdown_timeout

        self._logger.info("Stopping worker...")
        self._running = False
        self.app.broker.stop()

        # Stop heartbeat
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task

        # Wait for consume loop to finish
        if self._consume_task:
            try:
                await asyncio.wait_for(self._consume_task, timeout=5.0)
            except TimeoutError:
                self._logger.warning("Consume loop did not finish in time")
                self._consume_task.cancel()

        # Wait for running tasks with timeout
        if self._tasks:
            self._logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout,
                )
            except TimeoutError:
                self._logger.warning(
                    f"Tasks did not complete within {timeout}s, cancelling..."
                )
                for task in self._tasks:
                    task.cancel()
                await asyncio.gather(*self._tasks, return_exceptions=True)

        await self._send_heartbeat(is_running=False)

        await self._shutdown()

    async def _heartbeat_loop(self) -> None:
        """
        Periodic heartbeat sender.
        """
        self._logger.info(
            f"Starting heartbeat loop (interval={self.heartbeat_interval}s)",
            extra={"worker_id": self.worker_id},
        )

        try:
            while self._running:
                await self._send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            self._logger.debug("Heartbeat loop cancelled")
        finally:
            self._logger.info("Heartbeat loop exited")

    async def _send_heartbeat(self, is_running: bool = True) -> None:
        """
        Send heartbeat to backend/broker.
        """
        # Only send if backend exists
        if not self.app.backend:
            return

        self.last_heartbeat = datetime.now(UTC)

        heartbeat = self.get_stats()

        # Store in backend
        try:
            await self.app.backend.store_heartbeat(
                self.worker_id, heartbeat, ttl=self.heartbeat_ttl
            )

            self._logger.debug(
                f"Heartbeat sent: {self._tasks_processed} processed, "
                f"{len(self._tasks)} active",
                extra={"worker_id": self.worker_id},
            )
        except Exception:
            self._logger.exception("Failed to send heartbeat")

    async def run(self) -> None:
        """
        Run the worker until stopped (blocking).

        This sets up signal handlers and blocks until the worker is stopped
        via signal (SIGINT/SIGTERM) or programmatically via stop().

        Example:
            >>> worker = Worker(app)
            >>> await worker.run()  # Blocks
        """
        # Set up signal handlers (not supported on Windows with ProactorEventLoop)
        loop = asyncio.get_event_loop()
        signals_registered = False
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._handle_shutdown)
            signals_registered = True
        except NotImplementedError:  # pragma: no cover
            # Windows doesn't support signal handlers with ProactorEventLoop
            self._logger.warning(
                "Signal handlers not supported on this platform. "
                "Use Ctrl+C or stop() method to terminate."
            )

        try:
            await self.start()

            # Wait for worker to stop
            if self._consume_task:
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consume_task
        except KeyboardInterrupt:  # pragma: no cover
            # Handle Ctrl+C on Windows
            self._logger.info("Keyboard interrupt received")
            await self.stop()
        finally:
            # Clean up signal handlers if they were registered
            if signals_registered:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.remove_signal_handler(sig)

    def _handle_shutdown(self) -> None:
        """Handle shutdown signal."""
        self._logger.info("Shutdown signal received")
        if self._running:
            asyncio.create_task(self.stop())

    async def _shutdown(self) -> None:
        """Internal cleanup."""
        self._logger.info("Shutting down worker...")
        self._executor.shutdown(wait=True)
        await self.app.disconnect()
        self._logger.info("Worker shutdown complete")

    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        try:
            async for envelope in self.app.broker.consume(self.queue):
                if not self._running:
                    break

                t = asyncio.create_task(self._process_envelope(envelope))
                t.add_done_callback(self._tasks.discard)
                self._tasks.add(t)
        except Exception:
            self._logger.exception("Error in consume loop")
            raise
        finally:
            self._running = False
            self._logger.debug("Consume loop exited")

    async def _process_envelope(self, envelope: TaskEnvelope) -> None:
        """Process a single task envelope."""
        message = envelope.message
        task_id = message.id

        log_context = {
            "task_id": task_id,
            "task_name": message.name,
        }

        self._logger.info("Processing task", extra=log_context)

        async with self._semaphore:
            try:
                task = self.app.get_task(message.name)
            except Exception as e:
                self._logger.error(f"Task not found: {message.name}", extra=log_context)
                await self._store_failure(task_id, e)
                await self._move_to_dlq(envelope, str(e))
                return

            # Get retry policy
            retry_policy = task.options.get_retry_policy()

            # Set state to STARTED
            if self.app.backend:
                await self.app.backend.set_state(task_id, TaskState.STARTED)

            # ACK timing based on delivery mode
            if task.options.delivery_mode == DeliveryMode.AT_MOST_ONCE:
                await self.app.broker.ack(envelope)

            try:
                result = await self._execute_task(task, message)
                await self._store_success(task_id, result, task.options.ignore_result)

                # Only ack for AT_LEAST_ONCE after successful processing
                if task.options.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
                    await self.app.broker.ack(envelope)

                self._tasks_processed += 1
                self._logger.info("Task completed successfully", extra=log_context)

            except RetryError as e:
                self._tasks_failed += 1
                await self._handle_retry(envelope, message, task, retry_policy, e)
            except MaxRetriesExceededError as e:
                self._tasks_failed += 1
                await self._store_failure(task_id, e)
                await self._move_to_dlq(envelope, str(e))
                self._logger.error(
                    "Max retries exceeded, moved to DLQ", extra=log_context
                )
            except ValidationError as e:
                # Validation errors should not be retried
                self._tasks_failed += 1
                await self._store_failure(task_id, e)
                await self._move_to_dlq(envelope, str(e))
                self._logger.error(
                    f"Validation error, moved to DLQ: {e}", extra=log_context
                )
            except Exception as e:
                self._tasks_failed += 1
                self._logger.exception("Task failed with exception", extra=log_context)
                await self._handle_exception_with_policy(
                    envelope, message, task, retry_policy, e
                )

    async def _execute_task(self, task: Task[Any, Any], message: TaskMessage) -> Any:
        args = message.args
        kwargs = message.kwargs

        # Worker time validation
        validation_mode = (
            task.options.validation_mode or self.app.config.validation_mode
        )

        if validation_mode in (ValidationMode.INPUTS, ValidationMode.STRICT):
            validated_inputs = task._validate_inputs(*args, **kwargs)
            args = []
            kwargs = validated_inputs

        # Build context if needed
        if task.has_context:
            retry_policy = task.options.get_retry_policy()
            ctx = TaskContext(
                task_id=message.id,
                task_name=message.name,
                retries=message.retries,
                max_retries=retry_policy.max_retries,
                retry_policy=retry_policy,
            )
            args = [ctx] + args

        # Execute
        if task.is_async:
            result = await task.fn(*args, **kwargs)
        else:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self._executor, functools.partial(task.fn, *args, **kwargs)
            )

        # Output validation
        if validation_mode in (ValidationMode.OUTPUTS, ValidationMode.STRICT):
            # TODO @dadodimauro: https://github.com/chicory-dev/chicory/issues/15
            # Implement output validation
            pass  # pragma: no cover

        return result

    async def _handle_exception_with_policy(
        self,
        envelope: TaskEnvelope,
        message: TaskMessage,
        task: Task[Any, Any],
        retry_policy: RetryPolicy,
        error: Exception,
    ) -> None:
        """Handle an exception using the retry policy."""

        task_id = message.id
        log_context = {"task_id": task_id, "task_name": message.name}

        # Check if this exception should be retried
        if not retry_policy.should_retry(error):
            self._logger.info(
                f"Exception {type(error).__name__} is not retryable, moving to DLQ",
                extra=log_context,
            )
            await self._store_failure(task_id, error)
            await self._move_to_dlq(envelope, str(error))
            return

        # Check if we have retries remaining
        if message.retries >= retry_policy.max_retries:
            self._logger.error(
                f"Max retries ({retry_policy.max_retries}) exceeded, moving to DLQ",
                extra=log_context,
            )
            await self._store_failure(task_id, MaxRetriesExceededError(str(error)))
            await self._move_to_dlq(envelope, str(error))
            return

        self._logger.warning(
            f"Task failed with {type(error).__name__}, scheduling retry "
            f"{message.retries + 1}/{retry_policy.max_retries}",
            extra={**log_context, "error": str(error)},
        )

        # Calculate delay for next retry
        next_attempt = message.retries + 1
        delay = retry_policy.calculate_delay(next_attempt)

        retry_error = RetryError(countdown=delay)
        await self._handle_retry(
            envelope, message, task, retry_policy, retry_error, str(error)
        )

    async def _handle_retry(
        self,
        envelope: TaskEnvelope,
        message: TaskMessage,
        task: Task[Any, Any],
        retry_policy: RetryPolicy,
        error: RetryError,
        last_error: str | None = None,
    ) -> None:
        """Handle retry with policy tracking."""

        new_message = TaskMessage(
            id=message.id,
            name=message.name,
            args=message.args,
            kwargs=message.kwargs,
            retries=message.retries + 1,
            eta=(
                datetime.now(UTC) + timedelta(seconds=error.countdown)
                if error.countdown
                else None
            ),
            first_failure_at=message.first_failure_at or datetime.now(UTC),
            last_error=last_error,
        )

        # Update state
        if self.app.backend:
            await self.app.backend.set_state(message.id, TaskState.RETRY)

        # Republish
        await self.app.broker.publish(new_message)

        # ACK original
        if task.options.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            await self.app.broker.ack(envelope)

        self._logger.info(
            (
                f"Task {message.name} scheduled for retry "
                f"{new_message.retries}/{retry_policy.max_retries}"
            ),
            extra={"task_id": message.id, "task_name": message.name},
        )

    async def _move_to_dlq(self, envelope: TaskEnvelope, error: str) -> None:
        """Move a failed message to the Dead Letter Queue."""
        try:
            # Check if broker supports DLQ
            if self.use_dead_letter_queue:
                await self.app.broker.move_to_dlq(envelope, error, self.queue)
                self._logger.info(
                    f"Task {envelope.message.id} moved to DLQ",
                    extra={
                        "task_id": envelope.message.id,
                        "task_name": envelope.message.name,
                    },
                )
            else:
                await self._finalize_failure(envelope, task=None)
        except Exception:
            self._logger.exception("Failed to move message to DLQ")
            # Still try to ack to prevent infinite loop
            await self._finalize_failure(envelope, task=None)

    async def _store_success(
        self, task_id: str, result: Any, ignore_result: bool
    ) -> None:
        if not self.app.backend:
            return

        if ignore_result:
            await self.app.backend.set_state(task_id, TaskState.SUCCESS)
        else:
            await self.app.backend.store_result(
                task_id,
                TaskResult(
                    task_id=task_id,
                    state=TaskState.SUCCESS,
                    result=result,
                ),
            )

    async def _store_failure(self, task_id: str, exc: Exception) -> None:
        if not self.app.backend:
            return

        await self.app.backend.store_result(
            task_id,
            TaskResult(
                task_id=task_id,
                state=TaskState.FAILURE,
                error=str(exc),
                traceback=traceback.format_exc(),
            ),
        )

    async def _finalize_failure(
        self, envelope: TaskEnvelope, task: Task[Any, Any] | None
    ) -> None:
        """
        Finalize a failed task delivery.

        For at-least-once delivery we use nack(requeue=False) to make the intent
        explicit (do not requeue this message). For at-most-once we already acked
        earlier and do nothing here.
        """
        delivery = (
            task.options.delivery_mode
            if task is not None
            else DeliveryMode.AT_LEAST_ONCE
        )
        if delivery == DeliveryMode.AT_LEAST_ONCE:
            await self.app.broker.nack(envelope, requeue=False)

    def get_stats(self) -> WorkerStats:
        """
        Get worker statistics.

        Returns:
            WorkerStats: Current worker statistics.
        """
        return WorkerStats(
            worker_id=self.worker_id,
            hostname=self.hostname,
            pid=self.pid,
            queue=self.queue,
            concurrency=self.concurrency,
            tasks_processed=self._tasks_processed,
            tasks_failed=self._tasks_failed,
            active_tasks=len(self._tasks),
            started_at=self.started_at,
            last_heartbeat=datetime.now(UTC),
            is_running=self._running,
        )

    async def healthcheck(self) -> WorkerStats:
        """
        Perform a healthcheck on the worker.

        Returns:
            WorkerStats: Current worker statistics.
        """
        stats = self.get_stats()

        # Check broker connectivity
        stats.broker = await self.app.broker.healthcheck()

        # Check backend connectivity
        if self.app.backend:
            stats.backend = await self.app.backend.healthcheck()

        return stats
