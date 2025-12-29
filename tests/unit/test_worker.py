from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from chicory.app import Chicory
from chicory.backend.base import Backend
from chicory.broker.base import Broker, TaskEnvelope
from chicory.config import ChicoryConfig, WorkerConfig
from chicory.context import TaskContext
from chicory.exceptions import MaxRetriesExceededError, RetryError, ValidationError
from chicory.types import (
    BackendStatus,
    BrokerStatus,
    DeliveryMode,
    RetryPolicy,
    TaskMessage,
    TaskOptions,
    TaskState,
    ValidationMode,
)
from chicory.worker import Worker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable


@pytest.fixture
def broker() -> MagicMock:
    broker = MagicMock(spec=Broker)
    broker.consume = AsyncMock()
    broker.ack = AsyncMock()
    broker.nack = AsyncMock()
    broker.publish = AsyncMock()
    broker.stop = MagicMock()
    broker.move_to_dlq = AsyncMock()
    return broker


@pytest.fixture
def backend() -> MagicMock:
    backend = MagicMock(spec=Backend)
    backend.set_state = AsyncMock()
    backend.store_result = AsyncMock()
    backend.store_heartbeat = AsyncMock()
    return backend


@pytest.fixture
def app_config() -> ChicoryConfig:
    return ChicoryConfig(
        validation_mode=ValidationMode.NONE,
    )


@pytest.fixture
def worker_config() -> WorkerConfig:
    return WorkerConfig()


@pytest.fixture
def app(broker: MagicMock, backend: MagicMock, app_config: ChicoryConfig) -> MagicMock:
    app = MagicMock(spec=Chicory)
    app.broker = broker
    app.backend = backend
    app.connect = AsyncMock()
    app.disconnect = AsyncMock()
    app.config = app_config
    return app


def make_task(
    *,
    fn: Callable[..., Any],
    name: str = "test_task",
    is_async: bool = True,
    has_context: bool = False,
    retries: int = 0,
    retry_delay: int = 0,
    delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
    ignore_result: bool = False,
    validation_mode: ValidationMode = ValidationMode.NONE,
) -> MagicMock:
    options = TaskOptions(
        delivery_mode=delivery_mode,
        retry_policy=RetryPolicy(
            max_retries=retries,
            retry_delay=retry_delay,
        ),
        ignore_result=ignore_result,
        validation_mode=validation_mode,
    )

    task = MagicMock()
    task.fn = fn
    task.name = name
    task.options = options
    task.is_async = is_async
    task.has_context = has_context
    task._validate_inputs = MagicMock(return_value={})
    return task


def make_envelope(message: TaskMessage) -> TaskEnvelope:
    envelope = MagicMock(spec=TaskEnvelope)
    envelope.message = message
    return envelope


@pytest.mark.asyncio
class TestWorkerProcessEnvelope:
    async def test_success_async_task_at_least_once(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn(x: int) -> int:
            return x + 1

        task = make_task(fn=fn)
        app.get_task.return_value = task

        msg = TaskMessage(id="1", name="test", args=[1], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        backend.set_state.assert_any_call("1", TaskState.STARTED)
        backend.store_result.assert_awaited_once()
        broker.ack.assert_awaited_once()
        broker.nack.assert_not_called()

    async def test_success_sync_task(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        def fn(x: int) -> int:
            return x * 2

        task = make_task(fn=fn, is_async=False)
        app.get_task.return_value = task

        msg = TaskMessage(id="2", name="sync", args=[2], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        result_call = backend.store_result.await_args[0][1]
        assert result_call.result == 4

    async def test_at_most_once_ack_before_execution(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn() -> str:
            return "ok"

        task = make_task(fn=fn, delivery_mode=DeliveryMode.AT_MOST_ONCE)
        app.get_task.return_value = task

        msg = TaskMessage(id="3", name="amo", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        assert broker.ack.await_count == 1
        broker.nack.assert_not_called()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_task_not_found(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        app.get_task.side_effect = KeyError("missing")

        msg = TaskMessage(id="4", name="missing", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()
        if use_dead_letter_queue:
            broker.move_to_dlq.assert_awaited_once()
            broker.nack.assert_not_called()
        else:
            broker.nack.assert_awaited_once()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_retry_error_schedules_retry(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        async def fn() -> None:
            raise RetryError(countdown=5)

        task = make_task(fn=fn, retries=3)
        app.get_task.return_value = task

        msg = TaskMessage(id="5", name="retry", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        # When retrying, we ACK the original and publish a new message
        # DLQ is not involved in retry scenarios
        broker.ack.assert_awaited_once()  # Changed: expect ACK, not NACK
        broker.move_to_dlq.assert_not_called()
        broker.nack.assert_not_called()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_max_retries_exceeded(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        async def fn() -> None:
            raise MaxRetriesExceededError()

        task = make_task(fn=fn)
        app.get_task.return_value = task

        msg = TaskMessage(id="6", name="max", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()
        if use_dead_letter_queue:
            broker.move_to_dlq.assert_awaited_once()
            broker.nack.assert_not_called()
        else:
            broker.nack.assert_awaited_once()
            broker.move_to_dlq.assert_not_called()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_validation_error(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        async def fn() -> None:
            raise ValidationError("bad input")

        task = make_task(fn=fn)
        app.get_task.return_value = task

        msg = TaskMessage(id="7", name="val", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()
        if use_dead_letter_queue:
            broker.move_to_dlq.assert_awaited_once()
            broker.nack.assert_not_called()
        else:
            broker.nack.assert_awaited_once()
            broker.move_to_dlq.assert_not_called()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_exception_no_retry(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        async def fn() -> None:
            raise RuntimeError("boom")

        task = make_task(fn=fn, retries=0, retry_delay=0)
        app.get_task.return_value = task

        msg = TaskMessage(id="8", name="boom_no_retry", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()
        broker.publish.assert_not_called()
        broker.publish.assert_not_called()
        if use_dead_letter_queue:
            broker.move_to_dlq.assert_awaited_once()
            broker.nack.assert_not_called()
        else:
            broker.nack.assert_awaited_once()
            broker.move_to_dlq.assert_not_called()

    async def test_exception_with_retry(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
    ) -> None:
        async def fn() -> None:
            raise RuntimeError("boom")

        task = make_task(fn=fn, retries=1, retry_delay=0)
        app.get_task.return_value = task

        msg = TaskMessage(id="8", name="boom", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        broker.publish.assert_awaited_once()
        backend.store_result.assert_not_called()
        broker.ack.assert_awaited_once()

    async def test_context_injection(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn(ctx: TaskContext, x: int) -> int:
            assert isinstance(ctx, TaskContext)
            return x

        task = make_task(fn=fn, has_context=True)
        app.get_task.return_value = task

        msg = TaskMessage(id="9", name="ctx", args=[42], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()

    async def test_input_validation(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn(**kwargs: Any) -> Any:
            return kwargs["x"]

        task = make_task(
            fn=fn,
            validation_mode=ValidationMode.INPUTS,
        )
        task._validate_inputs.return_value = {"x": 123}
        app.get_task.return_value = task

        msg = TaskMessage(
            id="10", name="validate", args=[], kwargs={"x": "bad"}, retries=0
        )
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        task._validate_inputs.assert_called_once()
        backend.store_result.assert_awaited_once()

    async def test_success_async_task_ignore_result(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn(x: int) -> int:
            return x + 1

        task = make_task(fn=fn, ignore_result=True)
        app.get_task.return_value = task

        msg = TaskMessage(id="1", name="test", args=[1], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        backend.set_state.assert_any_call("1", TaskState.STARTED)
        backend.store_result.assert_not_called()
        broker.ack.assert_awaited_once()
        broker.nack.assert_not_called()

    async def test_success_backend_not_set(
        self, app: MagicMock, broker: MagicMock
    ) -> None:
        async def fn(x: int) -> int:
            return x + 1

        task = make_task(fn=fn)
        app.get_task.return_value = task
        app.backend = None

        msg = TaskMessage(id="1", name="test", args=[1], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        broker.ack.assert_awaited_once()
        broker.nack.assert_not_called()

    async def test_failure_backend_not_set(
        self, app: MagicMock, broker: MagicMock, backend: MagicMock
    ) -> None:
        async def fn() -> None:
            raise RuntimeError("boom")

        task = make_task(fn=fn)
        app.get_task.return_value = task
        app.backend = None

        msg = TaskMessage(id="1", name="test", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker = Worker(app)

        await worker._process_envelope(envelope)

        broker.nack.assert_awaited_once()
        broker.ack.assert_not_called()

    async def test_move_to_dlq_fails(
        self,  # Add self parameter
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
    ) -> None:
        async def fn() -> None:
            raise MaxRetriesExceededError()

        task = make_task(fn=fn)
        app.get_task.return_value = task

        msg = TaskMessage(id="6", name="max", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        broker.move_to_dlq.side_effect = RuntimeError("DLQ failure")

        worker_config.use_dead_letter_queue = True
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        backend.store_result.assert_awaited_once()
        broker.move_to_dlq.assert_awaited_once()
        # When move_to_dlq fails, it should still try to finalize with nack
        broker.nack.assert_awaited_once()

    @pytest.mark.parametrize(
        "use_dead_letter_queue", [True, False], ids=["with_dlq", "without_dlq"]
    )
    async def test_exception_not_retryable_by_policy(
        self,
        app: MagicMock,
        broker: MagicMock,
        backend: MagicMock,
        worker_config: WorkerConfig,
        use_dead_letter_queue: bool,
    ) -> None:
        async def fn() -> None:
            raise ValueError("This should not be retried")

        # Create task with retry policy that ignores ValueError
        task = make_task(
            fn=fn,
            retries=3,  # Has retries available
            retry_delay=1,
        )
        # Configure retry policy to not retry ValueError
        task.options.retry_policy.ignore_on = ["ValueError"]

        app.get_task.return_value = task

        msg = TaskMessage(id="11", name="no_retry", args=[], kwargs={}, retries=0)
        envelope = make_envelope(msg)

        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker = Worker(app, config=worker_config)

        await worker._process_envelope(envelope)

        # Should store failure
        backend.store_result.assert_awaited_once()

        # Should NOT attempt to retry (publish not called)
        broker.publish.assert_not_called()

        # Should move to DLQ or nack
        if use_dead_letter_queue:
            broker.move_to_dlq.assert_awaited_once()
            broker.nack.assert_not_called()
        else:
            broker.nack.assert_awaited_once()
            broker.move_to_dlq.assert_not_called()


@pytest.mark.asyncio
class TestWorkerExecuteTask:
    async def test_execute_async_task_without_context(
        self, worker_config: WorkerConfig
    ) -> None:
        async def fn(x: int, y: int) -> int:
            return x + y

        task = make_task(fn=fn, is_async=True, has_context=False)
        message = TaskMessage(id="1", name="add", args=[2, 3], kwargs={}, retries=0)

        worker = Worker(MagicMock(), config=worker_config)

        result = await worker._execute_task(task, message)

        assert result == 5

    async def test_execute_sync_task_with_context(
        self, worker_config: WorkerConfig
    ) -> None:
        def fn(ctx: TaskContext, x: int) -> int:
            assert isinstance(ctx, TaskContext)
            return x * 2

        task = make_task(fn=fn, is_async=False, has_context=True)
        message = TaskMessage(id="2", name="double", args=[4], kwargs={}, retries=0)

        worker = Worker(MagicMock(), config=worker_config)

        result = await worker._execute_task(task, message)

        assert result == 8


@pytest.mark.asyncio
class TestWorkerConsumeLoop:
    async def test_consume_loop_processes_messages(
        self,
        app: MagicMock,
        broker: MagicMock,
    ) -> None:
        messages = [
            TaskMessage(id="1", name="t1", args=[], kwargs={}, retries=0),
            TaskMessage(id="2", name="t2", args=[], kwargs={}, retries=0),
        ]
        envelopes = [MagicMock(spec=TaskEnvelope, message=m) for m in messages]

        # Make broker.consume return an async generator when called
        async def mock_consume(queue: str) -> AsyncGenerator[Any]:
            for envelope in envelopes:
                yield envelope

        broker.consume = mock_consume

        worker = Worker(app)
        worker._running = True

        worker._process_envelope = AsyncMock()  # type: ignore

        await worker._consume_loop()

        # Wait for all tasks to complete
        if worker._tasks:
            await asyncio.gather(*worker._tasks)

        assert worker._process_envelope.await_count == 2  # ty:ignore[unresolved-attribute]

    async def test_consume_loop_tracks_tasks(
        self,
        app: MagicMock,
        broker: MagicMock,
    ) -> None:
        message = TaskMessage(id="1", name="t1", args=[], kwargs={}, retries=0)
        envelope = MagicMock(spec=TaskEnvelope, message=message)

        # Make broker.consume return an async generator when called
        async def mock_consume(queue: str) -> AsyncGenerator[Any]:
            yield envelope

        broker.consume = mock_consume

        worker = Worker(app)
        worker._running = True

        worker._process_envelope = AsyncMock()  # type: ignore

        await worker._consume_loop()

        # Wait for all tasks to complete
        if worker._tasks:
            await asyncio.gather(*worker._tasks)

        assert len(worker._tasks) == 0

    async def test_consume_loop_stops_when_not_running(
        self,
        app: MagicMock,
        broker: MagicMock,
    ) -> None:
        msg1 = TaskMessage(id="1", name="t1", args=[], kwargs={}, retries=0)
        msg2 = TaskMessage(id="2", name="t2", args=[], kwargs={}, retries=0)

        envelopes = [
            MagicMock(spec=TaskEnvelope, message=msg1),
            MagicMock(spec=TaskEnvelope, message=msg2),
        ]

        # Make broker.consume return an async generator when called
        async def mock_consume(queue: str) -> AsyncGenerator[Any]:
            for envelope in envelopes:
                await asyncio.sleep(0.5)  # give time to set _running to False
                yield envelope

        broker.consume = mock_consume

        worker = Worker(app)
        worker._running = True

        async def process_and_stop(_: Any) -> None:
            worker._running = False

        worker._process_envelope = AsyncMock(side_effect=process_and_stop)  # type: ignore

        await worker._consume_loop()

        # Wait for all tasks to complete
        if worker._tasks:
            await asyncio.gather(*worker._tasks)

        assert worker._process_envelope.await_count == 1  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
class TestWorkerStartStop:
    async def test_start_and_shutdown(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)

        worker._consume_loop = AsyncMock()  # type: ignore
        worker._shutdown = AsyncMock()  # type: ignore

        await worker.start()

        app.connect.assert_awaited_once()

        await worker.stop()

        app.broker.stop.assert_called_once()
        assert worker._running is False

    async def test_start_already_running(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)
        worker._running = True

        await worker.start()

        app.connect.assert_not_called()

    async def test_stop_not_running(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)
        worker._running = False

        await worker.stop()

        app.broker.stop.assert_not_called()

    async def test_stop_signal_handling(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)
        worker._consume_loop = AsyncMock()  # type: ignore

        await worker.start()
        assert worker._running is True

        worker._handle_shutdown()
        await asyncio.sleep(0.1)

        assert worker._running is False
        app.broker.stop.assert_called_once()
        app.disconnect.assert_awaited_once()

    @pytest.mark.slow
    async def test_stop_consume_task_timeout(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)

        # Simulate a long-running consume loop
        async def long_consume_loop() -> None:
            await asyncio.sleep(6)

        worker._consume_loop = long_consume_loop  # type: ignore

        await worker.start()
        assert worker._running is True

        await worker.stop()

        assert worker._running is False
        app.broker.stop.assert_called_once()
        app.disconnect.assert_awaited_once()

    async def test_stop_task_timeout(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)

        # Simulate a consume loop that spawns a long-running task
        async def consume_loop_with_long_task() -> None:
            async def long_task() -> None:
                await asyncio.sleep(1)

            task = asyncio.create_task(long_task())
            worker._tasks.add(task)

        worker._consume_loop = consume_loop_with_long_task  # type: ignore

        await worker.start()
        assert worker._running is True

        await worker.stop(timeout=0.1)

        assert worker._running is False
        app.broker.stop.assert_called_once()
        app.disconnect.assert_awaited_once()


@pytest.mark.asyncio
class TestWorkerRun:
    async def test_run_starts_and_stops_worker(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)

        async def consume_loop_stop_worker() -> None:
            await asyncio.sleep(0.1)
            worker._running = False

        worker._consume_loop = consume_loop_stop_worker  # type: ignore

        await worker.run()

        assert worker._running is False

    async def test_exception_in_run(
        self,
        app: MagicMock,
    ) -> None:
        worker = Worker(app)

        # Mock _consume_loop to raise the error and cleanup like the real implementation
        async def failing_consume_loop() -> None:
            worker._running = False
            raise RuntimeError("Very bad error")

        worker._consume_loop = failing_consume_loop  # type: ignore

        with pytest.raises(RuntimeError, match="Very bad error"):
            await worker.run()

        assert worker._running is False

    async def test_exception_in_consume_loop(
        self,
        app: MagicMock,
    ) -> None:
        """Test exception handling in the actual consume loop."""
        worker = Worker(app)

        # Make broker.consume raise an exception
        app.broker.consume = MagicMock(side_effect=RuntimeError("Very bad error"))

        with pytest.raises(RuntimeError, match="Very bad error"):
            await worker.run()

        # The _consume_loop's finally block sets _running to False
        assert worker._running is False


@pytest.mark.asyncio
class TestWorkerHeartbeat:
    async def test_heartbeat_sends_heartbeats(
        self,
        app: MagicMock,
        worker_config: WorkerConfig,
    ) -> None:
        worker_config.heartbeat_interval = 0.1
        worker_config.heartbeat_ttl = 1
        worker = Worker(app, config=worker_config)

        async def stop_worker_after_delay() -> None:
            await asyncio.sleep(0.3)
            worker._running = False

        worker._running = True
        asyncio.create_task(stop_worker_after_delay())

        await worker._heartbeat_loop()

        expected_calls = 3
        assert app.backend.store_heartbeat.await_count == expected_calls

    async def test_heartbeat_fails(
        self, app: MagicMock, worker_config: WorkerConfig
    ) -> None:
        worker_config.heartbeat_interval = 0.1
        worker_config.heartbeat_ttl = 1
        worker = Worker(app, config=worker_config)
        app.backend.store_heartbeat.side_effect = [
            RuntimeError("boom"),  # First call fails
            None,  # Second call succeeds
            None,  # Third call succeeds
        ]

        async def stop_worker_after_delay() -> None:
            await asyncio.sleep(0.30)
            worker._running = False

        worker._running = True
        asyncio.create_task(stop_worker_after_delay())

        await worker._heartbeat_loop()

        # Should have attempted 3 heartbeats (first failed, others succeeded)
        assert app.backend.store_heartbeat.await_count == 3

    async def test_heartbeat_no_backend(
        self, app: MagicMock, worker_config: WorkerConfig
    ) -> None:
        worker_config.heartbeat_interval = 0.1
        worker_config.heartbeat_ttl = 1
        worker = Worker(app, config=worker_config)
        app.backend = None  # Simulate no backend

        async def stop_worker_after_delay() -> None:
            await asyncio.sleep(0.30)
            worker._running = False

        worker._running = True
        asyncio.create_task(stop_worker_after_delay())

        await worker._heartbeat_loop()

        assert worker.last_heartbeat is None


@pytest.mark.asyncio
class TestWorkerHealtcheck:
    async def test_healthcheck_healthy(
        self,
        app: MagicMock,
    ) -> None:
        broker_status = BrokerStatus(connected=True)
        backend_status = BackendStatus(connected=True)
        app.broker.healthcheck = AsyncMock(return_value=broker_status)
        app.backend.healthcheck = AsyncMock(return_value=backend_status)
        worker = Worker(app)

        stats = await worker.healthcheck()
        assert stats.broker is not None
        assert stats.backend is not None
        assert stats.broker == broker_status
        assert stats.backend == backend_status
        assert stats.broker.connected is True
        assert stats.backend.connected is True

    async def test_healthcheck_broker_unhealthy(
        self,
        app: MagicMock,
    ) -> None:
        broker_status = BrokerStatus(connected=False, error="Connection lost")
        backend_status = BackendStatus(connected=True)
        app.broker.healthcheck = AsyncMock(return_value=broker_status)
        app.backend.healthcheck = AsyncMock(return_value=backend_status)

        worker = Worker(app)

        stats = await worker.healthcheck()
        assert stats.broker is not None
        assert stats.backend is not None
        assert stats.broker == broker_status
        assert stats.backend == backend_status
        assert stats.broker.connected is False
        assert stats.backend.connected is True

    async def test_healthcheck_backend_unhealthy(
        self,
        app: MagicMock,
    ) -> None:
        broker_status = BrokerStatus(connected=True)
        backend_status = BackendStatus(connected=False, error="Connection lost")
        app.broker.healthcheck = AsyncMock(return_value=broker_status)
        app.backend.healthcheck = AsyncMock(return_value=backend_status)

        worker = Worker(app)

        stats = await worker.healthcheck()
        assert stats.broker is not None
        assert stats.backend is not None
        assert stats.broker == broker_status
        assert stats.backend == backend_status
        assert stats.broker.connected is True
        assert stats.backend.connected is False
