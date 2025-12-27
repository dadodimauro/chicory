from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from chicory import RetryPolicy
from chicory.app import Chicory
from chicory.backend.base import Backend
from chicory.backend.redis import RedisBackend
from chicory.broker.redis import RedisBroker
from chicory.exceptions import TaskNotFoundError
from chicory.types import BackendType, BrokerType, DeliveryMode, ValidationMode


class TestChicoryAppCreateBroker:
    @pytest.mark.parametrize(
        "broker_type, expected_broker_class",
        [
            (BrokerType.REDIS, RedisBroker),
        ],
    )
    def test_create_broker(
        self,
        broker_type: BrokerType,
        expected_broker_class: type,
    ) -> None:
        app = Chicory(broker=broker_type)
        broker = app.broker
        assert isinstance(broker, expected_broker_class)

    def test_create_broker_not_implemented(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)
        with pytest.raises(NotImplementedError):
            app._create_broker(broker_type="unknown_broker")  # ty:ignore[invalid-argument-type]

    def test_create_broker_with_instance(self) -> None:
        custom_broker = RedisBroker(
            config=MagicMock(),
        )
        app = Chicory(
            broker=custom_broker,
            validation_mode=ValidationMode.STRICT,
            delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        )
        broker = app.broker
        assert broker is custom_broker


class TestChicoryAppCreateBackend:
    @pytest.mark.parametrize(
        "backend_type, expected_backend_class",
        [
            (BackendType.REDIS, RedisBackend),
        ],
    )
    def test_create_backend(
        self,
        backend_type: BackendType,
        expected_backend_class: type,
    ) -> None:
        app = Chicory(broker=BrokerType.REDIS, backend=backend_type)
        backend = app.backend
        assert isinstance(backend, expected_backend_class)

    def test_create_backend_not_implemented(self) -> None:
        app = Chicory(broker=BrokerType.REDIS, backend=BackendType.REDIS)
        with pytest.raises(NotImplementedError):
            app._create_backend(backend_type="unknown_backend")  # ty:ignore[invalid-argument-type]

    def test_create_backend_with_instance(self) -> None:
        custom_backend = RedisBackend(
            config=MagicMock(),
        )
        app = Chicory(
            broker=MagicMock(spec=RedisBroker),
            backend=custom_backend,
            validation_mode=ValidationMode.STRICT,
            delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        )
        backend = app.backend
        assert backend is custom_backend


class TestChicoryAppTask:
    def test_task_registration(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)

        @app.task(name="sample_task")
        async def sample_task(x: int, y: int) -> int:
            return x + y

        assert "sample_task" in app._tasks
        registered_task = app._tasks["sample_task"]
        assert registered_task.fn is sample_task.fn

    def task_registration_with_options(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)

        @app.task(
            name="some_task_name",
            retry_policy=RetryPolicy(max_retries=3, retry_delay=10.0),
            delivery_mode=DeliveryMode.AT_MOST_ONCE,
            ignore_result=True,
            validation_mode=ValidationMode.STRICT,
        )
        async def task_with_options(data: dict[Any, Any]) -> None:
            pass

        registered_task = app.get_task("some_task_name")
        registered_task = app._tasks["some_task_name"]
        assert registered_task.fn is task_with_options.fn

        assert registered_task.options.retry_policy is not None
        assert registered_task.options.retry_policy.max_retries == 3
        assert registered_task.options.retry_policy.retry_delay == 10.0
        assert registered_task.options.delivery_mode == DeliveryMode.AT_MOST_ONCE
        assert registered_task.options.ignore_result is True
        assert registered_task.options.validation_mode == ValidationMode.STRICT


class TestChicoryAppGetTask:
    def test_get_existing_task(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)

        @app.task(name="existing_task")
        async def existing_task() -> None:
            pass

        retrieved_task = app.get_task("existing_task")
        assert retrieved_task.fn is existing_task.fn

    def test_get_non_existing_task_raises(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)

        with pytest.raises(TaskNotFoundError):
            app.get_task("non_existing_task")


@pytest.mark.asyncio
class TestChicoryAppConnectDisconnect:
    async def test_connect_broker_and_backend(self) -> None:
        app = Chicory(broker=BrokerType.REDIS, backend=BackendType.REDIS)
        mock_backend = AsyncMock(spec=Backend)
        mock_backend.connect = AsyncMock()
        app._backend = mock_backend
        mock_broker = AsyncMock(spec=RedisBroker)
        mock_broker.connect = AsyncMock()
        app._broker = mock_broker

        await app.connect()
        mock_broker.connect.assert_awaited_once()
        mock_backend.connect.assert_awaited_once()

    async def test_connect_broker_only(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)
        mock_broker = AsyncMock(spec=RedisBroker)
        mock_broker.connect = AsyncMock()
        app._broker = mock_broker

        await app.connect()
        mock_broker.connect.assert_awaited_once()

    async def test_disconnect_broker_and_backend(self) -> None:
        app = Chicory(broker=BrokerType.REDIS, backend=BackendType.REDIS)
        mock_backend = AsyncMock(spec=Backend)
        mock_backend.disconnect = AsyncMock()
        app._backend = mock_backend
        mock_broker = AsyncMock(spec=RedisBroker)
        mock_broker.disconnect = AsyncMock()
        app._broker = mock_broker

        await app.disconnect()
        mock_broker.disconnect.assert_awaited_once()
        mock_backend.disconnect.assert_awaited_once()

    async def test_disconnect_broker_only(self) -> None:
        app = Chicory(broker=BrokerType.REDIS)
        mock_broker = AsyncMock(spec=RedisBroker)
        mock_broker.disconnect = AsyncMock()
        app._broker = mock_broker

        await app.disconnect()
        mock_broker.disconnect.assert_awaited_once()
