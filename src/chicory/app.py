from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, overload

from chicory.backend import Backend
from chicory.broker import Broker
from chicory.config import ChicoryConfig
from chicory.exceptions import TaskNotFoundError
from chicory.task import Task
from chicory.types import (
    BackendType,
    BrokerType,
    DeliveryMode,
    P,
    R,
    RetryPolicy,
    TaskOptions,
    ValidationMode,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class Chicory:
    @overload
    def __init__(
        self,
        broker: BrokerType,
        backend: BackendType | None = None,
        config: ChicoryConfig | None = None,
        validation_mode: ValidationMode | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        broker: Broker,
        backend: Backend | None = None,
        config: ChicoryConfig | None = None,
        validation_mode: ValidationMode | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> None: ...
    def __init__(
        self,
        broker: BrokerType | Broker,
        backend: BackendType | Backend | None = None,
        config: ChicoryConfig | None = None,
        validation_mode: ValidationMode | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> None:
        self.config = config or ChicoryConfig()
        if validation_mode is not None:
            self.config.validation_mode = validation_mode
        if delivery_mode is not None:
            self.config.delivery_mode = delivery_mode

        if isinstance(broker, Broker):
            self._broker = broker
        else:
            self._broker = self._create_broker(broker)

        if isinstance(backend, Backend):
            self._backend = backend
        elif backend is not None:
            self._backend = self._create_backend(backend)
        else:
            self._backend = None

        self._tasks: dict[str, Task[Any, Any]] = {}
        self.logger = logging.getLogger("chicory")

    def _create_broker(self, broker_type: BrokerType) -> Broker:
        match broker_type:
            case BrokerType.REDIS:
                from chicory.broker import RedisBroker

                return RedisBroker(
                    config=self.config.broker.redis,
                    delivery_mode=self.config.delivery_mode,
                )
            case BrokerType.RABBITMQ:
                from chicory.broker import RabbitMQBroker

                return RabbitMQBroker(
                    config=self.config.broker.rabbitmq,
                    delivery_mode=self.config.delivery_mode,
                )
            case _:
                raise NotImplementedError(
                    f"Broker type {broker_type} is not implemented"
                )

    def _create_backend(self, backend_type: BackendType) -> Backend:
        match backend_type:
            case BackendType.REDIS:
                from chicory.backend import RedisBackend

                return RedisBackend(config=self.config.backend.redis)
            case BackendType.POSTGRES:
                from chicory.backend import DatabaseBackend

                return DatabaseBackend(config=self.config.backend.postgres)
            case BackendType.MYSQL:
                from chicory.backend import DatabaseBackend

                return DatabaseBackend(config=self.config.backend.mysql)
            case BackendType.SQLITE:
                from chicory.backend import DatabaseBackend

                return DatabaseBackend(config=self.config.backend.sqlite)
            case BackendType.MSSQL:
                from chicory.backend import DatabaseBackend

                return DatabaseBackend(config=self.config.backend.mssql)
            case _:
                raise NotImplementedError(
                    f"Backend type {backend_type} is not implemented"
                )

    @property
    def broker(self) -> Broker:
        return self._broker

    @property
    def backend(self) -> Backend | None:
        return self._backend

    def task(
        self,
        *,
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        ignore_result: bool = False,
        validation_mode: ValidationMode | None = None,
        delivery_mode: DeliveryMode | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        options = TaskOptions(
            name=name,
            retry_policy=retry_policy,
            delivery_mode=delivery_mode or self.config.delivery_mode,
            ignore_result=ignore_result,
            validation_mode=validation_mode or self.config.validation_mode,
        )

        def decorator(fn: Callable[P, R]) -> Task[P, R]:
            task = Task(fn=fn, app=self, options=options)
            self._tasks[task.name] = task
            return task

        return decorator

    def get_task(self, name: str) -> Task[Any, Any]:
        """Retrieve a registered task by name."""
        if name not in self._tasks:
            raise TaskNotFoundError(f"Task '{name}' not found")
        return self._tasks[name]

    async def connect(self) -> None:
        """Connect to broker and backend."""
        await self._broker.connect()
        if self._backend:
            await self._backend.connect()

    async def disconnect(self) -> None:
        """Disconnect from broker and backend."""
        await self._broker.disconnect()
        if self._backend:
            await self._backend.disconnect()
