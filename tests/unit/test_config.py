from __future__ import annotations

from pydantic import SecretStr

from chicory.config import (
    BackendConfig,
    BrokerConfig,
    ChicoryConfig,
    RedisBackendConfig,
    RedisBrokerConfig,
)
from chicory.types import ValidationMode


class TestRedisBrokerConfig:
    def test_redis_broker_config_defaults(self) -> None:
        broker_config = RedisBrokerConfig()
        assert broker_config.host == "localhost"
        assert broker_config.port == 6379
        assert broker_config.db == 0
        assert broker_config.username is None
        assert broker_config.password is None
        assert broker_config.url is None

    def test_redis_dsn_with_url(self) -> None:
        url = "redis://user:password@localhost:6379/0"
        broker_config = RedisBrokerConfig(url=url)
        assert broker_config.dsn == url

    def test_redis_dsn_with_host_and_port(self) -> None:
        broker_config = RedisBrokerConfig(
            host="localhost",
            port=6379,
            username="user",
            password=SecretStr("password"),
        )
        expected_dsn = "redis://user:password@localhost:6379/0"
        assert broker_config.dsn == expected_dsn


class TestRedisBackendConfig:
    def test_redis_backend_config_defaults(self) -> None:
        backend_config = RedisBackendConfig()
        assert backend_config.host == "localhost"
        assert backend_config.port == 6379
        assert backend_config.db == 1
        assert backend_config.username is None
        assert backend_config.password is None
        assert backend_config.url is None

    def test_redis_dsn_with_url(self) -> None:
        url = "redis://user:password@localhost:6379/1"
        backend_config = RedisBackendConfig(url=url)
        assert backend_config.dsn == url

    def test_redis_dsn_with_host_and_port(self) -> None:
        backend_config = RedisBackendConfig(
            host="localhost",
            port=6379,
            username="user",
            password=SecretStr("password"),
        )
        expected_dsn = "redis://user:password@localhost:6379/1"
        assert backend_config.dsn == expected_dsn


class TestBrokerConfig:
    def test_broker_config_defaults(self) -> None:
        broker_config = BrokerConfig()
        assert isinstance(broker_config.redis, RedisBrokerConfig)
        assert broker_config.redis.host == "localhost"
        assert broker_config.redis.port == 6379
        assert broker_config.redis.db == 0


class TestBackendConfig:
    def test_backend_config_defaults(self) -> None:
        backend_config = BackendConfig()
        assert isinstance(backend_config.redis, RedisBackendConfig)
        assert backend_config.redis.host == "localhost"
        assert backend_config.redis.port == 6379
        assert backend_config.redis.db == 1


class TestChicoryConfig:
    def test_chicory_config_defaults(self) -> None:
        chicory_config = ChicoryConfig()
        assert isinstance(chicory_config.broker, BrokerConfig)
        assert isinstance(chicory_config.backend, BackendConfig)
        assert chicory_config.validation_mode == ValidationMode.INPUTS
