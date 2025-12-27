from __future__ import annotations

import os
from typing import Literal, Self

from pydantic import Field, RedisDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from chicory.types import DeliveryMode, ValidationMode


class BaseBrokerConfig(BaseSettings):
    """Base configuration for all broker types."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class BaseBackendConfig(BaseSettings):
    """Base configuration for all backend types."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class RedisBrokerConfig(BaseBrokerConfig):
    """Redis Streams broker configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BROKER_REDIS_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis port")
    db: int = Field(default=0, ge=0, le=15, description="Redis database number")
    username: str | None = Field(default=None, description="Redis username")
    password: SecretStr | None = Field(default=None, description="Redis password")
    url: str | None = Field(
        default=None,
        description="Full Redis URL (overrides host/port/db if set)",
    )

    consumer_group: str = Field(
        default="chicory-workers",
        description="Redis consumer group name",
    )
    block_ms: int = Field(
        default=5000,
        ge=0,
        description="Blocking time in ms for XREADGROUP",
    )
    claim_min_idle_ms: int = Field(
        default=30000,
        ge=0,
        description="Reclaim messages idle for this many ms",
    )
    max_stream_length: int | None = Field(
        default=100000,
        ge=1,
        description="Max stream length (None for unlimited)",
    )
    dlq_max_length: int | None = Field(
        default=10000,
        ge=1,
        description="Max DLQ size (None for unlimited)",
    )

    key_prefix: str = Field(
        default="chicory",
        description="Prefix for all Redis keys",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            RedisDsn(v)
        return v

    @property
    def dsn(self) -> str:
        """Build Redis DSN from config."""
        if self.url:
            return str(RedisDsn(self.url))
        return str(
            RedisDsn.build(
                scheme="redis",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=str(self.db),
            )
        )


class RedisBackendConfig(BaseBackendConfig):
    """Redis result backend configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_REDIS_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis port")
    db: int = Field(default=1, ge=0, le=15, description="Redis database number")
    username: str | None = Field(default=None, description="Redis username")
    password: SecretStr | None = Field(default=None, description="Redis password")
    url: str | None = Field(
        default=None,
        description="Full Redis URL (overrides host/port/db if set)",
    )

    result_ttl: int = Field(
        default=3600,
        ge=0,
        description="Result expiration in seconds (0 = no expiration)",
    )

    key_prefix: str = Field(
        default="chicory",
        description="Prefix for all Redis keys",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            RedisDsn(v)
        return v

    @property
    def dsn(self) -> str:
        """Build Redis DSN from config."""
        if self.url:
            return str(RedisDsn(self.url))
        return str(
            RedisDsn.build(
                scheme="redis",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=str(self.db),
            )
        )


class WorkerConfig(BaseSettings):
    """Worker process configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_WORKER_",
        extra="ignore",
    )

    concurrency: int = Field(
        default_factory=lambda: min(32, (os.cpu_count() or 1) + 4),
        ge=1,
        description="Number of concurrent task workers",
    )
    queue: str = Field(
        default="default",
        description="Default queue to consume from",
    )
    use_dead_letter_queue: bool = Field(
        default=False,
        description="Enable dead letter queue for failed tasks",
    )
    heartbeat_interval: float = Field(
        default=10.0,
        gt=0,
        description="Interval in seconds between heartbeats",
    )
    heartbeat_ttl: int = Field(
        default=30,
        ge=1,
        description="Heartbeat TTL in seconds",
    )
    shutdown_timeout: float = Field(
        default=30.0,
        ge=0,
        description="Grace period for shutdown in seconds",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level for worker",
    )


class BrokerConfig(BaseSettings):
    """Container for broker-specific configurations."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BROKER_",
        extra="ignore",
    )

    redis: RedisBrokerConfig = Field(default_factory=RedisBrokerConfig)


class BackendConfig(BaseSettings):
    """Container for backend-specific configurations."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_",
        extra="ignore",
    )

    redis: RedisBackendConfig = Field(default_factory=RedisBackendConfig)


class ChicoryConfig(BaseSettings):
    """
    Main configuration for the Chicory task queue.

    Configuration priority (highest to lowest):
    1. Explicit parameters passed to Chicory()
    2. This config object (if passed)
    3. Environment variables
    4. Default values

    Environment variables use the prefix CHICORY_.

    Example:
        # Using environment variables:
        # CHICORY_VALIDATION_MODE=strict
        # CHICORY_BROKER_REDIS_HOST=redis.example.com

        # Or programmatically:
        config = ChicoryConfig(
            validation_mode=ValidationMode.STRICT,
            delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        )
        app = Chicory(broker=BrokerType.REDIS, config=config)
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_",
        extra="ignore",
    )

    broker: BrokerConfig = Field(default_factory=BrokerConfig)
    backend: BackendConfig = Field(default_factory=BackendConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)

    # App-level defaults
    validation_mode: ValidationMode = Field(
        default=ValidationMode.INPUTS,
        description="Default validation mode for tasks",
    )
    delivery_mode: DeliveryMode = Field(
        default=DeliveryMode.AT_LEAST_ONCE,
        description="Default delivery mode for tasks",
    )

    # Result polling
    result_poll_interval: float = Field(
        default=0.1,
        gt=0,
        description="Poll interval for AsyncResult.get() in seconds",
    )
    result_default_timeout: float | None = Field(
        default=None,
        ge=0,
        description="Default timeout for AsyncResult.get() (None = no timeout)",
    )

    @model_validator(mode="after")
    def validate_config(self) -> Self:
        """Validate cross-field constraints."""
        # Ensure heartbeat_ttl > heartbeat_interval
        if self.worker.heartbeat_ttl <= self.worker.heartbeat_interval:
            raise ValueError(
                f"heartbeat_ttl ({self.worker.heartbeat_ttl}) must be greater than "
                f"heartbeat_interval ({self.worker.heartbeat_interval})"
            )
        return self
