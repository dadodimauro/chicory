from __future__ import annotations

import os
from typing import Literal, Self

from pydantic import (
    AmqpDsn,
    AnyUrl,
    Field,
    MySQLDsn,
    PostgresDsn,
    RedisDsn,
    SecretStr,
    field_validator,
    model_validator,
)
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


class RabbitMQBrokerConfig(BaseBrokerConfig):
    """RabbitMQ broker configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BROKER_RABBITMQ_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="RabbitMQ host")
    port: int = Field(default=5672, ge=1, le=65535, description="RabbitMQ port")
    username: str = Field(default="guest", description="RabbitMQ username")
    password: SecretStr = Field(
        default=SecretStr("guest"), description="RabbitMQ password"
    )
    vhost: str | None = Field(default=None, description="RabbitMQ virtual host")
    url: str | None = Field(
        default=None,
        description="Full AMQP URL (overrides host/port if set)",
    )

    connection_pool_size: int = Field(
        default=1,
        ge=1,
        description="Number of connections in the pool",
    )
    channel_pool_size: int = Field(
        default=10,
        ge=1,
        description="Number of channels in the pool",
    )
    prefetch_count: int = Field(
        default=1,
        ge=1,
        description="Number of messages to prefetch per consumer (QoS)",
    )
    queue_max_length: int | None = Field(
        default=None,
        ge=1,
        description="Maximum number of messages in queue (None for unlimited)",
    )
    queue_max_length_bytes: int | None = Field(
        default=None,
        ge=1,
        description="Maximum size in bytes for queue (None for unlimited)",
    )
    dlq_max_length: int | None = Field(
        default=10000,
        ge=1,
        description="Maximum number of messages in DLQ (None for unlimited)",
    )
    message_ttl: int | None = Field(
        default=None,
        ge=1,
        description="Message TTL in milliseconds (None for no expiration)",
    )
    dlq_message_ttl: int | None = Field(
        default=None,
        ge=1,
        description="DLQ message TTL in milliseconds (None for no expiration)",
    )
    durable_queues: bool = Field(
        default=True,
        description="Whether queues should be durable (survive broker restarts)",
    )
    queue_mode: Literal["default", "lazy"] | None = Field(
        default=None,
        description=(
            "Queue mode: 'lazy' moves messages to disk ASAP, 'default' keeps in memory"
        ),
    )
    max_priority: int | None = Field(
        default=None,
        ge=1,
        le=255,
        description="Maximum priority level for priority queues (None for no priority)",
    )
    channel_acquire_timeout: float = Field(
        default=10.0,
        gt=0,
        description="Timeout in seconds when acquiring a channel from the pool",
    )
    reconnect_delay_base: float = Field(
        default=1.0,
        gt=0,
        description="Base delay in seconds for reconnection attempts",
    )
    reconnect_delay_max: float = Field(
        default=60.0,
        gt=0,
        description="Maximum delay in seconds for reconnection attempts",
    )
    max_dlq_scan_limit: int = Field(
        default=10000,
        ge=1,
        description="Maximum number of messages to scan when processing DLQ",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            AmqpDsn(v)
        return v

    @property
    def dsn(self) -> str:
        """Build RabbitMQ DSN from config."""
        if self.url:
            return str(AmqpDsn(self.url))
        return str(
            AmqpDsn.build(
                scheme="amqp",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=self.vhost,
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


class PostgresBackendConfig(BaseBackendConfig):
    """PostgreSQL result backend configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_POSTGRES_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, ge=1, le=65535, description="PostgreSQL port")
    database: str = Field(default="chicory", description="PostgreSQL database name")
    username: str = Field(default="chicory", description="PostgreSQL username")
    password: SecretStr = Field(
        default=SecretStr("chicory"), description="PostgreSQL password"
    )
    url: str | None = Field(
        default=None,
        description="Full PostgreSQL DSN (overrides other settings if set)",
    )

    echo: bool = Field(
        default=False,
        description="Enable SQL query logging",
    )
    pool_size: int = Field(
        default=5,
        ge=1,
        description="Database connection pool size",
    )
    max_overflow: int = Field(
        default=5,
        ge=0,
        description="Maximum overflow size for the connection pool",
    )
    pool_recycle: int = Field(
        default=1800,
        ge=0,
        description="Recycle connections after this many seconds",
    )
    pool_timeout: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds to wait for a connection from the pool",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            PostgresDsn(v)
        return v

    @property
    def dsn(self) -> str:
        """Build PostgreSQL DSN from config."""
        if self.url:
            return str(PostgresDsn(self.url))
        return str(
            PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=self.database,
            )
        )


class MySQLBackendConfig(BaseBackendConfig):
    """MySQL result backend configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_MYSQL_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="MySQL host")
    port: int = Field(default=3306, ge=1, le=65535, description="MySQL port")
    database: str = Field(default="chicory", description="MySQL database name")
    username: str = Field(default="chicory", description="MySQL username")
    password: SecretStr = Field(
        default=SecretStr("chicory"), description="MySQL password"
    )
    url: str | None = Field(
        default=None,
        description="Full MySQL DSN (overrides other settings if set)",
    )

    echo: bool = Field(
        default=False,
        description="Enable SQL query logging",
    )
    pool_size: int = Field(
        default=5,
        ge=1,
        description="Database connection pool size",
    )
    max_overflow: int = Field(
        default=5,
        ge=0,
        description="Maximum overflow size for the connection pool",
    )
    pool_recycle: int = Field(
        default=1800,
        ge=0,
        description="Recycle connections after this many seconds",
    )
    pool_timeout: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds to wait for a connection from the pool",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            MySQLDsn(v)
        return v

    @property
    def dsn(self) -> str:
        """Build MySQL DSN from config."""
        if self.url:
            return str(MySQLDsn(self.url))
        return str(
            MySQLDsn.build(
                scheme="mysql+aiomysql",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=self.database,
            )
        )


class MSSQLBackendConfig(BaseBackendConfig):
    """MSSQL result backend configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_MSSQL_",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="MSSQL host")
    port: int = Field(default=1433, ge=1, le=65535, description="MSSQL port")
    database: str = Field(default="chicory", description="MSSQL database name")
    username: str = Field(default="chicory", description="MSSQL username")
    password: SecretStr = Field(
        default=SecretStr("chicory"), description="MSSQL password"
    )
    driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="ODBC driver name for MSSQL connection",
    )
    trust_server_certificate: bool = Field(
        default=True,
        description="Whether to trust the server certificate (for SSL connections)",
    )
    url: str | None = Field(
        default=None,
        description="Full MSSQL DSN (overrides other settings if set)",
    )

    echo: bool = Field(
        default=False,
        description="Enable SQL query logging",
    )
    pool_size: int = Field(
        default=5,
        ge=1,
        description="Database connection pool size",
    )
    max_overflow: int = Field(
        default=5,
        ge=0,
        description="Maximum overflow size for the connection pool",
    )
    pool_recycle: int = Field(
        default=1800,
        ge=0,
        description="Recycle connections after this many seconds",
    )
    pool_timeout: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds to wait for a connection from the pool",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            dsn = AnyUrl(v)
            if dsn.scheme != "mssql+aioodbc":
                raise ValueError("MSSQL DSN must use 'mssql+aioodbc' scheme")
        return v

    @field_validator("driver", mode="after")
    @classmethod
    def validate_driver(cls, v: str) -> str:
        """Replace spaces with '+' for URL encoding."""
        return v.replace(" ", "+")

    @property
    def dsn(self) -> str:
        """Build MSSQL DSN from config."""
        if self.url:
            return str(AnyUrl(self.url))
        return str(
            AnyUrl.build(
                scheme="mssql+aioodbc",
                username=self.username,
                password=self.password.get_secret_value() if self.password else None,
                host=self.host,
                port=self.port,
                path=self.database,
                query=(
                    f"driver={self.driver}&TrustServerCertificate="
                    f"{'yes' if self.trust_server_certificate else 'no'}"
                ),
            )
        )


class SQLiteBackendConfig(BaseBackendConfig):
    """SQLite result backend configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_SQLITE_",
        extra="ignore",
    )

    file_path: str = Field(
        default="chicory_results.db",
        description="Path to SQLite database file",
    )
    url: str | None = Field(
        default=None,
        description="Full SQLite DSN (overrides other settings if set)",
    )

    echo: bool = Field(
        default=False,
        description="Enable SQL query logging",
    )
    pool_size: int = Field(
        default=5,
        ge=1,
        description="Database connection pool size",
    )
    max_overflow: int = Field(
        default=5,
        ge=0,
        description="Maximum overflow size for the connection pool",
    )
    pool_recycle: int = Field(
        default=1800,
        ge=0,
        description="Recycle connections after this many seconds",
    )
    pool_timeout: int = Field(
        default=30,
        ge=1,
        description="Timeout in seconds to wait for a connection from the pool",
    )

    @field_validator("url", mode="after")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        if v is not None:
            dsn = AnyUrl(v)
            if dsn.scheme != "sqlite+aiosqlite":
                raise ValueError("SQLite DSN must use 'sqlite+aiosqlite' scheme")
        return v

    @property
    def dsn(self) -> str:
        """Build SQLite DSN from config."""
        if self.url:
            return str(AnyUrl(self.url))
        return str(
            AnyUrl.build(
                scheme="sqlite+aiosqlite",
                host=self.file_path,
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
    rabbitmq: RabbitMQBrokerConfig = Field(default_factory=RabbitMQBrokerConfig)


class BackendConfig(BaseSettings):
    """Container for backend-specific configurations."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHICORY_BACKEND_",
        extra="ignore",
    )

    redis: RedisBackendConfig = Field(default_factory=RedisBackendConfig)
    postgres: PostgresBackendConfig = Field(default_factory=PostgresBackendConfig)
    mysql: MySQLBackendConfig = Field(default_factory=MySQLBackendConfig)
    mssql: MSSQLBackendConfig = Field(default_factory=MSSQLBackendConfig)
    sqlite: SQLiteBackendConfig = Field(default_factory=SQLiteBackendConfig)


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
