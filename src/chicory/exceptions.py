from __future__ import annotations


class ChicoryError(Exception):
    """Base exception for Chicory."""

    pass


class TaskNotFoundError(ChicoryError):
    """Raised when a task is not registered."""

    pass


class ValidationError(ChicoryError):
    """Raised when input/output validation fails."""

    pass


class RetryError(ChicoryError):
    """Raised to trigger a task retry."""

    def __init__(
        self,
        retries: int | None = None,
        max_retries: int | None = None,
        countdown: float | None = None,
    ):
        self.retries = retries
        self.max_retries = max_retries
        self.countdown = countdown
        super().__init__(
            f"Retry requested with countdown={countdown} "
            f"(attempt {retries}/{max_retries})"
        )


class MaxRetriesExceededError(ChicoryError):
    """Raised when max retries are exhausted."""

    pass


class BackendNotConfiguredError(ChicoryError):
    """Raised when backend operations are attempted without a backend."""

    pass


class BrokerConnectionError(ChicoryError):
    """Raised when broker connection fails."""

    pass
