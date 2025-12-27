from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, NoReturn

from chicory.exceptions import MaxRetriesExceededError, RetryError

if TYPE_CHECKING:
    from chicory.types import RetryPolicy


@dataclass
class TaskContext:
    """Runtime context injected into tasks."""

    task_id: str
    task_name: str
    retries: int
    max_retries: int
    retry_policy: RetryPolicy | None = field(default=None, repr=False)

    async def retry(
        self,
        countdown: float | None = None,
        exc: Exception | None = None,
    ) -> NoReturn:
        """
        Request a task retry by raising a RetryError.

        Args:
            countdown: Optional delay in seconds before retry.
                        If None and a retry_policy is configured,
                        the delay will be calculated from the policy.
            exc: Optional exception that caused the retry. Used to check if the
                    exception is retryable according to the retry policy.
        """

        effective_max = (
            self.retry_policy.max_retries if self.retry_policy else self.max_retries
        )

        if self.retries >= effective_max:
            raise MaxRetriesExceededError(
                f"Task {self.task_name!r} exceeded max retries ({effective_max})"
            )

        # Check if exception is retryable (if provided and policy exists)
        if (
            exc is not None
            and self.retry_policy is not None
            and not self.retry_policy.should_retry(exc)
        ):
            raise exc  # Re-raise original exception, will go to DLQ

        # Calculate countdown from policy if not provided
        if countdown is None and self.retry_policy is not None:
            next_attempt = self.retries + 1
            countdown = self.retry_policy.calculate_delay(next_attempt)

        raise RetryError(
            retries=self.retries,
            max_retries=effective_max,
            countdown=countdown,
        )

    async def fail(self, exc: Exception) -> NoReturn:
        """Explicitly fail the task (will be moved to DLQ)."""
        raise exc

    @property
    def is_last_retry(self) -> bool:
        """Check if this is the last retry attempt."""
        effective_max = (
            self.retry_policy.max_retries if self.retry_policy else self.max_retries
        )
        return self.retries >= effective_max - 1

    @property
    def remaining_retries(self) -> int:
        """Get the number of remaining retry attempts."""
        effective_max = (
            self.retry_policy.max_retries if self.retry_policy else self.max_retries
        )
        return max(0, effective_max - self.retries)
