from __future__ import annotations

import pytest

from chicory.context import TaskContext
from chicory.exceptions import RetryError
from chicory.types import RetryBackoff, RetryPolicy


class TestTaskContextRetry:
    def test_retry_within_max_retries(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=1,
            max_retries=3,
        )
        with pytest.raises(RetryError) as exc_info:
            context.retry(countdown=5.0)
        assert exc_info.value.retries == 1
        assert exc_info.value.max_retries == 3
        assert exc_info.value.countdown == 5.0

    def test_retry_exception_not_retryable(self) -> None:
        class NonRetryableError(Exception):
            pass

        retry_policy = RetryPolicy(
            max_retries=3,
            retry_delay=1.0,
            ignore_on=["NonRetryableError"],  # Mark as non-retryable
        )

        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=1,
            max_retries=3,
            retry_policy=retry_policy,
        )

        with pytest.raises(NonRetryableError) as exc_info:
            context.retry(exc=NonRetryableError("Not retryable"))
        assert str(exc_info.value) == "Not retryable"

    def test_retry_exception_retryable(self) -> None:
        class RetryableError(Exception):
            pass

        retry_policy = RetryPolicy(
            max_retries=3,
            retry_delay=1.0,
            # No ignore_on or retry_on, so all exceptions are retryable by default
        )

        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=1,
            max_retries=3,
            retry_policy=retry_policy,
        )

        with pytest.raises(RetryError) as exc_info:
            context.retry(exc=RetryableError("Retryable"))
        assert exc_info.value.retries == 1
        assert exc_info.value.max_retries == 3

    def test_retry_calculates_countdown_from_policy(self) -> None:
        retry_policy = RetryPolicy(
            max_retries=5,
            retry_delay=2.0,
            backoff=RetryBackoff.LINEAR,
            jitter=False,
        )

        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=2,
            max_retries=3,
            retry_policy=retry_policy,
        )

        with pytest.raises(RetryError) as exc_info:
            context.retry()
        assert exc_info.value.retries == 2
        assert exc_info.value.max_retries == 5
        assert exc_info.value.countdown == 6.0  # 3rd attempt => 3 * 2.0

    def test_retry_exceeds_max_retries(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=3,
            max_retries=3,
        )
        with pytest.raises(Exception) as exc_info:
            context.retry()
        assert "exceeded max retries" in str(exc_info.value)


class TestTaskContextFail:
    def test_fail_raises_exception(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=0,
            max_retries=3,
        )
        test_exception = ValueError("Explicit failure")
        with pytest.raises(ValueError) as exc_info:
            context.fail(test_exception)
        assert str(exc_info.value) == "Explicit failure"

    @pytest.mark.parametrize(
        "exc_type, exc_message",
        [
            (ValueError, "Value error occurred"),
            (RuntimeError, "Runtime error occurred"),
            (
                CustomException := type("CustomException", (Exception,), {}),
                "Custom exception occurred",
            ),
        ],
    )
    def test_fail_raises_various_exceptions(self, exc_type, exc_message) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=0,
            max_retries=3,
        )
        test_exception = exc_type(exc_message)
        with pytest.raises(exc_type) as exc_info:
            context.fail(test_exception)
        assert str(exc_info.value) == exc_message


class TestContextProperties:
    def test_is_last_retry_true(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=2,
            max_retries=3,
        )
        assert context.is_last_retry is True

    def test_is_last_retry_false(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=1,
            max_retries=3,
        )
        assert context.is_last_retry is False

    def test_remaining_retries(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=1,
            max_retries=4,
        )
        assert context.remaining_retries == 3

    def test_remaining_retries_zero(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=4,
            max_retries=4,
        )
        assert context.remaining_retries == 0

    def test_remaining_retries_negative(self) -> None:
        context = TaskContext(
            task_id="test-task",
            task_name="test-task-name",
            retries=5,
            max_retries=4,
        )
        assert context.remaining_retries == 0
