from __future__ import annotations

import pytest

from chicory.types import RetryBackoff, RetryPolicy, TaskOptions


class TestRetryPolicy:
    @pytest.mark.parametrize(
        "backoff_strategy, expected",
        [
            (RetryBackoff.FIXED, 2.0),
            (RetryBackoff.LINEAR, 6.0),
            (RetryBackoff.EXPONENTIAL, 8.0),
        ],
    )
    def test_calculate_delay(
        self, backoff_strategy: RetryBackoff, expected: float
    ) -> None:
        policy = RetryPolicy(
            retry_delay=2.0,
            backoff=backoff_strategy,
            jitter=False,
            max_delay=100.0,
        )
        delay = policy.calculate_delay(attempt=3)
        assert delay == expected

    @pytest.mark.parametrize(
        "backoff_strategy, expected",
        [
            (RetryBackoff.FIXED, 2.0),
            (RetryBackoff.LINEAR, 6.0),
            (RetryBackoff.EXPONENTIAL, 8.0),
        ],
    )
    def test_calculate_delay_with_jitter(
        self, backoff_strategy: RetryBackoff, expected: float
    ) -> None:
        policy = RetryPolicy(
            retry_delay=2.0,
            backoff=backoff_strategy,
            jitter=True,
            max_delay=100.0,
        )
        delay = policy.calculate_delay(attempt=3)
        assert expected * 0.75 <= delay <= expected * 1.25

    @pytest.mark.parametrize(
        "retry_on, ignore_on, exception, expected",
        [
            (None, None, ValueError(), True),
            (["ValueError"], None, ValueError(), True),
            (["ValueError"], None, KeyError(), False),
            (None, ["ValueError"], ValueError(), False),
            (["ValueError"], ["KeyError"], ValueError(), True),
            (["ValueError"], ["KeyError"], KeyError(), False),
        ],
    )
    def test_should_retry(
        self,
        retry_on: list[str] | None,
        ignore_on: list[str] | None,
        exception: Exception,
        expected: bool,
    ) -> None:
        policy = RetryPolicy(
            retry_on=retry_on,
            ignore_on=ignore_on,
        )
        assert policy.should_retry(exception) == expected


class TestTaskOptions:
    def test_get_retry_policy_default(self) -> None:
        options = TaskOptions()
        policy = options.get_retry_policy()
        assert policy.max_retries == 0
        assert policy.retry_delay == 1.0
        assert policy.backoff == RetryBackoff.EXPONENTIAL

    def test_get_retry_policy_custom(self) -> None:
        custom_policy = RetryPolicy(max_retries=5, retry_delay=2.0)
        options = TaskOptions(retry_policy=custom_policy)
        policy = options.get_retry_policy()
        assert policy.max_retries == 5
        assert policy.retry_delay == 2.0

    def test_get_retry_policy_none(self) -> None:
        options = TaskOptions(retry_policy=None)
        policy = options.get_retry_policy()
        assert policy.max_retries == 0
