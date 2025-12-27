from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from chicory.backend.base import Backend
from chicory.exceptions import BackendNotConfiguredError
from chicory.result import AsyncResult
from chicory.types import TaskState


class TestAyncResultEnsureBackend:
    def test_ensure_backend_has_backend(self) -> None:
        backend_mock = Mock(spec=Backend)
        async_result = AsyncResult[Any](task_id="test-task", backend=backend_mock)
        backend = async_result._ensure_backend()
        assert backend is backend_mock

    def test_ensure_backend_no_backend(self) -> None:
        async_result = AsyncResult[Any](task_id="test-task", backend=None)
        with pytest.raises(Exception) as exc_info:
            async_result._ensure_backend()
        assert isinstance(exc_info.value, BackendNotConfiguredError)
        assert str(exc_info.value) == (
            "Cannot access result: no backend configured. "
            "Configure a backend or use .send() for fire-and-forget."
        )


@pytest.mark.asyncio
class TestAsyncResultGet:
    async def test_get_successful_result(self) -> None:
        backend_mock = AsyncMock(spec=Backend)
        # return a result after two polls
        backend_mock.get_result = AsyncMock(
            side_effect=[
                None,
                Mock(state=TaskState.PENDING, result=42, error=None),
                Mock(state=TaskState.SUCCESS, result=42, error=None),
            ]
        )
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        result = await async_result.get(timeout=1.0, poll_interval=0.01)
        assert result == 42
        assert backend_mock.get_result.call_count == 3

    async def test_get_failure_result(self) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(
            side_effect=[
                None,
                Mock(state=TaskState.FAILURE, result=None, error="Task failed"),
            ]
        )
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        with pytest.raises(Exception) as exc_info:
            await async_result.get(timeout=1.0, poll_interval=0.01)
        assert str(exc_info.value) == "Task failed"
        assert backend_mock.get_result.call_count == 2

    async def test_get_timeout(self) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(return_value=None)
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        with pytest.raises(TimeoutError) as exc_info:
            await async_result.get(timeout=0.05, poll_interval=0.01)
        assert str(exc_info.value) == "Task test-task did not complete in 0.05s"
        assert (
            backend_mock.get_result.call_count >= 5
        )  # At least 5 polls within timeout


@pytest.mark.asyncio
class TestAsyncResultState:
    async def test_state_pending(self) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(return_value=None)
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        state = await async_result.state()
        assert state == TaskState.PENDING
        backend_mock.get_result.assert_awaited_once_with("test-task")

    @pytest.mark.parametrize(
        "task_state",
        [
            TaskState.PENDING,
            TaskState.STARTED,
            TaskState.SUCCESS,
            TaskState.FAILURE,
            TaskState.RETRY,
        ],
    )
    async def test_state_success(self, task_state: TaskState) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(
            return_value=Mock(state=task_state, result=42, error=None)
        )
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        state = await async_result.state()
        assert state == task_state
        backend_mock.get_result.assert_awaited_once_with("test-task")


@pytest.mark.asyncio
class TestAsyncResultReady:
    @pytest.mark.parametrize(
        "task_state,expected_ready",
        [
            (TaskState.PENDING, False),
            (TaskState.STARTED, False),
            (TaskState.SUCCESS, True),
            (TaskState.FAILURE, True),
            (TaskState.RETRY, False),
        ],
    )
    async def test_ready(self, task_state: TaskState, expected_ready: bool) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(
            return_value=Mock(state=task_state, result=42, error=None)
        )
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        is_ready = await async_result.ready()
        assert is_ready == expected_ready
        backend_mock.get_result.assert_awaited_once_with("test-task")


@pytest.mark.asyncio
class TestAsyncResultFailed:
    @pytest.mark.parametrize(
        "task_state,expected_failed",
        [
            (TaskState.PENDING, False),
            (TaskState.STARTED, False),
            (TaskState.SUCCESS, False),
            (TaskState.FAILURE, True),
            (TaskState.RETRY, False),
        ],
    )
    async def test_failed(self, task_state: TaskState, expected_failed: bool) -> None:
        backend_mock = AsyncMock(spec=Backend)
        backend_mock.get_result = AsyncMock(
            return_value=Mock(state=task_state, result=42, error=None)
        )
        async_result = AsyncResult[int](task_id="test-task", backend=backend_mock)

        has_failed = await async_result.failed()
        assert has_failed == expected_failed
        backend_mock.get_result.assert_awaited_once_with("test-task")
