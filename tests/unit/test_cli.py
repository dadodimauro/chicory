from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from chicory.app import Chicory
from chicory.cli.cli import _import_app, app
from chicory.config import ChicoryConfig
from chicory.types import BackendStatus, BrokerStatus, BrokerType, WorkerStats

runner = CliRunner()


@pytest.fixture
def app_path() -> str:
    return "my_app:app"


class MockBackend:
    def __init__(self, workers: list[WorkerStats] | None = None):
        self.workers = workers

    async def get_active_workers(self) -> list[WorkerStats]:
        return self.workers if self.workers is not None else []


class MockApp:
    def __init__(self, backend: Any = None, config: ChicoryConfig | None = None):
        self.backend = backend
        self.config = config or ChicoryConfig()

    async def connect(self):
        pass

    async def disconnect(self):
        pass


class MockWorkerConfig:
    def __init__(
        self,
        concurrency: int = 1,
        queue: str = "default",
        use_dead_letter_queue: bool = False,
        heartbeat_interval: float = 5.0,
        heartbeat_ttl: int = 30,
    ):
        self.concurrency = concurrency
        self.queue = queue
        self.use_dead_letter_queue = use_dead_letter_queue
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl


class MockWorker:
    def __init__(
        self,
        app,
        config: MockWorkerConfig | None = None,
    ):
        self.app = app
        self.config = config or MockWorkerConfig()
        self.concurrency = self.config.concurrency
        self.queue = self.config.queue
        self.use_dead_letter_queue = self.config.use_dead_letter_queue
        self.heartbeat_interval = self.config.heartbeat_interval
        self.heartbeat_ttl = self.config.heartbeat_ttl

    async def run(self):
        pass


class TestMain:
    def test_version_command(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "Chicory version:" in result.output

    def test_no_command_shows_help(self) -> None:
        result = runner.invoke(app, [])
        assert result.exit_code == 0
        assert "Usage:" in result.output


class TestWorker:
    def test_worker_command_help(self) -> None:
        result = runner.invoke(app, ["worker", "--help"])
        assert result.exit_code == 0
        assert "Start a Chicory worker." in result.output

    def test_worker_command_execution(
        self, monkeypatch: pytest.MonkeyPatch, app_path: str
    ) -> None:
        from chicory.config import WorkerConfig

        # Create a config with worker settings
        config = ChicoryConfig()
        config.worker = WorkerConfig(
            concurrency=2,
            queue="test_queue",
            heartbeat_interval=10.0,
            heartbeat_ttl=60,
            log_level="DEBUG",
        )
        mock_app_instance = MockApp(config=config)

        def mock_import_app(path: str) -> MockApp:
            return mock_app_instance

        monkeypatch.setattr("chicory.cli.cli._import_app", mock_import_app)
        monkeypatch.setattr("chicory.cli.cli.Worker", MockWorker)

        result = runner.invoke(
            app,
            [
                "worker",
                f"{app_path}",
                "--concurrency",
                "2",
                "--queue",
                "test_queue",
                "--heartbeat-interval",
                "10",
                "--heartbeat-ttl",
                "60",
                "--log-level",
                "DEBUG",
            ],
        )
        assert result.exit_code == 0
        assert f"Starting Chicory worker for {app_path}" in result.output
        assert "Log Level: DEBUG" in result.output
        assert "Concurrency: 2" in result.output
        assert "Queue: test_queue" in result.output
        assert "Heartbeat Interval: 10.0s" in result.output
        assert "Heartbeat TTL: 60s" in result.output
        assert "Dead Letter Queue: Disabled" in result.output


class TestWorkers:
    def test_workers_command_help(self) -> None:
        result = runner.invoke(app, ["workers", "--help"])
        assert result.exit_code == 0
        assert "List all active workers." in result.output

    @pytest.mark.parametrize(
        "workers, expected_output",
        [
            (
                [
                    WorkerStats(
                        worker_id="worker1",
                        hostname="host1",
                        pid=1234,
                        queue="default",
                        concurrency=4,
                        tasks_processed=100,
                        tasks_failed=5,
                        active_tasks=2,
                        started_at=datetime.now(UTC) - timedelta(hours=1),
                        last_heartbeat=datetime.now(UTC) - timedelta(seconds=10),
                        is_running=True,
                        broker=BrokerStatus(connected=True, error=None),
                        backend=BackendStatus(connected=True, error=None),
                    ),
                    WorkerStats(
                        worker_id="worker2",
                        hostname="host2",
                        pid=5678,
                        queue="high_priority",
                        concurrency=2,
                        tasks_processed=50,
                        tasks_failed=0,
                        active_tasks=0,
                        started_at=datetime.now(UTC) - timedelta(minutes=30),
                        last_heartbeat=datetime.now(UTC) - timedelta(seconds=5),
                        is_running=False,
                        broker=BrokerStatus(connected=False, error="Connection lost"),
                        backend=BackendStatus(connected=True, error=None),
                    ),
                ],
                2,
            ),
            ([], 0),
        ],
    )
    def test_workers_command_execution(
        self,
        monkeypatch: pytest.MonkeyPatch,
        app_path: str,
        workers: list[WorkerStats],
        expected_output: int,
    ) -> None:
        mock_backend_instance = MockBackend(workers=workers)
        mock_app_instance = MockApp(backend=mock_backend_instance)

        def mock_import_app(path: str) -> MockApp:
            return mock_app_instance

        monkeypatch.setattr("chicory.cli.cli._import_app", mock_import_app)
        monkeypatch.setattr("chicory.cli.cli.Worker", MockWorker)

        result = runner.invoke(
            app,
            [
                "workers",
                f"{app_path}",
            ],
        )
        assert result.exit_code == 0
        if expected_output == 0:
            assert "No active workers found." in result.output
            return
        assert "Active Workers:" in result.output
        for i in range(expected_output):
            assert f"Worker ID: worker{i + 1}" in result.output

    def test_workers_command_execution_no_backend(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_app_instance = MockApp()

        def mock_import_app(path: str) -> MockApp:
            return mock_app_instance

        monkeypatch.setattr("chicory.cli.cli._import_app", mock_import_app)
        monkeypatch.setattr("chicory.cli.cli.Worker", MockWorker)

        result = runner.invoke(
            app,
            [
                "workers",
                "my_app:app",
            ],
        )
        assert result.exit_code == 0
        assert "No backend configured. Cannot retrieve worker info." in result.output


class TestCleanup:
    def test_cleanup_command_help(self) -> None:
        result = runner.invoke(app, ["cleanup", "--help"])
        assert result.exit_code == 0
        assert "Cleanup stale worker records." in result.output

    def test_cleanup_command_execution(
        self, monkeypatch: pytest.MonkeyPatch, app_path: str
    ) -> None:
        mock_backend_instance = MockBackend()
        mock_backend_instance.cleanup_stale_workers = AsyncMock(return_value=3)  # ty:ignore[unresolved-attribute]
        mock_app_instance = MockApp(backend=mock_backend_instance)

        def mock_import_app(path: str) -> MockApp:
            return mock_app_instance

        monkeypatch.setattr("chicory.cli.cli._import_app", mock_import_app)
        monkeypatch.setattr("chicory.cli.cli.Worker", MockWorker)

        result = runner.invoke(
            app,
            [
                "cleanup",
                f"{app_path}",
                "--stale-seconds",
                "300",
            ],
        )
        assert result.exit_code == 0
        assert "Removed 3 stale worker(s)." in result.output

    def test_cleanup_command_execution_no_backend(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_app_instance = MockApp()

        def mock_import_app(path: str) -> MockApp:
            return mock_app_instance

        monkeypatch.setattr("chicory.cli.cli._import_app", mock_import_app)
        monkeypatch.setattr("chicory.cli.cli.Worker", MockWorker)

        result = runner.invoke(
            app,
            [
                "cleanup",
                "my_app:app",
                "--stale-seconds",
                "300",
            ],
        )
        assert result.exit_code == 0
        assert "No backend configured. Cannot clean up workers." in result.output


class TestImportApp:
    def test_import_app_with_colon_syntax(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test importing app with 'module:app' syntax."""

        mock_chicory_app = Chicory(broker=BrokerType.REDIS)

        def mock_import_module(module_path: str):
            assert module_path == "my_module"
            return type("module", (), {"my_app": mock_chicory_app})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        from chicory.cli.cli import _import_app

        result = _import_app("my_module:my_app")
        assert result is mock_chicory_app

    def test_import_app_without_colon_defaults_to_app(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test importing without colon defaults to 'app' attribute."""

        mock_chicory_app = Chicory(broker=BrokerType.REDIS)

        def mock_import_module(module_path: str):
            assert module_path == "my_module"
            return type("module", (), {"app": mock_chicory_app})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        result = _import_app("my_module")
        assert result is mock_chicory_app

    def test_import_app_raises_type_error_for_non_chicory_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Test that TypeError is raised when imported object is not a Chicory instance.
        """
        not_a_chicory_app = {"some": "dict"}

        def mock_import_module(module_path: str):
            return type("module", (), {"app": not_a_chicory_app})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        with pytest.raises(TypeError) as exc_info:
            _import_app("my_module:app")
        assert "The object 'app' is not a Chicory app instance." in str(exc_info.value)

    def test_import_app_raises_attribute_error_for_missing_attribute(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that AttributeError is raised when app attribute doesn't exist."""

        def mock_import_module(module_path: str):
            return type("module", (), {})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        with pytest.raises(AttributeError):
            _import_app("my_module:nonexistent_app")

    def test_import_app_raises_import_error_for_missing_module(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that ImportError is raised when module doesn't exist."""

        def mock_import_module(module_path: str):
            raise ImportError(f"No module named '{module_path}'")

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        with pytest.raises(ImportError) as exc_info:
            _import_app("nonexistent_module:app")
        assert "No module named 'nonexistent_module'" in str(exc_info.value)

    def test_import_app_adds_cwd_to_sys_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that current working directory is added to sys.path."""
        import sys
        from pathlib import Path

        original_sys_path = sys.path.copy()
        mock_chicory_app = Chicory(broker=BrokerType.REDIS)
        cwd = str(Path.cwd())

        def mock_import_module(module_path: str):
            return type("module", (), {"app": mock_chicory_app})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        # Remove cwd from sys.path if it exists
        if cwd in sys.path:
            sys.path.remove(cwd)

        _import_app("my_module")

        assert cwd in sys.path

        # Cleanup: restore original sys.path
        sys.path[:] = original_sys_path

    def test_import_app_with_nested_module_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test importing from nested module path."""

        mock_chicory_app = Chicory(broker=BrokerType.REDIS)

        def mock_import_module(module_path: str):
            assert module_path == "my_package.submodule.tasks"
            return type("module", (), {"celery_app": mock_chicory_app})()

        monkeypatch.setattr(
            "chicory.cli.cli.importlib.import_module", mock_import_module
        )

        result = _import_app("my_package.submodule.tasks:celery_app")
        assert result is mock_chicory_app
