import subprocess
import venv
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import chicory  # Import to avoid errors with pytest-cov # noqa: F401


class TestOptionalImports:
    """Test actual installations in isolated environments like real users."""

    @pytest.fixture
    def project_root(self) -> Path:
        """Get the project root directory."""
        return Path(__file__).parent.parent.parent

    def _run_in_venv(self, venv_dir: Path, command: str) -> subprocess.CompletedProcess:
        """Run a Python command in a virtual environment."""
        python_bin = venv_dir / "bin" / "python"
        return subprocess.run(
            [str(python_bin), "-c", command],
            capture_output=True,
            text=True,
        )

    def _run_app_in_venv(
        self, venv_dir: Path, args: list[str]
    ) -> subprocess.CompletedProcess:
        """Run the chicory CLI app in a virtual environment."""
        chicory_bin = venv_dir / "bin" / "chicory"
        return subprocess.run(
            [str(chicory_bin), *args],
            capture_output=True,
            text=True,
        )

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "import_command",
        [
            "from chicory import broker; print('RedisBroker' in broker.__all__)",
            "from chicory import backend; print('RedisBackend' in backend.__all__)",
            "from chicory import broker; print('RabbitMQBroker' in broker.__all__)",
        ],
    )
    def test_base_install_without_extras(
        self, project_root: Path, import_command: str
    ) -> None:
        """Test that base install works without optional dependencies."""
        with TemporaryDirectory() as tmpdir:
            venv_dir = Path(tmpdir) / "venv"

            # Create virtual environment
            venv.create(venv_dir, with_pip=True)

            # Install chicory without extras
            pip_bin = venv_dir / "bin" / "pip"
            subprocess.run(
                [str(pip_bin), "install", "-e", str(project_root)],
                capture_output=True,
                check=True,
            )

            # Test that chicory imports
            result = self._run_in_venv(
                venv_dir,
                "import chicory; print('success')",
            )
            assert result.returncode == 0
            assert "success" in result.stdout

            # Test that optional components are NOT available
            result = self._run_in_venv(
                venv_dir,
                import_command,
            )
            assert result.returncode == 0
            assert "False" in result.stdout

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "extra,import_commands,import_command_all_names",
        [
            (
                "redis",
                [
                    "from chicory.broker import RedisBroker; print('success')",
                    "from chicory.backend import RedisBackend; print('success')",
                ],
                [
                    "from chicory import broker; print('RedisBroker' in broker.__all__)",  # noqa: E501
                    "from chicory import backend; print('RedisBackend' in backend.__all__)",  # noqa: E501
                ],
            ),
            (
                "rabbitmq",
                [
                    "from chicory.broker import RabbitMQBroker; print('success')",
                ],
                [
                    "from chicory import broker; print('RabbitMQBroker' in broker.__all__)",  # noqa: E501
                ],
            ),
            (
                "all",
                [
                    "from chicory.broker import RedisBroker, RabbitMQBroker; print('success')",  # noqa: E501
                    "from chicory.backend import RedisBackend; print('success')",
                ],
                [
                    "from chicory import broker; print('RedisBroker' in broker.__all__)",  # noqa: E501
                    "from chicory import broker; print('RabbitMQBroker' in broker.__all__)",  # noqa: E501
                    "from chicory import backend; print('RedisBackend' in backend.__all__)",  # noqa: E501
                ],
            ),
        ],
    )
    def test_optional_extras_install(
        self,
        project_root: Path,
        extra: str,
        import_commands: list[str],
        import_command_all_names: list[str],
    ) -> None:
        """Test that redis extra installs and works correctly."""
        with TemporaryDirectory() as tmpdir:
            venv_dir = Path(tmpdir) / "venv"

            # Create virtual environment
            venv.create(venv_dir, with_pip=True)

            # Install chicory with extra dependency
            pip_bin = venv_dir / "bin" / "pip"
            subprocess.run(
                [str(pip_bin), "install", "-e", f"{project_root}[{extra}]"],
                capture_output=True,
                check=True,
            )

            # Test that optional components ARE available
            for import_command in import_commands:
                result = self._run_in_venv(
                    venv_dir,
                    import_command,
                )
                assert result.returncode == 0, f"stderr: {result.stderr}"
                assert "success" in result.stdout

            # Test that optional components are in __all__
            for import_command_all in import_command_all_names:
                result = self._run_in_venv(
                    venv_dir,
                    import_command_all,
                )
                assert result.returncode == 0
                assert "True" in result.stdout

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "extra",
        [None, "cli", "all"],
    )
    def test_cli_install(self, project_root: Path, extra: str | None) -> None:
        """Test that CLI extra installs and works correctly."""
        with TemporaryDirectory() as tmpdir:
            venv_dir = Path(tmpdir) / "venv"

            # Create virtual environment
            venv.create(venv_dir, with_pip=True)

            # Install chicory with cli extra
            pip_bin = venv_dir / "bin" / "pip"
            install_cmd = (
                [str(pip_bin), "install", "-e", str(project_root)]
                if extra is None
                else [str(pip_bin), "install", "-e", f"{project_root}[{extra}]"]
            )
            subprocess.run(
                install_cmd,
                capture_output=True,
                check=True,
            )

            if extra is None:
                # Test that chicory CLI is NOT available
                result = self._run_app_in_venv(
                    venv_dir,
                    ["--help"],
                )
                assert result.returncode != 0
                assert "Chicory CLI is not installed." in result.stderr
                return

            # Test that chicory CLI imports
            result = self._run_app_in_venv(
                venv_dir,
                ["--help"],
            )
            assert result.returncode == 0
            assert "Chicory task queue worker CLI." in result.stdout
