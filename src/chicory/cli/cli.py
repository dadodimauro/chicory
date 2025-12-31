from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import logging
import signal
import sys
import time
from multiprocessing import Process
from pathlib import Path
from typing import Literal

import typer

from chicory.app import Chicory
from chicory.worker import Worker

app = typer.Typer(help="Chicory task queue worker CLI.")


def _import_app(app_path: str) -> Chicory:
    """Import the Chicory app from the given path."""
    # Add current directory to sys.path so we can import user modules
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if ":" in app_path:
        module_path, app_name = app_path.rsplit(":", 1)
    else:
        module_path = app_path
        app_name = "app"

    module = importlib.import_module(module_path)
    chicory_app = getattr(module, app_name)

    if not isinstance(chicory_app, Chicory):
        raise TypeError(f"The object '{app_name}' is not a Chicory app instance.")

    return chicory_app


@app.command()
def worker(
    app_path: str = typer.Argument(
        ..., help="Path to the Chicory app (e.g., 'myapp.tasks:app')"
    ),
    workers_count: int = typer.Option(
        1, "--workers", "-w", help="Number of worker processes"
    ),
    concurrency: int = typer.Option(
        32, "--concurrency", "-c", help="Number of concurrent workers"
    ),
    queue: str = typer.Option("default", "--queue", "-q", help="Queue to consume from"),
    use_dead_letter_queue: bool = typer.Option(
        False,
        "--dlq/--no-dlq",
        help="Enable or disable Dead Letter Queue handling",
    ),
    heartbeat_interval: float = typer.Option(
        10.0, "--heartbeat-interval", help="Interval in seconds for heartbeat signals"
    ),
    heartbeat_ttl: int = typer.Option(
        30, "--heartbeat-ttl", help="Time-to-live in seconds for heartbeat signals"
    ),
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = typer.Option(
        "INFO", "--log-level", help="Set the logging level (e.g., DEBUG, INFO, WARNING)"
    ),
    shutdown_timeout: float = typer.Option(
        10.0, "--shutdown-timeout", help="Timeout in seconds for graceful shutdown"
    ),
) -> None:
    """
    Start a Chicory worker.

    If no options are provided, the worker will use configuration from:
    1. Environment variables (CHICORY_WORKER_*)
    2. .env file
    3. Default values

    CLI options override environment/config settings.
    """

    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    typer.echo(f"Starting Chicory worker for {app_path}")
    typer.echo(f"Log Level: {log_level}")
    typer.echo(f"Workers: {workers}")
    typer.echo(f"Concurrency: {concurrency}")
    typer.echo(f"Queue: {queue}")
    typer.echo(f"Heartbeat Interval: {heartbeat_interval}s")
    typer.echo(f"Heartbeat TTL: {heartbeat_ttl}s")
    typer.echo(
        f"Dead Letter Queue: {'Enabled' if use_dead_letter_queue else 'Disabled'}"
    )

    def run_worker() -> None:
        chicory_app = _import_app(app_path)

        worker_config = chicory_app.config.worker
        worker_config.concurrency = concurrency
        worker_config.queue = queue
        worker_config.use_dead_letter_queue = use_dead_letter_queue
        worker_config.heartbeat_interval = heartbeat_interval
        worker_config.heartbeat_ttl = heartbeat_ttl
        worker_config.log_level = log_level

        asyncio.run(Worker(chicory_app, config=worker_config).run())

    # No need to use multiprocessing if only one worker is requested
    if workers_count == 1:
        run_worker()
        return

    processes: list[Process] = []
    for _ in range(workers_count):
        p = Process(target=run_worker)
        p.start()
        processes.append(p)

    shutdown_event = False

    def signal_handler(signum, frame):
        nonlocal shutdown_event
        typer.echo("Received termination signal. Shutting down workers...")
        shutdown_event = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Wait for termination signal
    try:
        while not shutdown_event:
            time.sleep(1)
    except KeyboardInterrupt:
        typer.echo("Keyboard interrupt received. Shutting down workers...")

    # Terminate all worker processes
    for p in processes:
        p.terminate()

    # Wait for workers to finish
    for p in processes:
        p.join(timeout=shutdown_timeout)
        if p.is_alive():
            typer.echo(f"Force killing worker {p.pid}")
            p.kill()


@app.command()
def workers(
    app_path: str = typer.Argument(
        ..., help="Path to the Chicory app (e.g., 'myapp.tasks:app')"
    ),
) -> None:
    """
    List all active workers.
    """

    chicory_app = _import_app(app_path)

    async def list_workers():
        if not chicory_app.backend:
            typer.echo("No backend configured. Cannot retrieve worker info.")
            return

        await chicory_app.connect()
        try:
            workers = await chicory_app.backend.get_active_workers()
            if not workers:
                typer.echo("No active workers found.")
                return

            typer.echo(f"\n{'=' * 80}")
            typer.echo(f"Active Workers: {len(workers)}")
            typer.echo(f"{'=' * 80}\n")

            for w in workers:
                status_icon = "🟢" if w.is_running else "🔴"
                typer.echo(f"{status_icon} Worker ID: {w.worker_id}")
                typer.echo(f"  Hostname: {w.hostname}")
                typer.echo(f"  PID: {w.pid}")
                typer.echo(f"  Queue: {w.queue}")
                typer.echo(f"  Started: {w.started_at}")
                typer.echo(f"  Tasks Processed: {w.tasks_processed}")
                typer.echo(f"  Tasks Failed: {w.tasks_failed}")
                typer.echo()
        finally:
            await chicory_app.disconnect()

    asyncio.run(list_workers())


@app.command()
def cleanup(
    app_path: str = typer.Argument(
        ..., help="Path to the Chicory app (e.g., 'myapp.tasks:app')"
    ),
    stale_seconds: int = typer.Option(
        60, help="Remove workers with no heartbeat for this many seconds"
    ),
) -> None:
    """
    Cleanup stale worker records.
    """

    chicory_app = _import_app(app_path)

    async def cleanup():
        if not chicory_app.backend:
            typer.echo("No backend configured. Cannot clean up workers.")
            return

        await chicory_app.connect()
        try:
            removed_count = await chicory_app.backend.cleanup_stale_workers(
                stale_seconds
            )
            typer.echo(f"Removed {removed_count} stale worker(s).")
        finally:
            await chicory_app.disconnect()

    asyncio.run(cleanup())


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", "-v", help="Show the Chicory version and exit"
    ),
) -> None:
    """
    Chicory task queue CLI.
    """
    if version:
        _version = importlib.metadata.version("chicory")
        typer.echo(f"Chicory version: {_version}")
        raise typer.Exit()

    # If no command was invoked, show help
    if ctx.invoked_subcommand is None and not version:
        typer.echo(ctx.get_help())
        raise typer.Exit()
