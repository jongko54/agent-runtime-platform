"""Leased worker CLI; PostgreSQL fencing permits independent worker processes."""

import argparse
import asyncio
import logging
import time
from uuid import uuid4

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.database import create_engine
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.telemetry.otel import OtelRuntimeTelemetry
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.application.telemetry import NoopTelemetry, RuntimeTelemetry
from agent_platform.settings import Settings
from agent_platform.worker.poller import WorkerPoller

LOGGER = logging.getLogger(__name__)


def _telemetry_warning() -> None:
    try:
        LOGGER.warning("Worker telemetry unavailable; execution continues")
    except Exception:
        pass


def _create_telemetry(settings: Settings) -> RuntimeTelemetry:
    if settings.telemetry_enabled:
        try:
            return OtelRuntimeTelemetry(
                endpoint=settings.telemetry_endpoint,
                queue_capacity=settings.telemetry_queue_capacity,
                export_timeout_seconds=settings.telemetry_export_timeout_seconds,
                shutdown_timeout_seconds=settings.telemetry_shutdown_timeout_seconds,
            )
        except Exception:
            _telemetry_warning()
    return NoopTelemetry()


def _log_telemetry(telemetry: RuntimeTelemetry) -> None:
    try:
        snapshot = telemetry.snapshot()
        if not snapshot:
            return
        counts = {
            key: value
            for key in (
                "ended",
                "enqueued",
                "exported",
                "failed",
                "dropped",
                "queue_depth",
                "in_flight",
                "instrumentation_errors",
            )
            if type(value := snapshot.get(key)) is int and value >= 0
        }
        LOGGER.info("Worker telemetry counters: %s", counts)
    except Exception:
        _telemetry_warning()


async def run_worker(settings: Settings, *, once: bool = False, drain: bool = False) -> None:
    engine = create_engine(settings.database_url)
    telemetry = _create_telemetry(settings)
    repository = PostgresRunRepository(
        engine,
        worker_id=str(uuid4()),
        lease_seconds=settings.lease_seconds,
        max_attempts=settings.max_attempts,
        retry_base_seconds=settings.retry_base_seconds,
    )
    poller = WorkerPoller(
        repository,
        RuntimeKernel(
            repository,
            MockModelGateway(),
            PersistentMockEvaluationTool(engine),
            settings.operation_timeout_seconds,
            telemetry=telemetry,
        ),
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
    next_telemetry_log = time.monotonic() + 30
    try:
        while True:
            if time.monotonic() >= next_telemetry_log:
                _log_telemetry(telemetry)
                next_telemetry_log = time.monotonic() + 30
            try:
                processed = await poller.poll_once()
            except Exception:
                LOGGER.error("Worker polling failed; unfinished leases remain recoverable")
                if once or drain:
                    raise RuntimeError("Worker polling failed") from None
                await asyncio.sleep(settings.worker_poll_interval_seconds)
                continue
            if once or (drain and not processed):
                return
            if not processed:
                await asyncio.sleep(settings.worker_poll_interval_seconds)
    finally:
        try:
            # Adapter shutdown has a bounded wait; network lives on its daemon.
            await asyncio.to_thread(telemetry.shutdown)
        except Exception:
            _telemetry_warning()
        finally:
            _log_telemetry(telemetry)
            await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the leased execution worker")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Claim at most one Step and exit")
    mode.add_argument(
        "--drain",
        action="store_true",
        help="Process currently due Steps without waiting for retries",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_worker(Settings(), once=args.once, drain=args.drain))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
