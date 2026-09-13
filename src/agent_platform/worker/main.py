"""Leased worker CLI; PostgreSQL fencing permits independent worker processes."""

import argparse
import asyncio
import logging
from uuid import uuid4

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.database import create_engine
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings
from agent_platform.worker.poller import WorkerPoller

LOGGER = logging.getLogger(__name__)


async def run_worker(settings: Settings, *, once: bool = False, drain: bool = False) -> None:
    engine = create_engine(settings.database_url)
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
        ),
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
    try:
        while True:
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
