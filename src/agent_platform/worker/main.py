"""Phase 1 worker CLI. A dedicated session lock excludes a second worker process."""

import argparse
import asyncio
import logging

from sqlalchemy import text

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.database import create_engine
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings
from agent_platform.worker.poller import WorkerPoller

LOGGER = logging.getLogger(__name__)
WORKER_LOCK_ID = 721643817


async def run_worker(settings: Settings, *, once: bool = False, drain: bool = False) -> None:
    engine = create_engine(settings.database_url)
    repository = PostgresRunRepository(engine)
    poller = WorkerPoller(
        repository,
        RuntimeKernel(
            repository, MockModelGateway(), MockEvaluationTool(), settings.operation_timeout_seconds
        ),
    )
    try:
        async with engine.connect() as guard:
            acquired = await guard.scalar(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": WORKER_LOCK_ID}
            )
            await guard.commit()
            if not acquired:
                raise RuntimeError("Another Phase 1 worker already holds the execution lock")
            try:
                while True:
                    # SQLAlchemy may reconnect a dropped connection, but session locks do not
                    # survive it. Fail closed before claiming another Step if ownership is lost.
                    owns_lock = await guard.scalar(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_locks "
                            "WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
                            "AND classid = 0 AND objid = :key AND objsubid = 1 AND granted)"
                        ),
                        {"key": WORKER_LOCK_ID},
                    )
                    await guard.commit()
                    if not owns_lock:
                        raise RuntimeError("Phase 1 worker lost its execution lock")
                    try:
                        processed = await poller.poll_once()
                    except Exception:
                        LOGGER.error("Worker polling failed; database state requires inspection")
                        if once or drain:
                            raise RuntimeError("Worker polling failed") from None
                        await asyncio.sleep(settings.worker_poll_interval_seconds)
                        continue
                    if once or (drain and not processed):
                        return
                    if not processed:
                        await asyncio.sleep(settings.worker_poll_interval_seconds)
            finally:
                await guard.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": WORKER_LOCK_ID}
                )
                await guard.commit()
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Phase 1 single-worker runtime")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Claim at most one Step and exit")
    mode.add_argument(
        "--drain", action="store_true", help="Process ready Steps until the queue is empty"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_worker(Settings(), once=args.once, drain=args.drain))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
