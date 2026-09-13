import asyncio
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.settings import Settings
from agent_platform.worker.main import run_worker


@pytest.mark.asyncio
async def test_worker_configures_fenced_owner_without_session_lock_and_drains_due_work():
    settings = Settings(
        _env_file=None,
        lease_seconds=12,
        heartbeat_interval_seconds=2,
        max_attempts=4,
        retry_base_seconds=0.5,
    )
    engine, poller = AsyncMock(), AsyncMock()
    poller.poll_once.side_effect = [True, False]
    with (
        patch("agent_platform.worker.main.create_engine", return_value=engine),
        patch("agent_platform.worker.main.PostgresRunRepository") as repository_type,
        patch("agent_platform.worker.main.WorkerPoller", return_value=poller) as poller_type,
    ):
        await run_worker(settings, drain=True)
    owner = repository_type.call_args.kwargs["worker_id"]
    assert str(UUID(owner)) == owner
    repository_type.assert_called_once_with(
        engine,
        worker_id=owner,
        lease_seconds=12,
        max_attempts=4,
        retry_base_seconds=0.5,
    )
    assert poller_type.call_args.kwargs["heartbeat_interval_seconds"] == 2
    tool_gateway = poller_type.call_args.args[1].tool_gateway
    assert isinstance(tool_gateway, PersistentMockEvaluationTool)
    assert tool_gateway.engine is engine
    assert poller.poll_once.await_count == 2
    engine.connect.assert_not_called()
    engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", [asyncio.CancelledError(), RuntimeError("secret DB error")])
async def test_worker_disposes_engine_on_shutdown_or_once_failure(exception):
    engine, poller = AsyncMock(), AsyncMock()
    poller.poll_once.side_effect = exception
    with (
        patch("agent_platform.worker.main.create_engine", return_value=engine),
        patch("agent_platform.worker.main.PostgresRunRepository"),
        patch("agent_platform.worker.main.WorkerPoller", return_value=poller),
    ):
        with pytest.raises(type(exception)) as raised:
            await run_worker(Settings(_env_file=None), once=True)
    assert "secret" not in str(raised.value)
    engine.dispose.assert_awaited_once()
