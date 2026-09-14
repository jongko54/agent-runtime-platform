import asyncio
from unittest.mock import AsyncMock, Mock, patch
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
async def test_default_worker_does_not_construct_exporter():
    engine, poller = AsyncMock(), AsyncMock()
    with (
        patch("agent_platform.worker.main.create_engine", return_value=engine),
        patch("agent_platform.worker.main.PostgresRunRepository"),
        patch("agent_platform.worker.main.WorkerPoller", return_value=poller),
        patch("agent_platform.worker.main.OtelRuntimeTelemetry") as telemetry_type,
    ):
        await run_worker(Settings(_env_file=None), once=True)
    telemetry_type.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["start", "shutdown", "snapshot", None])
async def test_worker_telemetry_failure_does_not_change_once_result(failure, caplog):
    engine, poller, telemetry = AsyncMock(), AsyncMock(), Mock()
    telemetry.snapshot.return_value = {"ended": 2, "dropped": 0, "SECRET_BAD_KEY": 99}
    if failure in {"shutdown", "snapshot"}:
        getattr(telemetry, failure).side_effect = RuntimeError("SECRET")
    with (
        patch("agent_platform.worker.main.create_engine", return_value=engine),
        patch("agent_platform.worker.main.PostgresRunRepository"),
        patch("agent_platform.worker.main.WorkerPoller", return_value=poller),
        patch("agent_platform.worker.main.OtelRuntimeTelemetry", return_value=telemetry) as factory,
    ):
        if failure == "start":
            factory.side_effect = RuntimeError("SECRET")
        await run_worker(Settings(_env_file=None, telemetry_enabled=True), once=True)
    poller.poll_once.assert_awaited_once()
    engine.dispose.assert_awaited_once()
    if failure != "start":
        telemetry.shutdown.assert_called_once()
    assert "SECRET" not in caplog.text


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
