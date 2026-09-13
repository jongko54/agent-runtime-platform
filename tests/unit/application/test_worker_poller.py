import asyncio
from unittest.mock import AsyncMock

import pytest

from agent_platform.application.errors import RuntimeConflict
from agent_platform.application.ports import ClaimedWork
from agent_platform.worker.poller import WorkerPoller


def claimed_work():
    return ClaimedWork(
        "work",
        "tenant",
        "project",
        "principal",
        "run",
        "step",
        "MODEL_CALL",
        {},
        {},
        attempt_id="attempt",
        lease_token=1,
        worker_id="worker",
    )


@pytest.mark.asyncio
async def test_recovery_runs_before_even_empty_claim():
    repository, kernel = AsyncMock(), AsyncMock()
    events = []

    async def recover():
        events.append("recover")
        return 0

    async def claim():
        events.append("claim")
        return None

    repository.recover_expired.side_effect = recover
    repository.claim_work.side_effect = claim
    assert await WorkerPoller(repository, kernel).poll_once() is False
    assert events == ["recover", "claim"]
    kernel.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_renews_while_execution_runs_and_stops_after_completion():
    repository, kernel = AsyncMock(), AsyncMock()
    claimed = claimed_work()
    repository.claim_work.return_value = claimed
    renewed = asyncio.Event()
    execution_done = asyncio.Event()

    async def heartbeat(work):
        assert work == claimed
        renewed.set()
        await execution_done.wait()
        return True

    async def execute(work):
        await renewed.wait()
        execution_done.set()

    repository.heartbeat.side_effect = heartbeat
    kernel.execute.side_effect = execute
    baseline = asyncio.all_tasks()
    async with asyncio.timeout(1):
        assert await WorkerPoller(repository, kernel, 0.001).poll_once() is True
    assert repository.heartbeat.await_count >= 1
    assert asyncio.all_tasks() == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("heartbeat_error", [False, RuntimeError("secret database error")])
async def test_heartbeat_failure_cancels_and_awaits_execution(heartbeat_error):
    repository, kernel = AsyncMock(), AsyncMock()
    repository.claim_work.return_value = claimed_work()
    finished = asyncio.Event()

    async def execute(work):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.set()

    kernel.execute.side_effect = execute
    if isinstance(heartbeat_error, Exception):
        repository.heartbeat.side_effect = heartbeat_error
    else:
        repository.heartbeat.return_value = heartbeat_error
    baseline = asyncio.all_tasks()
    async with asyncio.timeout(1):
        with pytest.raises(RuntimeConflict) as raised:
            await WorkerPoller(repository, kernel, 0.001).poll_once()
    assert "secret" not in str(raised.value)
    assert finished.is_set()
    assert asyncio.all_tasks() == baseline
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_cancels_and_awaits_both_execution_and_inflight_heartbeat():
    repository, kernel = AsyncMock(), AsyncMock()
    repository.claim_work.return_value = claimed_work()
    heartbeat_started = asyncio.Event()
    finished = set()

    async def execute(work):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.add("execution")

    async def heartbeat(work):
        heartbeat_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.add("heartbeat")

    kernel.execute.side_effect = execute
    repository.heartbeat.side_effect = heartbeat
    baseline = asyncio.all_tasks()
    task = asyncio.create_task(WorkerPoller(repository, kernel, 0.001).poll_once())
    try:
        async with asyncio.timeout(1):
            await heartbeat_started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert finished == {"execution", "heartbeat"}
    assert asyncio.all_tasks() == baseline


@pytest.mark.asyncio
async def test_execution_failure_propagates_without_orphan_heartbeat():
    repository, kernel = AsyncMock(), AsyncMock()
    repository.claim_work.return_value = claimed_work()
    kernel.execute.side_effect = RuntimeError("persist failed")
    baseline = asyncio.all_tasks()
    with pytest.raises(RuntimeError, match="persist failed"):
        await WorkerPoller(repository, kernel).poll_once()
    assert asyncio.all_tasks() == baseline


@pytest.mark.asyncio
async def test_execution_cancellation_propagates_without_orphan_heartbeat():
    repository, kernel = AsyncMock(), AsyncMock()
    repository.claim_work.return_value = claimed_work()
    kernel.execute.side_effect = asyncio.CancelledError
    baseline = asyncio.all_tasks()
    with pytest.raises(asyncio.CancelledError):
        await WorkerPoller(repository, kernel).poll_once()
    assert asyncio.all_tasks() == baseline


@pytest.mark.asyncio
async def test_recovery_failure_does_not_claim_more_work():
    repository, kernel = AsyncMock(), AsyncMock()
    repository.recover_expired.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await WorkerPoller(repository, kernel).poll_once()
    repository.claim_work.assert_not_awaited()
    kernel.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_hung_heartbeat_times_out_and_awaits_heartbeat_and_execution_cleanup():
    repository, kernel = AsyncMock(), AsyncMock()
    repository.claim_work.return_value = claimed_work()
    finished = set()

    async def execute(work):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.add("execution")

    async def heartbeat(work):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.add("heartbeat")

    kernel.execute.side_effect = execute
    repository.heartbeat.side_effect = heartbeat
    baseline = asyncio.all_tasks()
    async with asyncio.timeout(1):
        with pytest.raises(RuntimeConflict, match="renew"):
            await WorkerPoller(repository, kernel, 0.001).poll_once()
    assert finished == {"execution", "heartbeat"}
    assert asyncio.all_tasks() == baseline
    repository.fail_work.assert_not_awaited()
