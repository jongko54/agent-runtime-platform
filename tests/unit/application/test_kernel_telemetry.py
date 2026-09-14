import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from agent_platform.application.errors import RetryableGatewayError, RuntimeConflict
from agent_platform.application.ports import ClaimedWork, EffectDispatch
from agent_platform.application.runtime_kernel import RuntimeKernel


def work():
    return ClaimedWork(
        "work",
        "tenant",
        "project",
        "user",
        "run",
        "step",
        "MODEL_CALL",
        {},
        {"model_route": "mock/release-planner-v1", "tools": ["evaluation.run_suite:v1"]},
    )


def tool_work():
    return replace(
        work(),
        kind="TOOL_CALL",
        tool_spec={
            "name": "evaluation.run_suite",
            "version": 1,
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
        },
    )


def collaborators():
    repository = AsyncMock()
    repository.begin_tool_dispatch.return_value = EffectDispatch(
        "effect", "tenant", "project", "key", "token", "evaluation.run_suite:v1", {}
    )
    return repository, AsyncMock(), AsyncMock()


class RecordingTelemetry:
    def __init__(self):
        self.events = []

    def start(self, operation, claimed):
        self.events.append((operation, "START"))
        events = self.events

        class Handle:
            def finish(self, outcome):
                events.append((operation, outcome))

        return Handle()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["MODEL_CALL", "TOOL_CALL"])
async def test_gateway_return_is_distinct_from_persisted_attempt(kind):
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    tool.execute.return_value = {}
    recorder = RecordingTelemetry()
    await RuntimeKernel(repository, model, tool, telemetry=recorder).execute(
        work() if kind == "MODEL_CALL" else tool_work()
    )
    op = "model" if kind == "MODEL_CALL" else "tool"
    assert recorder.events == [
        ("attempt", "START"),
        (op, "START"),
        (op, "RETURNED"),
        ("attempt", "RECORDED"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["MODEL_CALL", "TOOL_CALL"])
async def test_provider_failure_preserves_retry_versus_unknown_boundary(kind):
    repository, model, tool = collaborators()
    model.decide.side_effect = RetryableGatewayError("SECRET")
    tool.execute.side_effect = RetryableGatewayError("SECRET")
    recorder = RecordingTelemetry()
    await RuntimeKernel(repository, model, tool, telemetry=recorder).execute(
        work() if kind == "MODEL_CALL" else tool_work()
    )
    assert recorder.events[-1] == (
        "attempt",
        "RETRY_HANDLED" if kind == "MODEL_CALL" else "OUTCOME_UNKNOWN",
    )
    assert recorder.events[-2][1] == "ERROR"  # Provider cannot claim a retry was persisted.


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeConflict("SECRET"), asyncio.CancelledError()])
async def test_telemetry_preserves_fencing_and_cancellation(error):
    repository, model, tool = collaborators()
    model.decide.side_effect = error
    recorder = RecordingTelemetry()
    with pytest.raises(type(error)) as raised:
        await RuntimeKernel(repository, model, tool, telemetry=recorder).execute(work())
    assert raised.value is error
    assert recorder.events[-1][1] == (
        "CANCELLED" if isinstance(error, asyncio.CancelledError) else "CONFLICT"
    )
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_tool_span_before_dispatch_is_authorized():
    repository, model, tool = collaborators()
    repository.begin_tool_dispatch.side_effect = RuntimeConflict("SECRET")
    recorder = RecordingTelemetry()
    with pytest.raises(RuntimeConflict):
        await RuntimeKernel(repository, model, tool, telemetry=recorder).execute(tool_work())
    assert recorder.events == [("attempt", "START"), ("attempt", "CONFLICT")]
    tool.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_commit_never_claims_recorded_or_scheduled_retry():
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    error = RetryableGatewayError("SECRET persistence failure")
    repository.complete_model.side_effect = error
    recorder = RecordingTelemetry()
    with pytest.raises(RetryableGatewayError) as raised:
        await RuntimeKernel(repository, model, tool, telemetry=recorder).execute(work())
    assert raised.value is error
    assert recorder.events[-2:] == [("model", "RETURNED"), ("attempt", "ERROR")]
    repository.retry_work.assert_not_awaited()
