import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from agent_platform.application.errors import (
    InvalidInput,
    PolicyDenied,
    RetryableGatewayError,
    RuntimeConflict,
)
from agent_platform.application.ports import ClaimedWork, EffectDispatch
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.worker.poller import WorkerPoller


def work(kind="MODEL_CALL"):
    return ClaimedWork(
        id="work",
        tenant_id="tenant",
        project_id="project",
        principal_id="principal",
        run_id="run",
        step_id="step",
        kind=kind,
        input={},
        agent_spec={"model_route": "mock/release-planner-v1", "tools": ["evaluation.run_suite:v1"]},
    )


def collaborators():
    repository = AsyncMock()
    repository.begin_tool_dispatch.return_value = EffectDispatch(
        "effect", "tenant", "project", "key", "token", "evaluation.run_suite:v1", {}
    )
    return repository, AsyncMock(), AsyncMock()


@pytest.mark.asyncio
async def test_model_work_only_schedules_tool_and_does_not_execute_it():
    repository, model, tool = collaborators()
    decision = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    model.decide.return_value = decision
    await RuntimeKernel(repository, model, tool).execute(work())
    repository.complete_model.assert_awaited_once_with(work(), decision)
    tool.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_bad_provider_response_fails_work_without_leaking_payload():
    repository, model, tool = collaborators()
    model.decide.side_effect = ValueError("secret-provider-payload")
    await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_awaited_once()
    assert "secret-provider-payload" not in str(repository.fail_work.call_args)


@pytest.mark.asyncio
async def test_timeout_persists_terminal_failure():
    repository, model, tool = collaborators()

    async def slow(**kwargs):
        await asyncio.sleep(10)

    model.decide.side_effect = slow
    await RuntimeKernel(repository, model, tool, operation_timeout_seconds=0.001).execute(work())
    assert repository.fail_work.call_args.kwargs["timed_out"] is True


@pytest.mark.asyncio
async def test_worker_shutdown_cancellation_is_not_swallowed():
    repository, model, tool = collaborators()
    model.decide.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_job_does_not_prevent_processing_next_job():
    repository, model, tool = collaborators()
    repository.claim_work.side_effect = [work(), work(), None]
    model.decide.side_effect = [
        ValueError("invalid"),
        {
            "tool_version": "evaluation.run_suite:v1",
            "arguments": {},
        },
    ]
    poller = WorkerPoller(repository, RuntimeKernel(repository, model, tool))
    assert await poller.poll_once() is True
    assert await poller.poll_once() is True
    assert await poller.poll_once() is False
    repository.fail_work.assert_awaited_once()
    repository.complete_model.assert_awaited_once()


@pytest.mark.asyncio
async def test_unregistered_route_fails_before_model_invocation():
    repository, model, tool = collaborators()
    unsupported = replace(work(), agent_spec={"model_route": "external/provider"})
    await RuntimeKernel(repository, model, tool).execute(unsupported)
    model.decide.assert_not_awaited()
    assert repository.fail_work.call_args.args[1] == "POLICY_DENIED"


@pytest.mark.asyncio
async def test_model_cannot_select_tool_outside_agent_allowlist():
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "unauthorized:v1", "arguments": {}}
    await RuntimeKernel(repository, model, tool).execute(work())
    repository.complete_model.assert_not_awaited()
    assert repository.fail_work.call_args.args[1] == "POLICY_DENIED"


@pytest.mark.asyncio
async def test_invalid_tool_output_is_never_committed_as_success():
    repository, model, tool = collaborators()
    tool_work = replace(
        work("TOOL_CALL"),
        tool_spec={
            "name": "evaluation.run_suite",
            "version": 1,
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object", "additionalProperties": False},
        },
    )
    tool.execute.return_value = {"unexpected": "secret-output"}
    await RuntimeKernel(repository, model, tool).execute(tool_work)
    tool.execute.assert_awaited_once()
    repository.complete_tool.assert_not_awaited()
    assert "secret-output" not in str(repository.fail_work.call_args)


@pytest.mark.asyncio
async def test_retryable_provider_error_only_schedules_safe_retry():
    repository, model, tool = collaborators()
    model.decide.side_effect = RetryableGatewayError("secret-provider-payload")
    await RuntimeKernel(repository, model, tool).execute(work())
    repository.retry_work.assert_awaited_once_with(
        work(), "PROVIDER_TRANSIENT", "Provider temporarily unavailable"
    )
    repository.fail_work.assert_not_awaited()
    repository.complete_model.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["MODEL_CALL", "TOOL_CALL"])
@pytest.mark.parametrize(
    "error",
    [
        RuntimeConflict("stale"),
        RuntimeError("DB down"),
        RetryableGatewayError("not a provider error"),
        TimeoutError(),
        ValueError("database value error"),
    ],
)
async def test_completion_failure_propagates_without_terminal_failure_or_retry(kind, error):
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    tool.execute.return_value = {}
    claimed = replace(
        work(kind),
        tool_spec={
            "name": "evaluation.run_suite",
            "version": 1,
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
        },
    )
    repository.complete_model.side_effect = error
    repository.complete_tool.side_effect = error
    with pytest.raises(type(error)):
        await RuntimeKernel(repository, model, tool).execute(claimed)
    repository.fail_work.assert_not_awaited()
    repository.retry_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_gateway_result_is_not_failed_again():
    repository, model, tool = collaborators()
    model.decide.side_effect = RuntimeConflict("stale")
    with pytest.raises(RuntimeConflict):
        await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_provider_exception_remains_terminal():
    repository, model, tool = collaborators()
    model.decide.side_effect = RuntimeError("secret-provider-payload")
    await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_awaited_once_with(
        work(), "RUNTIME_ERROR", "Execution could not be completed"
    )
    repository.retry_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_failure_is_not_written_twice():
    repository, model, tool = collaborators()
    model.decide.side_effect = ValueError("invalid payload")
    repository.fail_work.side_effect = RuntimeConflict("stale")
    with pytest.raises(RuntimeConflict):
        await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_awaited_once()
    repository.retry_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_persistence_failure_propagates_without_terminal_write():
    repository, model, tool = collaborators()
    model.decide.side_effect = RetryableGatewayError("transient")
    repository.retry_work.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await RuntimeKernel(repository, model, tool).execute(work())
    repository.retry_work.assert_awaited_once()
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["MODEL_CALL", "TOOL_CALL"])
@pytest.mark.parametrize(
    "error,code,message",
    [
        (InvalidInput("secret schema rejection"), "CLIENT_INVALID", "Execution payload is invalid"),
        (
            PolicyDenied("secret policy rejection"),
            "POLICY_DENIED",
            "Execution policy denied operation",
        ),
    ],
)
async def test_completion_semantic_rejection_preserves_effect_uncertainty(
    kind, error, code, message
):
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    tool.execute.return_value = {}
    claimed = replace(
        work(kind),
        tool_spec={
            "name": "evaluation.run_suite",
            "version": 1,
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
        },
    )
    repository.complete_model.side_effect = error
    repository.complete_tool.side_effect = error
    await RuntimeKernel(repository, model, tool).execute(claimed)
    if kind == "TOOL_CALL":
        repository.mark_tool_unknown.assert_awaited_once_with(claimed)
        repository.fail_work.assert_not_awaited()
    else:
        repository.fail_work.assert_awaited_once_with(claimed, code, message)
    repository.retry_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_terminal_write_after_semantic_rejection_is_not_repeated():
    repository, model, tool = collaborators()
    model.decide.return_value = {"tool_version": "evaluation.run_suite:v1", "arguments": {}}
    repository.complete_model.side_effect = InvalidInput("schema rejected")
    repository.fail_work.side_effect = RuntimeConflict("lease expired")
    with pytest.raises(RuntimeConflict):
        await RuntimeKernel(repository, model, tool).execute(work())
    repository.fail_work.assert_awaited_once()
    repository.retry_work.assert_not_awaited()


def tool_work():
    return replace(
        work("TOOL_CALL"),
        tool_spec={
            "name": "evaluation.run_suite",
            "version": 1,
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object", "additionalProperties": False},
        },
    )


@pytest.mark.asyncio
async def test_tool_dispatch_is_committed_before_provider_call_and_passes_effect():
    repository, model, tool = collaborators()
    claimed = tool_work()
    effect = EffectDispatch(
        "effect", "tenant", "project", "key", "token", "evaluation.run_suite:v1", {}
    )
    repository.begin_tool_dispatch.return_value = effect

    async def execute(**kwargs):
        repository.begin_tool_dispatch.assert_awaited_once_with(claimed)
        assert kwargs == {"tool_version": effect.tool_version, "arguments": {}, "effect": effect}
        return {}

    tool.execute.side_effect = execute
    await RuntimeKernel(repository, model, tool).execute(claimed)
    repository.complete_tool.assert_awaited_once_with(claimed, {})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [RuntimeConflict("cancelled"), RuntimeError("DB"), ValueError("DB"), TimeoutError()]
)
async def test_dispatch_persistence_rejection_prevents_provider_and_propagates(error):
    repository, model, tool = collaborators()
    repository.begin_tool_dispatch.side_effect = error
    with pytest.raises(type(error)):
        await RuntimeKernel(repository, model, tool).execute(tool_work())
    tool.execute.assert_not_awaited()
    repository.fail_work.assert_not_awaited()
    repository.retry_work.assert_not_awaited()
    repository.mark_tool_unknown.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("secret"),
        RetryableGatewayError("secret"),
        TimeoutError(),
        InvalidInput("secret"),
        PolicyDenied("secret"),
    ],
)
async def test_post_dispatch_provider_failure_is_unknown_never_retried_or_failed(error):
    repository, model, tool = collaborators()
    tool.execute.side_effect = error
    claimed = tool_work()
    await RuntimeKernel(repository, model, tool).execute(claimed)
    repository.mark_tool_unknown.assert_awaited_once_with(claimed)
    repository.fail_work.assert_not_awaited()
    repository.retry_work.assert_not_awaited()
    repository.complete_tool.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_post_dispatch_output_is_unknown():
    repository, model, tool = collaborators()
    tool.execute.return_value = {"secret": "invalid"}
    claimed = tool_work()
    await RuntimeKernel(repository, model, tool).execute(claimed)
    repository.mark_tool_unknown.assert_awaited_once_with(claimed)
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_write_failure_is_not_misclassified_as_provider_failure():
    repository, model, tool = collaborators()
    tool.execute.side_effect = TimeoutError()
    repository.mark_tool_unknown.side_effect = ValueError("DB write failed")
    with pytest.raises(ValueError, match="DB write failed"):
        await RuntimeKernel(repository, model, tool).execute(tool_work())
    repository.mark_tool_unknown.assert_awaited_once()
    repository.fail_work.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_tool_input_is_rejected_before_dispatch():
    repository, model, tool = collaborators()
    claimed = replace(tool_work(), input=[])
    await RuntimeKernel(repository, model, tool).execute(claimed)
    repository.fail_work.assert_awaited_once()
    repository.begin_tool_dispatch.assert_not_awaited()
    tool.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [PolicyDenied("secret policy"), InvalidInput("secret input")])
async def test_dispatch_semantic_rejection_is_safe_and_never_calls_provider(error):
    repository, model, tool = collaborators()
    repository.begin_tool_dispatch.side_effect = error
    await RuntimeKernel(repository, model, tool).execute(tool_work())
    repository.fail_work.assert_awaited_once()
    assert "secret" not in str(repository.fail_work.call_args)
    tool.execute.assert_not_awaited()
    repository.mark_tool_unknown.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeConflict("stale"), asyncio.CancelledError()])
async def test_post_dispatch_fencing_and_shutdown_propagate_without_writing(error):
    repository, model, tool = collaborators()
    tool.execute.side_effect = error
    with pytest.raises(type(error)):
        await RuntimeKernel(repository, model, tool).execute(tool_work())
    repository.fail_work.assert_not_awaited()
    repository.mark_tool_unknown.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_tool_timeout_marks_unknown():
    repository, model, tool = collaborators()

    async def slow(**kwargs):
        await asyncio.sleep(10)

    tool.execute.side_effect = slow
    await RuntimeKernel(repository, model, tool, operation_timeout_seconds=0.001).execute(
        tool_work()
    )
    repository.mark_tool_unknown.assert_awaited_once_with(tool_work())
    repository.fail_work.assert_not_awaited()
