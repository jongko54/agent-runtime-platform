from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from agent_platform.domain.errors import InvalidTransition
from agent_platform.domain.ids import AgentVersionId, ProjectId, RunId, TenantId
from agent_platform.domain.runs import TERMINAL_STATES, Run, enforce_transition
from agent_platform.domain.states import RunState, StepState
from agent_platform.domain.steps import enforce_step_transition


def test_execution_yields_to_separately_scheduled_tool() -> None:
    run = Run(
        RunId("run-1"),
        TenantId("tenant-1"),
        ProjectId("project-1"),
        AgentVersionId("agent-1"),
        RunState.CREATED,
        0,
        datetime.now(UTC),
    )
    states = [
        RunState.QUEUED,
        RunState.RUNNING,
        RunState.WAITING_MODEL,
        RunState.RUNNING,
        RunState.WAITING_TOOL,
        RunState.QUEUED,
        RunState.RUNNING,
        RunState.COMPLETED,
    ]
    for state in states:
        run = run.transition_to(state)
    assert run.state == RunState.COMPLETED
    assert run.state_version == len(states)


@given(st.sampled_from(tuple(TERMINAL_STATES)), st.sampled_from(tuple(RunState)))
def test_terminal_runs_cannot_reenter_execution(current: RunState, target: RunState) -> None:
    with pytest.raises(InvalidTransition):
        enforce_transition(current, target)


def test_approval_resumes_run_and_step_through_queue() -> None:
    assert enforce_transition("WAITING_APPROVAL", "QUEUED") == "QUEUED"
    assert enforce_step_transition("WAITING_APPROVAL", "READY") == "READY"
    with pytest.raises(InvalidTransition):
        enforce_transition("WAITING_APPROVAL", "RUNNING")


def test_pause_and_timeout_are_reachable() -> None:
    assert enforce_transition("QUEUED", "PAUSED") == "PAUSED"
    assert enforce_transition("PAUSED", "QUEUED") == "QUEUED"
    assert enforce_transition("WAITING_MODEL", "TIMED_OUT") == "TIMED_OUT"


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED"])
def test_terminal_steps_cannot_resume(state: str) -> None:
    for target in StepState:
        with pytest.raises(InvalidTransition):
            enforce_step_transition(state, target)


def test_unknown_state_is_a_domain_error() -> None:
    with pytest.raises(InvalidTransition):
        enforce_transition("arbitrary", "QUEUED")
