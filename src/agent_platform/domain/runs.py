"""Legal state edges; the repository must additionally enforce transactional guards.

Approval, pause, cancellation and reconciliation are vocabulary for later phases.
Their presence here does not expose those operations in the Phase 1 API.
"""

from dataclasses import dataclass, replace
from datetime import datetime

from agent_platform.domain.errors import InvalidTransition
from agent_platform.domain.ids import AgentVersionId, ProjectId, RunId, TenantId
from agent_platform.domain.states import RunState

TERMINAL_STATES = frozenset(
    {RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED, RunState.TIMED_OUT, RunState.REJECTED}
)

ALLOWED_TRANSITIONS: dict[RunState, frozenset[RunState]] = {
    RunState.CREATED: frozenset({RunState.QUEUED}),
    RunState.QUEUED: frozenset(
        {
            RunState.RUNNING,
            RunState.PAUSED,
            RunState.CANCEL_REQUESTED,
            RunState.TIMED_OUT,
            RunState.FAILED,
        }
    ),
    RunState.RUNNING: frozenset(
        {
            RunState.WAITING_MODEL,
            RunState.WAITING_TOOL,
            RunState.QUEUED,
            RunState.PAUSED,
            RunState.COMPLETED,
            RunState.FAILED,
            RunState.CANCEL_REQUESTED,
            RunState.OUTCOME_UNKNOWN,
            RunState.TIMED_OUT,
        }
    ),
    RunState.WAITING_MODEL: frozenset(
        {
            RunState.RUNNING,
            RunState.QUEUED,
            RunState.FAILED,
            RunState.CANCEL_REQUESTED,
            RunState.OUTCOME_UNKNOWN,
            RunState.TIMED_OUT,
        }
    ),
    RunState.WAITING_TOOL: frozenset(
        {
            RunState.QUEUED,
            RunState.WAITING_APPROVAL,
            RunState.FAILED,
            RunState.CANCEL_REQUESTED,
            RunState.OUTCOME_UNKNOWN,
            RunState.TIMED_OUT,
        }
    ),
    RunState.WAITING_APPROVAL: frozenset(
        {
            RunState.QUEUED,
            RunState.REJECTED,
            RunState.CANCEL_REQUESTED,
            RunState.TIMED_OUT,
            RunState.FAILED,
        }
    ),
    RunState.PAUSED: frozenset(
        {
            RunState.QUEUED,
            RunState.CANCEL_REQUESTED,
            RunState.TIMED_OUT,
            RunState.FAILED,
        }
    ),
    RunState.CANCEL_REQUESTED: frozenset({RunState.CANCELLED, RunState.OUTCOME_UNKNOWN}),
    RunState.OUTCOME_UNKNOWN: frozenset(
        {
            RunState.QUEUED,
            RunState.COMPLETED,
            RunState.FAILED,
            RunState.CANCELLED,
        }
    ),
}


def enforce_transition(current: str, target: str) -> str:
    """Reject illegal edges without changing any state or revealing external input."""
    try:
        current_state, target_state = RunState(current), RunState(target)
    except ValueError:
        raise InvalidTransition("Unknown run state") from None
    if target_state not in ALLOWED_TRANSITIONS.get(current_state, frozenset()):
        raise InvalidTransition(f"{current_state} -> {target_state}")
    return target_state.value


@dataclass(frozen=True, slots=True)
class Run:
    id: RunId
    tenant_id: TenantId
    project_id: ProjectId
    agent_version_id: AgentVersionId
    state: RunState
    state_version: int
    created_at: datetime

    def transition_to(self, target: RunState) -> "Run":
        next_state = RunState(enforce_transition(self.state, target))
        return replace(self, state=next_state, state_version=self.state_version + 1)
