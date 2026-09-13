from agent_platform.domain.errors import InvalidTransition
from agent_platform.domain.states import StepState

ALLOWED_STEP_TRANSITIONS: dict[StepState, frozenset[StepState]] = {
    StepState.PENDING: frozenset({StepState.READY, StepState.CANCELLED, StepState.SKIPPED}),
    StepState.READY: frozenset({StepState.RUNNING, StepState.CANCELLED, StepState.SKIPPED}),
    StepState.RUNNING: frozenset(
        {
            StepState.WAITING_APPROVAL,
            StepState.READY,
            StepState.SUCCEEDED,
            StepState.FAILED,
            StepState.CANCELLED,
            StepState.OUTCOME_UNKNOWN,
        }
    ),
    StepState.WAITING_APPROVAL: frozenset(
        {StepState.READY, StepState.CANCELLED, StepState.SKIPPED}
    ),
    StepState.OUTCOME_UNKNOWN: frozenset(
        {
            StepState.READY,
            StepState.SUCCEEDED,
            StepState.FAILED,
            StepState.CANCELLED,
        }
    ),
}


def enforce_step_transition(current: str, target: str) -> str:
    """Validate an edge; effect and approval guards belong to the atomic command."""
    try:
        current_state, target_state = StepState(current), StepState(target)
    except ValueError:
        raise InvalidTransition("Unknown step state") from None
    if target_state not in ALLOWED_STEP_TRANSITIONS.get(current_state, frozenset()):
        raise InvalidTransition(f"{current_state} -> {target_state}")
    return target_state.value
