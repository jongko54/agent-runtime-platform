"""Metadata-only observation contracts; independent from worker execution."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from agent_platform.application.ports import PrincipalContext

REDACTION_POLICY = "metadata-only-v1"
EXPECTED_STATES = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED"})


@dataclass(frozen=True, slots=True)
class EvaluationCandidate:
    id: str
    run_id: str
    project_id: str
    source_state_version: int
    expected_state: str
    status: str
    snapshot: dict[str, Any]
    snapshot_digest: str
    redaction_policy: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AcceptedCandidate:
    candidate: EvaluationCandidate
    duplicate: bool


class ObservationRepository(Protocol):
    async def get_trace(self, principal: PrincipalContext, run_id: str) -> dict[str, Any]: ...

    async def create_evaluation_candidate(
        self,
        principal: PrincipalContext,
        run_id: str,
        *,
        source_state_version: int,
        expected_state: str,
        idempotency_key: str,
    ) -> AcceptedCandidate: ...

    async def get_evaluation_candidate(
        self, principal: PrincipalContext, candidate_id: str
    ) -> EvaluationCandidate: ...
