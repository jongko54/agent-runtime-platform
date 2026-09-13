"""Metadata-only traces and immutable, non-executable evaluation candidates.

Candidate creation uses the runtime's Work -> Run -> Step -> Effect lock order,
then reads the projection on that same transaction. No runtime event is added:
the source version and its captured history remain unchanged by observation.
"""

import json
from typing import Any
from uuid import uuid4

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.repositories import (
    PostgresRunRepository,
    _WorkSetChanged,  # pyright: ignore[reportPrivateUsage]
)
from agent_platform.adapters.postgres.trace import load_trace, read_trace
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    RuntimeConflict,
)
from agent_platform.application.observability import (
    EXPECTED_STATES,
    REDACTION_POLICY,
    AcceptedCandidate,
    EvaluationCandidate,
)
from agent_platform.application.ports import PrincipalContext
from agent_platform.domain.runs import TERMINAL_STATES


def _candidate(row: RowMapping) -> EvaluationCandidate:
    return EvaluationCandidate(
        **{name: row[name] for name in EvaluationCandidate.__dataclass_fields__}
    )


def _duplicate(
    row: RowMapping, run_id: str, source_state_version: int, expected_state: str
) -> AcceptedCandidate:
    if (
        row["run_id"] != run_id
        or row["source_state_version"] != source_state_version
        or row["expected_state"] != expected_state
    ):
        raise IdempotencyConflict("Evaluation candidate key was used for another request")
    return AcceptedCandidate(_candidate(row), duplicate=True)


class PostgresObservationRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.runs = PostgresRunRepository(engine)

    async def get_trace(self, principal: PrincipalContext, run_id: str) -> dict[str, Any]:
        return await read_trace(self.engine, principal, run_id)

    async def get_evaluation_candidate(
        self, principal: PrincipalContext, candidate_id: str
    ) -> EvaluationCandidate:
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            row = (
                (
                    await conn.execute(
                        text("""
                SELECT * FROM evaluation_candidates WHERE tenant_id=:tenant AND id=:id
            """),
                        {"tenant": principal.tenant_id, "id": candidate_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise ExecutionScopeNotFound()
            # Deliberate reuse inside the Postgres adapter: preserve one scope contract.
            await self.runs._authorized_run(  # pyright: ignore[reportPrivateUsage]
                conn, principal, str(row["run_id"])
            )
            return _candidate(row)

    async def create_evaluation_candidate(
        self,
        principal: PrincipalContext,
        run_id: str,
        *,
        source_state_version: int,
        expected_state: str,
        idempotency_key: str,
    ) -> AcceptedCandidate:
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise InvalidInput("Idempotency key must contain 1 to 200 nonblank characters")
        if type(source_state_version) is not int or source_state_version < 1:
            raise InvalidInput("Source state version must be a positive integer")
        if expected_state not in EXPECTED_STATES:
            raise InvalidInput("Unsupported expected state")
        while True:
            try:
                async with unit_of_work(
                    self.engine, principal.tenant_id, principal.principal_id
                ) as conn:
                    # Reuse runtime locking; do not duplicate its Work-set race handling.
                    run = await self.runs._locked_run(  # pyright: ignore[reportPrivateUsage]
                        conn, principal, run_id
                    )
                    params = {
                        "tenant": principal.tenant_id,
                        "project": run["project_id"],
                        "principal": principal.principal_id,
                        "key": idempotency_key,
                    }
                    lookup = text("""
                        SELECT * FROM evaluation_candidates WHERE tenant_id=:tenant
                          AND project_id=:project AND principal_id=:principal
                          AND idempotency_key=:key
                    """)
                    previous = (await conn.execute(lookup, params)).mappings().one_or_none()
                    if previous is not None:
                        # A replay names the captured source version, even if a later
                        # reconciliation changed the Run; current authorization still applies.
                        return _duplicate(previous, run_id, source_state_version, expected_state)
                    if run["state_version"] != source_state_version:
                        raise RuntimeConflict("Source state version changed")
                    if run["state"] not in TERMINAL_STATES and run["state"] != "OUTCOME_UNKNOWN":
                        raise RuntimeConflict("Only terminal or unknown runs can become candidates")
                    snapshot = await load_trace(conn, principal, run_id)
                    integrity = snapshot.get("integrity", {})
                    if (
                        integrity.get("complete") is not True
                        or integrity.get("truncated") is not False
                    ):
                        raise RuntimeConflict(
                            "Incomplete traces cannot become evaluation candidates"
                        )
                    captured = snapshot.get("run", {})
                    if (
                        captured.get("id") != run_id
                        or captured.get("state") != run["state"]
                        or captured.get("state_version") != source_state_version
                    ):
                        raise RuntimeConflict("Trace does not match the locked source version")
                    inserted = (
                        (
                            await conn.execute(
                                text("""
                        INSERT INTO evaluation_candidates
                          (id,tenant_id,project_id,run_id,principal_id,source_state_version,
                           expected_state,status,snapshot,snapshot_digest,redaction_policy,idempotency_key)
                        VALUES(:id,:tenant,:project,:run,:principal,:version,:expected,'DRAFT',
                          CAST(:snapshot AS jsonb),:digest,:policy,:key)
                        ON CONFLICT(tenant_id,project_id,principal_id,idempotency_key) DO NOTHING
                        RETURNING *
                    """),
                                {
                                    **params,
                                    "id": uuid4().hex,
                                    "run": run_id,
                                    "version": source_state_version,
                                    "expected": expected_state,
                                    "snapshot": json.dumps(
                                        snapshot, allow_nan=False, separators=(",", ":")
                                    ),
                                    "digest": canonical_digest(snapshot),
                                    "policy": REDACTION_POLICY,
                                },
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if inserted is not None:
                        return AcceptedCandidate(_candidate(inserted), duplicate=False)
                    # Different source Runs can race for the same scoped key. A
                    # second READ COMMITTED statement observes the committed winner.
                    previous = (await conn.execute(lookup, params)).mappings().one()
                    return _duplicate(previous, run_id, source_state_version, expected_state)
            except _WorkSetChanged:
                continue
