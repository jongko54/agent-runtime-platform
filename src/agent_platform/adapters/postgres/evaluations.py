"""Explicit curation from immutable provenance; never read source input or execute tools."""

import json
import re
from copy import deepcopy
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import RowMapping, bindparam, text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_platform.adapters.postgres.database import set_context, unit_of_work
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    RuntimeConflict,
)
from agent_platform.application.evaluation import (
    MODEL_REVISION,
    REVIEW_POLICY,
    AcceptedCase,
    EvaluationCase,
    case_content_digest,
    validate_case_content,
)
from agent_platform.application.observability import REDACTION_POLICY
from agent_platform.application.ports import PrincipalContext
from agent_platform.contracts.validation import validate_payload


def _case(row: RowMapping) -> EvaluationCase:
    values = {name: row[name] for name in EvaluationCase.__dataclass_fields__}
    values["allowed_tools"] = tuple(row["allowed_tools"])
    return EvaluationCase(**values)


def _duplicate(row: RowMapping, digest: str) -> AcceptedCase:
    if row["content_digest"] != digest:
        raise IdempotencyConflict("Evaluation case key was used for another request")
    return AcceptedCase(_case(row), duplicate=True)


class PostgresEvaluationRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.runs = PostgresRunRepository(engine)

    async def create_case(
        self,
        principal: PrincipalContext,
        candidate_id: str,
        *,
        source_snapshot_digest: str,
        input: dict[str, Any],
        expected_decision: dict[str, Any],
        review_confirmed: bool,
        idempotency_key: str,
    ) -> AcceptedCase:
        if review_confirmed is not True:
            raise InvalidInput("Explicit curation review confirmation is required")
        if not re.fullmatch(r"[0-9a-f]{64}", source_snapshot_digest):
            raise InvalidInput("Source snapshot digest must be lowercase SHA-256")
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise InvalidInput("Idempotency key must contain 1 to 200 nonblank characters")
        # Freeze nested caller-owned values before the first database await.
        try:
            input, expected_decision = deepcopy((input, expected_decision))
        except RecursionError:
            raise InvalidInput("Curated content exceeds structural limits") from None
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            source = (
                (
                    await conn.execute(
                        text(
                            "SELECT * FROM evaluation_candidates WHERE tenant_id=:tenant AND id=:id"
                        ),
                        {"tenant": principal.tenant_id, "id": candidate_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if source is None:
                raise ExecutionScopeNotFound()
            run = await self.runs._authorized_run(  # pyright: ignore[reportPrivateUsage]
                conn, principal, str(source["run_id"]), metadata_only=True
            )
            snapshot = cast(dict[str, Any], source["snapshot"])
            captured = snapshot.get("run", {})
            integrity = snapshot.get("integrity", {})
            if (
                source["status"] != "DRAFT"
                or source["redaction_policy"] != REDACTION_POLICY
                or source["snapshot_digest"] != source_snapshot_digest
                or canonical_digest(snapshot) != source_snapshot_digest
                or captured.get("id") != source["run_id"]
                or captured.get("project_id") != source["project_id"]
                or captured.get("state_version") != source["source_state_version"]
                or captured.get("agent_version_id") != run["agent_version_id"]
                or integrity.get("complete") is not True
                or integrity.get("truncated") is not False
            ):
                raise RuntimeConflict("Candidate provenance does not match the source snapshot")
            agent = (
                (
                    await conn.execute(
                        text("""
                    SELECT spec,digest FROM agent_versions
                    WHERE tenant_id=:tenant AND project_id=:project AND id=:agent
                """),
                        {
                            "tenant": principal.tenant_id,
                            "project": source["project_id"],
                            "agent": run["agent_version_id"],
                        },
                    )
                )
                .mappings()
                .one()
            )
            spec = cast(dict[str, Any], agent["spec"])
            if (
                captured.get("agent_digest") != agent["digest"]
                or canonical_digest(spec) != agent["digest"]
            ):
                raise RuntimeConflict("Source agent revision integrity check failed")
            if spec.get("model_route") != MODEL_REVISION:
                raise InvalidInput("Source model route is unsupported for offline evaluation")
            allowed_tools = tuple(cast(list[str], spec["tools"]))
            normalized = validate_case_content(input, expected_decision, allowed_tools)
            validate_payload(input, cast(dict[str, Any], spec["input_schema"]))
            digest = case_content_digest(
                candidate_id=candidate_id,
                source_snapshot_digest=source_snapshot_digest,
                source_agent_version_id=str(run["agent_version_id"]),
                input=input,
                expected_decision=normalized,
                allowed_tools=allowed_tools,
            )
            params = {
                "tenant": principal.tenant_id,
                "project": source["project_id"],
                "principal": principal.principal_id,
                "key": idempotency_key,
            }
            lookup = text("""
                SELECT * FROM evaluation_cases WHERE tenant_id=:tenant AND project_id=:project
                  AND principal_id=:principal AND idempotency_key=:key
            """)
            previous = (await conn.execute(lookup, params)).mappings().one_or_none()
            if previous is not None:
                return _duplicate(previous, digest)
            inserted = (
                (
                    await conn.execute(
                        text("""
                    INSERT INTO evaluation_cases
                      (id,tenant_id,project_id,run_id,candidate_id,principal_id,
                       source_agent_version_id,source_snapshot_digest,content_digest,
                       input,expected_decision,allowed_tools,review_policy,idempotency_key)
                    VALUES(:id,:tenant,:project,:run,:candidate,:principal,:agent,:source_digest,
                      :digest,CAST(:input AS jsonb),CAST(:expected AS jsonb),
                      CAST(:tools AS jsonb),:policy,:key)
                    ON CONFLICT(tenant_id,project_id,principal_id,idempotency_key) DO NOTHING
                    RETURNING *
                """),
                        {
                            **params,
                            "id": uuid4().hex,
                            "run": source["run_id"],
                            "candidate": candidate_id,
                            "agent": run["agent_version_id"],
                            "source_digest": source_snapshot_digest,
                            "digest": digest,
                            "input": json.dumps(input, allow_nan=False),
                            "expected": json.dumps(normalized, allow_nan=False),
                            "tools": json.dumps(allowed_tools),
                            "policy": REVIEW_POLICY,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if inserted is not None:
                return AcceptedCase(_case(inserted), duplicate=False)
            # A READ COMMITTED statement sees the concurrently committed scoped-key winner.
            previous = (await conn.execute(lookup, params)).mappings().one()
            return _duplicate(previous, digest)

    async def get_case(self, principal: PrincipalContext, case_id: str) -> EvaluationCase:
        return (await self.get_cases(principal, [case_id]))[0]

    async def get_cases(
        self, principal: PrincipalContext, case_ids: list[str]
    ) -> list[EvaluationCase]:
        if (
            not 1 <= len(case_ids) <= 50
            or any(type(value) is not str or not value.strip() for value in case_ids)
            or len(set(case_ids)) != len(case_ids)
        ):
            raise InvalidInput("A suite requires 1 to 50 distinct case identifiers")
        case_ids = list(case_ids)
        # One immutable suite snapshot and one current authorization snapshot. No partial returns.
        async with self.engine.connect() as raw:
            conn = await raw.execution_options(isolation_level="REPEATABLE READ")
            async with conn.begin():
                await conn.execute(text("SET TRANSACTION READ ONLY"))
                await set_context(conn, principal.tenant_id, principal.principal_id)
                rows = (
                    (
                        await conn.execute(
                            text("""
                    SELECT * FROM evaluation_cases WHERE tenant_id=:tenant AND id IN :ids
                """).bindparams(bindparam("ids", expanding=True)),
                            {
                                "tenant": principal.tenant_id,
                                "ids": case_ids,
                            },
                        )
                    )
                    .mappings()
                    .all()
                )
                if len(rows) != len(case_ids):
                    raise ExecutionScopeNotFound()
                for run_id in sorted({str(row["run_id"]) for row in rows}):
                    await self.runs._authorized_run(  # pyright: ignore[reportPrivateUsage]
                        conn, principal, run_id, metadata_only=True
                    )
                if len({row["project_id"] for row in rows}) != 1:
                    raise InvalidInput("All evaluation cases must belong to one project")
                cases = {str(row["id"]): _case(row) for row in rows}
                return [cases[case_id] for case_id in case_ids]
