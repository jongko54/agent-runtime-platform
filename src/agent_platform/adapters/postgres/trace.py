"""Bounded metadata projection, not an OTel span or a provider latency trace.

Only explicit columns cross this boundary. In particular, no JSON execution
payload is loaded except the run error's code, which is allowlist-normalized.
"""

import re
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from agent_platform.adapters.postgres.database import set_context

# Reuse the same adapter's authorization rule without fetching raw execution JSON.
from agent_platform.adapters.postgres.repositories import (
    _scope,  # pyright: ignore[reportPrivateUsage]
)
from agent_platform.application.errors import ExecutionScopeNotFound
from agent_platform.application.observability import REDACTION_POLICY
from agent_platform.application.ports import PrincipalContext
from agent_platform.domain.states import RunState, StepKind, StepState

COLLECTION_LIMIT = 1000
ERROR_CODES = frozenset(
    {
        "CLIENT_INVALID",
        "POLICY_DENIED",
        "PROVIDER_TRANSIENT",
        "OPERATION_TIMEOUT",
        "RUNTIME_ERROR",
        "LEASE_EXPIRED",
        "RETRY_EXHAUSTED",
        "OUTCOME_UNKNOWN",
        "CANCEL_REQUESTED",
        "RUNTIME_CONFLICT",
        "PROVIDER_UNAVAILABLE",
    }
)
EVENT_TYPES = frozenset(
    {
        "RUN_ACCEPTED",
        "WORK_CLAIMED",
        "MODEL_DISPATCHED",
        "MODEL_RECORDED",
        "TOOL_PROPOSED",
        "TOOL_AUTO_SCHEDULED",
        "TOOL_DISPATCHED",
        "RUN_COMPLETED",
        "RUN_FAILED",
        "RUN_TIMED_OUT",
        "WORK_RECOVERED",
        "WORK_RETRY_SCHEDULED",
        "RUN_OUTCOME_UNKNOWN",
        "RUN_CANCEL_REQUESTED",
        "RUN_CANCELLED",
        "EFFECT_RECONCILED",
        "DEAD_LETTER_REDRIVEN",
        "RUN_REDRIVEN",
    }
)
CALL_STATES = frozenset(
    {"PENDING", "DISPATCHED", "SUCCEEDED", "FAILED", "CANCELLED", "OUTCOME_UNKNOWN"}
)
WORK_STATES = frozenset({"READY", "PROCESSING", "DONE", "FAILED", "CANCELLED", "OUTCOME_UNKNOWN"})


def _registered(value: str, permitted: frozenset[str], issue: str, issues: list[str]) -> str:
    if value in permitted:
        return value
    issues.append(issue)
    return "UNREGISTERED"


def _error_code(value: Any) -> str | None:
    return None if value is None else value if value in ERROR_CODES else "UNCLASSIFIED"


def _failure_category(state: str, code: str | None) -> str | None:
    if state == "OUTCOME_UNKNOWN":
        return "OUTCOME_UNKNOWN"
    if code == "CLIENT_INVALID":
        return "VALIDATION"
    if code == "POLICY_DENIED":
        return "POLICY"
    if code in {"OPERATION_TIMEOUT", "LEASE_EXPIRED"} or state == "TIMED_OUT":
        return "TIMEOUT"
    if code in {"PROVIDER_TRANSIENT", "PROVIDER_UNAVAILABLE"}:
        return "PROVIDER"
    if code == "RETRY_EXHAUSTED":
        return "RETRY_EXHAUSTED"
    if state == "CANCELLED" or code == "CANCEL_REQUESTED":
        return "CANCELLATION"
    if code is not None or state in {"FAILED", "REJECTED"}:
        return "UNCLASSIFIED"
    return None


def _json_metadata(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in row.items()
    }


async def _collection(
    conn: AsyncConnection,
    source: str,
    columns: str,
    order: str,
    scope: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    # SQL identifiers are module-owned constants, never supplied by clients.
    rows = (
        (
            await conn.execute(
                text(f"""
        SELECT {columns} FROM {source}
        WHERE x.tenant_id=:tenant AND x.project_id=:project AND x.run_id=:run
        ORDER BY {order} LIMIT :limit
    """),
                {**scope, "limit": COLLECTION_LIMIT + 1},
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows[:COLLECTION_LIMIT]], len(rows) > COLLECTION_LIMIT


async def read_trace(
    engine: AsyncEngine, principal: PrincipalContext, run_id: str
) -> dict[str, Any]:
    """Authorize and read one snapshot without locking the runtime's hot rows."""
    async with engine.connect() as raw:
        conn = await raw.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            await conn.execute(text("SET TRANSACTION READ ONLY"))
            await set_context(conn, principal.tenant_id, principal.principal_id)
            return await load_trace(conn, principal, run_id)


async def load_trace(
    conn: AsyncConnection, principal: PrincipalContext, run_id: str
) -> dict[str, Any]:
    """Read in the caller's transaction, which must establish scope and consistency.

    Candidate creation locks its source before calling; ordinary reads use the
    repeatable-read wrapper above. This helper does not commit or acquire locks.
    """
    if not conn.in_transaction():
        raise RuntimeError("Trace projection requires an active transaction")
    row = (
        (
            await conn.execute(
                text("""
        SELECT r.id,r.project_id,r.state,r.state_version,r.agent_version_id,
          a.digest AS agent_digest,r.created_at,r.cancel_epoch,r.cancellation_outcome,
          r.error->>'code' AS error_code
        FROM runs r LEFT JOIN agent_versions a ON a.id=r.agent_version_id
          AND a.tenant_id=r.tenant_id AND a.project_id=r.project_id
        WHERE r.id=:run AND r.tenant_id=:tenant
    """),
                {"run": run_id, "tenant": principal.tenant_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not await _scope(
        conn, principal.tenant_id, str(row["project_id"]), principal.principal_id
    ):
        raise ExecutionScopeNotFound()
    run = dict(row)
    run["error_code"] = _error_code(run["error_code"])
    run["failure_category"] = _failure_category(str(run["state"]), run["error_code"])
    scope = {"tenant": principal.tenant_id, "project": run["project_id"], "run": run_id}
    specifications = {
        "steps": ("run_steps x", "x.id,x.kind,x.state,x.ordinal", "x.ordinal,x.id"),
        "work_items": ("work_items x", "x.id,x.step_id,x.status,x.attempt_count", "x.id"),
        "attempts": (
            "run_attempts x",
            "x.id,x.step_id,x.work_id,x.attempt_no,x.status,x.started_at,x.finished_at,x.error_code",
            "x.started_at,x.id",
        ),
        "model_calls": ("model_calls x", "x.id,x.step_id,x.status,x.model_route", "x.id"),
        "tool_calls": (
            "tool_calls x LEFT JOIN tool_versions v ON v.id=x.tool_version_id "
            "AND v.tenant_id=x.tenant_id AND v.project_id=x.project_id",
            "x.id,x.step_id,x.status,x.tool_version_id,v.version,v.schema_digest",
            "x.id",
        ),
        "effects": ("tool_effects x", "x.id,x.step_id,x.status,x.dispatch_attempt_id", "x.id"),
        "checkpoints": ("checkpoints x", "x.id,x.step_id,x.attempt_id,x.created_at", "x.id"),
        "events": ("run_events x", "x.sequence,x.type,x.occurred_at", "x.sequence"),
        "usage": ("usage_entries x", "x.id,x.step_id,x.source,x.quantity,x.unit", "x.id"),
    }
    collections: dict[str, list[dict[str, Any]]] = {}
    issues: list[str] = []
    if not isinstance(run["agent_digest"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", run["agent_digest"]
    ):
        run["agent_digest"] = None
        issues.append("AGENT_DIGEST_INVALID")
    run["state"] = _registered(run["state"], frozenset(RunState), "RUN_STATE_UNREGISTERED", issues)
    truncated = False
    for name, (source, columns, order) in specifications.items():
        rows, exceeded = await _collection(conn, source, columns, order, scope)
        collections[name] = rows
        if exceeded:
            truncated = True
            issues.append(f"{name.upper()}_TRUNCATED")
    for step in collections["steps"]:
        step["state"] = _registered(
            step["state"], frozenset(StepState), "STEP_STATE_UNREGISTERED", issues
        )
        step["kind"] = _registered(
            step["kind"], frozenset(StepKind), "STEP_KIND_UNREGISTERED", issues
        )
    for name in ("model_calls", "tool_calls"):
        for call in collections[name]:
            call["status"] = _registered(
                call["status"], CALL_STATES, "CALL_STATUS_UNREGISTERED", issues
            )
    for work in collections["work_items"]:
        work["status"] = _registered(
            work["status"], WORK_STATES, "WORK_STATUS_UNREGISTERED", issues
        )
    for call in collections["tool_calls"]:
        if not isinstance(call["schema_digest"], str) or not re.fullmatch(
            r"[0-9a-f]{64}", call["schema_digest"]
        ):
            call["schema_digest"] = None
            issues.append("TOOL_SCHEMA_DIGEST_INVALID")
    for attempt in collections["attempts"]:
        start, finish = attempt["started_at"], attempt["finished_at"]
        duration = (finish - start).total_seconds() * 1000 if start and finish else None
        attempt["duration_ms"] = (
            round(duration, 3) if duration is not None and duration >= 0 else None
        )
        if duration is not None and duration < 0:
            issues.append("ATTEMPT_TIME_INVALID")
        attempt["error_code"] = _error_code(attempt["error_code"])
    for call in collections["model_calls"]:
        if call["model_route"] != "mock/release-planner-v1":
            call["model_route"] = "UNREGISTERED"
    for item in collections["usage"]:
        if item["source"] not in {"MODEL", "TOOL"}:
            item["source"] = "UNREGISTERED"
        if item["unit"] != "call":
            item["unit"] = "UNREGISTERED"
    for event in collections["events"]:
        if event["type"] not in EVENT_TYPES:
            event["type"] = "UNREGISTERED"
            issues.append("EVENT_TYPE_UNREGISTERED")
    _check_integrity(run, collections, issues)
    return {
        "schema_version": 1,
        "redaction_policy": REDACTION_POLICY,
        "run": _json_metadata(run),
        **{name: [_json_metadata(row) for row in rows] for name, rows in collections.items()},
        "metrics": {"tokens": None, "cost": None, "provider_latency_ms": None},
        "versions": {"prompt_version": None, "runtime_version": None},
        "limitations": [
            "DURABLE_METADATA_PROJECTION_NOT_OTEL_SPANS",
            "ATTEMPT_DURATION_IS_LEASE_LIFETIME_NOT_PROVIDER_LATENCY",
            "USAGE_COUNTS_COMMITTED_LOGICAL_CALLS_NOT_PHYSICAL_ATTEMPTS",
            "TOKEN_COST_AND_PROVIDER_TIMING_NOT_COLLECTED",
            "RAW_CONTENT_NOT_INCLUDED",
        ],
        "integrity": {
            "complete": not issues,
            "truncated": truncated,
            "issues": sorted(set(issues)),
        },
    }


def _check_integrity(
    run: dict[str, Any], collections: dict[str, list[dict[str, Any]]], issues: list[str]
) -> None:
    if not run["agent_digest"]:
        issues.append("AGENT_VERSION_MISSING")
    steps = {s["id"]: s for s in collections["steps"]}
    if not steps:
        issues.append("STEPS_MISSING")
    if [s["ordinal"] for s in collections["steps"]] != list(range(1, len(steps) + 1)):
        issues.append("STEP_ORDINAL_GAP")
    attempts = {a["id"]: a for a in collections["attempts"]}
    for name in (
        "work_items",
        "attempts",
        "model_calls",
        "tool_calls",
        "effects",
        "checkpoints",
        "usage",
    ):
        for child in collections[name]:
            if child["step_id"] not in steps:
                issues.append("STEP_REFERENCE_MISSING")
    work_items = {w["id"]: w for w in collections["work_items"]}
    for step in steps.values():
        if sum(w["step_id"] == step["id"] for w in work_items.values()) != 1:
            issues.append("STEP_WORK_ITEM_MISSING")
    for attempt in attempts.values():
        work = work_items.get(attempt["work_id"])
        if work is None or work["step_id"] != attempt["step_id"]:
            issues.append("ATTEMPT_WORK_REFERENCE_MISSING")
    for work in work_items.values():
        numbers = sorted(a["attempt_no"] for a in attempts.values() if a["work_id"] == work["id"])
        if len(numbers) != work["attempt_count"]:
            issues.append("WORK_ATTEMPT_COUNT_MISMATCH")
        # Enumerate only the bounded result, never allocate up to an untrusted DB count.
        if any(number != index for index, number in enumerate(numbers, 1)):
            issues.append("ATTEMPT_SEQUENCE_GAP")
    for checkpoint in collections["checkpoints"]:
        attempt = attempts.get(checkpoint["attempt_id"])
        if attempt is None or attempt["step_id"] != checkpoint["step_id"]:
            issues.append("CHECKPOINT_ATTEMPT_MISSING")
    for effect in collections["effects"]:
        attempt = attempts.get(effect["dispatch_attempt_id"])
        if effect["dispatch_attempt_id"] is not None and (
            attempt is None or attempt["step_id"] != effect["step_id"]
        ):
            issues.append("EFFECT_ATTEMPT_MISSING")
    for call in collections["tool_calls"]:
        if not call["schema_digest"]:
            issues.append("TOOL_VERSION_MISSING")
        if not any(effect["step_id"] == call["step_id"] for effect in collections["effects"]):
            issues.append("TOOL_EFFECT_MISSING")
    for step in steps.values():
        if step["state"] == "SUCCEEDED":
            if not any(c["step_id"] == step["id"] for c in collections["checkpoints"]):
                issues.append("SUCCEEDED_STEP_CHECKPOINT_MISSING")
            calls = collections["model_calls" if step["kind"] == "MODEL_CALL" else "tool_calls"]
            if not any(c["step_id"] == step["id"] and c["status"] == "SUCCEEDED" for c in calls):
                issues.append("SUCCEEDED_STEP_CALL_MISSING")
    sequences = [event["sequence"] for event in collections["events"]]
    if sequences != list(range(1, len(sequences) + 1)):
        issues.append("EVENT_SEQUENCE_GAP")
    if not sequences or sequences[-1] != run["state_version"]:
        issues.append("EVENT_STATE_VERSION_MISMATCH")
