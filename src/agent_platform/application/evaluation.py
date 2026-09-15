"""Explicitly curated cases; evaluation never executes tools or mutates a Run."""

import asyncio
import json
import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol, cast

from pydantic import ValidationError

from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import InvalidInput, PolicyDenied, RuntimeConflict
from agent_platform.application.ports import ModelGateway, PrincipalContext
from agent_platform.contracts.tools import ToolInvocation

REVIEW_POLICY = "explicit-curation-v1"
MODEL_REVISION = "mock/release-planner-v1"
SCORER_REVISION = "tool-invocation-exact-v1"


@dataclass(frozen=True, slots=True)
class EvaluationCase:
    id: str
    candidate_id: str
    run_id: str
    project_id: str
    source_snapshot_digest: str
    source_agent_version_id: str
    content_digest: str
    input: dict[str, Any]
    expected_decision: dict[str, Any]
    allowed_tools: tuple[str, ...]
    review_policy: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AcceptedCase:
    case: EvaluationCase
    duplicate: bool


class EvaluationRepository(Protocol):
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
    ) -> AcceptedCase: ...

    async def get_case(self, principal: PrincipalContext, case_id: str) -> EvaluationCase: ...

    async def get_cases(
        self, principal: PrincipalContext, case_ids: list[str]
    ) -> list[EvaluationCase]: ...


def validate_case_content(
    input: dict[str, Any], expected_decision: dict[str, Any], allowed_tools: tuple[str, ...]
) -> dict[str, Any]:
    _bounded_json({"input": input, "expected_decision": expected_decision})
    if type(input) is not dict:
        raise InvalidInput("Curated input must be an object")
    try:
        invocation = ToolInvocation.model_validate(expected_decision, strict=True)
    except ValidationError:
        raise InvalidInput("Expected decision must be a ToolInvocation") from None
    if invocation.tool_version not in allowed_tools:
        raise InvalidInput("Expected tool is not allowed")
    return invocation.model_dump(mode="json")


def _bounded_json(value: Any) -> None:
    pending: list[tuple[Any, int]] = [(value, 0)]
    visited = 0
    while pending:
        item, depth = pending.pop()
        visited += 1
        if depth > 12 or visited > 2048:
            raise InvalidInput("Curated JSON exceeds structural limits")
        if type(item) is dict:
            mapping = cast(dict[Any, Any], item)
            if not all(type(key) is str and "\x00" not in key for key in mapping):
                raise InvalidInput("JSON object keys must be strings")
            pending.extend((child, depth + 1) for child in mapping.values())
        elif type(item) is list:
            pending.extend((child, depth + 1) for child in cast(list[Any], item))
        elif type(item) is str and "\x00" in item:
            raise InvalidInput("NUL is not supported in curated JSON")
        elif item is not None and type(item) not in {str, int, float, bool}:
            raise InvalidInput("Only JSON values are supported")
    try:
        size = len(
            json.dumps(
                _normalize_numbers(value),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        )
    except (ValueError, TypeError, UnicodeError):
        raise InvalidInput("Invalid JSON content") from None
    if size > 16384:
        raise InvalidInput("Curated content exceeds 16 KiB")


def case_content_digest(
    *,
    candidate_id: str,
    source_snapshot_digest: str,
    source_agent_version_id: str,
    input: dict[str, Any],
    expected_decision: dict[str, Any],
    allowed_tools: tuple[str, ...],
) -> str:
    return _evaluation_digest(
        {
            "candidate_id": candidate_id,
            "source_snapshot_digest": source_snapshot_digest,
            "source_agent_version_id": source_agent_version_id,
            "input": input,
            "expected_decision": expected_decision,
            "allowed_tools": list(allowed_tools),
            "review_policy": REVIEW_POLICY,
        }
    )


def _normalize_numbers(value: Any) -> Any:
    """Match JSONB's numeric value, not Python's float spelling or signed zero."""
    if type(value) is float and value.is_integer():
        # Decimal(str(...)) preserves the JSON spelling: int(1e23) would instead
        # expose binary floating-point rounding not present in the JSON payload.
        return int(Decimal(str(value)))
    if type(value) is dict:
        return {key: _normalize_numbers(item) for key, item in cast(dict[str, Any], value).items()}
    if type(value) is list:
        return [_normalize_numbers(item) for item in cast(list[Any], value)]
    return value


def _evaluation_digest(value: Any) -> str:
    return canonical_digest(_normalize_numbers(value))


async def evaluate_cases(
    cases: list[EvaluationCase],
    model: ModelGateway,
    *,
    model_revision: str = MODEL_REVISION,
    timeout_seconds: float = 1,
) -> dict[str, Any]:
    if model_revision != MODEL_REVISION:
        raise PolicyDenied("Only the local mock model revision is supported")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 5:
        raise InvalidInput("Evaluation timeout must be positive and bounded")
    if (
        not 1 <= len(cases) <= 50
        or len({case.id for case in cases}) != len(cases)
        or len({case.project_id for case in cases}) != 1
    ):
        raise InvalidInput("Select 1 to 50 distinct cases from one Project")
    # Freeze before the first await; nested dicts in frozen dataclasses are still mutable.
    captured = sorted(deepcopy(cases), key=lambda case: case.id)
    for case in captured:
        validate_case_content(case.input, case.expected_decision, case.allowed_tools)
        digest = case_content_digest(
            candidate_id=case.candidate_id,
            source_snapshot_digest=case.source_snapshot_digest,
            source_agent_version_id=case.source_agent_version_id,
            input=case.input,
            expected_decision=case.expected_decision,
            allowed_tools=case.allowed_tools,
        )
        if digest != case.content_digest or case.review_policy != REVIEW_POLICY:
            raise RuntimeConflict("Evaluation case integrity mismatch")
    manifest = {
        "schema_version": 1,
        "cases": [{"id": case.id, "content_digest": case.content_digest} for case in captured],
    }
    items: list[dict[str, Any]] = []
    for case in captured:
        try:
            async with asyncio.timeout(timeout_seconds):
                decision = await model.decide(
                    input=deepcopy(case.input),
                    allowed_tools=case.allowed_tools,
                )
        except TimeoutError:
            outcome = "TIMED_OUT"
        except Exception:
            outcome = "PROVIDER_ERROR"
        else:
            try:
                normalized = validate_case_content({}, decision, case.allowed_tools)
            except InvalidInput:
                outcome = "INVALID_OUTPUT"
            else:
                outcome = (
                    "PASS"
                    if _evaluation_digest(normalized) == _evaluation_digest(case.expected_decision)
                    else "MISMATCH"
                )
        items.append(
            {
                "case_id": case.id,
                "content_digest": case.content_digest,
                "candidate_id": case.candidate_id,
                "source_snapshot_digest": case.source_snapshot_digest,
                "source_agent_version_id": case.source_agent_version_id,
                "outcome": outcome,
            }
        )
    passed = sum(item["outcome"] == "PASS" for item in items)
    return {
        "schema_version": 1,
        "kind": "OFFLINE_MODEL_DECISION",
        "model_revision": MODEL_REVISION,
        "scorer_revision": SCORER_REVISION,
        "manifest": manifest,
        "suite_digest": canonical_digest(manifest),
        "total": len(items),
        "passed": passed,
        "failed": len(items) - passed,
        "items": items,
    }
