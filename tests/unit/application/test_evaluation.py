import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.application import evaluation as subject
from agent_platform.application.errors import InvalidInput, PolicyDenied, RuntimeConflict

INPUT = {"candidate_model_ref": "mock://SECRET_candidate", "evaluation_suite_ref": "mock://suite"}
TOOLS = ("evaluation.run_suite:v1",)
EXPECTED = {"tool_version": TOOLS[0], "arguments": INPUT}


def case(case_id="case-a", **changes):
    content = dict(
        candidate_id="candidate",
        source_snapshot_digest="a" * 64,
        source_agent_version_id="b" * 48,
        input=INPUT.copy(),
        expected_decision={"tool_version": TOOLS[0], "arguments": INPUT.copy()},
        allowed_tools=TOOLS,
    )
    content.update(changes)
    return subject.EvaluationCase(
        id=case_id,
        run_id="run",
        project_id="project",
        **content,
        content_digest=subject.case_content_digest(**content),
        review_policy=subject.REVIEW_POLICY,
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_real_mock_model_scores_decision_without_tools_or_raw_report_content():
    result = await subject.evaluate_cases([case()], MockModelGateway())
    assert result["passed"] == result["total"] == 1
    assert result["failed"] == 0
    assert result["items"][0]["outcome"] == "PASS"
    assert result["model_revision"] == subject.MODEL_REVISION
    assert result["scorer_revision"] == subject.SCORER_REVISION
    assert "SECRET" not in json.dumps(result)
    assert "arguments" not in json.dumps(result)


@pytest.mark.asyncio
async def test_manifest_and_report_are_deterministic_independent_of_case_order():
    first, second = case("a"), case("b")
    assert await subject.evaluate_cases([first, second], MockModelGateway()) == (
        await subject.evaluate_cases([second, first], MockModelGateway())
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value,outcome",
    [
        ({"tool_version": TOOLS[0], "arguments": {}}, "MISMATCH"),
        ({"tool_version": "forbidden:v1", "arguments": {}}, "INVALID_OUTPUT"),
        ({"bad": "SECRET"}, "INVALID_OUTPUT"),
        ({"tool_version": TOOLS[0], "arguments": {"bad": float("nan")}}, "INVALID_OUTPUT"),
    ],
)
async def test_exact_scorer_rejects_wrong_or_invalid_decisions(value, outcome):
    class Model:
        async def decide(self, **kwargs):
            return value

    result = await subject.evaluate_cases([case()], Model())
    assert result["failed"] == 1
    assert result["items"][0]["outcome"] == outcome
    assert "SECRET" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ValueError("SECRET"), RuntimeError("SECRET")])
async def test_provider_error_is_safe_and_next_case_continues(error):
    class Model:
        def __init__(self):
            self.calls = 0

        async def decide(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise error
            return EXPECTED

    result = await subject.evaluate_cases([case("a"), case("b")], Model())
    assert result["passed"] == result["failed"] == 1
    assert result["items"][0]["outcome"] == "PROVIDER_ERROR"
    assert "SECRET" not in json.dumps(result)


@pytest.mark.asyncio
async def test_timeout_and_cancellation_have_distinct_boundaries():
    class Slow:
        async def decide(self, **kwargs):
            await asyncio.sleep(10)

    assert (await subject.evaluate_cases([case()], Slow(), timeout_seconds=0.001))["items"][0][
        "outcome"
    ] == "TIMED_OUT"

    class Cancelled:
        async def decide(self, **kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await subject.evaluate_cases([case()], Cancelled())


@pytest.mark.asyncio
async def test_case_content_tampering_is_rejected_before_model_call():
    with pytest.raises(RuntimeConflict):
        await subject.evaluate_cases([replace(case(), input={})], MockModelGateway())


@pytest.mark.asyncio
async def test_gateway_cannot_mutate_expected_or_persisted_case_content():
    original = case()

    class Mutating:
        async def decide(self, *, input, **kwargs):
            input["candidate_model_ref"] = "changed"
            return {"tool_version": TOOLS[0], "arguments": input}

    result = await subject.evaluate_cases([original], Mutating())
    assert result["items"][0]["outcome"] == "MISMATCH"
    assert original.input == original.expected_decision["arguments"] == INPUT


@pytest.mark.asyncio
async def test_scope_size_and_route_are_validated_before_evaluation():
    for cases in (
        [],
        [case()] * 2,
        [case(str(n)) for n in range(51)],
        [case("a"), replace(case("b"), project_id="other")],
    ):
        with pytest.raises(InvalidInput):
            await subject.evaluate_cases(cases, MockModelGateway())
    with pytest.raises(PolicyDenied):
        await subject.evaluate_cases([case()], MockModelGateway(), model_revision="external/llm")


@pytest.mark.parametrize("payload", [{"value": float("inf")}, {"x": "s" * 17000}, {"x": object()}])
def test_curated_json_is_bounded_and_json_only(payload):
    with pytest.raises(InvalidInput):
        subject.validate_case_content(payload, EXPECTED, TOOLS)


def test_curated_json_rejects_depth_and_expected_tool_not_in_allowlist():
    nested = {}
    for _ in range(20):
        nested = {"x": nested}
    with pytest.raises(InvalidInput):
        subject.validate_case_content(nested, EXPECTED, TOOLS)
    with pytest.raises(InvalidInput):
        subject.validate_case_content(INPUT, EXPECTED, ())


@pytest.mark.parametrize("payload", [{"value": "nul\x00value"}, {"bad\x00key": "value"}])
def test_postgres_incompatible_nul_is_rejected_before_persistence(payload):
    with pytest.raises(InvalidInput):
        subject.validate_case_content(payload, EXPECTED, TOOLS)


@pytest.mark.parametrize(
    "before,after",
    [
        (1e20, 100000000000000000000),
        (1e23, 100000000000000000000000),
        (-0.0, 0.0),
        (1.0, 1),
    ],
)
def test_case_hash_is_stable_across_jsonb_numeric_representation(before, after):
    first = case(expected_decision={"tool_version": TOOLS[0], "arguments": {"n": before}})
    second = case(expected_decision={"tool_version": TOOLS[0], "arguments": {"n": after}})
    assert first.content_digest == second.content_digest


@pytest.mark.asyncio
async def test_numeric_value_equivalence_does_not_conflate_booleans_with_numbers():
    class Model:
        async def decide(self, **kwargs):
            return {"tool_version": TOOLS[0], "arguments": {"n": 1}}

    numeric = case("a", expected_decision={"tool_version": TOOLS[0], "arguments": {"n": 1.0}})
    boolean = case("b", expected_decision={"tool_version": TOOLS[0], "arguments": {"n": True}})
    result = await subject.evaluate_cases([numeric, boolean], Model())
    assert [item["outcome"] for item in result["items"]] == ["PASS", "MISMATCH"]


def test_content_limit_applies_to_jsonb_expanded_numbers_before_creation():
    with pytest.raises(InvalidInput):
        subject.validate_case_content(
            INPUT,
            {
                "tool_version": TOOLS[0],
                "arguments": {"n": [1e308] * 100},
            },
            TOOLS,
        )
