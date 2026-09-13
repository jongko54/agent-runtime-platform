import json
from pathlib import Path

import pytest

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.application.errors import InvalidInput, PolicyDenied
from agent_platform.contracts.agents import AgentVersionSpec
from agent_platform.contracts.tools import ToolVersionSpec
from agent_platform.contracts.validation import validate_payload

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "ai-model-release"
PAYLOAD = {
    "candidate_model_ref": "models://candidate/v1",
    "evaluation_suite_ref": "eval://golden/v1",
}


@pytest.mark.asyncio
async def test_example_model_and_tool_obey_registered_contracts() -> None:
    agent = AgentVersionSpec.model_validate_json((EXAMPLES / "agent-version.json").read_text())
    tool = ToolVersionSpec.model_validate_json(
        (EXAMPLES / "evaluation-tool-version.json").read_text()
    )
    validate_payload(PAYLOAD, agent.input_schema)
    decision = await MockModelGateway().decide(input=PAYLOAD, allowed_tools=agent.tools)
    assert decision["tool_version"] == "evaluation.run_suite:v1"
    validate_payload(decision["arguments"], tool.input_schema)
    result = await MockEvaluationTool().execute(**decision)
    validate_payload(result, tool.output_schema)
    assert result == {
        **PAYLOAD,
        "decision": "EVALUATED",
        "quality_score": 0.86,
        "safety_score": 0.99,
    }
    assert json.loads(json.dumps(result)) == result


@pytest.mark.asyncio
async def test_model_cannot_choose_an_unregistered_tool() -> None:
    with pytest.raises(PolicyDenied):
        await MockModelGateway().decide(input=PAYLOAD, allowed_tools=())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [{}, {**PAYLOAD, "unexpected": "secret"}, {**PAYLOAD, "candidate_model_ref": 4}]
)
async def test_bad_inputs_fail_safely(payload: dict) -> None:
    with pytest.raises(InvalidInput):
        await MockModelGateway().decide(input=payload, allowed_tools=("evaluation.run_suite:v1",))
    with pytest.raises(InvalidInput):
        await MockEvaluationTool().execute(
            tool_version="evaluation.run_suite:v1", arguments=payload
        )


@pytest.mark.asyncio
async def test_tool_rejects_unknown_version() -> None:
    with pytest.raises(PolicyDenied):
        await MockEvaluationTool().execute(
            tool_version="evaluation.run_suite:v2", arguments=PAYLOAD
        )
