import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import InvalidInput
from agent_platform.contracts.agents import AgentVersionSpec
from agent_platform.contracts.tools import ToolVersionSpec
from agent_platform.contracts.validation import validate_payload

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "ai-model-release"


def agent_data() -> dict:
    return json.loads((EXAMPLES / "agent-version.json").read_text())


def test_example_versions_validate_and_reject_unknown_fields() -> None:
    agent = AgentVersionSpec.model_validate(agent_data())
    assert agent.tools == ("evaluation.run_suite:v1",)
    ToolVersionSpec.model_validate_json((EXAMPLES / "evaluation-tool-version.json").read_text())
    with pytest.raises(ValidationError):
        AgentVersionSpec.model_validate({**agent_data(), "unknown": True})


def test_input_schema_is_required() -> None:
    data = agent_data()
    del data["input_schema"]
    with pytest.raises(ValidationError):
        AgentVersionSpec.model_validate(data)


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "nonsense"},
        {"$ref": "https://example.com/schema.json"},
        {"properties": {"nested": {"$ref": "#/$defs/value"}}},
        {"properties": {"nested": {"$dynamicRef": "file:///etc/passwd"}}},
    ],
)
def test_registry_rejects_invalid_or_reference_resolving_schemas(schema: dict) -> None:
    with pytest.raises(ValidationError):
        AgentVersionSpec.model_validate({**agent_data(), "input_schema": schema})


def test_payload_error_does_not_disclose_input() -> None:
    with pytest.raises(InvalidInput) as error:
        validate_payload(
            {"secret": "sensitive-value"}, {"type": "object", "additionalProperties": False}
        )
    assert "sensitive-value" not in str(error.value)
    assert "secret" not in str(error.value)


def test_digest_is_order_independent_and_rejects_non_json_numbers() -> None:
    assert canonical_digest({"a": 1, "b": [2]}) == canonical_digest({"b": [2], "a": 1})
    assert canonical_digest({"a": 1}) != canonical_digest({"a": 2})
    with pytest.raises(ValueError):
        canonical_digest({"number": float("nan")})
