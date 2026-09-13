from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from agent_platform.application.errors import InvalidInput, PolicyDenied

TOOL_VERSION = "evaluation.run_suite:v1"


class EvaluationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    candidate_model_ref: str = Field(min_length=1)
    evaluation_suite_ref: str = Field(min_length=1)


def parse_evaluation_input(arguments: dict[str, Any]) -> EvaluationInput:
    try:
        return EvaluationInput.model_validate(arguments)
    except ValidationError:
        raise InvalidInput("Invalid evaluation input") from None


class MockEvaluationTool:
    async def execute(self, *, tool_version: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_version != TOOL_VERSION:
            raise PolicyDenied("Unsupported tool version")
        payload = parse_evaluation_input(arguments)
        return {
            "decision": "EVALUATED",
            **payload.model_dump(),
            "quality_score": 0.86,
            "safety_score": 0.99,
        }
