from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator

from agent_platform.contracts.validation import validate_schema


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_steps: int = Field(ge=1, le=100, strict=True)
    deadline_seconds: int = Field(ge=1, le=86_400, strict=True)
    max_input_tokens: int = Field(ge=1, strict=True)
    max_output_tokens: int = Field(ge=1, strict=True)
    max_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)


class ApprovalPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    required_tools: tuple[StrictStr, ...]


class CompensationRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: StrictStr
    requires_original_effect: StrictStr
    authorization_source: StrictStr
    max_age_seconds: int = Field(ge=1, strict=True)


class CompensationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rules: tuple[CompensationRule, ...]


class AgentVersionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instructions_ref: StrictStr = Field(min_length=1)
    model_route: StrictStr = Field(min_length=1)
    tools: tuple[StrictStr, ...]
    input_schema: dict[str, Any]
    execution_policy: ExecutionPolicy | None = None
    approval_policy: ApprovalPolicy | None = None
    compensation_policy: CompensationPolicy | None = None

    @field_validator("input_schema")
    @classmethod
    def check_input_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_schema(value)
