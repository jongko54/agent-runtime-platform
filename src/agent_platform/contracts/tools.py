from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator

from agent_platform.contracts.validation import validate_schema


class ToolInvocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool_version: StrictStr = Field(min_length=1)
    arguments: dict[str, Any]


class ToolResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    output: dict[str, Any]
    usage_units: int = Field(default=1, ge=0, strict=True)


class ToolVersionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: StrictStr = Field(min_length=1)
    version: int = Field(ge=1, strict=True)
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    risk_tier: Literal["T0", "T1", "T2", "T3"]
    connection_kind: StrictStr = Field(min_length=1)

    @field_validator("input_schema", "output_schema")
    @classmethod
    def check_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_schema(value)
