from typing import Any

from agent_platform.adapters.tools.mock_evaluation import TOOL_VERSION, parse_evaluation_input
from agent_platform.application.errors import PolicyDenied


class MockModelGateway:
    async def decide(
        self, *, input: dict[str, Any], allowed_tools: tuple[str, ...]
    ) -> dict[str, Any]:
        if TOOL_VERSION not in allowed_tools:
            raise PolicyDenied("Required tool is not allowed")
        payload = parse_evaluation_input(input)
        return {"tool_version": TOOL_VERSION, "arguments": payload.model_dump()}
