"""One claimed Step per execution; all external I/O is outside repository transactions."""

import asyncio
from typing import Any, cast

from pydantic import ValidationError

from agent_platform.application.errors import (
    InvalidInput,
    PolicyDenied,
    RetryableGatewayError,
    RuntimeConflict,
)
from agent_platform.application.ports import ClaimedWork, ModelGateway, RunRepository, ToolGateway
from agent_platform.contracts.tools import ToolInvocation
from agent_platform.contracts.validation import validate_payload


class RuntimeKernel:
    def __init__(
        self,
        repository: RunRepository,
        model_gateway: ModelGateway,
        tool_gateway: ToolGateway,
        operation_timeout_seconds: float = 30,
    ) -> None:
        self.repository = repository
        self.model_gateway = model_gateway
        self.tool_gateway = tool_gateway
        self.operation_timeout_seconds = operation_timeout_seconds

    async def execute(self, work: ClaimedWork) -> None:
        if work.kind == "TOOL_CALL":
            await self._execute_tool(work)
            return
        try:
            if work.kind == "MODEL_CALL":
                result = await self._model(work)
            else:
                raise PolicyDenied("Unsupported step kind")
        except RuntimeConflict:
            raise
        except RetryableGatewayError:
            await self.repository.retry_work(
                work, "PROVIDER_TRANSIENT", "Provider temporarily unavailable"
            )
        except TimeoutError:
            await self.repository.fail_work(
                work, "OPERATION_TIMEOUT", "Operation exceeded its deadline", timed_out=True
            )
        except PolicyDenied:
            await self.repository.fail_work(
                work, "POLICY_DENIED", "Execution policy denied operation"
            )
        except (InvalidInput, ValidationError, ValueError, KeyError, TypeError):
            await self.repository.fail_work(work, "CLIENT_INVALID", "Execution payload is invalid")
        except Exception:
            await self.repository.fail_work(
                work, "RUNTIME_ERROR", "Execution could not be completed"
            )
        else:
            await self._complete(work, result)

    async def _complete(self, work: ClaimedWork, result: dict[str, Any]) -> None:
        try:
            if work.kind == "MODEL_CALL":
                await self.repository.complete_model(work, result)
            else:
                await self.repository.complete_tool(work, result)
        except InvalidInput:
            # Explicit semantic rejections rolled back before reaching here.
            if work.kind == "TOOL_CALL":
                await self.repository.mark_tool_unknown(work)
            else:
                await self.repository.fail_work(
                    work, "CLIENT_INVALID", "Execution payload is invalid"
                )
        except PolicyDenied:
            if work.kind == "TOOL_CALL":
                await self.repository.mark_tool_unknown(work)
            else:
                await self.repository.fail_work(
                    work, "POLICY_DENIED", "Execution policy denied operation"
                )
        # Other persistence errors and stale leases propagate: an ambiguous commit
        # must never be mistaken for a provider failure or safely retried here.

    async def _model(self, work: ClaimedWork) -> dict[str, Any]:
        if work.agent_spec.get("model_route") != "mock/release-planner-v1":
            raise PolicyDenied("Unsupported model route")
        tool_values = work.agent_spec.get("tools", [])
        if not isinstance(tool_values, list | tuple):
            raise InvalidInput("Invalid tool allowlist")
        tool_items = cast(list[Any] | tuple[Any, ...], tool_values)
        if not all(isinstance(value, str) for value in tool_items):
            raise InvalidInput("Invalid tool allowlist")
        allowed_tools = tuple(str(value) for value in tool_items)
        async with asyncio.timeout(self.operation_timeout_seconds):
            decision = await self.model_gateway.decide(
                input=work.input, allowed_tools=allowed_tools
            )
        invocation = ToolInvocation.model_validate(decision)
        if invocation.tool_version not in allowed_tools:
            raise PolicyDenied("Tool not allowed")
        return invocation.model_dump(mode="json")

    def _validate_tool(self, work: ClaimedWork) -> tuple[str, dict[str, Any]]:
        if work.tool_spec is None:
            raise PolicyDenied("Tool definition is missing")
        tool_version = f"{work.tool_spec.get('name')}:v{work.tool_spec.get('version')}"
        if tool_version != "evaluation.run_suite:v1":
            raise PolicyDenied("Unsupported tool version")
        if tool_version not in work.agent_spec.get("tools", []):
            raise PolicyDenied("Tool not allowed")
        validate_payload(work.input, work.tool_spec["input_schema"])
        return tool_version, work.tool_spec["output_schema"]

    async def _execute_tool(self, work: ClaimedWork) -> None:
        try:
            tool_version, output_schema = self._validate_tool(work)
        except PolicyDenied:
            await self.repository.fail_work(
                work, "POLICY_DENIED", "Execution policy denied operation"
            )
            return
        except (InvalidInput, ValidationError, ValueError, KeyError, TypeError):
            await self.repository.fail_work(work, "CLIENT_INVALID", "Execution payload is invalid")
            return

        # This short transaction serializes dispatch with cancellation. Catch only
        # known semantic rejections: DB failures must reach the worker unchanged.
        try:
            effect = await self.repository.begin_tool_dispatch(work)
        except PolicyDenied:
            await self.repository.fail_work(
                work, "POLICY_DENIED", "Execution policy denied operation"
            )
            return
        except InvalidInput:
            await self.repository.fail_work(work, "CLIENT_INVALID", "Execution payload is invalid")
            return

        try:
            async with asyncio.timeout(self.operation_timeout_seconds):
                result = await self.tool_gateway.execute(
                    tool_version=tool_version, arguments=work.input, effect=effect
                )
            validate_payload(result, output_schema)
        except RuntimeConflict:
            raise
        except Exception:
            # Dispatch is durable, so even a timeout or malformed response cannot
            # establish whether the provider effect happened. Never blind retry.
            await self.repository.mark_tool_unknown(work)
            return
        await self._complete(work, result)
