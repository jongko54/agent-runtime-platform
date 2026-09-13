"""Durable idempotent provider simulator, independent of runtime transactions.

The provider shares PostgreSQL infrastructure for protocol testing only; this is
not evidence of independent external-provider durability or exactly-once delivery.
"""

import json
from typing import Any, cast

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from agent_platform.adapters.postgres.database import set_context
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import IdempotencyConflict, InvalidInput
from agent_platform.application.ports import EffectDispatch


def _request_hash(effect: EffectDispatch) -> str:
    return canonical_digest({"tool_version": effect.tool_version, "arguments": effect.arguments})


async def _context(connection: AsyncConnection, effect: EffectDispatch) -> None:
    await set_context(connection, effect.tenant_id)
    await connection.execute(
        text("SELECT set_config('app.project_id', :project, true)"),
        {"project": effect.project_id},
    )


async def _stored(connection: AsyncConnection, effect: EffectDispatch) -> RowMapping | None:
    return (
        (
            await connection.execute(
                text("""
                    SELECT request_hash,result FROM mock_provider_results
                    WHERE idempotency_key=:key AND tenant_id=:tenant AND project_id=:project
                """),
                {
                    "key": effect.idempotency_key,
                    "tenant": effect.tenant_id,
                    "project": effect.project_id,
                },
            )
        )
        .mappings()
        .one_or_none()
    )


def _result(row: RowMapping, request_hash: str) -> dict[str, Any]:
    if row["request_hash"] != request_hash:
        raise IdempotencyConflict("Provider request conflicts with an existing effect")
    return cast(dict[str, Any], row["result"])


class PersistentMockEvaluationTool:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.calculator = MockEvaluationTool()

    async def execute(
        self, *, tool_version: str, arguments: dict[str, Any], effect: EffectDispatch | None = None
    ) -> dict[str, Any]:
        if effect is None or tool_version != effect.tool_version or arguments != effect.arguments:
            raise InvalidInput("A matching effect dispatch is required")
        request_hash = _request_hash(effect)
        async with self.engine.begin() as connection:
            await _context(connection, effect)
            stored = await _stored(connection, effect)
            if stored is not None:
                return _result(stored, request_hash)
            result = await self.calculator.execute(tool_version=tool_version, arguments=arguments)
            await connection.execute(
                text("""
                    INSERT INTO mock_provider_results
                        (idempotency_key,tenant_id,project_id,request_hash,result)
                    VALUES (:key,:tenant,:project,:hash,CAST(:result AS jsonb))
                    ON CONFLICT (idempotency_key) DO NOTHING
                """),
                {
                    "key": effect.idempotency_key,
                    "tenant": effect.tenant_id,
                    "project": effect.project_id,
                    "hash": request_hash,
                    "result": json.dumps(result, allow_nan=False, separators=(",", ":")),
                },
            )
            # A separate statement sees the winning concurrent transaction in
            # READ COMMITTED. No UPDATE grant or mutable provider result needed.
            stored = await _stored(connection, effect)
            if stored is None:
                # A global key collision outside RLS scope must not disclose it.
                raise IdempotencyConflict("Provider request could not be recorded")
            return _result(stored, request_hash)

    async def lookup(self, effect: EffectDispatch) -> dict[str, Any] | None:
        async with self.engine.begin() as connection:
            await _context(connection, effect)
            stored = await _stored(connection, effect)
            return None if stored is None else _result(stored, _request_hash(effect))
