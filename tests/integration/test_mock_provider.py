import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from agent_platform.adapters.postgres.database import set_context
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.errors import IdempotencyConflict, InvalidInput
from agent_platform.application.ports import EffectDispatch


def effect():
    return EffectDispatch(
        id=uuid4().hex,
        tenant_id=uuid4().hex,
        project_id=uuid4().hex,
        idempotency_key=uuid4().hex,
        dispatch_token=uuid4().hex,
        tool_version="evaluation.run_suite:v1",
        arguments={"candidate_model_ref": "candidate", "evaluation_suite_ref": "suite"},
    )


async def execute(provider, dispatch):
    return await provider.execute(
        tool_version=dispatch.tool_version, arguments=dispatch.arguments, effect=dispatch
    )


@pytest.mark.asyncio
async def test_concurrent_provider_calls_reuse_one_durable_result(admin_engine, runtime_engine):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    assert await provider.lookup(dispatch) is None
    results = await asyncio.gather(*[execute(provider, dispatch) for _ in range(30)])
    assert all(result == results[0] for result in results)
    assert await PersistentMockEvaluationTool(runtime_engine).lookup(dispatch) == results[0]
    assert await execute(provider, replace(dispatch, dispatch_token=uuid4().hex)) == results[0]
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM mock_provider_results"))
        ).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_concurrent_conflicting_provider_requests_cannot_overwrite_winner(
    admin_engine, runtime_engine
):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    changed = replace(
        dispatch, arguments={**dispatch.arguments, "candidate_model_ref": "different"}
    )
    outcomes = await asyncio.gather(
        execute(provider, dispatch), execute(provider, changed), return_exceptions=True
    )
    assert sum(isinstance(outcome, IdempotencyConflict) for outcome in outcomes) == 1
    successful = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    assert len(successful) == 1
    async with admin_engine.connect() as conn:
        stored = (
            (await conn.execute(text("SELECT result FROM mock_provider_results"))).scalars().all()
        )
        assert stored == successful


@pytest.mark.asyncio
async def test_provider_result_survives_callers_transaction_rollback(runtime_engine):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with runtime_engine.begin():
            result = await execute(provider, dispatch)
            raise RuntimeError("caller rollback")
    assert await provider.lookup(dispatch) == result


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["arguments", "tool_version"])
async def test_provider_rejects_same_key_with_different_request(runtime_engine, field):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    result = await execute(provider, dispatch)
    changed = replace(
        dispatch,
        **{
            field: {**dispatch.arguments, "candidate_model_ref": "other"}
            if field == "arguments"
            else "evaluation.run_suite:v2"
        },
    )
    with pytest.raises(IdempotencyConflict):
        await execute(provider, changed)
    with pytest.raises(IdempotencyConflict):
        await provider.lookup(changed)
    assert await provider.lookup(dispatch) == result


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["tenant_id", "project_id"])
async def test_provider_hides_other_scope_and_rejects_collision(runtime_engine, field):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    await execute(provider, dispatch)
    changed = replace(dispatch, **{field: uuid4().hex})
    assert await provider.lookup(changed) is None
    with pytest.raises(IdempotencyConflict):
        await execute(provider, changed)
    async with runtime_engine.begin() as conn:
        await set_context(conn, changed.tenant_id)
        await conn.execute(
            text("SELECT set_config('app.project_id', :project, true)"),
            {"project": changed.project_id},
        )
        assert (
            await conn.execute(text("SELECT count(*) FROM mock_provider_results"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_provider_does_not_allow_app_result_mutation(runtime_engine):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    await execute(provider, dispatch)
    for statement in (
        "UPDATE mock_provider_results SET result='{}'::jsonb",
        "DELETE FROM mock_provider_results",
    ):
        async with runtime_engine.begin() as conn:
            await set_context(conn, dispatch.tenant_id)
            await conn.execute(
                text("SELECT set_config('app.project_id', :project, true)"),
                {"project": dispatch.project_id},
            )
            with pytest.raises(DBAPIError):
                await conn.execute(text(statement))


@pytest.mark.asyncio
async def test_provider_requires_effect_and_exact_dispatch_payload(admin_engine, runtime_engine):
    provider = PersistentMockEvaluationTool(runtime_engine)
    dispatch = effect()
    with pytest.raises(InvalidInput):
        await provider.execute(tool_version=dispatch.tool_version, arguments=dispatch.arguments)
    with pytest.raises(InvalidInput):
        await provider.execute(
            tool_version=dispatch.tool_version,
            arguments={**dispatch.arguments, "candidate_model_ref": "other"},
            effect=dispatch,
        )
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM mock_provider_results"))
        ).scalar_one() == 0
