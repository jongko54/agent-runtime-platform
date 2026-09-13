import asyncio

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import text

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.api.app import create_app
from agent_platform.application.errors import RuntimeConflict
from agent_platform.application.ports import CreateRunCommand, PrincipalContext
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings


async def queued_tool(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine)
    repo = PostgresRunRepository(runtime_engine)
    provider = PersistentMockEvaluationTool(runtime_engine)
    accepted = await repo.accept_run(
        command=CreateRunCommand(
            seed.principal,
            seed.agent_version_id,
            {"candidate_model_ref": "mock://candidate", "evaluation_suite_ref": "mock://suite"},
        ),
        idempotency_key="cancel-e2e",
    )
    model = await repo.claim_work()
    assert model is not None
    await RuntimeKernel(repo, MockModelGateway(), provider).execute(model)
    return seed, repo, provider, accepted.run.id


def app_for(seed, repo, provider):
    return create_app(
        settings=Settings(_env_file=None),
        repository=repo,
        tool_gateway=provider,
        identity_verifier=StaticTokenVerifier(
            {
                "valid": seed.principal,
                "other": PrincipalContext("other", "intruder"),
            }
        ),
    )


HEADERS = {"Authorization": "Bearer valid"}


@pytest.mark.asyncio
async def test_cancel_before_dispatch_blocks_effect_and_is_idempotent(admin_engine, runtime_engine):
    seed, repo, provider, run_id = await queued_tool(admin_engine, runtime_engine)
    work = await repo.claim_work()
    assert work is not None
    app = app_for(seed, repo, provider)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        first = await client.post(f"/v1/runs/{run_id}/cancel", headers=HEADERS)
        second = await client.post(f"/v1/runs/{run_id}/cancel", headers=HEADERS)
        assert first.status_code == second.status_code == 202
        assert first.json() == second.json()
        assert first.json()["state"] == "CANCELLED"
        assert first.json()["cancellation_outcome"] == "NO_EFFECT"
        with pytest.raises(RuntimeConflict):
            await repo.begin_tool_dispatch(work)
        for action in ("cancel", "reconcile"):
            denied = await client.post(
                f"/v1/runs/{run_id}/{action}", headers={"Authorization": "Bearer other"}
            )
            assert denied.status_code == 404
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM mock_provider_results")) == 0


@pytest.mark.asyncio
async def test_cancel_after_dispatch_requires_provider_proof_and_never_claims_rollback(
    admin_engine, runtime_engine
):
    seed, repo, provider, run_id = await queued_tool(admin_engine, runtime_engine)
    work = await repo.claim_work()
    assert work is not None
    effect = await repo.begin_tool_dispatch(work)
    app = app_for(seed, repo, provider)
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        canceled = await client.post(f"/v1/runs/{run_id}/cancel", headers=HEADERS)
        assert canceled.json()["state"] == "OUTCOME_UNKNOWN"
        absent = await client.post(f"/v1/runs/{run_id}/reconcile", headers=HEADERS)
        assert absent.json()["state"] == "OUTCOME_UNKNOWN"
        # The already-linearized send can finish after cancel. It is in-flight,
        # not a newly authorized dispatch after cancellation.
        result = await provider.execute(
            tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
        )
        with pytest.raises(RuntimeConflict):
            await repo.complete_tool(work, result)
        confirmed = await client.post(f"/v1/runs/{run_id}/reconcile", headers=HEADERS)
        assert confirmed.status_code == 200
        assert confirmed.json()["state"] == "CANCELLED"
        assert confirmed.json()["cancellation_outcome"] == "EFFECT_SUCCEEDED"
        again = await client.post(f"/v1/runs/{run_id}/reconcile", headers=HEADERS)
        assert again.json() == confirmed.json()
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM mock_provider_results")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("_case", range(20))
async def test_concurrent_cancel_and_dispatch_have_one_order(admin_engine, runtime_engine, _case):
    seed, repo, _provider, run_id = await queued_tool(admin_engine, runtime_engine)
    work = await repo.claim_work()
    assert work is not None
    dispatch, canceled = await asyncio.gather(
        repo.begin_tool_dispatch(work),
        repo.cancel_run(seed.principal, run_id),
        return_exceptions=True,
    )
    assert not isinstance(canceled, Exception)
    if isinstance(dispatch, RuntimeConflict):
        assert canceled.state == "CANCELLED" and canceled.cancellation_outcome == "NO_EFFECT"
    else:
        assert not isinstance(dispatch, Exception)
        assert canceled.state == "OUTCOME_UNKNOWN"
    with pytest.raises(RuntimeConflict):
        await repo.begin_tool_dispatch(work)
