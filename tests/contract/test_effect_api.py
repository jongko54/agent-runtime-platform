from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.api.app import create_app
from agent_platform.application.errors import ExecutionScopeNotFound
from agent_platform.application.ports import EffectDispatch, PrincipalContext, RunRecord
from agent_platform.settings import Settings


def run(state="OUTCOME_UNKNOWN", outcome=None):
    return RunRecord(
        "run",
        "tenant",
        "project",
        "user",
        "agent",
        state,
        5,
        {},
        None,
        None,
        datetime.now(UTC),
        1,
        outcome,
    )


def app_for(repo, provider):
    app = create_app(
        settings=Settings(_env_file=None),
        repository=repo,
        identity_verifier=StaticTokenVerifier({"valid": PrincipalContext("tenant", "user")}),
    )
    app.state.tool_gateway = provider
    return app


HEADERS = {"Authorization": "Bearer valid"}


@pytest.mark.asyncio
async def test_cancel_returns_durable_outcome_and_rejects_scope_body():
    repo = AsyncMock()
    repo.cancel_run.return_value = run("CANCELLED", "NO_EFFECT")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(repo, AsyncMock())), base_url="http://test"
    ) as client:
        response = await client.post("/v1/runs/run/cancel", headers=HEADERS)
        assert response.status_code == 202
        assert response.json()["cancellation_outcome"] == "NO_EFFECT"
        assert response.json()["cancel_epoch"] == 1
        invalid = await client.post(
            "/v1/runs/run/cancel", headers=HEADERS, json={"tenant_id": "other"}
        )
        assert invalid.status_code == 422
        assert (await client.post("/v1/runs/run/cancel")).status_code == 401
    repo.cancel_run.assert_awaited_once_with(PrincipalContext("tenant", "user"), "run")


@pytest.mark.asyncio
async def test_reconcile_never_accepts_caller_success_proof_and_absence_stays_unknown():
    repo, provider = AsyncMock(), AsyncMock()
    repo.pending_effect.return_value = EffectDispatch(
        "effect", "tenant", "project", "key", "token", "evaluation.run_suite:v1", {}
    )
    repo.get_run.return_value = run()
    provider.lookup.return_value = None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(repo, provider)), base_url="http://test"
    ) as client:
        forged = await client.post(
            "/v1/runs/run/reconcile", headers=HEADERS, json={"result": {"decision": "EVALUATED"}}
        )
        assert forged.status_code == 422
        response = await client.post("/v1/runs/run/reconcile", headers=HEADERS)
        assert response.status_code == 200
        assert response.json()["state"] == "OUTCOME_UNKNOWN"
    provider.lookup.assert_awaited_once()
    repo.reconcile_effect.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_checks_scope_before_provider_lookup_and_masks_provider_error():
    repo, provider = AsyncMock(), AsyncMock()
    repo.pending_effect.side_effect = ExecutionScopeNotFound()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(repo, provider)), base_url="http://test"
    ) as client:
        assert (await client.post("/v1/runs/run/reconcile", headers=HEADERS)).status_code == 404
        provider.lookup.assert_not_awaited()
        repo.pending_effect.side_effect = None
        repo.pending_effect.return_value = EffectDispatch(
            "effect", "tenant", "project", "key", "token", "evaluation.run_suite:v1", {}
        )
        provider.lookup.side_effect = RuntimeError("secret-provider-detail")
        response = await client.post("/v1/runs/run/reconcile", headers=HEADERS)
        assert response.status_code == 503
        assert "secret-provider-detail" not in response.text
