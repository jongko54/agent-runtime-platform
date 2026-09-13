from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.api.app import create_app
from agent_platform.application.errors import ExecutionScopeNotFound
from agent_platform.application.ports import AcceptedRun, EventRecord, PrincipalContext, RunRecord
from agent_platform.settings import Settings


def record(state="QUEUED"):
    return RunRecord(
        "run", "tenant", "project", "user", "agent", state, 1, {}, None, None, datetime.now(UTC)
    )


def make_app(repository=None, identity=True):
    repo = repository or AsyncMock()
    repo.accept_run.return_value = AcceptedRun(record(), False)
    return create_app(
        settings=Settings(database_url="postgresql+psycopg://unused"),
        repository=repo,
        identity_verifier=StaticTokenVerifier({"valid": PrincipalContext("tenant", "user")})
        if identity
        else None,
    )


@pytest.mark.asyncio
async def test_unconfigured_identity_fails_closed():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(identity=False)), base_url="http://test"
    ) as client:
        response = await client.post(
            "/v1/runs",
            json={"agent_version_id": "agent", "input": {}},
            headers={"Authorization": "Bearer valid", "Idempotency-Key": "x"},
        )
    assert response.status_code == 503


@pytest.mark.asyncio
async def test_missing_bearer_is_401_even_when_identity_not_configured():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(identity=False)), base_url="http://test"
    ) as client:
        response = await client.get("/v1/runs/run")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_run_and_event_envelopes_expose_public_identifiers():
    repo = AsyncMock()
    repo.get_run.return_value = record()
    repo.list_events.return_value = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        run = await client.get("/v1/runs/run", headers={"Authorization": "Bearer valid"})
        events = await client.get("/v1/runs/run/events", headers={"Authorization": "Bearer valid"})
    assert run.json()["run_id"] == "run"
    assert "tenant_id" not in run.json()
    assert events.json() == {"items": []}


@pytest.mark.asyncio
async def test_create_derives_scope_from_identity_and_rejects_body_scope():
    repo = AsyncMock()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        headers = {"Authorization": "Bearer valid", "Idempotency-Key": "x"}
        response = await client.post(
            "/v1/runs", json={"agent_version_id": "agent", "input": {}}, headers=headers
        )
        bad = await client.post(
            "/v1/runs",
            json={"agent_version_id": "agent", "input": {}, "tenant_id": "secret-other-tenant"},
            headers=headers,
        )
    assert response.status_code == 202
    assert response.json()["project_id"] == "project"
    assert repo.accept_run.call_args.kwargs["command"].principal == PrincipalContext(
        "tenant", "user"
    )
    assert bad.status_code == 422
    assert "secret-other-tenant" not in bad.text


@pytest.mark.asyncio
async def test_body_limit_and_idempotency_key_are_enforced():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app()), base_url="http://test"
    ) as client:
        response = await client.post("/v1/runs", content=b"x" * 65537)
        missing_key = await client.post(
            "/v1/runs",
            json={"agent_version_id": "agent", "input": {}},
            headers={"Authorization": "Bearer valid"},
        )
    assert response.status_code == 413
    assert missing_key.status_code == 422


@pytest.mark.asyncio
async def test_sse_reconnect_emits_only_new_events_and_closes_on_terminal():
    repo = AsyncMock()
    repo.get_run.return_value = record("COMPLETED")
    repo.list_events.side_effect = [
        [EventRecord(2, "RUN_COMPLETED", 1, "worker", {}, datetime.now(UTC))],
        [],
    ]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        response = await client.get(
            "/v1/runs/run/events/stream",
            headers={"Authorization": "Bearer valid", "Last-Event-ID": "1"},
        )
    assert response.status_code == 200
    assert "id: 2" in response.text
    assert "id: 1" not in response.text
    assert repo.list_events.call_args_list[0].kwargs["after_sequence"] == 1


@pytest.mark.asyncio
async def test_missing_or_cross_scope_run_is_404():
    repo = AsyncMock()
    repo.get_run.side_effect = ExecutionScopeNotFound()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        response = await client.get("/v1/runs/other", headers={"Authorization": "Bearer valid"})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_sse_drains_multiple_batches_without_gaps():
    repo = AsyncMock()
    repo.get_run.return_value = record("COMPLETED")
    events = [
        EventRecord(i, "STEP_EVENT", 1, "worker", {}, datetime.now(UTC)) for i in range(1, 102)
    ]
    repo.list_events.side_effect = [events[:100], events[100:]]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        response = await client.get(
            "/v1/runs/run/events/stream", headers={"Authorization": "Bearer valid"}
        )
    emitted = [int(line[4:]) for line in response.text.splitlines() if line.startswith("id: ")]
    assert emitted == list(range(1, 102))
    assert repo.list_events.call_args_list[1].kwargs["after_sequence"] == 100


@pytest.mark.asyncio
async def test_sse_rechecks_scope_and_stops_when_membership_revoked():
    repo = AsyncMock()
    repo.get_run.side_effect = [record(), ExecutionScopeNotFound()]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app(repo)), base_url="http://test"
    ) as client:
        response = await client.get(
            "/v1/runs/run/events/stream", headers={"Authorization": "Bearer valid"}
        )
    assert "STREAM_ACCESS_DENIED" in response.text
    repo.list_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_stream_invalid_cursor_and_event_limit_are_rejected():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_app()), base_url="http://test"
    ) as client:
        cursor = await client.get(
            "/v1/runs/run/events/stream",
            headers={
                "Authorization": "Bearer valid",
                "Last-Event-ID": "-1",
            },
        )
        limit = await client.get(
            "/v1/runs/run/events?limit=1001", headers={"Authorization": "Bearer valid"}
        )
    assert cursor.status_code == 422
    assert limit.status_code == 422
