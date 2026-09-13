from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.api.app import create_app
from agent_platform.application.errors import ExecutionScopeNotFound, RuntimeConflict
from agent_platform.application.ports import (
    AcceptedRun,
    DeadLetterRecord,
    PrincipalContext,
    RunRecord,
)
from agent_platform.settings import Settings

HEADERS = {"Authorization": "Bearer valid", "Idempotency-Key": "retry-one"}
URL = "/v1/runs/source/dead-letters/item/redrive"
BODY = {"reason": "WORKER_RECOVERED"}


def client_for(repo):
    app = create_app(
        settings=Settings(_env_file=None),
        repository=repo,
        identity_verifier=StaticTokenVerifier({"valid": PrincipalContext("tenant", "user")}),
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_redrive_returns_new_run_and_preserves_duplicate_flag():
    repo = AsyncMock()
    child = RunRecord(
        "child",
        "tenant",
        "project",
        "user",
        "agent",
        "QUEUED",
        1,
        {},
        None,
        None,
        datetime.now(UTC),
    )
    repo.redrive_dead_letter.return_value = AcceptedRun(child, False)
    async with client_for(repo) as client:
        response = await client.post(URL, headers=HEADERS, json=BODY)
        assert response.status_code == 202
        assert response.json() == {
            "run_id": "child",
            "source_run_id": "source",
            "dead_letter_id": "item",
            "project_id": "project",
            "state": "QUEUED",
            "duplicate": False,
        }
        repo.redrive_dead_letter.return_value = AcceptedRun(child, True)
        assert (await client.post(URL, headers=HEADERS, json=BODY)).json()["duplicate"]
    repo.redrive_dead_letter.assert_awaited_with(
        PrincipalContext("tenant", "user"),
        "source",
        "item",
        idempotency_key="retry-one",
        reason="WORKER_RECOVERED",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        {"reason": "free text"},
        {**BODY, "input": {}},
        {**BODY, "tenant_id": "other"},
        {**BODY, "force": True},
    ],
)
async def test_redrive_rejects_missing_reason_or_untrusted_overrides(body):
    repo = AsyncMock()
    async with client_for(repo) as client:
        assert (await client.post(URL, headers=HEADERS, json=body)).status_code == 422
    repo.redrive_dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_redrive_requires_auth_key_and_masks_conflict():
    repo = AsyncMock()
    async with client_for(repo) as client:
        assert (await client.post(URL, json=BODY)).status_code == 401
        assert (
            await client.post(URL, headers={"Authorization": "Bearer valid"}, json=BODY)
        ).status_code == 422
        assert (
            await client.post(URL, headers={**HEADERS, "Idempotency-Key": "   "}, json=BODY)
        ).status_code == 422
        repo.redrive_dead_letter.assert_not_awaited()
        repo.redrive_dead_letter.side_effect = RuntimeConflict("internal-sensitive-detail")
        response = await client.post(URL, headers=HEADERS, json=BODY)
        assert response.status_code == 409
        assert "internal-sensitive-detail" not in response.text
        repo.redrive_dead_letter.side_effect = ExecutionScopeNotFound()
        assert (await client.post(URL, headers=HEADERS, json=BODY)).status_code == 404


@pytest.mark.asyncio
async def test_dlq_listing_is_bounded_and_metadata_only():
    repo = AsyncMock()
    repo.list_dead_letters.return_value = [
        DeadLetterRecord(
            "item", "source", "step", "attempt", "RETRY_EXHAUSTED", datetime.now(UTC), "child"
        )
    ]
    async with client_for(repo) as client:
        response = await client.get(
            "/v1/runs/source/dead-letters?limit=1&after_id=before", headers=HEADERS
        )
        assert response.status_code == 200
        item = response.json()["items"][0]
        assert item["redriven_run_id"] == "child"
        assert not ({"input", "result", "tenant_id", "principal_id"} & item.keys())
        repo.list_dead_letters.assert_awaited_once_with(
            PrincipalContext("tenant", "user"), "source", "before", 1
        )
        assert (
            await client.get("/v1/runs/source/dead-letters?limit=1001", headers=HEADERS)
        ).status_code == 422
        repo.list_dead_letters.side_effect = ExecutionScopeNotFound()
        assert (
            await client.get("/v1/runs/source/dead-letters", headers=HEADERS)
        ).status_code == 404
