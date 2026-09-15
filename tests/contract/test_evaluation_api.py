from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.api.app import create_app
from agent_platform.application.errors import ExecutionScopeNotFound
from agent_platform.application.evaluation import (
    REVIEW_POLICY,
    AcceptedCase,
    EvaluationCase,
    case_content_digest,
)
from agent_platform.application.ports import PrincipalContext
from agent_platform.settings import Settings

INPUT = {"candidate_model_ref": "mock://curated", "evaluation_suite_ref": "mock://suite"}
EXPECTED = {"tool_version": "evaluation.run_suite:v1", "arguments": INPUT}
BODY = {
    "source_snapshot_digest": "a" * 64,
    "input": INPUT,
    "expected_decision": EXPECTED,
    "review_confirmed": True,
}
HEADERS = {"Authorization": "Bearer valid", "Idempotency-Key": "case-1"}
URL = "/v1/evaluation-candidates/candidate/cases"


def evaluated_case():
    content = dict(
        candidate_id="candidate",
        source_snapshot_digest="a" * 64,
        source_agent_version_id="b" * 48,
        input=INPUT,
        expected_decision=EXPECTED,
        allowed_tools=("evaluation.run_suite:v1",),
    )
    return EvaluationCase(
        "case",
        run_id="run",
        project_id="project",
        **content,
        content_digest=case_content_digest(**content),
        review_policy=REVIEW_POLICY,
        created_at=datetime.now(UTC),
    )


def client_for(store):
    app = create_app(
        settings=Settings(_env_file=None),
        repository=AsyncMock(),
        identity_verifier=StaticTokenVerifier({"valid": PrincipalContext("t", "u")}),
    )
    app.state.evaluations = store
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_case_create_get_and_side_effect_free_report():
    store = AsyncMock()
    record = evaluated_case()
    store.create_case.return_value = AcceptedCase(record, False)
    store.get_case.return_value = record
    store.get_cases.return_value = [record]
    async with client_for(store) as client:
        result = await client.post(URL, headers=HEADERS, json=BODY)
        assert result.status_code == 201
        assert result.json()["case"]["review_policy"] == REVIEW_POLICY
        assert (
            await client.get("/v1/evaluation-cases/case", headers=HEADERS)
        ).json() == result.json()["case"]
        report = await client.post(
            "/v1/evaluations/offline", headers=HEADERS, json={"case_ids": ["case"]}
        )
        assert report.status_code == 200 and report.json()["passed"] == 1
        assert "mock://curated" not in report.text
    assert store.get_cases.await_count == 2  # Reauthorize before returning report.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "patch",
    [
        {"review_confirmed": False},
        {"review_confirmed": 1},
        {"review_confirmed": "true"},
        {"source_snapshot_digest": "invalid"},
        {"status": "APPROVED"},
        {"tenant_id": "other"},
    ],
)
async def test_case_invalid_review_scope_or_digest_rejected(patch):
    store = AsyncMock()
    async with client_for(store) as client:
        assert (await client.post(URL, headers=HEADERS, json={**BODY, **patch})).status_code == 422
    store.create_case.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"case_ids": []},
        {"case_ids": ["c"] * 51},
        {"case_ids": ["c", "c"]},
        {"case_ids": ["c"], "model_revision": "external/llm"},
        {"case_ids": ["c"], "input": {}},
    ],
)
async def test_report_rejects_unbounded_duplicate_or_external_evaluation(body):
    store = AsyncMock()
    async with client_for(store) as client:
        assert (
            await client.post("/v1/evaluations/offline", headers=HEADERS, json=body)
        ).status_code == 422
    store.get_cases.assert_not_awaited()


@pytest.mark.asyncio
async def test_scope_revoked_during_evaluation_discards_report():
    store = AsyncMock()
    store.get_cases.side_effect = [[evaluated_case()], ExecutionScopeNotFound()]
    async with client_for(store) as client:
        assert (
            await client.post(
                "/v1/evaluations/offline", headers=HEADERS, json={"case_ids": ["case"]}
            )
        ).status_code == 404


@pytest.mark.asyncio
async def test_authentication_and_unconfigured_store_fail_closed():
    async with client_for(None) as client:
        assert (await client.post(URL, json=BODY)).status_code == 401
        assert (await client.post(URL, headers=HEADERS, json=BODY)).status_code == 503
