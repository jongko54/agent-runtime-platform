from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.api.app import create_app
from agent_platform.application.errors import ExecutionScopeNotFound, RuntimeConflict
from agent_platform.application.observability import AcceptedCandidate, EvaluationCandidate
from agent_platform.application.ports import PrincipalContext
from agent_platform.settings import Settings

HEADERS = {"Authorization": "Bearer valid", "Idempotency-Key": "candidate-one"}
BODY = {"source_state_version": 7, "expected_state": "COMPLETED"}
URL = "/v1/runs/run/evaluation-candidates"


def client_for(observations):
    app = create_app(
        settings=Settings(_env_file=None),
        repository=AsyncMock(),
        identity_verifier=StaticTokenVerifier({"valid": PrincipalContext("tenant", "user")}),
    )
    # State injection permits a meaningful 404 RED before the new API exists.
    app.state.observations = observations
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def candidate():
    return EvaluationCandidate(
        "case",
        "run",
        "project",
        7,
        "COMPLETED",
        "DRAFT",
        {"schema_version": 1},
        "digest",
        "metadata-only-v1",
        datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_metadata_trace_and_candidate_roundtrip_contract():
    observations = AsyncMock()
    observations.get_trace.return_value = {"schema_version": 1, "run": {"state_version": 7}}
    observations.create_evaluation_candidate.return_value = AcceptedCandidate(candidate(), False)
    observations.get_evaluation_candidate.return_value = candidate()
    async with client_for(observations) as client:
        trace = await client.get("/v1/runs/run/trace", headers=HEADERS)
        assert trace.status_code == 200 and trace.json()["run"]["state_version"] == 7
        first = await client.post(URL, headers=HEADERS, json=BODY)
        assert first.status_code == 201
        assert first.json()["candidate"]["status"] == "DRAFT"
        assert first.json()["candidate"]["source_state_version"] == 7
        observations.create_evaluation_candidate.return_value = AcceptedCandidate(candidate(), True)
        second = await client.post(URL, headers=HEADERS, json=BODY)
        assert second.status_code == 200 and second.json()["duplicate"]
        fetched = await client.get("/v1/evaluation-candidates/case", headers=HEADERS)
        assert fetched.status_code == 200 and fetched.json()["id"] == "case"
    observations.create_evaluation_candidate.assert_awaited_with(
        PrincipalContext("tenant", "user"),
        "run",
        source_state_version=7,
        expected_state="COMPLETED",
        idempotency_key="candidate-one",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        {**BODY, "input": {}},
        {**BODY, "snapshot": {}},
        {**BODY, "tenant_id": "other"},
        {**BODY, "status": "APPROVED"},
        {**BODY, "source_state_version": 0},
        {**BODY, "source_state_version": True},
        {**BODY, "expected_state": "RUNNING"},
        {**BODY, "expected_state": "secret-free-text"},
    ],
)
async def test_candidate_rejects_raw_content_scope_and_invalid_labels(body):
    observations = AsyncMock()
    async with client_for(observations) as client:
        assert (await client.post(URL, headers=HEADERS, json=body)).status_code == 422
    observations.create_evaluation_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_observation_auth_and_safe_errors():
    observations = AsyncMock()
    async with client_for(observations) as client:
        assert (await client.get("/v1/runs/run/trace")).status_code == 401
        assert (
            await client.post(URL, headers={"Authorization": "Bearer valid"}, json=BODY)
        ).status_code == 422
        assert (
            await client.post(URL, headers={**HEADERS, "Idempotency-Key": "   "}, json=BODY)
        ).status_code == 422
        observations.create_evaluation_candidate.assert_not_awaited()
        observations.get_trace.side_effect = ExecutionScopeNotFound()
        assert (await client.get("/v1/runs/run/trace", headers=HEADERS)).status_code == 404
        observations.get_evaluation_candidate.side_effect = ExecutionScopeNotFound()
        assert (
            await client.get("/v1/evaluation-candidates/case", headers=HEADERS)
        ).status_code == 404
        observations.create_evaluation_candidate.side_effect = RuntimeConflict("secret-detail")
        response = await client.post(URL, headers=HEADERS, json=BODY)
        assert response.status_code == 409 and "secret-detail" not in response.text


@pytest.mark.asyncio
async def test_unconfigured_observation_dependency_fails_safely():
    async with client_for(None) as client:
        response = await client.get("/v1/runs/run/trace", headers=HEADERS)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "OBSERVATION_UNAVAILABLE"
