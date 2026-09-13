import asyncio
import json

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
from agent_platform.application.digest import canonical_digest
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings


@pytest.mark.asyncio
async def test_default_api_wiring_metadata_trace_and_immutable_draft(
    admin_engine, runtime_engine, runtime_url
):
    seed = await seed_example(admin_engine)
    other = await seed_example(admin_engine, project_id="other-project", principal_id="other")
    repo = PostgresRunRepository(runtime_engine)
    kernel = RuntimeKernel(repo, MockModelGateway(), PersistentMockEvaluationTool(runtime_engine))
    app = create_app(
        settings=Settings(_env_file=None, database_url=runtime_url),
        identity_verifier=StaticTokenVerifier({"valid": seed.principal, "other": other.principal}),
    )
    headers = {"Authorization": "Bearer valid", "Idempotency-Key": "trace-flow"}
    secret = "SECRET_trace_payload@example.invalid"
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        accepted = await client.post(
            "/v1/runs",
            headers=headers,
            json={
                "agent_version_id": seed.agent_version_id,
                "input": {"candidate_model_ref": secret, "evaluation_suite_ref": secret},
            },
        )
        assert accepted.status_code == 202
        run_id = accepted.json()["run_id"]
        trace_url = f"/v1/runs/{run_id}/trace"
        candidate_url = f"/v1/runs/{run_id}/evaluation-candidates"
        queued = await client.get(trace_url, headers=headers)
        assert queued.status_code == 200
        early = await client.post(
            candidate_url,
            headers=headers,
            json={
                "source_state_version": queued.json()["run"]["state_version"],
                "expected_state": "COMPLETED",
            },
        )
        assert early.status_code == 409  # no frozen candidate of a moving live Run
        for _ in range(2):
            work = await repo.claim_work()
            assert work is not None
            await kernel.execute(work)
        before = await repo.get_run(seed.principal, run_id)
        assert before.state == "COMPLETED"
        trace = await client.get(trace_url, headers=headers)
        assert trace.status_code == 200
        snapshot = trace.json()
        assert snapshot["integrity"]["complete"]
        assert snapshot["redaction_policy"] == "metadata-only-v1"
        assert len(snapshot["steps"]) == len(snapshot["attempts"]) == 2
        assert len(snapshot["model_calls"]) == len(snapshot["tool_calls"]) == 1
        assert secret not in trace.text
        body = {"source_state_version": before.state_version, "expected_state": "COMPLETED"}
        stale = await client.post(
            candidate_url,
            headers=headers,
            json={**body, "source_state_version": before.state_version - 1},
        )
        assert stale.status_code == 409
        results = await asyncio.gather(
            *(client.post(candidate_url, headers=headers, json=body) for _ in range(10))
        )
        assert sorted(r.status_code for r in results) == [200] * 9 + [201]
        candidates = [r.json()["candidate"] for r in results]
        assert len({c["id"] for c in candidates}) == 1
        draft = candidates[0]
        assert draft["snapshot"] == snapshot
        assert draft["snapshot_digest"] == canonical_digest(snapshot)
        assert draft["status"] == "DRAFT"
        assert secret not in json.dumps(draft)
        assert await repo.get_run(seed.principal, run_id) == before
        case_url = f"/v1/evaluation-candidates/{draft['id']}"
        assert (await client.get(case_url, headers=headers)).json() == draft
        for url in (trace_url, case_url):
            denied = await client.get(url, headers={"Authorization": "Bearer other"})
            assert denied.status_code == 404
        assert (
            await client.post(
                candidate_url, headers={**headers, "Authorization": "Bearer other"}, json=body
            )
        ).status_code == 404
        async with admin_engine.begin() as conn:
            await conn.execute(
                text("UPDATE project_memberships SET status='REVOKED' WHERE principal_id=:id"),
                {"id": seed.principal.principal_id},
            )
        for url in (trace_url, case_url):
            assert (await client.get(url, headers=headers)).status_code == 404
        assert (await client.post(candidate_url, headers=headers, json=body)).status_code == 404
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM evaluation_candidates")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM mock_provider_results")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM runs")) == 1
