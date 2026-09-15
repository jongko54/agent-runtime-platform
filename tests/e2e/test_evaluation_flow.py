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
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings


@pytest.mark.asyncio
async def test_candidate_to_curated_cases_and_offline_regression_without_runtime_replay(
    admin_engine, runtime_engine, runtime_url, monkeypatch
):
    seed = await seed_example(admin_engine)
    other = await seed_example(admin_engine, project_id="other-project", principal_id="other")
    repository = PostgresRunRepository(runtime_engine)
    kernel = RuntimeKernel(
        repository, MockModelGateway(), PersistentMockEvaluationTool(runtime_engine)
    )
    app = create_app(
        settings=Settings(_env_file=None, database_url=runtime_url),
        identity_verifier=StaticTokenVerifier({"valid": seed.principal, "other": other.principal}),
    )
    headers = {"Authorization": "Bearer valid", "Idempotency-Key": "evaluation-flow"}
    original = {"candidate_model_ref": "SECRET_original", "evaluation_suite_ref": "SECRET_original"}
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        accepted = await client.post(
            "/v1/runs",
            headers=headers,
            json={"agent_version_id": seed.agent_version_id, "input": original},
        )
        assert accepted.status_code == 202
        run_id = accepted.json()["run_id"]
        for _ in range(2):
            work = await repository.claim_work()
            assert work is not None
            await kernel.execute(work)
        before = await repository.get_run(seed.principal, run_id)
        events_before = await repository.list_events(seed.principal, run_id)
        draft_response = await client.post(
            f"/v1/runs/{run_id}/evaluation-candidates",
            headers=headers,
            json={"source_state_version": before.state_version, "expected_state": "COMPLETED"},
        )
        assert draft_response.status_code == 201
        draft = draft_response.json()["candidate"]
        assert "SECRET_original" not in json.dumps(draft)
        curated = {"candidate_model_ref": "mock://curated", "evaluation_suite_ref": "mock://suite"}
        body = {
            "source_snapshot_digest": draft["snapshot_digest"],
            "input": curated,
            "expected_decision": {"tool_version": "evaluation.run_suite:v1", "arguments": curated},
            "review_confirmed": True,
        }
        url = f"/v1/evaluation-candidates/{draft['id']}/cases"
        missing_review = await client.post(
            url, headers=headers, json={k: v for k, v in body.items() if k != "review_confirmed"}
        )
        assert missing_review.status_code == 422
        response = await client.post(url, headers=headers, json=body)
        assert response.status_code == 201, response.text
        case = response.json()["case"]
        assert "SECRET_original" not in response.text
        assert case["input"] == curated
        replay = await client.post(url, headers=headers, json=body)
        assert replay.status_code == 200 and replay.json()["case"] == case
        wrong = await client.post(
            url,
            headers={**headers, "Idempotency-Key": "wrong-expectation"},
            json={
                **body,
                "expected_decision": {
                    "tool_version": "evaluation.run_suite:v1",
                    "arguments": {**curated, "candidate_model_ref": "mock://different"},
                },
            },
        )
        assert wrong.status_code == 201
        ids = [case["id"], wrong.json()["case"]["id"]]

        def forbid_execution(*args, **kwargs):
            raise AssertionError("Offline evaluation must not execute tools or runtime")

        monkeypatch.setattr(PersistentMockEvaluationTool, "execute", forbid_execution)
        monkeypatch.setattr(RuntimeKernel, "execute", forbid_execution)
        first = await client.post(
            "/v1/evaluations/offline", headers=headers, json={"case_ids": ids}
        )
        assert first.status_code == 200, first.text
        report = first.json()
        assert report["total"] == 2 and report["passed"] == report["failed"] == 1
        assert {item["outcome"] for item in report["items"]} == {"PASS", "MISMATCH"}
        assert "SECRET" not in first.text and "mock://curated" not in first.text
        repeated = await client.post(
            "/v1/evaluations/offline", headers=headers, json={"case_ids": list(reversed(ids))}
        )
        assert repeated.json() == report
        assert await repository.get_run(seed.principal, run_id) == before
        assert await repository.list_events(seed.principal, run_id) == events_before
        assert (
            await client.get(f"/v1/evaluation-candidates/{draft['id']}", headers=headers)
        ).json() == draft
        for identity in ("other", "valid"):
            if identity == "valid":
                async with admin_engine.begin() as conn:
                    await conn.execute(
                        text(
                            "UPDATE project_memberships SET status='REVOKED' WHERE principal_id=:id"
                        ),
                        {"id": seed.principal.principal_id},
                    )
            denied_headers = {**headers, "Authorization": f"Bearer {identity}"}
            assert (
                await client.get(f"/v1/evaluation-cases/{case['id']}", headers=denied_headers)
            ).status_code == 404
            assert (
                await client.post(
                    "/v1/evaluations/offline", headers=denied_headers, json={"case_ids": ids}
                )
            ).status_code == 404
            assert (await client.post(url, headers=denied_headers, json=body)).status_code == 404
    async with admin_engine.connect() as conn:
        for table, expected in (
            ("runs", 1),
            ("mock_provider_results", 1),
            ("evaluation_cases", 2),
            ("evaluation_candidates", 1),
            ("run_attempts", 2),
        ):
            assert await conn.scalar(text(f"SELECT count(*) FROM {table}")) == expected
