import json

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.postgres.tables import model_calls, run_steps, tool_calls, work_items
from agent_platform.adapters.tools.mock_evaluation import MockEvaluationTool
from agent_platform.api.app import create_app
from agent_platform.application.ports import CreateRunCommand
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings
from agent_platform.worker.poller import WorkerPoller

INPUT = {
    "candidate_model_ref": "mock://candidate-17",
    "evaluation_suite_ref": "mock://release-gate-v3",
}


@pytest.mark.asyncio
async def test_http_to_two_worker_steps_and_resumable_events(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    app = create_app(
        settings=Settings(_env_file=None),
        repository=repository,
        identity_verifier=StaticTokenVerifier({"test-token": seed.principal}),
    )
    poller = WorkerPoller(
        repository, RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool())
    )
    headers = {"Authorization": "Bearer test-token", "Idempotency-Key": "e2e-1"}
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.post(
            "/v1/runs",
            headers=headers,
            json={"agent_version_id": seed.agent_version_id, "input": INPUT},
        )
        assert response.status_code == 202, response.text
        run_id = response.json()["run_id"]
        assert response.json()["state"] == "QUEUED"
        assert await poller.poll_once() is True
        intermediate = await repository.get_run(seed.principal, run_id)
        assert intermediate.state == "QUEUED"  # Tool is separately scheduled.
        assert await poller.poll_once() is True
        assert await poller.poll_once() is False
        run = (await client.get(f"/v1/runs/{run_id}", headers=headers)).json()
        assert run["state"] == "COMPLETED"
        assert run["result"]["decision"] == "EVALUATED"
        assert run["result"]["candidate_model_ref"] == INPUT["candidate_model_ref"]
        events = (await client.get(f"/v1/runs/{run_id}/events", headers=headers)).json()["items"]
        assert [e["sequence"] for e in events] == list(range(1, len(events) + 1))
        assert all(e["schema_version"] == 1 for e in events)
        assert (await repository.get_run(seed.principal, run_id)).state_version == len(events)
        stream = await client.get(
            f"/v1/runs/{run_id}/events/stream", headers={**headers, "Last-Event-ID": "2"}
        )
        assert stream.status_code == 200
        ids = [int(line[4:]) for line in stream.text.splitlines() if line.startswith("id: ")]
        assert ids == list(range(3, len(events) + 1))
        final_cursor = await client.get(
            f"/v1/runs/{run_id}/events/stream",
            headers={**headers, "Last-Event-ID": str(len(events))},
        )
        assert final_cursor.status_code == 200
        assert "data:" not in final_cursor.text
        assert "candidate_model_ref" not in json.dumps(events)  # Events carry metadata only.
    async with admin_engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(model_calls)) == 1
        assert await connection.scalar(select(func.count()).select_from(tool_calls)) == 1
        assert await connection.scalar(select(func.count()).select_from(run_steps)) == 2
        assert await connection.scalar(select(func.count()).select_from(work_items)) == 2


@pytest.mark.asyncio
async def test_tool_failure_does_not_block_next_run(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    command = CreateRunCommand(seed.principal, seed.agent_version_id, INPUT)
    first = await repository.accept_run(command=command, idempotency_key="failure")

    class FailingTool:
        async def execute(self, *, tool_version, arguments):
            raise RuntimeError("SECRET_provider_detail_do_not_expose")

    failing_poller = WorkerPoller(
        repository, RuntimeKernel(repository, MockModelGateway(), FailingTool())
    )
    assert await failing_poller.poll_once()
    assert await failing_poller.poll_once()
    failed = await repository.get_run(seed.principal, first.run.id)
    assert failed.state == "FAILED"
    assert "SECRET" not in json.dumps(failed.error)
    second = await repository.accept_run(command=command, idempotency_key="following")
    healthy_poller = WorkerPoller(
        repository, RuntimeKernel(repository, MockModelGateway(), MockEvaluationTool())
    )
    assert await healthy_poller.poll_once()
    assert await healthy_poller.poll_once()
    assert (await repository.get_run(seed.principal, second.run.id)).state == "COMPLETED"


@pytest.mark.asyncio
async def test_agent_input_schema_rejects_bad_request_before_acceptance(
    admin_engine, runtime_engine
):
    seed = await seed_example(admin_engine)
    app = create_app(
        settings=Settings(_env_file=None),
        repository=PostgresRunRepository(runtime_engine),
        identity_verifier=StaticTokenVerifier({"test-token": seed.principal}),
    )
    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        response = await client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "bad-input"},
            json={"agent_version_id": seed.agent_version_id, "input": {}},
        )
        assert response.status_code == 422
    async with admin_engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(work_items)) == 0
