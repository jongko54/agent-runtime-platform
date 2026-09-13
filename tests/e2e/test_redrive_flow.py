"""HTTP redrive through actual persistence and worker execution."""

import asyncio

import httpx
import pytest
from sqlalchemy import text

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.api.app import create_app
from agent_platform.application.ports import CreateRunCommand
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings


async def fixture(admin_engine, runtime_engine, *, tool=False, dispatch=False):
    seed = await seed_example(admin_engine)
    operator = await seed_example(admin_engine, principal_id="operator")
    async with admin_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE project_memberships SET role_set_id='operator' "
                "WHERE principal_id='operator'"
            )
        )
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    provider = PersistentMockEvaluationTool(runtime_engine)
    kernel = RuntimeKernel(repo, MockModelGateway(), provider)
    source = (
        await repo.accept_run(
            command=CreateRunCommand(
                seed.principal,
                seed.agent_version_id,
                {"candidate_model_ref": "a", "evaluation_suite_ref": "b"},
            ),
            idempotency_key="source",
        )
    ).run
    work = await repo.claim_work()
    if tool:
        await kernel.execute(work)
        work = await repo.claim_work()
    if dispatch:
        await repo.begin_tool_dispatch(work)
        await repo.mark_tool_unknown(work)
    else:
        await repo.retry_work(work, "PROVIDER_TRANSIENT", "Temporary failure")
    app = create_app(
        settings=Settings(_env_file=None),
        repository=repo,
        tool_gateway=provider,
        identity_verifier=StaticTokenVerifier({"operator": operator.principal}),
    )
    return seed, operator, source, repo, kernel, app


HEADERS = {"Authorization": "Bearer operator", "Idempotency-Key": "redrive-once"}
BODY = {"reason": "WORKER_RECOVERED"}


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", [False, True])
async def test_authorized_redrive_preserves_source_and_child_completes(
    admin_engine, runtime_engine, tool
):
    seed, operator, source, repo, kernel, app = await fixture(
        admin_engine, runtime_engine, tool=tool
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        listing = await client.get(f"/v1/runs/{source.id}/dead-letters", headers=HEADERS)
        assert listing.status_code == 200
        item = listing.json()["items"][0]
        assert item["reason_code"] == "RETRY_EXHAUSTED"
        path = f"/v1/runs/{source.id}/dead-letters/{item['id']}/redrive"
        replies = await asyncio.gather(
            *(client.post(path, headers=HEADERS, json=BODY) for _ in range(10))
        )
        assert all(r.status_code == 202 for r in replies), [r.text for r in replies]
        assert sum(not r.json()["duplicate"] for r in replies) == 1
        child_id = replies[0].json()["run_id"]
        assert child_id != source.id
        assert {r.json()["run_id"] for r in replies} == {child_id}
        child = await repo.get_run(operator.principal, child_id)
        assert child.principal_id == operator.principal.principal_id
        assert child.input == source.input and child.agent_version_id == source.agent_version_id
        for _ in range(2):
            work = await repo.claim_work()
            assert work.run_id == child_id and work.attempt_no == 1
            await kernel.execute(work)
        assert (await repo.get_run(operator.principal, child_id)).state == "COMPLETED"
        old = await repo.get_run(seed.principal, source.id)
        assert old.state == "FAILED" and old.error["code"] == "RETRY_EXHAUSTED"
        replay = await client.post(path, headers=HEADERS, json=BODY)
        assert replay.status_code == 202
        assert replay.json()["duplicate"] and replay.json()["run_id"] == child_id
        assert (
            await client.post(path, headers={**HEADERS, "Idempotency-Key": "different"}, json=BODY)
        ).status_code == 409
        listed = (await client.get(f"/v1/runs/{source.id}/dead-letters", headers=HEADERS)).json()
        assert listed["items"][0]["redriven_run_id"] == child_id
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM runs")) == 2
        assert await conn.scalar(text("SELECT count(*) FROM dead_letter_redrives")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM mock_provider_results")) == 1
    assert await repo.claim_work() is None


@pytest.mark.asyncio
async def test_unknown_redrive_is_blocked_and_permission_revocation_is_current(
    admin_engine, runtime_engine
):
    _seed, _operator, source, repo, _kernel, app = await fixture(
        admin_engine, runtime_engine, tool=True, dispatch=True
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        item = (await client.get(f"/v1/runs/{source.id}/dead-letters", headers=HEADERS)).json()[
            "items"
        ][0]
        path = f"/v1/runs/{source.id}/dead-letters/{item['id']}/redrive"
        assert (await client.post(path, headers=HEADERS, json=BODY)).status_code == 409
        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE project_memberships SET status='REVOKED' WHERE principal_id='operator'"
                )
            )
        assert (await client.post(path, headers=HEADERS, json=BODY)).status_code == 404
        assert (
            await client.get(f"/v1/runs/{source.id}/dead-letters", headers=HEADERS)
        ).status_code == 404
    assert await repo.claim_work() is None
    async with admin_engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM runs")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM dead_letter_redrives")) == 0
