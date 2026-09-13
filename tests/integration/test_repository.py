import asyncio
import json
from uuid import uuid4

import pytest
from sqlalchemy import event, text

from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    PolicyDenied,
    RuntimeConflict,
)
from agent_platform.application.ports import CreateRunCommand, PrincipalContext


@pytest.mark.asyncio
async def test_concurrent_acceptance_and_step_handoff(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)
    command = CreateRunCommand(
        seed.principal,
        seed.agent_version_id,
        {"candidate_model_ref": "models/candidate", "evaluation_suite_ref": "suites/default"},
    )
    results = await asyncio.gather(
        *[repo.accept_run(command=command, idempotency_key="same-key") for _ in range(100)]
    )
    assert len({item.run.id for item in results}) == 1
    assert sum(not item.duplicate for item in results) == 1
    run_id = results[0].run.id
    model = await repo.claim_work()
    assert model is not None and model.run_id == run_id and model.kind == "MODEL_CALL"
    await repo.complete_model(
        model,
        {
            "tool_version": "evaluation.run_suite:v1",
            "arguments": command.input,
        },
    )
    tool = await repo.claim_work()
    assert tool is not None and tool.id != model.id and tool.step_id != model.step_id
    assert tool.kind == "TOOL_CALL"
    async with admin_engine.connect() as connection:
        assert (
            await connection.execute(
                text("SELECT status FROM tool_calls WHERE step_id=:step"), {"step": tool.step_id}
            )
        ).scalar_one() == "PENDING"
    output = {**command.input, "decision": "EVALUATED", "quality_score": 0.86, "safety_score": 0.99}
    await repo.begin_tool_dispatch(tool)
    await repo.complete_tool(tool, output)
    with pytest.raises(RuntimeConflict):
        await repo.complete_tool(tool, output)
    run = await repo.get_run(seed.principal, run_id)
    assert run.state == "COMPLETED" and run.result == output
    events = await repo.list_events(seed.principal, run_id)
    assert [event.sequence for event in events] == list(range(1, run.state_version + 1))
    assert all(event.schema_version == 1 and event.actor for event in events)
    async with admin_engine.connect() as connection:
        assert (
            await connection.execute(
                text("SELECT count(*) FROM usage_entries WHERE run_id=:run"), {"run": run_id}
            )
        ).scalar_one() == 2


@pytest.mark.asyncio
async def test_scope_and_idempotency_conflict(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)
    payload = {"candidate_model_ref": "a", "evaluation_suite_ref": "b"}
    accepted = await repo.accept_run(
        command=CreateRunCommand(seed.principal, seed.agent_version_id, payload),
        idempotency_key="key",
    )
    with pytest.raises(IdempotencyConflict):
        await repo.accept_run(
            command=CreateRunCommand(
                seed.principal,
                seed.agent_version_id,
                {
                    **payload,
                    "candidate_model_ref": "different",
                },
            ),
            idempotency_key="key",
        )
    with pytest.raises(ExecutionScopeNotFound):
        await repo.get_run(PrincipalContext(seed.principal.tenant_id, "stranger"), accepted.run.id)
    async with admin_engine.begin() as connection:
        await connection.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE tenant_id=:tenant"),
            {"tenant": seed.principal.tenant_id},
        )
    with pytest.raises(ExecutionScopeNotFound):
        await repo.get_run(seed.principal, accepted.run.id)


@pytest.mark.asyncio
async def test_failure_closes_work_and_allows_next_run(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)
    command = CreateRunCommand(
        seed.principal,
        seed.agent_version_id,
        {"candidate_model_ref": "a", "evaluation_suite_ref": "b"},
    )
    accepted = await repo.accept_run(command=command, idempotency_key="failed")
    work = await repo.claim_work()
    assert work is not None and work.run_id == accepted.run.id
    await repo.fail_work(work, "DEADLINE_EXCEEDED", "Execution timed out", timed_out=True)
    assert (await repo.get_run(seed.principal, accepted.run.id)).state == "TIMED_OUT"
    with pytest.raises(RuntimeConflict):
        await repo.fail_work(work, "SECOND_FAILURE", "No second transition")
    next_run = await repo.accept_run(command=command, idempotency_key="next")
    next_work = await repo.claim_work()
    assert next_work is not None and next_work.run_id == next_run.run.id
    await repo.fail_work(next_work, "TEST_FINISHED", "Test cleanup")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["approval_policy", "execution_policy", "compensation_policy"])
async def test_unsupported_policy_fails_closed(admin_engine, runtime_engine, field):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    agent_id = uuid4().hex
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO agent_versions
                (id,tenant_id,project_id,definition_id,version,digest,spec)
            SELECT :id,tenant_id,project_id,definition_id,2,'test-digest',
                spec || CAST(:policy AS jsonb) FROM agent_versions WHERE id=:old
        """),
            {
                "id": agent_id,
                "old": seed.agent_version_id,
                "policy": json.dumps({field: {"required": True}}),
            },
        )
    repo = PostgresRunRepository(runtime_engine)
    with pytest.raises(PolicyDenied):
        await repo.accept_run(
            command=CreateRunCommand(
                seed.principal, agent_id, {"candidate_model_ref": "a", "evaluation_suite_ref": "b"}
            ),
            idempotency_key="policy",
        )
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_revoked_job_does_not_hide_next_healthy_job(admin_engine, runtime_engine):
    revoked = await seed_example(admin_engine, tenant_id=uuid4().hex)
    healthy = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)
    payload = {"candidate_model_ref": "a", "evaluation_suite_ref": "b"}
    rejected = await repo.accept_run(
        command=CreateRunCommand(revoked.principal, revoked.agent_version_id, payload),
        idempotency_key="first",
    )
    accepted = await repo.accept_run(
        command=CreateRunCommand(healthy.principal, healthy.agent_version_id, payload),
        idempotency_key="second",
    )
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE tenant_id=:tenant"),
            {"tenant": revoked.principal.tenant_id},
        )
    work = await repo.claim_work()
    assert work is not None and work.run_id == accepted.run.id
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT state FROM runs WHERE id=:id"), {"id": rejected.run.id})
        ).scalar_one() == "FAILED"
    await repo.fail_work(work, "TEST_FINISHED", "Test complete")
    assert await repo.claim_work() is None


@pytest.mark.asyncio
async def test_acceptance_event_failure_rolls_back_every_record(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)

    def fail_event_insert(connection, cursor, statement, parameters, context, executemany):
        if "INSERT INTO run_events" in statement:
            raise RuntimeError("injected event insert failure")

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", fail_event_insert)
    try:
        with pytest.raises(RuntimeError, match="injected event insert failure"):
            await repo.accept_run(
                command=CreateRunCommand(
                    seed.principal,
                    seed.agent_version_id,
                    {"candidate_model_ref": "a", "evaluation_suite_ref": "b"},
                ),
                idempotency_key="rollback",
            )
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", fail_event_insert)
    async with admin_engine.connect() as conn:
        for relation in ("runs", "run_steps", "work_items", "run_events", "idempotency_records"):
            assert (await conn.execute(text(f"SELECT count(*) FROM {relation}"))).scalar_one() == 0
