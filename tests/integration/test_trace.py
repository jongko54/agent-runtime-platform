import asyncio
import json

import pytest
from sqlalchemy import text
from test_effects import tool_ready
from test_recovery import accepted, expire, ready

from agent_platform.adapters.postgres import trace as trace_module
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.postgres.trace import read_trace
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.errors import ExecutionScopeNotFound
from agent_platform.application.ports import PrincipalContext


@pytest.mark.asyncio
async def test_completed_trace_links_metadata_without_raw_content(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, work = await tool_ready(admin_engine, repo)
    effect = await repo.begin_tool_dispatch(work)
    result = await PersistentMockEvaluationTool(runtime_engine).execute(
        tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
    )
    await repo.complete_tool(work, result)
    canary = "SECRET-trace-must-never-export-this"
    async with admin_engine.begin() as conn:
        for relation, column in (
            ("runs", "input"),
            ("runs", "result"),
            ("runs", "error"),
            ("run_steps", "input"),
            ("run_steps", "output"),
            ("model_calls", "response"),
            ("tool_calls", "arguments"),
            ("tool_calls", "result"),
            ("tool_effects", "arguments"),
            ("tool_effects", "result"),
            ("run_events", "payload"),
        ):
            await conn.execute(
                text(f"UPDATE {relation} SET {column}=CAST(:secret AS jsonb)"),
                {"secret": json.dumps({"code": canary, "message": canary})},
            )
        await conn.execute(text("UPDATE run_events SET actor=:secret"), {"secret": canary})
        await conn.execute(
            text("UPDATE run_attempts SET worker_id=:secret,error_code=:secret"), {"secret": canary}
        )
        await conn.execute(text("UPDATE model_calls SET model_route=:secret"), {"secret": canary})
        await conn.execute(text("UPDATE checkpoints SET result_ref=:secret"), {"secret": canary})
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert trace["schema_version"] == 1
    assert trace["redaction_policy"] == "metadata-only-v1"
    assert canary not in json.dumps(trace)
    assert trace["run"]["state"] == "COMPLETED"
    assert trace["run"]["error_code"] == "UNCLASSIFIED"
    assert trace["model_calls"][0]["model_route"] == "UNREGISTERED"
    assert len(trace["steps"]) == len(trace["attempts"]) == len(trace["checkpoints"]) == 2
    assert trace["effects"][0]["dispatch_attempt_id"] == work.attempt_id
    assert trace["tool_calls"][0]["schema_digest"]
    assert all(a["duration_ms"] >= 0 for a in trace["attempts"])
    assert all(u["quantity"] == 1 and u["unit"] == "call" for u in trace["usage"])
    assert trace["metrics"]["tokens"] is None and trace["metrics"]["cost"] is None
    assert trace["integrity"] == {"complete": True, "truncated": False, "issues": []}


@pytest.mark.asyncio
async def test_trace_preserves_recovery_attempts_and_unknown_effect(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, work = await tool_ready(admin_engine, repo)
    await repo.begin_tool_dispatch(work)
    await expire(admin_engine, work)
    await repo.recover_expired()
    await ready(admin_engine, work)
    replacement = await repo.claim_work()
    await repo.begin_tool_dispatch(replacement)
    await repo.mark_tool_unknown(replacement)
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert trace["run"]["state"] == "OUTCOME_UNKNOWN"
    assert trace["run"]["failure_category"] == "OUTCOME_UNKNOWN"
    assert len(trace["attempts"]) == 3
    assert trace["effects"][0]["dispatch_attempt_id"] == replacement.attempt_id
    assert trace["effects"][0]["status"] == "OUTCOME_UNKNOWN"
    assert len([e for e in trace["events"] if e["type"] == "TOOL_DISPATCHED"]) == 2
    assert trace["integrity"]["complete"]


@pytest.mark.asyncio
async def test_trace_checks_tenant_project_and_revoked_permission(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run = await accepted(admin_engine, repo)
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="different",
        principal_id="different",
    )
    for principal in (PrincipalContext("other-tenant", "other-person"), other.principal):
        with pytest.raises(ExecutionScopeNotFound):
            await read_trace(runtime_engine, principal, accepted_run.run.id)
    async with admin_engine.begin() as conn:
        await conn.execute(text("UPDATE project_memberships SET status='REVOKED'"))
    with pytest.raises(ExecutionScopeNotFound):
        await read_trace(runtime_engine, seed.principal, accepted_run.run.id)


@pytest.mark.asyncio
async def test_trace_detects_event_gap_and_sequence_mismatch(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run = await accepted(admin_engine, repo)
    await repo.claim_work()
    async with admin_engine.begin() as conn:
        await conn.execute(text("DELETE FROM run_events WHERE sequence=2"))
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert not trace["integrity"]["complete"]
    assert "EVENT_SEQUENCE_GAP" in trace["integrity"]["issues"]


@pytest.mark.asyncio
async def test_trace_caps_collections_and_marks_incomplete(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run = await accepted(admin_engine, repo)
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO run_events
              (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
            SELECT tenant_id,project_id,id,n,'WORK_RECOVERED',1,'worker','{}'::jsonb
            FROM runs CROSS JOIN generate_series(2,1001) n
        """)
        )
        await conn.execute(text("UPDATE runs SET state_version=1001"))
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert len(trace["events"]) == 1000
    assert trace["integrity"]["truncated"] and not trace["integrity"]["complete"]
    assert "EVENTS_TRUNCATED" in trace["integrity"]["issues"]


@pytest.mark.asyncio
async def test_trace_uses_one_nonblocking_read_only_snapshot(
    admin_engine, runtime_engine, monkeypatch
):
    repo = PostgresRunRepository(runtime_engine)
    seed, command, accepted_run = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    reached, release = asyncio.Event(), asyncio.Event()
    original = trace_module._collection

    async def paused(conn, *args, **kwargs):
        rows = await original(conn, *args, **kwargs)
        if not reached.is_set():
            assert (await conn.execute(text("SHOW transaction_read_only"))).scalar_one() == "on"
            reached.set()
            await release.wait()
        return rows

    monkeypatch.setattr(trace_module, "_collection", paused)
    pending = asyncio.create_task(read_trace(runtime_engine, seed.principal, accepted_run.run.id))
    try:
        await asyncio.wait_for(reached.wait(), 2)
        await asyncio.wait_for(
            repo.complete_model(
                work, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
            ),
            2,
        )
    finally:
        release.set()
    trace = await pending
    assert trace["run"]["state"] == "WAITING_MODEL"
    assert trace["model_calls"][0]["status"] == "DISPATCHED"
    assert len(trace["steps"]) == 1 and trace["tool_calls"] == []
    assert trace["integrity"]["complete"]


@pytest.mark.asyncio
async def test_unrecognized_metadata_is_redacted_and_flagged(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, _ = await tool_ready(admin_engine, repo)
    canary = "SECRET_UNREGISTERED_METADATA"
    async with admin_engine.begin() as conn:
        for relation, column in (
            ("runs", "state"),
            ("run_steps", "state"),
            ("run_steps", "kind"),
            ("model_calls", "status"),
            ("tool_calls", "status"),
            ("run_events", "type"),
            ("usage_entries", "source"),
            ("usage_entries", "unit"),
        ):
            await conn.execute(text(f"UPDATE {relation} SET {column}=:secret"), {"secret": canary})
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert canary not in json.dumps(trace)
    assert trace["run"]["state"] == "UNREGISTERED"
    assert not trace["integrity"]["complete"]


@pytest.mark.asyncio
async def test_missing_checkpoint_prevents_complete_trace(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, _ = await tool_ready(admin_engine, repo)
    async with admin_engine.begin() as conn:
        await conn.execute(text("DELETE FROM checkpoints"))
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert not trace["integrity"]["complete"]
    assert "SUCCEEDED_STEP_CHECKPOINT_MISSING" in trace["integrity"]["issues"]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_attempt", [1, 2])
async def test_missing_physical_attempt_is_incomplete(
    admin_engine, runtime_engine, missing_attempt
):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run = await accepted(admin_engine, repo)
    first = await repo.claim_work()
    await expire(admin_engine, first)
    await repo.recover_expired()
    await ready(admin_engine, first)
    await repo.claim_work()
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM run_attempts WHERE attempt_no=:number"), {"number": missing_attempt}
        )
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert not trace["integrity"]["complete"]
    assert "WORK_ATTEMPT_COUNT_MISMATCH" in trace["integrity"]["issues"]


@pytest.mark.asyncio
async def test_invalid_fingerprints_are_not_exported(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, _ = await tool_ready(admin_engine, repo)
    canary = "SECRET_invalid_fingerprint"
    async with admin_engine.begin() as conn:
        for relation, trigger, field in (
            ("agent_versions", "agent_version_immutable", "digest"),
            ("tool_versions", "tool_version_immutable", "schema_digest"),
        ):
            # Fault injection only in disposable test DB; production rows are immutable.
            await conn.execute(text(f"ALTER TABLE {relation} DISABLE TRIGGER {trigger}"))
            await conn.execute(text(f"UPDATE {relation} SET {field}=:secret"), {"secret": canary})
            await conn.execute(text(f"ALTER TABLE {relation} ENABLE TRIGGER {trigger}"))
    trace = await read_trace(runtime_engine, seed.principal, accepted_run.run.id)
    assert canary not in json.dumps(trace)
    assert trace["run"]["agent_digest"] is None
    assert trace["tool_calls"][0]["schema_digest"] is None
    assert not trace["integrity"]["complete"]
