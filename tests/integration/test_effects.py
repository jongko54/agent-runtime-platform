import asyncio
from dataclasses import replace

import pytest
from alembic import command as migration_command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from test_recovery import accepted, expire, ready
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    InvalidInput,
    PolicyDenied,
    RuntimeConflict,
)
from agent_platform.application.ports import PrincipalContext


async def tool_ready(admin_engine, repo):
    seed, command, run = await accepted(admin_engine, repo)
    model = await repo.claim_work()
    await repo.complete_model(
        model, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
    )
    return seed, command, run, await repo.claim_work()


@pytest.mark.asyncio
async def test_claim_prepares_but_cancel_fences_dispatch(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM tool_effects"))
        ).scalar_one() == "PREPARED"
    cancelled = await repo.cancel_run(seed.principal, run.run.id)
    assert cancelled.state == "CANCELLED" and cancelled.cancellation_outcome == "NO_EFFECT"
    assert await repo.cancel_run(seed.principal, run.run.id) == cancelled
    with pytest.raises(RuntimeConflict):
        await repo.begin_tool_dispatch(work)
    assert not await repo.heartbeat(work)


@pytest.mark.asyncio
async def test_dispatch_cancel_is_unknown_and_stale_worker_fenced(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, command, run, work = await tool_ready(admin_engine, repo)
    effect = await repo.begin_tool_dispatch(work)
    cancelled = await repo.cancel_run(seed.principal, run.run.id)
    assert cancelled.state == "OUTCOME_UNKNOWN" and cancelled.cancel_epoch == 1
    assert await repo.pending_effect(seed.principal, run.run.id) == effect
    result = {**command.input, "decision": "EVALUATED", "quality_score": 0.86, "safety_score": 0.99}
    with pytest.raises(RuntimeConflict):
        await repo.complete_tool(work, result)


@pytest.mark.asyncio
async def test_dispatched_expiry_reuses_effect_key(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    _, _, _, work = await tool_ready(admin_engine, repo)
    old = await repo.begin_tool_dispatch(work)
    await expire(admin_engine, work)
    assert await repo.recover_expired() == 1
    await ready(admin_engine, work)
    new_work = await repo.claim_work()
    new = await repo.begin_tool_dispatch(new_work)
    assert new.idempotency_key == old.idempotency_key
    assert new.dispatch_token != old.dispatch_token
    with pytest.raises(RuntimeConflict):
        await repo.begin_tool_dispatch(work)


@pytest.mark.asyncio
async def test_cancel_dispatch_race_serializes(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    dispatch, cancelled = await asyncio.gather(
        repo.begin_tool_dispatch(work),
        repo.cancel_run(seed.principal, run.run.id),
        return_exceptions=True,
    )
    assert not isinstance(cancelled, Exception)
    if isinstance(dispatch, RuntimeConflict):
        assert cancelled.state == "CANCELLED"
    else:
        assert not isinstance(dispatch, Exception)
        assert cancelled.state == "OUTCOME_UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_reconcile_requires_stored_proof_and_is_idempotent(
    admin_engine, runtime_engine, cancel
):
    repo = PostgresRunRepository(runtime_engine)
    seed, command, run, work = await tool_ready(admin_engine, repo)
    effect = await repo.begin_tool_dispatch(work)
    if cancel:
        await repo.cancel_run(seed.principal, run.run.id)
    else:
        await repo.mark_tool_unknown(work)
    unknown = await repo.get_run(seed.principal, run.run.id)
    provider = PersistentMockEvaluationTool(runtime_engine)
    assert await provider.lookup(effect) is None
    result = {**command.input, "decision": "EVALUATED", "quality_score": 0.86, "safety_score": 0.99}
    with pytest.raises(RuntimeConflict, match="Stored provider result"):
        await repo.reconcile_effect(seed.principal, run.run.id, effect, result)
    assert await repo.get_run(seed.principal, run.run.id) == unknown
    result = await provider.execute(
        tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
    )
    with pytest.raises(RuntimeConflict, match="snapshot"):
        await repo.reconcile_effect(
            seed.principal, run.run.id, replace(effect, dispatch_token="stale"), result
        )
    with pytest.raises(RuntimeConflict, match="Stored provider result"):
        await repo.reconcile_effect(
            seed.principal, run.run.id, effect, {**result, "quality_score": 0.1}
        )
    resolved = await repo.reconcile_effect(seed.principal, run.run.id, effect, result)
    assert resolved.state == ("CANCELLED" if cancel else "COMPLETED")
    assert resolved.cancellation_outcome == ("EFFECT_SUCCEEDED" if cancel else None)
    assert resolved.result == result
    assert await repo.reconcile_effect(seed.principal, run.run.id, effect, result) == resolved
    assert await repo.cancel_run(seed.principal, run.run.id) == resolved
    assert await repo.pending_effect(seed.principal, run.run.id) is None
    async with admin_engine.connect() as conn:
        for relation, count in (
            ("usage_entries", 2),
            ("checkpoints", 2),
            ("mock_provider_results", 1),
        ):
            assert (
                await conn.execute(text(f"SELECT count(*) FROM {relation}"))
            ).scalar_one() == count


@pytest.mark.asyncio
async def test_reconcile_invalid_stored_schema_stays_unknown(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    effect = await repo.begin_tool_dispatch(work)
    provider = PersistentMockEvaluationTool(runtime_engine)
    await provider.execute(
        tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
    )
    await repo.mark_tool_unknown(work)
    async with admin_engine.begin() as conn:
        await conn.execute(text("UPDATE mock_provider_results SET result='{}'::jsonb"))
    with pytest.raises(InvalidInput):
        await repo.reconcile_effect(seed.principal, run.run.id, effect, {})
    assert (await repo.get_run(seed.principal, run.run.id)).state == "OUTCOME_UNKNOWN"


@pytest.mark.asyncio
async def test_cancel_reconcile_scope_and_ledger_rls(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    effect = await repo.begin_tool_dispatch(work)
    await repo.mark_tool_unknown(work)
    other_project = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="other-project",
        principal_id="other-person",
    )
    for principal in (PrincipalContext("other-tenant", "other-person"), other_project.principal):
        for operation in (
            repo.cancel_run(principal, run.run.id),
            repo.pending_effect(principal, run.run.id),
            repo.reconcile_effect(principal, run.run.id, effect, {}),
        ):
            with pytest.raises(ExecutionScopeNotFound):
                await operation
    async with unit_of_work(runtime_engine, "other-tenant") as conn:
        for relation in ("tool_effects", "dead_letter_items", "checkpoints"):
            assert (await conn.execute(text(f"SELECT count(*) FROM {relation}"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_dispatch_rechecks_current_permission(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, _, work = await tool_ready(admin_engine, repo)
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE tenant_id=:tenant"),
            {"tenant": seed.principal.tenant_id},
        )
    with pytest.raises(PolicyDenied):
        await repo.begin_tool_dispatch(work)
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM tool_effects"))
        ).scalar_one() == "PREPARED"


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_provider", [False, True])
async def test_dispatched_exhaustion_or_unsupported_provider_is_unknown(
    admin_engine, runtime_engine, unknown_provider
):
    repo = PostgresRunRepository(runtime_engine, max_attempts=3 if unknown_provider else 1)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    await repo.begin_tool_dispatch(work)
    if unknown_provider:
        async with admin_engine.begin() as conn:
            await conn.execute(text("UPDATE tool_effects SET tool_version='future-provider/v1'"))
    await expire(admin_engine, work)
    assert await repo.recover_expired() == 1
    assert (await repo.get_run(seed.principal, run.run.id)).state == "OUTCOME_UNKNOWN"
    assert await repo.claim_work() is None
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM tool_effects"))
        ).scalar_one() == "OUTCOME_UNKNOWN"
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_items"))
        ).scalar_one() == 1


@pytest.mark.asyncio
async def test_cancel_retries_work_set_after_model_handoff(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, command, run = await accepted(admin_engine, repo)
    model = await repo.claim_work()
    model_waiting, cancel_waiting = asyncio.Event(), asyncio.Event()

    def observe(connection, cursor, statement, parameters, context, executemany):
        if "SELECT id FROM work_items WHERE id=" in statement:
            model_waiting.set()
        if "SELECT id FROM work_items WHERE run_id=" in statement:
            cancel_waiting.set()

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", observe)
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(
                text("SELECT id FROM work_items WHERE id=:id FOR UPDATE"), {"id": model.id}
            )
            completion = asyncio.create_task(
                repo.complete_model(
                    model, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
                )
            )
            await asyncio.wait_for(model_waiting.wait(), 2)
            # Wait for PostgreSQL to register the first waiter, then queue cancel.
            await wait_for_database_lock(conn, "id")
            cancellation = asyncio.create_task(repo.cancel_run(seed.principal, run.run.id))
            await asyncio.wait_for(cancel_waiting.wait(), 2)
            await wait_for_database_lock(conn, "run_id")
        await asyncio.wait_for(completion, 2)
        cancelled = await asyncio.wait_for(cancellation, 2)
        assert cancelled.state == "CANCELLED" and cancelled.cancellation_outcome == "NO_EFFECT"
        async with admin_engine.connect() as conn:
            assert (
                await conn.execute(text("SELECT status FROM tool_effects"))
            ).scalar_one() == "CANCELLED"
            assert (
                await conn.execute(text("SELECT count(*) FROM work_items WHERE status='READY'"))
            ).scalar_one() == 0
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", observe)


async def wait_for_database_lock(conn, field):
    for _ in range(200):
        # PostgreSQL caches statistics snapshots for a transaction; refresh them.
        await conn.execute(text("SELECT pg_stat_clear_snapshot()"))
        waiting = await conn.execute(
            text("""
            SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE :pattern
        """),
            {"pattern": f"%SELECT id FROM work_items WHERE {field}=%"},
        )
        if waiting.first():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Operation did not reach its database lock")


@pytest.mark.asyncio
async def test_effect_attempt_fk_rejects_other_step(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    _, _, _, work = await tool_ready(admin_engine, repo)
    await repo.begin_tool_dispatch(work)
    async with admin_engine.connect() as conn:
        model_attempt = (
            await conn.execute(
                text("""
            SELECT a.id FROM run_attempts a JOIN run_steps s ON s.id=a.step_id
            WHERE s.kind='MODEL_CALL'
        """)
            )
        ).scalar_one()
    with pytest.raises(IntegrityError):
        async with admin_engine.begin() as conn:
            await conn.execute(
                text("UPDATE tool_effects SET dispatch_attempt_id=:attempt"),
                {"attempt": model_attempt},
            )


@pytest.mark.asyncio
async def test_dispatch_history_preserves_replay_tokens(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run, work = await tool_ready(admin_engine, repo)
    first = await repo.begin_tool_dispatch(work)
    await expire(admin_engine, work)
    await repo.recover_expired()
    await ready(admin_engine, work)
    second_work = await repo.claim_work()
    second = await repo.begin_tool_dispatch(second_work)
    events = await repo.list_events(seed.principal, run.run.id)
    dispatches = [e for e in events if e.type == "TOOL_DISPATCHED"]
    assert [e.payload["dispatch_token"] for e in dispatches] == [
        first.dispatch_token,
        second.dispatch_token,
    ]
    assert [e.payload["attempt_id"] for e in dispatches] == [
        work.attempt_id,
        second_work.attempt_id,
    ]
    assert all("arguments" not in e.payload and e.payload["state"] == "RUNNING" for e in dispatches)
    assert [e.sequence for e in events] == list(range(1, len(events) + 1))
    assert (await repo.get_run(seed.principal, run.run.id)).state_version == len(events)


@pytest.mark.asyncio
async def test_migration_active_guard_backfill_and_unresolved_downgrade():
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        migration_command.upgrade(config, "0002")
        engine = create_async_engine(url)
        try:
            repo = PostgresRunRepository(engine)
            seed, command, run = await accepted(engine, repo)
            work = await repo.claim_work()
            with pytest.raises(DBAPIError, match="Drain PROCESSING"):
                migration_command.upgrade(config, "head")
            async with engine.begin() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0002"
                # Restore a queued Tool intent, as produced by the previous runtime.
                await conn.execute(text("DELETE FROM model_calls"))
                await conn.execute(text("DELETE FROM run_attempts"))
                await conn.execute(text("UPDATE run_steps SET kind='TOOL_CALL',state='READY'"))
                await conn.execute(text("UPDATE runs SET state='QUEUED'"))
                await conn.execute(
                    text(
                        "UPDATE work_items SET status='READY',worker_id=NULL,lease_expires_at=NULL"
                    )
                )
                await conn.execute(
                    text("""
                    INSERT INTO tool_calls
                      (id,tenant_id,project_id,run_id,step_id,tool_version_id,status,arguments)
                    SELECT s.id,s.tenant_id,s.project_id,s.run_id,s.id,t.id,'PENDING',s.input
                    FROM run_steps s JOIN tool_versions t ON t.project_id=s.project_id
                """)
                )
            migration_command.upgrade(config, "head")
            async with engine.connect() as conn:
                effect = (await conn.execute(text("SELECT * FROM tool_effects"))).mappings().one()
                assert effect["id"] == work.step_id and effect["status"] == "PREPARED"
                assert effect["request_hash"] == canonical_digest(
                    {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
                )
            tool = await repo.claim_work()
            effect = await repo.begin_tool_dispatch(tool)
            await repo.cancel_run(seed.principal, run.run.id)
            with pytest.raises(DBAPIError, match="resolve effects"):
                migration_command.downgrade(config, "0002")
            provider = PersistentMockEvaluationTool(engine)
            result = await provider.execute(
                tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
            )
            await repo.reconcile_effect(seed.principal, run.run.id, effect, result)
            migration_command.downgrade(config, "0002")
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0002"
        finally:
            await engine.dispose()
