import asyncio
from uuid import uuid4

import pytest
from alembic import command as migration_command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.errors import RuntimeConflict
from agent_platform.application.ports import CreateRunCommand


async def accepted(admin_engine, repo, key="recovery"):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    command = CreateRunCommand(
        seed.principal,
        seed.agent_version_id,
        {"candidate_model_ref": "a", "evaluation_suite_ref": "b"},
    )
    run = await repo.accept_run(command=command, idempotency_key=key)
    return seed, command, run


async def expire(admin_engine, work):
    async with admin_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE work_items SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE id=:id"
            ),
            {"id": work.id},
        )


async def ready(admin_engine, work):
    async with admin_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE work_items SET available_at=clock_timestamp()-interval '1 second' "
                "WHERE id=:id"
            ),
            {"id": work.id},
        )


@pytest.mark.asyncio
async def test_expired_owner_fenced_and_new_attempt_recovers(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, worker_id="first")
    other = PostgresRunRepository(runtime_engine, worker_id="second", retry_base_seconds=60)
    seed, command, run = await accepted(admin_engine, repo)
    old = await repo.claim_work()
    assert old.attempt_no == 1 and old.lease_token == 1 and old.worker_id == "first"
    assert await repo.heartbeat(old)
    assert await other.recover_expired() == 0
    await expire(admin_engine, old)
    assert not await repo.heartbeat(old)
    decision = {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
    for operation in (
        repo.complete_model(old, decision),
        repo.fail_work(old, "LATE", "late"),
        repo.retry_work(old, "LATE", "late"),
    ):
        with pytest.raises(RuntimeConflict):
            await operation
    assert sum(await asyncio.gather(*[other.recover_expired() for _ in range(8)])) == 1
    assert await other.claim_work() is None
    await ready(admin_engine, old)
    new = await other.claim_work()
    assert new.id == old.id and new.step_id == old.step_id
    assert new.attempt_no == 2 and new.lease_token > old.lease_token
    assert new.attempt_id != old.attempt_id and new.worker_id == "second"
    with pytest.raises(RuntimeConflict):
        await repo.complete_model(old, decision)
    await other.complete_model(new, decision)
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM model_calls"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM checkpoints"))).scalar_one() == 1
        assert (
            await conn.execute(text("SELECT status FROM run_attempts ORDER BY attempt_no"))
        ).scalars().all() == ["ABANDONED", "SUCCEEDED"]
    events = await repo.list_events(seed.principal, run.run.id)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))


@pytest.mark.asyncio
async def test_retry_delay_exhaustion_and_tenant_rls(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=2, retry_base_seconds=60)
    seed, _, run = await accepted(admin_engine, repo)
    first = await repo.claim_work()
    await repo.retry_work(first, "TRANSIENT", "temporary")
    assert await repo.claim_work() is None
    await ready(admin_engine, first)
    second = await repo.claim_work()
    await repo.retry_work(second, "TRANSIENT", "temporary")
    assert (await repo.get_run(seed.principal, run.run.id)).state == "FAILED"
    assert await repo.claim_work() is None
    async with unit_of_work(runtime_engine, seed.principal.tenant_id) as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_items"))
        ).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM run_attempts"))).scalar_one() == 2
    async with unit_of_work(runtime_engine, "other-tenant") as conn:
        for relation in ("dead_letter_items", "run_attempts", "checkpoints"):
            assert (await conn.execute(text(f"SELECT count(*) FROM {relation}"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_parallel_claims_create_one_attempt_per_work(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    await accepted(admin_engine, repo)
    claims = await asyncio.gather(*[repo.claim_work() for _ in range(15)])
    assert len([work for work in claims if work]) == 1
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM run_attempts"))).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["heartbeat", "complete", "fail", "retry"])
async def test_lease_expiring_while_waiting_for_lock_is_rejected(
    admin_engine, runtime_engine, operation
):
    repo = PostgresRunRepository(runtime_engine)
    _, command, _ = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    blocked = asyncio.Event()

    def before_lock(connection, cursor, statement, parameters, context, executemany):
        if "SELECT id FROM work_items" in statement:
            blocked.set()

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", before_lock)
    try:
        async with admin_engine.begin() as conn:
            await conn.execute(
                text("SELECT id FROM work_items WHERE id=:id FOR UPDATE"), {"id": work.id}
            )
            if operation == "heartbeat":
                task = asyncio.create_task(repo.heartbeat(work))
            elif operation == "complete":
                task = asyncio.create_task(
                    repo.complete_model(
                        work,
                        {"tool_version": "evaluation.run_suite:v1", "arguments": command.input},
                    )
                )
            elif operation == "fail":
                task = asyncio.create_task(repo.fail_work(work, "LATE", "late"))
            else:
                task = asyncio.create_task(repo.retry_work(work, "LATE", "late"))
            await asyncio.wait_for(blocked.wait(), timeout=2)
            await conn.execute(
                text("""
                UPDATE work_items SET lease_expires_at=clock_timestamp()-interval '1 second'
                WHERE id=:id
            """),
                {"id": work.id},
            )
        if operation == "heartbeat":
            assert await asyncio.wait_for(task, timeout=2) is False
        else:
            with pytest.raises(RuntimeConflict):
                await asyncio.wait_for(task, timeout=2)
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", before_lock)


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [True, False])
async def test_unknown_provider_is_quarantined_not_replayed(admin_engine, runtime_engine, expired):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    # Simulate a future provider's dispatch record, without altering published versions.
    async with admin_engine.begin() as conn:
        await conn.execute(text("UPDATE model_calls SET model_route='unknown/provider'"))
    if expired:
        await expire(admin_engine, work)
        assert await repo.recover_expired() == 1
    else:
        await repo.retry_work(work, "TRANSIENT", "temporary")
    assert (await repo.get_run(seed.principal, run.run.id)).state == "OUTCOME_UNKNOWN"
    assert await repo.claim_work() is None
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM work_items"))
        ).scalar_one() == "OUTCOME_UNKNOWN"
        assert (
            await conn.execute(text("SELECT reason_code FROM dead_letter_items"))
        ).scalar_one() == "OUTCOME_UNKNOWN"


@pytest.mark.asyncio
async def test_model_checkpoint_and_next_work_roll_back_on_event_error(
    admin_engine, runtime_engine
):
    repo = PostgresRunRepository(runtime_engine)
    _, command, _ = await accepted(admin_engine, repo)
    work = await repo.claim_work()

    def fail_event(connection, cursor, statement, parameters, context, executemany):
        if "INSERT INTO run_events" in statement:
            raise RuntimeError("injected completion event failure")

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    try:
        with pytest.raises(RuntimeError, match="injected completion"):
            await repo.complete_model(
                work, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
            )
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM checkpoints"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM run_steps"))).scalar_one() == 1
        assert (
            await conn.execute(text("SELECT status FROM run_attempts"))
        ).scalar_one() == "RUNNING"
        assert (
            await conn.execute(text("SELECT status FROM work_items"))
        ).scalar_one() == "PROCESSING"


@pytest.mark.asyncio
async def test_recovery_state_and_attempt_roll_back_on_event_error(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine)
    seed, _, run = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    await expire(admin_engine, work)

    def fail_event(connection, cursor, statement, parameters, context, executemany):
        if "INSERT INTO run_events" in statement:
            raise RuntimeError("injected recovery event failure")

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    try:
        with pytest.raises(RuntimeError, match="injected recovery"):
            await repo.recover_expired()
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    assert (await repo.get_run(seed.principal, run.run.id)).state == "WAITING_MODEL"
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM run_attempts"))
        ).scalar_one() == "RUNNING"
        assert (
            await conn.execute(text("SELECT status FROM work_items"))
        ).scalar_one() == "PROCESSING"
    assert await repo.recover_expired() == 1


@pytest.mark.asyncio
async def test_tool_retry_retains_model_checkpoint_and_one_logical_call(
    admin_engine, runtime_engine
):
    repo = PostgresRunRepository(runtime_engine)
    seed, command, run = await accepted(admin_engine, repo)
    model = await repo.claim_work()
    await repo.complete_model(
        model, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
    )
    tool = await repo.claim_work()
    await expire(admin_engine, tool)
    assert await repo.recover_expired() == 1
    await ready(admin_engine, tool)
    retried = await repo.claim_work()
    assert retried.kind == "TOOL_CALL" and retried.id == tool.id
    assert retried.attempt_no == 2
    result = {**command.input, "decision": "EVALUATED", "quality_score": 0.86, "safety_score": 0.99}
    with pytest.raises(RuntimeConflict):
        await repo.complete_tool(tool, result)
    await repo.begin_tool_dispatch(retried)
    await repo.complete_tool(retried, result)
    assert (await repo.get_run(seed.principal, run.run.id)).state == "COMPLETED"
    async with admin_engine.connect() as conn:
        for relation, count in (
            ("model_calls", 1),
            ("tool_calls", 1),
            ("run_attempts", 3),
            ("checkpoints", 2),
            ("usage_entries", 2),
        ):
            assert (
                await conn.execute(text(f"SELECT count(*) FROM {relation}"))
            ).scalar_one() == count


@pytest.mark.asyncio
async def test_expiry_exhaustion_is_dlq_not_infinite_retry(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    await expire(admin_engine, work)
    assert await repo.recover_expired() == 1
    assert await repo.recover_expired() == 0
    assert (await repo.get_run(seed.principal, run.run.id)).state == "FAILED"
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_items"))
        ).scalar_one() == 1
        assert (
            await conn.execute(text("SELECT status FROM run_attempts"))
        ).scalar_one() == "ABANDONED"


@pytest.mark.asyncio
async def test_migration_requires_active_work_to_be_drained():
    # Explicit second disposable database, never the ambient development URL.
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        migration_command.upgrade(config, "0001")
        engine = create_async_engine(url)
        try:
            repo = PostgresRunRepository(engine)
            await accepted(engine, repo)
            async with engine.begin() as conn:
                await conn.execute(text("UPDATE work_items SET status='PROCESSING'"))
            with pytest.raises(DBAPIError, match="Drain Phase 1 PROCESSING"):
                migration_command.upgrade(config, "head")
            async with engine.begin() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0001"
                await conn.execute(text("UPDATE work_items SET status='READY'"))
            migration_command.upgrade(config, "head")
            work = await repo.claim_work()
            with pytest.raises(DBAPIError, match="Drain PROCESSING"):
                migration_command.downgrade(config, "0001")
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0004"
                assert (
                    await conn.execute(text("SELECT status FROM run_attempts"))
                ).scalar_one() == "RUNNING"
            await repo.fail_work(work, "DRAIN", "Drain disposable test work")
            migration_command.downgrade(config, "0001")
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0001"
        finally:
            await engine.dispose()
