import asyncio
from uuid import uuid4

import pytest
from alembic import command as migration_command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from test_recovery import accepted
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    RuntimeConflict,
)
from agent_platform.application.ports import CreateRunCommand, PrincipalContext


async def exhausted(admin_engine, repo):
    seed, command, run = await accepted(admin_engine, repo)
    work = await repo.claim_work()
    await repo.retry_work(work, "TRANSIENT", "Temporary mock failure")
    return seed, command, run, work


@pytest.mark.asyncio
async def test_redrive_concurrent_duplicate_creates_one_child(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, command, run, _ = await exhausted(admin_engine, repo)
    items = await repo.list_dead_letters(seed.principal, run.run.id)
    assert len(items) == 1 and items[0].reason_code == "RETRY_EXHAUSTED"
    children = await asyncio.gather(
        *[
            repo.redrive_dead_letter(
                seed.principal,
                run.run.id,
                items[0].id,
                idempotency_key="redrive-1",
                reason="WORKER_RECOVERED",
            )
            for _ in range(20)
        ]
    )
    assert len({child.run.id for child in children}) == 1
    assert sum(not child.duplicate for child in children) == 1
    child = children[0].run
    assert child.id != run.run.id and child.state == "QUEUED"
    assert child.input == command.input and child.agent_version_id == command.agent_version_id
    assert (await repo.get_run(seed.principal, run.run.id)).state == "FAILED"
    assert (await repo.list_dead_letters(seed.principal, run.run.id))[0].redriven_run_id == child.id
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_redrives"))
        ).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 2
    next_work = await repo.claim_work()
    assert next_work.run_id == child.id and next_work.attempt_no == 1


@pytest.mark.asyncio
async def test_redrive_preserves_history_and_uses_requestor_identity(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run, _ = await exhausted(admin_engine, repo)
    operator = await seed_example(
        admin_engine, tenant_id=seed.principal.tenant_id, principal_id="operator"
    )
    items = await repo.list_dead_letters(operator.principal, run.run.id)
    async with admin_engine.connect() as conn:
        original = {}
        for relation in (
            "run_steps",
            "work_items",
            "run_attempts",
            "model_calls",
            "dead_letter_items",
        ):
            original[relation] = (
                (
                    await conn.execute(
                        text(f"SELECT to_jsonb(t) FROM {relation} t WHERE run_id=:run ORDER BY id"),
                        {"run": run.run.id},
                    )
                )
                .scalars()
                .all()
            )
    before = await repo.get_run(seed.principal, run.run.id)
    child = await repo.redrive_dead_letter(
        operator.principal,
        run.run.id,
        items[0].id,
        idempotency_key="operator-recovery",
        reason="TRANSIENT_FAILURE_RESOLVED",
    )
    assert child.run.principal_id == operator.principal.principal_id
    assert child.run.principal_id != seed.principal.principal_id
    after = await repo.get_run(seed.principal, run.run.id)
    assert (
        after.state == before.state
        and after.error == before.error
        and after.result == before.result
    )
    assert after.state_version == before.state_version + 1
    async with admin_engine.connect() as conn:
        for relation, rows in original.items():
            assert (
                await conn.execute(
                    text(f"SELECT to_jsonb(t) FROM {relation} t WHERE run_id=:run ORDER BY id"),
                    {"run": run.run.id},
                )
            ).scalars().all() == rows
    source_events = await repo.list_events(seed.principal, run.run.id)
    child_events = await repo.list_events(operator.principal, child.run.id)
    assert source_events[-1].type == "DEAD_LETTER_REDRIVEN"
    assert child_events[-1].type == "RUN_REDRIVEN"
    assert source_events[-1].payload == {**child_events[-1].payload, "state": "FAILED"}
    assert source_events[-1].actor == operator.principal.principal_id


@pytest.mark.asyncio
async def test_redrive_conflicts_and_acceptance_namespace_are_separate(
    admin_engine, runtime_engine
):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, command, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    # The original normal acceptance key is "recovery". Redrive must not return it.
    child = await repo.redrive_dead_letter(
        seed.principal, run.run.id, item.id, idempotency_key="recovery", reason="WORKER_RECOVERED"
    )
    assert child.run.id != run.run.id
    duplicate_accept = await repo.accept_run(command=command, idempotency_key="recovery")
    assert duplicate_accept.run.id == run.run.id and duplicate_accept.duplicate
    for key, reason in (
        ("other-key", "WORKER_RECOVERED"),
        ("recovery", "TRANSIENT_FAILURE_RESOLVED"),
    ):
        with pytest.raises(IdempotencyConflict):
            await repo.redrive_dead_letter(
                seed.principal, run.run.id, item.id, idempotency_key=key, reason=reason
            )
    operator = await seed_example(
        admin_engine, tenant_id=seed.principal.tenant_id, principal_id="operator"
    )
    with pytest.raises(IdempotencyConflict):
        await repo.redrive_dead_letter(
            operator.principal,
            run.run.id,
            item.id,
            idempotency_key="recovery",
            reason="WORKER_RECOVERED",
        )


@pytest.mark.asyncio
async def test_same_redrive_key_different_source_rolls_back_provisional_child(
    admin_engine, runtime_engine
):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, command, first, _ = await exhausted(admin_engine, repo)
    second = await repo.accept_run(command=command, idempotency_key="second-source")
    work = await repo.claim_work()
    await repo.retry_work(work, "TRANSIENT", "Temporary")
    items = [
        (await repo.list_dead_letters(seed.principal, source.run.id))[0]
        for source in (first, second)
    ]
    outcomes = await asyncio.gather(
        *[
            repo.redrive_dead_letter(
                seed.principal,
                source.run.id,
                item.id,
                idempotency_key="same-key",
                reason="WORKER_RECOVERED",
            )
            for source, item in zip((first, second), items, strict=True)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(outcome, IdempotencyConflict) for outcome in outcomes) == 1
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 3
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_redrives"))
        ).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_type", ["DEAD_LETTER_REDRIVEN", "RUN_REDRIVEN"])
async def test_redrive_audit_failure_rolls_back_all_writes(admin_engine, runtime_engine, fail_type):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    before = await repo.get_run(seed.principal, run.run.id)

    def fail_event(connection, cursor, statement, parameters, context, executemany):
        if "INSERT INTO run_events" in statement and parameters.get("type") == fail_type:
            raise RuntimeError("Injected redrive audit failure")

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    try:
        with pytest.raises(RuntimeError, match="Injected redrive"):
            await repo.redrive_dead_letter(
                seed.principal,
                run.run.id,
                item.id,
                idempotency_key="rollback",
                reason="WORKER_RECOVERED",
            )
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", fail_event)
    assert await repo.get_run(seed.principal, run.run.id) == before
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM work_items"))).scalar_one() == 1
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_redrives"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_redrive_scope_revocation_cursor_and_rls(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id, limit=1))[0]
    assert await repo.list_dead_letters(seed.principal, run.run.id, after_id=item.id) == []
    assert await repo.list_dead_letters(seed.principal, run.run.id, after_id="z") == []
    for after, limit in (("x" * 65, 1), ("", 0), ("", 101)):
        with pytest.raises(InvalidInput):
            await repo.list_dead_letters(seed.principal, run.run.id, after_id=after, limit=limit)
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="other-project",
        principal_id="other-user",
    )
    for principal in (PrincipalContext("other-tenant", "other-user"), other.principal):
        with pytest.raises(ExecutionScopeNotFound):
            await repo.list_dead_letters(principal, run.run.id)
        with pytest.raises(ExecutionScopeNotFound):
            await repo.redrive_dead_letter(
                principal, run.run.id, item.id, idempotency_key="other", reason="WORKER_RECOVERED"
            )
    await repo.redrive_dead_letter(
        seed.principal, run.run.id, item.id, idempotency_key="scope", reason="WORKER_RECOVERED"
    )
    async with unit_of_work(runtime_engine, "other-tenant") as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM dead_letter_redrives"))
        ).scalar_one() == 0
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE principal_id=:id"),
            {"id": seed.principal.principal_id},
        )
    with pytest.raises(ExecutionScopeNotFound):
        await repo.redrive_dead_letter(
            seed.principal, run.run.id, item.id, idempotency_key="scope", reason="WORKER_RECOVERED"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE runs SET state='OUTCOME_UNKNOWN'",
        "UPDATE runs SET state='CANCELLED'",
        "UPDATE runs SET state='COMPLETED'",
        "UPDATE runs SET cancel_epoch=1",
        "UPDATE dead_letter_items SET reason_code='VALIDATION_FAILED'",
        "UPDATE work_items SET status='READY'",
        "UPDATE model_calls SET model_route='unsupported/provider'",
    ],
)
async def test_ineligible_history_fails_closed(admin_engine, runtime_engine, mutation):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    async with admin_engine.begin() as conn:
        await conn.execute(text(mutation))
    with pytest.raises(RuntimeConflict):
        await repo.redrive_dead_letter(
            seed.principal, run.run.id, item.id, idempotency_key="denied", reason="WORKER_RECOVERED"
        )
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch", [False, True])
async def test_tool_prepared_failure_can_restart_but_dispatched_never_can(
    admin_engine, runtime_engine, dispatch
):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, command, run = await accepted(admin_engine, repo)
    model = await repo.claim_work()
    await repo.complete_model(
        model, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
    )
    tool = await repo.claim_work()
    if dispatch:
        await repo.begin_tool_dispatch(tool)
    await repo.retry_work(tool, "TRANSIENT", "Temporary")
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    if dispatch:
        with pytest.raises(RuntimeConflict):
            await repo.redrive_dead_letter(
                seed.principal,
                run.run.id,
                item.id,
                idempotency_key="never",
                reason="WORKER_RECOVERED",
            )
        # Even if statuses are made FAILED by a future adapter, historical
        # dispatch evidence must continue to forbid full-run redrive.
        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE runs SET state='FAILED',"
                    "error=jsonb_build_object('code','RETRY_EXHAUSTED')"
                )
            )
            await conn.execute(
                text("UPDATE work_items SET status='FAILED' WHERE id=:id"), {"id": tool.id}
            )
            await conn.execute(text("UPDATE dead_letter_items SET reason_code='RETRY_EXHAUSTED'"))
        with pytest.raises(RuntimeConflict):
            await repo.redrive_dead_letter(
                seed.principal,
                run.run.id,
                item.id,
                idempotency_key="still-never",
                reason="WORKER_RECOVERED",
            )
    else:
        child = await repo.redrive_dead_letter(
            seed.principal, run.run.id, item.id, idempotency_key="safe", reason="WORKER_RECOVERED"
        )
        next_work = await repo.claim_work()
        assert next_work.run_id == child.run.id and next_work.kind == "MODEL_CALL"


@pytest.mark.asyncio
async def test_redrive_unknown_item_invalid_request_and_policy_revalidation(
    admin_engine, runtime_engine
):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    with pytest.raises(ExecutionScopeNotFound):
        await repo.redrive_dead_letter(
            seed.principal,
            run.run.id,
            "unknown-item",
            idempotency_key="valid",
            reason="WORKER_RECOVERED",
        )
    for key, reason in (
        ("", "WORKER_RECOVERED"),
        ("   ", "WORKER_RECOVERED"),
        ("x" * 201, "WORKER_RECOVERED"),
        ("valid", "free text"),
    ):
        with pytest.raises(InvalidInput):
            await repo.redrive_dead_letter(
                seed.principal, run.run.id, item.id, idempotency_key=key, reason=reason
            )
    # A corrupt/legacy stored input must still pass today's acceptance schema.
    async with admin_engine.begin() as conn:
        await conn.execute(text("UPDATE runs SET input='{}'::jsonb"))
    with pytest.raises(InvalidInput):
        await repo.redrive_dead_letter(
            seed.principal, run.run.id, item.id, idempotency_key="valid", reason="WORKER_RECOVERED"
        )
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1


@pytest.mark.asyncio
async def test_redrive_records_are_append_only_and_scope_fenced(admin_engine, runtime_engine):
    repo = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, command, run, _ = await exhausted(admin_engine, repo)
    item = (await repo.list_dead_letters(seed.principal, run.run.id))[0]
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="other-project",
        principal_id="other-user",
    )
    other_run = await repo.accept_run(
        command=CreateRunCommand(other.principal, other.agent_version_id, command.input),
        idempotency_key="other-project-run",
    )
    insertion = text("""
        INSERT INTO dead_letter_redrives
          (id,tenant_id,project_id,source_run_id,source_dead_letter_id,new_run_id,
           principal_id,idempotency_key,reason)
        VALUES(:id,:tenant,:project,:source,:item,:child,:principal,'scope','WORKER_RECOVERED')
    """)
    values = {
        "id": uuid4().hex,
        "tenant": seed.principal.tenant_id,
        "project": seed.project_id,
        "source": run.run.id,
        "item": item.id,
        "child": other_run.run.id,
        "principal": seed.principal.principal_id,
    }
    with pytest.raises(IntegrityError, match="foreign key"):
        async with admin_engine.begin() as conn:
            await conn.execute(insertion, values)
    with pytest.raises(DBAPIError, match="row-level security"):
        async with unit_of_work(runtime_engine, "other-tenant") as conn:
            await conn.execute(insertion, values)
    child = await repo.redrive_dead_letter(
        seed.principal,
        run.run.id,
        item.id,
        idempotency_key="append-only",
        reason="WORKER_RECOVERED",
    )
    for mutation in (
        "UPDATE dead_letter_redrives SET reason='WORKER_RECOVERED'",
        "DELETE FROM dead_letter_redrives",
    ):
        with pytest.raises(DBAPIError, match="permission denied"):
            async with unit_of_work(runtime_engine, seed.principal.tenant_id) as conn:
                await conn.execute(text(mutation))
        with pytest.raises(DBAPIError, match="immutable"):
            async with admin_engine.begin() as conn:
                await conn.execute(text(mutation))
    assert (await repo.list_dead_letters(seed.principal, run.run.id))[
        0
    ].redriven_run_id == child.run.id


@pytest.mark.asyncio
async def test_redrive_migration_preserves_legacy_dlq_and_refuses_history_loss():
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        migration_command.upgrade(config, "0003")
        engine = create_async_engine(url)
        try:
            repo = PostgresRunRepository(engine, max_attempts=1)
            seed, _, run, _ = await exhausted(engine, repo)
            async with engine.connect() as conn:
                before = (
                    await conn.execute(text("SELECT to_jsonb(d) FROM dead_letter_items d"))
                ).scalar_one()
            migration_command.upgrade(config, "0004")
            items = await repo.list_dead_letters(seed.principal, run.run.id)
            assert len(items) == 1 and items[0].id == before["id"]
            migration_command.downgrade(config, "0003")
            migration_command.upgrade(config, "0004")
            await repo.redrive_dead_letter(
                seed.principal,
                run.run.id,
                items[0].id,
                idempotency_key="retain-history",
                reason="WORKER_RECOVERED",
            )
            with pytest.raises(DBAPIError, match="Preserve redrive history"):
                migration_command.downgrade(config, "0003")
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0004"
                assert (
                    await conn.execute(text("SELECT to_jsonb(d) FROM dead_letter_items d"))
                ).scalar_one() == before
                assert (
                    await conn.execute(text("SELECT count(*) FROM dead_letter_redrives"))
                ).scalar_one() == 1
        finally:
            await engine.dispose()
