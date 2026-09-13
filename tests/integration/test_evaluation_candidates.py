import asyncio
import json
from uuid import uuid4

import pytest
from alembic import command as migration_command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from test_effects import tool_ready, wait_for_database_lock
from test_recovery import accepted
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.postgres import observations as observations_module
from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.observations import PostgresObservationRepository
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    RuntimeConflict,
)
from agent_platform.application.ports import CreateRunCommand, PrincipalContext


async def failed_source(admin_engine, runtime_engine):
    runs = PostgresRunRepository(runtime_engine, max_attempts=1)
    seed, _, accepted_run = await accepted(admin_engine, runs)
    work = await runs.claim_work()
    await runs.retry_work(work, "TRANSIENT", "Secret error message must not be copied")
    run = await runs.get_run(seed.principal, accepted_run.run.id)
    return runs, seed, run


@pytest.mark.asyncio
async def test_concurrent_candidates_freeze_one_draft_without_mutating_run(
    admin_engine, runtime_engine
):
    runs, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    results = await asyncio.gather(
        *[
            observations.create_evaluation_candidate(
                seed.principal,
                run.id,
                source_state_version=run.state_version,
                expected_state="COMPLETED",
                idempotency_key="case-1",
            )
            for _ in range(20)
        ]
    )
    assert len({result.candidate.id for result in results}) == 1
    assert sum(not result.duplicate for result in results) == 1
    candidate = results[0].candidate
    assert candidate.status == "DRAFT" and candidate.source_state_version == run.state_version
    assert candidate.snapshot["run"]["id"] == run.id
    assert candidate.snapshot_digest == canonical_digest(candidate.snapshot)
    assert candidate.redaction_policy == "metadata-only-v1"
    assert await runs.get_run(seed.principal, run.id) == run
    assert await observations.get_evaluation_candidate(seed.principal, candidate.id) == candidate
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
        ).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1
        assert (
            await conn.execute(text("SELECT count(*) FROM work_items WHERE status='READY'"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_candidates_conflict_for_different_source_same_scoped_key(
    admin_engine, runtime_engine
):
    runs, seed, first = await failed_source(admin_engine, runtime_engine)
    second_accepted = await runs.accept_run(
        command=CreateRunCommand(seed.principal, first.agent_version_id, first.input),
        idempotency_key="second-source",
    )
    work = await runs.claim_work()
    await runs.retry_work(work, "TRANSIENT", "Temporary")
    second = await runs.get_run(seed.principal, second_accepted.run.id)
    observations = PostgresObservationRepository(runtime_engine)
    results = await asyncio.gather(
        *[
            observations.create_evaluation_candidate(
                seed.principal,
                source.id,
                source_state_version=source.state_version,
                expected_state="COMPLETED",
                idempotency_key="same-key",
            )
            for source in (first, second)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(result, IdempotencyConflict) for result in results) == 1
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
        ).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 2


@pytest.mark.asyncio
async def test_candidate_key_conflicting_request_and_acceptance_namespace(
    admin_engine, runtime_engine
):
    runs, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    candidate = await observations.create_evaluation_candidate(
        seed.principal,
        run.id,
        source_state_version=run.state_version,
        expected_state="COMPLETED",
        idempotency_key="recovery",
    )
    original = await runs.accept_run(
        command=CreateRunCommand(seed.principal, run.agent_version_id, run.input),
        idempotency_key="recovery",
    )
    assert original.duplicate and original.run.id == run.id
    for version, expected in ((run.state_version, "FAILED"), (run.state_version + 1, "COMPLETED")):
        with pytest.raises(IdempotencyConflict):
            await observations.create_evaluation_candidate(
                seed.principal,
                run.id,
                source_state_version=version,
                expected_state=expected,
                idempotency_key="recovery",
            )
    assert (
        await observations.get_evaluation_candidate(seed.principal, candidate.candidate.id)
    ).id == candidate.candidate.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"idempotency_key": ""},
        {"idempotency_key": "  "},
        {"idempotency_key": "x" * 201},
        {"source_state_version": 0},
        {"source_state_version": True},
        {"expected_state": "OUTCOME_UNKNOWN"},
    ],
)
async def test_candidate_request_validation(admin_engine, runtime_engine, overrides):
    _, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    params = {
        "source_state_version": run.state_version,
        "expected_state": "COMPLETED",
        "idempotency_key": "valid",
        **overrides,
    }
    with pytest.raises(InvalidInput):
        await observations.create_evaluation_candidate(seed.principal, run.id, **params)


@pytest.mark.asyncio
async def test_active_run_and_stale_version_do_not_become_candidates(admin_engine, runtime_engine):
    runs = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run = await accepted(admin_engine, runs)
    observations = PostgresObservationRepository(runtime_engine)
    for source in (accepted_run.run,):
        with pytest.raises(RuntimeConflict):
            await observations.create_evaluation_candidate(
                seed.principal,
                source.id,
                source_state_version=source.state_version,
                expected_state="COMPLETED",
                idempotency_key="queued",
            )
    work = await runs.claim_work()
    waiting = await runs.get_run(seed.principal, accepted_run.run.id)
    with pytest.raises(RuntimeConflict):
        await observations.create_evaluation_candidate(
            seed.principal,
            waiting.id,
            source_state_version=waiting.state_version,
            expected_state="COMPLETED",
            idempotency_key="waiting",
        )
    await runs.fail_work(work, "INVALID_INPUT", "No retry")
    failed = await runs.get_run(seed.principal, waiting.id)
    with pytest.raises(RuntimeConflict, match="version"):
        await observations.create_evaluation_candidate(
            seed.principal,
            failed.id,
            source_state_version=waiting.state_version,
            expected_state="COMPLETED",
            idempotency_key="stale",
        )
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_unknown_snapshot_stays_fixed_after_reconciliation(admin_engine, runtime_engine):
    runs = PostgresRunRepository(runtime_engine)
    seed, command, accepted_run = await accepted(admin_engine, runs)
    model = await runs.claim_work()
    await runs.complete_model(
        model, {"tool_version": "evaluation.run_suite:v1", "arguments": command.input}
    )
    tool = await runs.claim_work()
    effect = await runs.begin_tool_dispatch(tool)
    provider = PersistentMockEvaluationTool(runtime_engine)
    result = await provider.execute(
        tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
    )
    await runs.mark_tool_unknown(tool)
    unknown = await runs.get_run(seed.principal, accepted_run.run.id)
    observations = PostgresObservationRepository(runtime_engine)
    accepted_candidate = await observations.create_evaluation_candidate(
        seed.principal,
        unknown.id,
        source_state_version=unknown.state_version,
        expected_state="COMPLETED",
        idempotency_key="before-reconcile",
    )
    stored = accepted_candidate.candidate
    resolved = await runs.reconcile_effect(seed.principal, unknown.id, effect, result)
    assert resolved.state == "COMPLETED" and resolved.state_version > unknown.state_version
    duplicate = await observations.create_evaluation_candidate(
        seed.principal,
        unknown.id,
        source_state_version=unknown.state_version,
        expected_state="COMPLETED",
        idempotency_key="before-reconcile",
    )
    assert duplicate.duplicate and duplicate.candidate == stored
    assert stored.snapshot["run"]["state"] == "OUTCOME_UNKNOWN"
    assert await observations.get_evaluation_candidate(seed.principal, stored.id) == stored
    with pytest.raises(RuntimeConflict, match="version"):
        await observations.create_evaluation_candidate(
            seed.principal,
            unknown.id,
            source_state_version=unknown.state_version,
            expected_state="COMPLETED",
            idempotency_key="new-key-old-version",
        )
    current = await observations.create_evaluation_candidate(
        seed.principal,
        resolved.id,
        source_state_version=resolved.state_version,
        expected_state="COMPLETED",
        idempotency_key="new-version",
    )
    assert current.candidate.snapshot["run"]["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_candidate_snapshot_excludes_raw_payloads_and_free_strings(
    admin_engine, runtime_engine
):
    canary = "SECRET-CANDIDATE-CANARY-DO-NOT-COPY"
    runs = PostgresRunRepository(runtime_engine, worker_id=canary)
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    accepted_run = await runs.accept_run(
        command=CreateRunCommand(
            seed.principal,
            seed.agent_version_id,
            {"candidate_model_ref": canary, "evaluation_suite_ref": canary},
        ),
        idempotency_key=canary,
    )
    model = await runs.claim_work()
    await runs.fail_work(model, canary, canary)
    async with admin_engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE model_calls SET model_route=CAST(:secret AS text),"
                "response=jsonb_build_object('secret',CAST(:secret AS text))"
            ),
            {"secret": canary},
        )
        await conn.execute(
            text(
                "UPDATE run_events SET actor=CAST(:secret AS text),"
                "payload=jsonb_build_object('secret',CAST(:secret AS text))"
            ),
            {"secret": canary},
        )
    source = await runs.get_run(seed.principal, accepted_run.run.id)
    observations = PostgresObservationRepository(runtime_engine)
    candidate = (
        await observations.create_evaluation_candidate(
            seed.principal,
            source.id,
            source_state_version=source.state_version,
            expected_state="COMPLETED",
            idempotency_key="safe-key",
        )
    ).candidate
    assert canary not in json.dumps(candidate.snapshot)
    assert candidate.snapshot_digest == canonical_digest(candidate.snapshot)


@pytest.mark.asyncio
async def test_candidate_scope_and_revoked_access_apply_to_duplicates(admin_engine, runtime_engine):
    _, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    candidate = (
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="COMPLETED",
            idempotency_key="scope",
        )
    ).candidate
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="other-project",
        principal_id="other-user",
    )
    for principal in (PrincipalContext("other-tenant", "other-user"), other.principal):
        with pytest.raises(ExecutionScopeNotFound):
            await observations.get_evaluation_candidate(principal, candidate.id)
        with pytest.raises(ExecutionScopeNotFound):
            await observations.create_evaluation_candidate(
                principal,
                run.id,
                source_state_version=run.state_version,
                expected_state="COMPLETED",
                idempotency_key="scope",
            )
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE principal_id=:id"),
            {"id": seed.principal.principal_id},
        )
    with pytest.raises(ExecutionScopeNotFound):
        await observations.get_evaluation_candidate(seed.principal, candidate.id)
    with pytest.raises(ExecutionScopeNotFound):
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="COMPLETED",
            idempotency_key="scope",
        )


@pytest.mark.asyncio
async def test_incomplete_trace_cannot_be_persisted(admin_engine, runtime_engine):
    _, seed, run = await failed_source(admin_engine, runtime_engine)
    async with admin_engine.begin() as conn:
        await conn.execute(text("DELETE FROM run_events WHERE sequence=1"))
    observations = PostgresObservationRepository(runtime_engine)
    with pytest.raises(RuntimeConflict, match="Incomplete"):
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="COMPLETED",
            idempotency_key="gap",
        )
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_truncated_trace_cannot_be_persisted(admin_engine, runtime_engine):
    runs, seed, run = await failed_source(admin_engine, runtime_engine)
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO run_events
              (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
            SELECT :tenant,:project,:run,sequence,'WORK_RETRY_SCHEDULED',1,'worker','{}'::jsonb
            FROM generate_series(CAST(:first AS bigint),CAST(:last AS bigint)) AS sequence
        """),
            {
                "tenant": seed.principal.tenant_id,
                "project": seed.project_id,
                "run": run.id,
                "first": run.state_version + 1,
                "last": run.state_version + 1001,
            },
        )
        await conn.execute(
            text("UPDATE runs SET state_version=state_version+1001 WHERE id=:run"), {"run": run.id}
        )
    source = await runs.get_run(seed.principal, run.id)
    observations = PostgresObservationRepository(runtime_engine)
    with pytest.raises(RuntimeConflict, match="Incomplete"):
        await observations.create_evaluation_candidate(
            seed.principal,
            source.id,
            source_state_version=source.state_version,
            expected_state="COMPLETED",
            idempotency_key="too-large",
        )


@pytest.mark.asyncio
async def test_candidate_table_rls_scoped_fk_and_immutable_draft(admin_engine, runtime_engine):
    _, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    candidate = (
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="COMPLETED",
            idempotency_key="schema",
        )
    ).candidate
    async with unit_of_work(runtime_engine, "other-tenant") as conn:
        assert (
            await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
        ).scalar_one() == 0
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="other-project",
        principal_id="other-user",
    )
    insertion = text("""
        INSERT INTO evaluation_candidates(id,tenant_id,project_id,run_id,principal_id,
          source_state_version,expected_state,status,snapshot,snapshot_digest,redaction_policy,idempotency_key)
        VALUES(:id,:tenant,:project,:run,:principal,1,'COMPLETED',:status,'{}'::jsonb,
          :digest,'metadata-only-v1','invalid-row')
    """)
    values = {
        "id": uuid4().hex,
        "tenant": seed.principal.tenant_id,
        "project": other.project_id,
        "run": run.id,
        "principal": seed.principal.principal_id,
        "status": "DRAFT",
        "digest": "a" * 64,
    }
    with pytest.raises(IntegrityError, match="foreign key"):
        async with admin_engine.begin() as conn:
            await conn.execute(insertion, values)
    with pytest.raises(DBAPIError, match="row-level security"):
        async with unit_of_work(runtime_engine, "other-tenant") as conn:
            await conn.execute(insertion, values)
    with pytest.raises(IntegrityError, match="check constraint"):
        async with admin_engine.begin() as conn:
            await conn.execute(
                insertion, {**values, "project": seed.project_id, "status": "APPROVED"}
            )
    for mutation in (
        "UPDATE evaluation_candidates SET expected_state='FAILED'",
        "DELETE FROM evaluation_candidates",
    ):
        with pytest.raises(DBAPIError, match="permission denied"):
            async with unit_of_work(runtime_engine, seed.principal.tenant_id) as conn:
                await conn.execute(text(mutation))
        with pytest.raises(DBAPIError, match="immutable"):
            async with admin_engine.begin() as conn:
                await conn.execute(text(mutation))
    assert await observations.get_evaluation_candidate(seed.principal, candidate.id) == candidate


@pytest.mark.asyncio
async def test_candidate_migration_preserves_source_and_refuses_nonempty_downgrade():
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        migration_command.upgrade(config, "0004")
        engine = create_async_engine(url)
        try:
            runs, seed, run = await failed_source(engine, engine)
            migration_command.upgrade(config, "head")
            migration_command.downgrade(config, "0004")
            migration_command.upgrade(config, "head")
            observations = PostgresObservationRepository(engine)
            await observations.create_evaluation_candidate(
                seed.principal,
                run.id,
                source_state_version=run.state_version,
                expected_state="COMPLETED",
                idempotency_key="preserve",
            )
            assert await runs.get_run(seed.principal, run.id) == run
            with pytest.raises(DBAPIError, match="Preserve evaluation candidates"):
                migration_command.downgrade(config, "0004")
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0005"
                assert (
                    await conn.execute(text("SELECT count(*) FROM evaluation_candidates"))
                ).scalar_one() == 1
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_candidate_creation_serializes_with_reconciliation(
    admin_engine, runtime_engine, monkeypatch
):
    runs = PostgresRunRepository(runtime_engine)
    seed, _, accepted_run, tool = await tool_ready(admin_engine, runs)
    effect = await runs.begin_tool_dispatch(tool)
    result = await PersistentMockEvaluationTool(runtime_engine).execute(
        tool_version=effect.tool_version, arguments=effect.arguments, effect=effect
    )
    await runs.mark_tool_unknown(tool)
    source = await runs.get_run(seed.principal, accepted_run.run.id)
    reached, release = asyncio.Event(), asyncio.Event()
    candidate_backend = []
    original = observations_module.load_trace

    async def paused(conn, *args, **kwargs):
        # Keep the real projection and real transaction; pause only at the test barrier.
        snapshot = await original(conn, *args, **kwargs)
        assert (
            await conn.execute(text("SHOW transaction_isolation"))
        ).scalar_one() == "read committed"
        candidate_backend.append((await conn.execute(text("SELECT pg_backend_pid()"))).scalar_one())
        reached.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(observations_module, "load_trace", paused)
    observations = PostgresObservationRepository(runtime_engine)
    creating = asyncio.create_task(
        observations.create_evaluation_candidate(
            seed.principal,
            source.id,
            source_state_version=source.state_version,
            expected_state="COMPLETED",
            idempotency_key="race",
        )
    )
    reconciling = None
    try:
        await asyncio.wait_for(reached.wait(), 2)
        reconciling = asyncio.create_task(
            runs.reconcile_effect(seed.principal, source.id, effect, result)
        )
        async with admin_engine.connect() as conn:
            await wait_for_database_lock(conn, "run_id")
            assert (
                await conn.execute(
                    text("""
                SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock'
                  AND :candidate=ANY(pg_blocking_pids(pid))
            """),
                    {"candidate": candidate_backend[0]},
                )
            ).first()
        assert not reconciling.done()
    finally:
        release.set()
    candidate = (await asyncio.wait_for(creating, 2)).candidate
    assert reconciling is not None
    resolved = await asyncio.wait_for(reconciling, 2)
    assert candidate.snapshot["run"]["state"] == "OUTCOME_UNKNOWN"
    assert candidate.snapshot["effects"][0]["status"] == "OUTCOME_UNKNOWN"
    assert candidate.source_state_version == source.state_version
    assert resolved.state == "COMPLETED" and resolved.state_version > source.state_version
    assert await observations.get_evaluation_candidate(seed.principal, candidate.id) == candidate
