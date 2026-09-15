import asyncio
import json
from uuid import uuid4

import pytest
from alembic import command as migration_command
from alembic.config import Config
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from test_effects import tool_ready
from test_evaluation_candidates import failed_source
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.evaluations import PostgresEvaluationRepository
from agent_platform.adapters.postgres.observations import PostgresObservationRepository
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    RuntimeConflict,
)
from agent_platform.application.evaluation import evaluate_cases
from agent_platform.application.ports import CreateRunCommand, PrincipalContext


async def source_candidate(admin_engine, runtime_engine):
    runs, seed, run = await failed_source(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    candidate = (
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="COMPLETED",
            idempotency_key="draft",
        )
    ).candidate
    return runs, seed, run, candidate


def case_request(candidate):
    curated = {"candidate_model_ref": "mock://curated", "evaluation_suite_ref": "mock://suite"}
    return {
        "source_snapshot_digest": candidate.snapshot_digest,
        "input": curated,
        "expected_decision": {"tool_version": "evaluation.run_suite:v1", "arguments": curated},
        "review_confirmed": True,
        "idempotency_key": "case",
    }


@pytest.mark.asyncio
async def test_curated_case_concurrent_idempotency_preserves_source(admin_engine, runtime_engine):
    runs, seed, run, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    results = await asyncio.gather(
        *[
            repo.create_case(seed.principal, candidate.id, **case_request(candidate))
            for _ in range(12)
        ]
    )
    assert len({result.case.id for result in results}) == 1
    assert sum(not result.duplicate for result in results) == 1
    case = results[0].case
    assert case.input != run.input
    assert case.source_agent_version_id == run.agent_version_id
    assert case.source_snapshot_digest == candidate.snapshot_digest
    assert case.allowed_tools == ("evaluation.run_suite:v1",)
    assert case.review_policy == "explicit-curation-v1"
    assert len(case.content_digest) == 64
    assert await repo.get_case(seed.principal, case.id) == case
    assert await runs.get_run(seed.principal, run.id) == run
    observations = PostgresObservationRepository(runtime_engine)
    assert await observations.get_evaluation_candidate(seed.principal, candidate.id) == candidate
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM evaluation_cases"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"review_confirmed": False},
        {"review_confirmed": 1},
        {"source_snapshot_digest": "bad"},
        {"source_snapshot_digest": "A" * 64},
        {"idempotency_key": ""},
        {"idempotency_key": " \t"},
        {"idempotency_key": "x" * 201},
        {"input": {"candidate_model_ref": "missing required suite"}},
        {"expected_decision": {"tool_version": "unauthorized:v1", "arguments": {}}},
        {
            "expected_decision": {
                "tool_version": "evaluation.run_suite:v1",
                "arguments": {},
                "extra": 1,
            }
        },
    ],
)
async def test_case_validation_rejects_unreviewed_or_invalid_content(
    admin_engine, runtime_engine, overrides
):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    with pytest.raises(InvalidInput):
        await repo.create_case(
            seed.principal, candidate.id, **{**case_request(candidate), **overrides}
        )
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM evaluation_cases"))).scalar_one() == 0


@pytest.mark.asyncio
async def test_case_source_digest_mismatch_and_same_key_content_conflict(
    admin_engine, runtime_engine
):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    params = case_request(candidate)
    with pytest.raises(RuntimeConflict):
        await repo.create_case(
            seed.principal, candidate.id, **{**params, "source_snapshot_digest": "a" * 64}
        )
    accepted = await repo.create_case(seed.principal, candidate.id, **params)
    changed = {
        **params,
        "expected_decision": {
            "tool_version": "evaluation.run_suite:v1",
            "arguments": {"different": True},
        },
    }
    with pytest.raises(IdempotencyConflict):
        await repo.create_case(seed.principal, candidate.id, **changed)
    assert (await repo.create_case(seed.principal, candidate.id, **params)).case == accepted.case


@pytest.mark.asyncio
async def test_different_candidates_racing_same_key_cannot_share_case(admin_engine, runtime_engine):
    _, seed, run, candidate = await source_candidate(admin_engine, runtime_engine)
    observations = PostgresObservationRepository(runtime_engine)
    other = (
        await observations.create_evaluation_candidate(
            seed.principal,
            run.id,
            source_state_version=run.state_version,
            expected_state="FAILED",
            idempotency_key="different-expectation",
        )
    ).candidate
    repo = PostgresEvaluationRepository(runtime_engine)
    results = await asyncio.gather(
        *[
            repo.create_case(seed.principal, source.id, **case_request(source))
            for source in (candidate, other)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(result, IdempotencyConflict) for result in results) == 1
    async with admin_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM evaluation_cases"))).scalar_one() == 1


@pytest.mark.asyncio
async def test_case_permissions_apply_to_reads_creation_and_duplicate(admin_engine, runtime_engine):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    created = await repo.create_case(seed.principal, candidate.id, **case_request(candidate))
    other = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="another",
        principal_id="another",
    )
    for principal in (other.principal, PrincipalContext("other-tenant", "other-user")):
        with pytest.raises(ExecutionScopeNotFound):
            await repo.get_case(principal, created.case.id)
        with pytest.raises(ExecutionScopeNotFound):
            await repo.create_case(principal, candidate.id, **case_request(candidate))
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("UPDATE project_memberships SET status='REVOKED' WHERE principal_id=:id"),
            {"id": seed.principal.principal_id},
        )
    with pytest.raises(ExecutionScopeNotFound):
        await repo.get_case(seed.principal, created.case.id)
    with pytest.raises(ExecutionScopeNotFound):
        await repo.create_case(seed.principal, candidate.id, **case_request(candidate))


@pytest.mark.asyncio
@pytest.mark.parametrize("identifiers", [[], ["same", "same"], [""], [str(i) for i in range(51)]])
async def test_suite_identifiers_are_bounded(runtime_engine, identifiers):
    with pytest.raises(InvalidInput):
        await PostgresEvaluationRepository(runtime_engine).get_cases(
            PrincipalContext("unused", "unused"), identifiers
        )


@pytest.mark.asyncio
async def test_suite_reads_preserve_request_order_and_reject_missing_and_multiple_projects(
    admin_engine, runtime_engine
):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    first = (await repo.create_case(seed.principal, candidate.id, **case_request(candidate))).case
    second = (
        await repo.create_case(
            seed.principal, candidate.id, **{**case_request(candidate), "idempotency_key": "second"}
        )
    ).case
    assert await repo.get_cases(seed.principal, [second.id, first.id]) == [second, first]
    with pytest.raises(ExecutionScopeNotFound):
        await repo.get_cases(seed.principal, [first.id, "missing"])
    # A user with valid membership in both projects still cannot mix a suite's scopes.
    other_seed = await seed_example(
        admin_engine,
        tenant_id=seed.principal.tenant_id,
        project_id="another-project",
        principal_id="another-user",
    )
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO project_memberships(tenant_id,project_id,principal_id,role_set_id,status)
            VALUES(:tenant,:project,:principal,'owner','ACTIVE')
        """),
            {
                "tenant": seed.principal.tenant_id,
                "project": other_seed.project_id,
                "principal": seed.principal.principal_id,
            },
        )
    runs = PostgresRunRepository(runtime_engine, max_attempts=1)
    other_run = await runs.accept_run(
        command=CreateRunCommand(
            other_seed.principal, other_seed.agent_version_id, case_request(candidate)["input"]
        ),
        idempotency_key="other",
    )
    work = await runs.claim_work()
    await runs.fail_work(work, "CLIENT_INVALID", "invalid")
    final = await runs.get_run(other_seed.principal, other_run.run.id)
    other_candidate = (
        await PostgresObservationRepository(runtime_engine).create_evaluation_candidate(
            other_seed.principal,
            final.id,
            source_state_version=final.state_version,
            expected_state="COMPLETED",
            idempotency_key="draft",
        )
    ).candidate
    other_case = (
        await repo.create_case(
            other_seed.principal, other_candidate.id, **case_request(other_candidate)
        )
    ).case
    with pytest.raises(InvalidInput, match="one project"):
        await repo.get_cases(seed.principal, [first.id, other_case.id])


@pytest.mark.asyncio
async def test_unknown_candidate_curation_does_not_execute_or_requeue_tools(
    admin_engine, runtime_engine
):
    runs = PostgresRunRepository(runtime_engine)
    seed, _, accepted, tool = await tool_ready(admin_engine, runs)
    await runs.begin_tool_dispatch(tool)
    await runs.mark_tool_unknown(tool)
    source = await runs.get_run(seed.principal, accepted.run.id)
    candidate = (
        await PostgresObservationRepository(runtime_engine).create_evaluation_candidate(
            seed.principal,
            source.id,
            source_state_version=source.state_version,
            expected_state="COMPLETED",
            idempotency_key="unknown",
        )
    ).candidate
    statements = []

    def observe(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(runtime_engine.sync_engine, "before_cursor_execute", observe)
    try:
        repo = PostgresEvaluationRepository(runtime_engine)
        case = (
            await repo.create_case(seed.principal, candidate.id, **case_request(candidate))
        ).case
        assert await repo.get_case(seed.principal, case.id) == case
    finally:
        event.remove(runtime_engine.sync_engine, "before_cursor_execute", observe)
    assert not any("SELECT * FROM runs" in sql for sql in statements)
    assert not any("UPDATE " in sql or "INSERT INTO work_items" in sql for sql in statements)
    assert await runs.get_run(seed.principal, source.id) == source
    async with admin_engine.connect() as conn:
        assert (
            await conn.execute(text("SELECT status FROM tool_effects"))
        ).scalar_one() == "OUTCOME_UNKNOWN"
        assert (
            await conn.execute(text("SELECT count(*) FROM mock_provider_results"))
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_case_table_rls_scoped_fk_and_immutability(admin_engine, runtime_engine):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    case = (await repo.create_case(seed.principal, candidate.id, **case_request(candidate))).case
    async with unit_of_work(runtime_engine, "other-tenant") as conn:
        assert (await conn.execute(text("SELECT count(*) FROM evaluation_cases"))).scalar_one() == 0
    async with admin_engine.connect() as conn:
        record = dict((await conn.execute(text("SELECT * FROM evaluation_cases"))).mappings().one())
    insertion = text("""
        INSERT INTO evaluation_cases(id,tenant_id,project_id,run_id,candidate_id,principal_id,
          source_agent_version_id,source_snapshot_digest,content_digest,input,expected_decision,
          allowed_tools,review_policy,idempotency_key)
        VALUES(:id,:tenant_id,:project_id,:run_id,:candidate_id,:principal_id,
          :source_agent_version_id,:source_snapshot_digest,:content_digest,
          CAST(:input AS jsonb),CAST(:expected_decision AS jsonb),CAST(:allowed_tools AS jsonb),
          :review_policy,:idempotency_key)
    """)
    record.update(id=uuid4().hex, idempotency_key="direct-insert")
    for field in ("input", "expected_decision", "allowed_tools"):
        record[field] = json.dumps(record[field])
    for overrides in (
        {"source_snapshot_digest": "b" * 64},
        {"run_id": "absent"},
        {"source_agent_version_id": "absent"},
        {"candidate_id": "absent"},
        {"project_id": "absent"},
    ):
        with pytest.raises(IntegrityError, match="foreign key"):
            async with admin_engine.begin() as conn:
                await conn.execute(insertion, {**record, **overrides})
    with pytest.raises(DBAPIError, match="row-level security"):
        async with unit_of_work(runtime_engine, "other-tenant") as conn:
            await conn.execute(insertion, record)
    for mutation in (
        "UPDATE evaluation_cases SET review_policy='explicit-curation-v1'",
        "DELETE FROM evaluation_cases",
    ):
        with pytest.raises(DBAPIError, match="permission denied"):
            async with unit_of_work(runtime_engine, seed.principal.tenant_id) as conn:
                await conn.execute(text(mutation))
        with pytest.raises(DBAPIError, match="immutable"):
            async with admin_engine.begin() as conn:
                await conn.execute(text(mutation))
    assert await repo.get_case(seed.principal, case.id) == case


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["incomplete", "digest", "agent", "run", "version"])
async def test_case_rejects_tampered_candidate_snapshot(admin_engine, runtime_engine, corruption):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    altered = json.loads(json.dumps(candidate.snapshot))
    if corruption == "incomplete":
        altered["integrity"] = {"complete": False, "truncated": False}
    if corruption == "agent":
        altered["run"]["agent_version_id"] = "other-agent"
    if corruption == "run":
        altered["run"]["id"] = "other-run"
    if corruption == "version":
        altered["run"]["state_version"] += 1
    identifier = uuid4().hex
    digest = "a" * 64 if corruption == "digest" else canonical_digest(altered)
    async with admin_engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO evaluation_candidates(id,tenant_id,project_id,run_id,principal_id,
              source_state_version,expected_state,status,snapshot,snapshot_digest,
              redaction_policy,idempotency_key)
            SELECT :id,tenant_id,project_id,run_id,principal_id,source_state_version,expected_state,
              status,CAST(:snapshot AS jsonb),:digest,redaction_policy,'tampered'
            FROM evaluation_candidates WHERE id=:source
        """),
            {
                "id": identifier,
                "snapshot": json.dumps(altered),
                "digest": digest,
                "source": candidate.id,
            },
        )
    with pytest.raises(RuntimeConflict, match="provenance"):
        await PostgresEvaluationRepository(runtime_engine).create_case(
            seed.principal,
            identifier,
            **{**case_request(candidate), "source_snapshot_digest": digest},
        )


@pytest.mark.asyncio
async def test_case_migration_preserves_source_and_refuses_nonempty_downgrade():
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        migration_command.upgrade(config, "0005")
        engine = create_async_engine(url)
        try:
            runs, seed, run, candidate = await source_candidate(engine, engine)
            migration_command.upgrade(config, "head")
            migration_command.downgrade(config, "0005")
            migration_command.upgrade(config, "head")
            repo = PostgresEvaluationRepository(engine)
            case = (
                await repo.create_case(seed.principal, candidate.id, **case_request(candidate))
            ).case
            with pytest.raises(DBAPIError, match="Preserve evaluation cases"):
                migration_command.downgrade(config, "0005")
            assert await repo.get_case(seed.principal, case.id) == case
            assert await runs.get_run(seed.principal, run.id) == run
            async with engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one() == "0006"
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_case_captures_request_before_await_and_keeps_content_digest_consistent(
    admin_engine, runtime_engine, monkeypatch
):
    from agent_platform.adapters.postgres import evaluations

    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    params = case_request(candidate)
    original = dict(params["input"])
    real_digest = evaluations.case_content_digest

    def mutate_caller_after_digest(**values):
        digest = real_digest(**values)
        params["input"]["candidate_model_ref"] = "caller-mutated"
        return digest

    monkeypatch.setattr(evaluations, "case_content_digest", mutate_caller_after_digest)
    case = (
        await PostgresEvaluationRepository(runtime_engine).create_case(
            seed.principal, candidate.id, **params
        )
    ).case
    assert case.input == original
    assert case.content_digest == real_digest(
        candidate_id=case.candidate_id,
        source_snapshot_digest=case.source_snapshot_digest,
        source_agent_version_id=case.source_agent_version_id,
        input=case.input,
        expected_decision=case.expected_decision,
        allowed_tools=case.allowed_tools,
    )


@pytest.mark.asyncio
async def test_case_rejects_unsupported_source_model_route(admin_engine, runtime_engine):
    _, seed, source, _ = await source_candidate(admin_engine, runtime_engine)
    agent_id = uuid4().hex
    async with admin_engine.begin() as conn:
        spec = (
            await conn.execute(
                text("SELECT spec FROM agent_versions WHERE id=:id"), {"id": seed.agent_version_id}
            )
        ).scalar_one()
        spec["model_route"] = "unsupported-provider/model"
        await conn.execute(
            text("""
            INSERT INTO agent_versions(id,tenant_id,project_id,definition_id,version,digest,spec)
            SELECT :new,tenant_id,project_id,definition_id,2,:digest,CAST(:spec AS jsonb)
            FROM agent_versions WHERE id=:old
        """),
            {
                "new": agent_id,
                "old": seed.agent_version_id,
                "digest": canonical_digest(spec),
                "spec": json.dumps(spec),
            },
        )
        # Simulate an imported historical revision. The current acceptance API
        # independently rejects non-mock routes before a Run can be created.
        await conn.execute(
            text("UPDATE runs SET agent_version_id=:agent WHERE id=:run"),
            {"agent": agent_id, "run": source.id},
        )
    candidate = (
        await PostgresObservationRepository(runtime_engine).create_evaluation_candidate(
            seed.principal,
            source.id,
            source_state_version=source.state_version,
            expected_state="COMPLETED",
            idempotency_key="unsupported",
        )
    ).candidate
    with pytest.raises(InvalidInput, match="route"):
        await PostgresEvaluationRepository(runtime_engine).create_case(
            seed.principal, candidate.id, **case_request(candidate)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("number", [1e20, 1e23, -0.0, 1.0, 1.2])
async def test_case_jsonb_numeric_roundtrip_keeps_digest_and_replay_stable(
    admin_engine, runtime_engine, number
):
    _, seed, _, candidate = await source_candidate(admin_engine, runtime_engine)
    repo = PostgresEvaluationRepository(runtime_engine)
    params = {
        **case_request(candidate),
        "expected_decision": {
            "tool_version": "evaluation.run_suite:v1",
            "arguments": {"number": number},
        },
    }
    created = await repo.create_case(seed.principal, candidate.id, **params)
    duplicate = await repo.create_case(seed.principal, candidate.id, **params)
    assert duplicate.duplicate and duplicate.case == created.case
    loaded = await repo.get_case(seed.principal, created.case.id)
    assert loaded == created.case
    report = await evaluate_cases([loaded], MockModelGateway())
    assert report["items"][0]["outcome"] == "MISMATCH"
    if number.is_integer():
        from decimal import Decimal

        equivalent = {
            **params,
            "expected_decision": {
                "tool_version": "evaluation.run_suite:v1",
                "arguments": {"number": int(Decimal(str(number)))},
            },
        }
        numeric_duplicate = await repo.create_case(seed.principal, candidate.id, **equivalent)
        assert numeric_duplicate.duplicate and numeric_duplicate.case == loaded
