from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError, ProgrammingError
from testcontainers.community.postgres import PostgresContainer

from agent_platform.adapters.postgres.database import unit_of_work
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.application.ports import CreateRunCommand


@pytest.mark.asyncio
async def test_rls_default_deny_and_version_immutability(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    async with runtime_engine.connect() as connection:
        role = (
            await connection.execute(
                text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname=current_user")
            )
        ).one()
        assert role == (False, False)
        assert (
            await connection.execute(text("SELECT count(*) FROM agent_versions"))
        ).scalar_one() == 0
    async with unit_of_work(
        runtime_engine, seed.principal.tenant_id, seed.principal.principal_id
    ) as connection:
        assert (
            await connection.execute(text("SELECT count(*) FROM agent_versions"))
        ).scalar_one() == 1
    with pytest.raises(DBAPIError, match="immutable"):
        async with admin_engine.begin() as connection:
            await connection.execute(
                text("UPDATE agent_versions SET version=2 WHERE id=:id"),
                {"id": seed.agent_version_id},
            )


@pytest.mark.asyncio
async def test_cross_project_agent_reference_rejected(admin_engine):
    seed = await seed_example(admin_engine, tenant_id=uuid4().hex)
    project_id = uuid4().hex
    async with admin_engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO projects (id,tenant_id,name,status) "
                "VALUES (:id,:tenant,'other','ACTIVE')"
            ),
            {"id": project_id, "tenant": seed.principal.tenant_id},
        )
    with pytest.raises(IntegrityError):
        async with admin_engine.begin() as connection:
            await connection.execute(
                text("""
                INSERT INTO runs
                    (id,tenant_id,project_id,principal_id,agent_version_id,state,state_version,input)
                VALUES (:id,:tenant,:project,:principal,:agent,'QUEUED',1,'{}')
            """),
                {
                    "id": uuid4().hex,
                    "tenant": seed.principal.tenant_id,
                    "project": project_id,
                    "principal": seed.principal.principal_id,
                    "agent": seed.agent_version_id,
                },
            )


def test_migration_downgrade_upgrade_uses_explicit_disposable_url(monkeypatch):
    # A second isolated database proves destructive migration never targets the
    # fixture database or an ambient deployment URL.
    monkeypatch.setenv("AGENT_PLATFORM_MIGRATION_DATABASE_URL", "postgresql://invalid.invalid/no")
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        engine = create_engine(url)
        try:
            command.upgrade(config, "head")
            assert "runs" in inspect(engine).get_table_names()
            command.downgrade(config, "base")
            assert set(inspect(engine).get_table_names()) == {"alembic_version"}
            command.upgrade(config, "head")
            assert "runs" in inspect(engine).get_table_names()
            with engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM runs")).scalar_one() == 0
        finally:
            engine.dispose()


@pytest.mark.asyncio
async def test_cross_run_step_reference_and_cross_tenant_rls(admin_engine, runtime_engine):
    first = await seed_example(admin_engine, tenant_id=uuid4().hex)
    second = await seed_example(admin_engine, tenant_id=uuid4().hex)
    repo = PostgresRunRepository(runtime_engine)
    command = CreateRunCommand(
        first.principal,
        first.agent_version_id,
        {"candidate_model_ref": "a", "evaluation_suite_ref": "b"},
    )
    run_one = await repo.accept_run(command=command, idempotency_key="one")
    run_two = await repo.accept_run(command=command, idempotency_key="two")
    with pytest.raises(IntegrityError):
        async with admin_engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE work_items SET step_id=(SELECT id FROM run_steps WHERE run_id=:other)
                WHERE run_id=:run
            """),
                {"run": run_one.run.id, "other": run_two.run.id},
            )
    async with unit_of_work(runtime_engine, second.principal.tenant_id) as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 0
        with pytest.raises(ProgrammingError, match="row-level security policy"):
            await conn.execute(
                text("""
                INSERT INTO run_events
                  (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
                VALUES (:tenant,:project,:run,99,'FORGED',1,'intruder','{}')
            """),
                {
                    "tenant": first.principal.tenant_id,
                    "project": first.project_id,
                    "run": run_one.run.id,
                },
            )
    async with runtime_engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one() == 0
