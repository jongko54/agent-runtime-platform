"""Explicit, loopback-only seed for the local Compose example."""

import asyncio
import json

from sqlalchemy import text
from sqlalchemy.engine import make_url

from agent_platform.adapters.postgres.database import create_engine
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.settings import Settings


async def seed_local() -> None:
    settings = Settings()
    migration_url = settings.migration_database_url
    if not settings.development_mode or migration_url is None:
        raise ValueError("Local seed requires development mode and a migration database URL")
    if settings.development_tenant_id != "demo":
        raise ValueError("Local example installer supports only the demo tenant")
    runtime = make_url(settings.database_url)
    migration = make_url(migration_url)
    allowed_hosts = {"localhost", "127.0.0.1", "::1"}
    if (
        runtime.host not in allowed_hosts
        or migration.host not in allowed_hosts
        or runtime.username != "runtime_local"
        or runtime.database != migration.database
        or runtime.port != migration.port
        or runtime.host != migration.host
    ):
        raise ValueError("Local seed accepts only the matching loopback Compose database")
    engine = create_engine(migration_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("GRANT agent_app TO runtime_local"))
        seed = await seed_example(
            engine,
            tenant_id=settings.development_tenant_id,
            principal_id=settings.development_principal_id,
        )
        print(
            json.dumps({"agent_version_id": seed.agent_version_id, "project_id": seed.project_id})
        )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed_local())
