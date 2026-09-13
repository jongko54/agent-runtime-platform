from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def create_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)


async def set_context(
    connection: AsyncConnection, tenant_id: str, principal_id: str | None = None
) -> None:
    await connection.execute(
        text(
            "SELECT set_config('app.tenant_id', :tenant, true), "
            "set_config('app.principal_id', :principal, true)"
        ),
        {"tenant": tenant_id, "principal": principal_id or ""},
    )


@asynccontextmanager
async def unit_of_work(
    engine: AsyncEngine, tenant_id: str, principal_id: str | None = None
) -> AsyncGenerator[AsyncConnection]:
    async with engine.begin() as connection:
        await set_context(connection, tenant_id, principal_id)
        yield connection
