"""Real, disposable PostgreSQL; never connect tests to a user database."""

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine") as container:
        url = container.get_connection_url(driver="psycopg")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", url)
        command.upgrade(config, "head")
        engine = create_engine(url)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE ROLE runtime_test LOGIN PASSWORD 'test-runtime-only' "
                    "NOSUPERUSER NOBYPASSRLS"
                )
            )
            connection.execute(text("GRANT agent_app TO runtime_test"))
        engine.dispose()
        yield url


@pytest.fixture
def runtime_url(postgres_url: str) -> str:
    return (
        make_url(postgres_url)
        .set(username="runtime_test", password="test-runtime-only")
        .render_as_string(hide_password=False)
    )


@pytest_asyncio.fixture
async def admin_engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url, pool_size=5, max_overflow=5)
    async with engine.begin() as connection:
        names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        tables = [name for name in names if name != "alembic_version"]
        quoted = ", ".join(engine.dialect.identifier_preparer.quote(name) for name in tables)
        if tables:
            await connection.execute(text(f"TRUNCATE {quoted} CASCADE"))
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def runtime_engine(admin_engine: AsyncEngine, runtime_url: str) -> AsyncIterator[AsyncEngine]:
    # Dependency ensures each test starts with an empty disposable database.
    engine = create_async_engine(runtime_url, pool_size=5, max_overflow=5)
    yield engine
    await engine.dispose()
