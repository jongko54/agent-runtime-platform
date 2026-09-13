"""Migrations use an explicitly privileged URL, never the runtime connection."""

import os

from alembic import context
from sqlalchemy import create_engine, pool

config = context.config
url = config.get_main_option("sqlalchemy.url") or os.environ.get(
    "AGENT_PLATFORM_MIGRATION_DATABASE_URL"
)
if not url:
    raise ValueError("Set AGENT_PLATFORM_MIGRATION_DATABASE_URL or explicit sqlalchemy.url")

if context.is_offline_mode():
    context.configure(url=url, literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()
