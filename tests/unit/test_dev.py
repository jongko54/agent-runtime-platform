import pytest

from agent_platform import dev
from agent_platform.settings import Settings


@pytest.mark.asyncio
async def test_local_seed_rejects_custom_tenant_before_connecting(monkeypatch):
    settings = Settings(
        _env_file=None,
        development_mode=True,
        development_token="local-test-token-only",
        development_tenant_id="another-tenant",
        migration_database_url="postgresql+psycopg://admin:local@localhost:55432/agent_runtime",
    )
    monkeypatch.setattr(dev, "Settings", lambda: settings)

    def unexpected_connection(*args, **kwargs):
        pytest.fail("Invalid local seed must not open a database connection")

    monkeypatch.setattr(dev, "create_engine", unexpected_connection)
    with pytest.raises(ValueError, match="only the demo tenant"):
        await dev.seed_local()
