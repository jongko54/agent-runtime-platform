import pytest
from pydantic import ValidationError

from agent_platform.settings import Settings


def test_default_identity_is_disabled():
    settings = Settings(_env_file=None)
    assert settings.development_mode is False
    assert settings.development_token is None


def test_explicit_development_identity_requires_token():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, development_mode=True)


@pytest.mark.parametrize("value", [0, -1, 31])
def test_invalid_poll_interval_rejected(value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, worker_poll_interval_seconds=value)


def test_invalid_database_protocol_rejected():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, database_url="sqlite:///runtime.db")
