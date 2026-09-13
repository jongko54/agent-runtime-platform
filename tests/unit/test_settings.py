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


def test_default_lease_settings():
    settings = Settings(_env_file=None)
    assert settings.lease_seconds == 30
    assert settings.heartbeat_interval_seconds == 5
    assert settings.max_attempts == 3
    assert settings.retry_base_seconds == 1


@pytest.mark.parametrize("lease,heartbeat", [(30, 10), (30, 11), (1, 0.5)])
def test_heartbeat_must_be_less_than_one_third_of_lease(lease, heartbeat):
    with pytest.raises(ValidationError, match=r"heartbeat.*lease"):
        Settings(_env_file=None, lease_seconds=lease, heartbeat_interval_seconds=heartbeat)


@pytest.mark.parametrize(
    "field,value",
    [
        ("lease_seconds", 0),
        ("lease_seconds", 301),
        ("heartbeat_interval_seconds", 0),
        ("heartbeat_interval_seconds", 61),
        ("max_attempts", 0),
        ("max_attempts", 101),
        ("max_attempts", 1.5),
        ("retry_base_seconds", 0),
        ("retry_base_seconds", 61),
    ],
)
def test_lease_settings_bounds(field, value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_short_lease_with_matching_heartbeat_is_supported():
    settings = Settings(_env_file=None, lease_seconds=0.3, heartbeat_interval_seconds=0.05)
    assert settings.lease_seconds == 0.3
