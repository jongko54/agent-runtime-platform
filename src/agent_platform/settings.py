"""Explicit development settings; external identity fails closed by default."""

from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENT_PLATFORM_", env_file=".env", extra="forbid")

    database_url: str = (
        "postgresql+psycopg://runtime_local:local-only@localhost:55432/agent_runtime"
    )
    migration_database_url: str | None = None
    worker_poll_interval_seconds: float = Field(default=0.25, gt=0, le=10)
    operation_timeout_seconds: float = Field(default=30, gt=0, le=300)
    sse_poll_interval_seconds: float = Field(default=0.1, gt=0, le=10)
    sse_heartbeat_seconds: float = Field(default=15, gt=0, le=60)
    development_mode: bool = False
    development_token: SecretStr | None = None
    development_tenant_id: str = "demo"
    development_principal_id: str = "demo-user"

    @field_validator("database_url", "migration_database_url")
    @classmethod
    def require_postgres(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith("postgresql+psycopg://"):
            raise ValueError("Use a PostgreSQL Psycopg connection URL")
        return value

    @model_validator(mode="after")
    def require_development_token(self) -> Self:
        if self.development_mode and (
            self.development_token is None
            or len(self.development_token.get_secret_value().strip()) < 16
        ):
            raise ValueError(
                "Development identity requires an explicit token of at least 16 characters"
            )
        return self
