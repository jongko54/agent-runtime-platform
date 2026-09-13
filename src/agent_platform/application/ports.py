"""Framework-independent contracts shared by API, workers and persistence."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class PrincipalContext:
    tenant_id: str
    principal_id: str


@dataclass(frozen=True, slots=True)
class CreateRunCommand:
    principal: PrincipalContext
    agent_version_id: str
    input: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: str
    tenant_id: str
    project_id: str
    principal_id: str
    agent_version_id: str
    state: str
    state_version: int
    input: dict[str, Any]
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class AcceptedRun:
    run: RunRecord
    duplicate: bool


@dataclass(frozen=True, slots=True)
class EventRecord:
    sequence: int
    type: str
    schema_version: int
    actor: str
    payload: dict[str, Any]
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class ClaimedWork:
    id: str
    tenant_id: str
    project_id: str
    principal_id: str
    run_id: str
    step_id: str
    kind: str
    input: dict[str, Any]
    agent_spec: dict[str, Any]
    tool_version_id: str | None = None
    tool_spec: dict[str, Any] | None = None
    attempt_id: str = ""
    attempt_no: int = 1
    lease_token: int = 0
    worker_id: str = ""


@dataclass(frozen=True, slots=True)
class SeedResult:
    principal: PrincipalContext
    project_id: str
    agent_version_id: str


class IdentityVerifier(Protocol):
    async def verify(self, bearer_token: str) -> PrincipalContext: ...


class ModelGateway(Protocol):
    async def decide(
        self, *, input: dict[str, Any], allowed_tools: tuple[str, ...]
    ) -> dict[str, Any]: ...


class ToolGateway(Protocol):
    async def execute(self, *, tool_version: str, arguments: dict[str, Any]) -> dict[str, Any]: ...


class RunRepository(Protocol):
    async def accept_run(
        self, *, command: CreateRunCommand, idempotency_key: str
    ) -> AcceptedRun: ...

    async def get_run(self, principal: PrincipalContext, run_id: str) -> RunRecord: ...

    async def list_events(
        self, principal: PrincipalContext, run_id: str, after_sequence: int = 0, limit: int = 100
    ) -> list[EventRecord]: ...

    async def claim_work(self) -> ClaimedWork | None: ...

    async def heartbeat(self, work: ClaimedWork) -> bool: ...

    async def recover_expired(self, limit: int = 100) -> int: ...

    async def retry_work(self, work: ClaimedWork, code: str, message: str) -> None: ...

    async def complete_model(self, work: ClaimedWork, decision: dict[str, Any]) -> None: ...

    async def complete_tool(self, work: ClaimedWork, result: dict[str, Any]) -> None: ...

    async def fail_work(
        self, work: ClaimedWork, code: str, message: str, *, timed_out: bool = False
    ) -> None: ...

    async def check_health(self) -> None: ...
