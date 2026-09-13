# Phase 0·1 Python Agent Runtime Implementation Plan

> 2026-09-13 구현 추가: 아래 체크박스·코드 조각은 최초 실행 계획의 기록이다. 현재 파일명, 실제 실행 명령, 지원 기능과 검증 범위는 [Phase 0·1 구현 현황](../../implementation/phase-0-1.md)과 [README](../../../README.md)를 따른다. 특히 아래 downgrade 명령은 데이터를 제거하므로 기존 사용자 DB에서 실행하지 않는다.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Python으로 멀티테넌트 Agent Platform의 Contract·Invariant와 요청 → mock model 판단 → 단일 mock Tool 실행 → 결과 저장 vertical slice를 구현한다.

**Architecture:** FastAPI API와 asyncio worker는 같은 `agent_platform` package를 사용하되 별도 process로 실행한다. PostgreSQL이 Run·Step·Event·Work Item의 유일한 권위이며, application service는 순수 Python domain과 port에 의존하고 SQLAlchemy/Psycopg 구현은 adapter에 둔다.

**Tech Stack:** Python 3.13, uv, FastAPI 0.141+, Uvicorn 0.52+, Pydantic 2.13+, SQLAlchemy 2.0.52, Psycopg 3.3+, Alembic 1.19+, PostgreSQL 17, pytest 9, pytest-asyncio 1.4, Hypothesis 6.167+, Testcontainers 4.15+, Ruff 0.16+, Pyright 1.1+

---

## 범위와 실행 규칙

- 이 계획은 Phase 0과 Phase 1만 구현한다.
- Phase 1 Step kind는 `MODEL_CALL`, `TOOL_CALL` 두 종류다.
- Run 입력의 candidate reference를 mock model이 읽고 `evaluation.run_suite:v1` 호출을 선택한다.
- Phase 1은 worker 한 개만 지원한다. lease, heartbeat, fencing, retry, DLQ는 Phase 2에서 구현한다.
- API는 Run을 실행하지 않고 PostgreSQL에 durable하게 접수한다.
- 외부 호출 중 DB transaction을 유지하지 않는다.
- 각 Task는 테스트 실패 → 최소 구현 → 전체 관련 테스트 → 커밋 순서를 지킨다.

## 파일 지도

```text
agent-runtime-platform/
├── pyproject.toml
├── uv.lock
├── .python-version
├── .env.example
├── compose.yaml
├── alembic.ini
├── migrations/
│   ├── env.py
│   └── versions/
├── src/agent_platform/
│   ├── domain/
│   │   ├── errors.py
│   │   ├── ids.py
│   │   ├── runs.py
│   │   └── states.py
│   ├── contracts/
│   │   ├── agents.py
│   │   └── tools.py
│   ├── application/
│   │   ├── digest.py
│   │   ├── errors.py
│   │   ├── ports.py
│   │   ├── run_service.py
│   │   └── runtime_kernel.py
│   ├── adapters/
│   │   ├── models/mock.py
│   │   ├── postgres/database.py
│   │   ├── postgres/repositories.py
│   │   ├── postgres/tables.py
│   │   └── tools/mock_evaluation.py
│   ├── api/
│   │   ├── app.py
│   │   ├── dependencies.py
│   │   ├── errors.py
│   │   ├── schemas.py
│   │   └── routes/runs.py
│   ├── worker/
│   │   ├── main.py
│   │   └── poller.py
│   └── settings.py
└── tests/
    ├── unit/
    ├── contract/
    ├── integration/
    └── e2e/
```

### Task 1: Python project와 품질 gate 생성

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `.gitignore`
- Create: `src/agent_platform/__init__.py`
- Create: `tests/unit/test_package.py`

- [ ] **Step 1: import smoke test 작성**

```python
# tests/unit/test_package.py
import agent_platform


def test_package_exposes_version() -> None:
    assert agent_platform.__version__ == "0.1.0"
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/test_package.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'agent_platform'`.

- [ ] **Step 3: project metadata와 package 생성**

```toml
# pyproject.toml
[build-system]
requires = ["hatchling>=1.27,<2"]
build-backend = "hatchling.build"

[project]
name = "agent-runtime-platform"
version = "0.1.0"
description = "Durable multi-tenant agent runtime"
requires-python = ">=3.13,<3.14"
dependencies = [
  "alembic>=1.19.1,<2",
  "fastapi>=0.141.1,<1",
  "pydantic>=2.13.5,<3",
  "pydantic-settings>=2.15.0,<3",
  "psycopg[binary,pool]>=3.3.5,<4",
  "sqlalchemy[asyncio]>=2.0.52,<2.1",
  "uvicorn[standard]>=0.52.4,<1",
]

[dependency-groups]
dev = [
  "asgi-lifespan>=2.1,<3",
  "httpx>=0.28.1,<1",
  "hypothesis>=6.167.1,<7",
  "pyright>=1.1.411,<2",
  "pytest>=9.1.1,<10",
  "pytest-asyncio>=1.4,<2",
  "ruff>=0.16.6,<1",
  "testcontainers[postgres]>=4.15,<5",
]

[tool.hatch.build.targets.wheel]
packages = ["src/agent_platform"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "strict"

[tool.ruff]
line-length = 100
target-version = "py313"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "ASYNC", "RUF"]

[tool.pyright]
include = ["src"]
pythonVersion = "3.13"
typeCheckingMode = "strict"
```

```text
# .python-version
3.13
```

```gitignore
# .gitignore
.env
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
.hypothesis/
.coverage
htmlcov/
dist/
```

```python
# src/agent_platform/__init__.py
__version__ = "0.1.0"
```

- [ ] **Step 4: dependency lock과 품질 gate 실행**

Run: `uv lock && uv sync --frozen && uv run ruff check . && uv run pyright && uv run pytest tests/unit/test_package.py -q`

Expected: all commands exit 0 and pytest reports `1 passed`.

- [ ] **Step 5: commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore src/agent_platform/__init__.py tests/unit/test_package.py
git commit -m "build: initialize Python runtime project"
```

### Task 2: 순수 Python Run 상태 머신 구현

**Files:**
- Create: `src/agent_platform/domain/ids.py`
- Create: `src/agent_platform/domain/states.py`
- Create: `src/agent_platform/domain/errors.py`
- Create: `src/agent_platform/domain/runs.py`
- Create: `tests/unit/domain/test_runs.py`
- Create: `tests/unit/domain/test_run_properties.py`

- [ ] **Step 1: 허용·거부 전이 테스트 작성**

```python
# tests/unit/domain/test_runs.py
from datetime import UTC, datetime

import pytest

from agent_platform.domain.errors import InvalidTransition
from agent_platform.domain.ids import AgentVersionId, ProjectId, RunId, TenantId
from agent_platform.domain.runs import Run
from agent_platform.domain.states import RunState


def make_run(state: RunState = RunState.CREATED) -> Run:
    return Run(
        id=RunId("run-1"),
        tenant_id=TenantId("tenant-1"),
        project_id=ProjectId("project-1"),
        agent_version_id=AgentVersionId("agent-version-1"),
        state=state,
        state_version=0,
        created_at=datetime(2026, 9, 4, tzinfo=UTC),
    )


def test_created_run_can_be_queued() -> None:
    queued = make_run().transition_to(RunState.QUEUED)
    assert queued.state is RunState.QUEUED
    assert queued.state_version == 1


def test_terminal_run_cannot_transition() -> None:
    with pytest.raises(InvalidTransition):
        make_run(RunState.COMPLETED).transition_to(RunState.RUNNING)
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/domain/test_runs.py -q`

Expected: FAIL because the domain modules do not exist.

- [ ] **Step 3: ID, state, error, Run 구현**

```python
# src/agent_platform/domain/ids.py
from typing import NewType

AgentVersionId = NewType("AgentVersionId", str)
ProjectId = NewType("ProjectId", str)
RunId = NewType("RunId", str)
StepId = NewType("StepId", str)
TenantId = NewType("TenantId", str)
ToolVersionId = NewType("ToolVersionId", str)
WorkItemId = NewType("WorkItemId", str)
```

```python
# src/agent_platform/domain/states.py
from enum import StrEnum


class RunState(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_MODEL = "WAITING_MODEL"
    WAITING_TOOL = "WAITING_TOOL"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    REJECTED = "REJECTED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class StepKind(StrEnum):
    MODEL_CALL = "MODEL_CALL"
    TOOL_CALL = "TOOL_CALL"


class StepState(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class AttemptState(StrEnum):
    CREATED = "CREATED"
    LEASED = "LEASED"
    DISPATCHED = "DISPATCHED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"
    STALE = "STALE"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class EffectState(StrEnum):
    PREPARED = "PREPARED"
    DISPATCHED = "DISPATCHED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    COMPENSATION_REQUIRED = "COMPENSATION_REQUIRED"
    COMPENSATED = "COMPENSATED"
```

```python
# src/agent_platform/domain/errors.py
class DomainError(Exception):
    code = "DOMAIN_ERROR"


class InvalidTransition(DomainError):
    code = "INVALID_TRANSITION"
```

```python
# src/agent_platform/domain/runs.py
from dataclasses import dataclass, replace
from datetime import datetime

from agent_platform.domain.errors import InvalidTransition
from agent_platform.domain.ids import AgentVersionId, ProjectId, RunId, TenantId
from agent_platform.domain.states import RunState

TERMINAL_STATES = frozenset(
    {
        RunState.COMPLETED,
        RunState.FAILED,
        RunState.CANCELLED,
        RunState.TIMED_OUT,
        RunState.REJECTED,
    }
)

ALLOWED_TRANSITIONS: dict[RunState, frozenset[RunState]] = {
    RunState.CREATED: frozenset({RunState.QUEUED}),
    RunState.QUEUED: frozenset({RunState.RUNNING, RunState.CANCEL_REQUESTED}),
    RunState.RUNNING: frozenset(
        {
            RunState.WAITING_MODEL,
            RunState.WAITING_TOOL,
            RunState.COMPLETED,
            RunState.FAILED,
            RunState.CANCEL_REQUESTED,
            RunState.OUTCOME_UNKNOWN,
        }
    ),
    RunState.WAITING_MODEL: frozenset(
        {RunState.RUNNING, RunState.FAILED, RunState.CANCEL_REQUESTED}
    ),
    RunState.WAITING_TOOL: frozenset(
        {
            RunState.RUNNING,
            RunState.WAITING_APPROVAL,
            RunState.FAILED,
            RunState.CANCEL_REQUESTED,
            RunState.OUTCOME_UNKNOWN,
        }
    ),
    RunState.WAITING_APPROVAL: frozenset(
        {RunState.RUNNING, RunState.REJECTED, RunState.CANCEL_REQUESTED}
    ),
    RunState.PAUSED: frozenset({RunState.QUEUED, RunState.CANCEL_REQUESTED}),
    RunState.CANCEL_REQUESTED: frozenset({RunState.CANCELLED, RunState.OUTCOME_UNKNOWN}),
    RunState.OUTCOME_UNKNOWN: frozenset(
        {RunState.QUEUED, RunState.COMPLETED, RunState.FAILED, RunState.CANCELLED}
    ),
}


@dataclass(frozen=True, slots=True)
class Run:
    id: RunId
    tenant_id: TenantId
    project_id: ProjectId
    agent_version_id: AgentVersionId
    state: RunState
    state_version: int
    created_at: datetime

    def transition_to(self, target: RunState) -> "Run":
        if self.state in TERMINAL_STATES or target not in ALLOWED_TRANSITIONS.get(
            self.state, frozenset()
        ):
            raise InvalidTransition(f"{self.state} -> {target}")
        return replace(self, state=target, state_version=self.state_version + 1)
```

Phase 1 vertical slice는 위 vocabulary 중 approval·pause·attempt·effect 전이를 실행하지 않는다.
다만 이름과 terminal invariant를 Phase 0 contract로 먼저 고정해 Phase 2가 기존 상태 의미를
바꾸지 않고 확장되도록 한다.

- [ ] **Step 4: terminal 불변성 property test 작성**

```python
# tests/unit/domain/test_run_properties.py
from hypothesis import given
from hypothesis import strategies as st

from agent_platform.domain.runs import TERMINAL_STATES
from agent_platform.domain.states import RunState


@given(st.sampled_from(tuple(TERMINAL_STATES)), st.sampled_from(tuple(RunState)))
def test_terminal_states_have_no_outgoing_transition(current: RunState, target: RunState) -> None:
    from agent_platform.domain.runs import ALLOWED_TRANSITIONS

    assert target not in ALLOWED_TRANSITIONS.get(current, frozenset())
```

- [ ] **Step 5: domain test와 framework import guard 실행**

Run: `uv run pytest tests/unit/domain -q && ! rg 'fastapi|pydantic|sqlalchemy|psycopg' src/agent_platform/domain`

Expected: all tests pass and `rg` finds no forbidden import.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/domain tests/unit/domain
git commit -m "feat: define runtime state machine"
```

### Task 3: versioned Agent·Tool contract와 digest 구현

**Files:**
- Create: `src/agent_platform/contracts/agents.py`
- Create: `src/agent_platform/contracts/tools.py`
- Create: `src/agent_platform/application/digest.py`
- Create: `tests/contract/test_version_contracts.py`

- [ ] **Step 1: extra field 거부와 digest 안정성 테스트 작성**

```python
# tests/contract/test_version_contracts.py
import pytest
from pydantic import ValidationError

from agent_platform.application.digest import canonical_digest
from agent_platform.contracts.agents import AgentVersionSpec


def test_agent_spec_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        AgentVersionSpec.model_validate(
            {
                "instructions_ref": "content://agent/v1",
                "model_route": "mock/release-planner-v1",
                "tools": ["evaluation.run_suite:v1"],
                "unknown": True,
            }
        )


def test_digest_is_independent_of_mapping_order() -> None:
    left = {"b": 2, "a": 1}
    right = {"a": 1, "b": 2}
    assert canonical_digest(left) == canonical_digest(right)
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/contract/test_version_contracts.py -q`

Expected: FAIL because contract modules do not exist.

- [ ] **Step 3: strict contract와 canonical digest 구현**

```python
# src/agent_platform/contracts/agents.py
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_steps: int = Field(ge=1, le=100)
    deadline_seconds: int = Field(ge=1, le=86_400)
    max_input_tokens: int = Field(ge=1)
    max_output_tokens: int = Field(ge=1)
    max_cost_usd: Decimal = Field(ge=0)


class ApprovalPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    required_tools: tuple[str, ...]


class CompensationRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool: str
    requires_original_effect: str
    authorization_source: str
    max_age_seconds: int = Field(ge=1)


class CompensationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rules: tuple[CompensationRule, ...]


class AgentVersionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instructions_ref: str
    model_route: str
    tools: tuple[str, ...]
    execution_policy: ExecutionPolicy | None = None
    approval_policy: ApprovalPolicy | None = None
    compensation_policy: CompensationPolicy | None = None
```

```python
# src/agent_platform/contracts/tools.py
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class ToolInvocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tool_version: str
    arguments: dict[str, Any]


class ToolResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    output: dict[str, Any]
    usage_units: int = 1


class ToolVersionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    version: int
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    risk_tier: Literal["T0", "T1", "T2", "T3"]
    connection_kind: str
```

```python
# src/agent_platform/application/digest.py
import hashlib
import json
from typing import Any


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

- [ ] **Step 4: contract test 실행**

Run: `uv run pytest tests/contract/test_version_contracts.py -q`

Expected: `2 passed`.

- [ ] **Step 5: commit**

```bash
git add src/agent_platform/contracts src/agent_platform/application/digest.py tests/contract/test_version_contracts.py
git commit -m "feat: add immutable agent and tool contracts"
```

### Task 4: PostgreSQL 개발 환경과 설정 구현

**Files:**
- Create: `compose.yaml`
- Create: `.env.example`
- Create: `src/agent_platform/settings.py`
- Create: `tests/unit/test_settings.py`

- [ ] **Step 1: 설정 검증 테스트 작성**

```python
# tests/unit/test_settings.py
from agent_platform.settings import Settings


def test_settings_accepts_explicit_database_url() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://agent:agent@localhost:55432/agent_runtime"
    )
    assert settings.database_url.endswith("/agent_runtime")
    assert settings.worker_poll_interval_seconds == 0.25
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/test_settings.py -q`

Expected: FAIL because `agent_platform.settings` does not exist.

- [ ] **Step 3: Compose와 Settings 구현**

```yaml
# compose.yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_DB: agent_runtime
      POSTGRES_USER: agent
      POSTGRES_PASSWORD: agent
    ports:
      - "55432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U agent -d agent_runtime"]
      interval: 2s
      timeout: 2s
      retries: 20
    volumes:
      - agent-runtime-postgres:/var/lib/postgresql/data

volumes:
  agent-runtime-postgres:
```

```dotenv
# .env.example
AGENT_PLATFORM_DATABASE_URL=postgresql+psycopg://agent:agent@localhost:55432/agent_runtime
AGENT_PLATFORM_WORKER_POLL_INTERVAL_SECONDS=0.25
```

```python
# src/agent_platform/settings.py
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="AGENT_PLATFORM_",
        extra="forbid",
    )

    database_url: str
    worker_poll_interval_seconds: float = Field(default=0.25, gt=0, le=10)
```

- [ ] **Step 4: 설정과 PostgreSQL health 검증**

Run: `uv run pytest tests/unit/test_settings.py -q && docker compose up -d postgres && docker compose exec -T postgres pg_isready -U agent -d agent_runtime`

Expected: pytest passes and `pg_isready` reports `accepting connections`.

- [ ] **Step 5: commit**

```bash
git add compose.yaml .env.example src/agent_platform/settings.py tests/unit/test_settings.py
git commit -m "build: add PostgreSQL development environment"
```

### Task 5: Phase 1 schema와 Alembic migration 구현

**Files:**
- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `src/agent_platform/adapters/postgres/tables.py`
- Generate: `migrations/versions/0001_phase_1_runtime.py`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_migrations.py`

- [ ] **Step 1: migration smoke test 작성**

```python
# tests/integration/test_migrations.py
from sqlalchemy import inspect


def test_phase_1_tables_exist(sync_engine) -> None:
    names = set(inspect(sync_engine).get_table_names())
    expected = {
        "tenants",
        "projects",
        "principals",
        "project_memberships",
        "connections",
        "agent_versions",
        "tool_versions",
        "runs",
        "run_steps",
        "run_events",
        "work_items",
        "idempotency_records",
        "model_calls",
        "tool_calls",
        "usage_entries",
    }
    assert names == expected | {"alembic_version"}
```

- [ ] **Step 2: PostgreSQL Testcontainer fixture 작성**

```python
# tests/integration/conftest.py
from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine") as container:
        yield container.get_connection_url(driver="psycopg")


@pytest.fixture(scope="session")
def sync_engine(postgres_url: str) -> Iterator[Engine]:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    engine = create_engine(postgres_url)
    yield engine
    engine.dispose()
```

- [ ] **Step 3: 실패 확인**

Run: `uv run pytest tests/integration/test_migrations.py -q`

Expected: FAIL because Alembic configuration and tables do not exist.

- [ ] **Step 4: SQLAlchemy Core metadata 작성**

Implement `src/agent_platform/adapters/postgres/tables.py` with these mandatory constraints:

```python
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

tenants = Table(
    "tenants",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("status", String(32), nullable=False),
)

projects = Table(
    "projects",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("name", String(200), nullable=False),
    Column("status", String(32), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
)

principals = Table(
    "principals",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("issuer", String(500), nullable=False),
    Column("subject", String(500), nullable=False),
    Column("type", String(32), nullable=False),
    Column("status", String(32), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    UniqueConstraint("tenant_id", "issuer", "subject"),
    ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
)

project_memberships = Table(
    "project_memberships",
    metadata,
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("principal_id", String(64), nullable=False),
    Column("role_set_id", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    PrimaryKeyConstraint("tenant_id", "project_id", "principal_id"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
    ForeignKeyConstraint(["tenant_id", "principal_id"], ["principals.tenant_id", "principals.id"]),
)

connections = Table(
    "connections",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("kind", String(100), nullable=False),
    Column("config_ref", String(500), nullable=False),
    Column("credential_ref", String(500)),
    Column("status", String(32), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
)

agent_versions = Table(
    "agent_versions",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("version", Integer, nullable=False),
    Column("digest", String(64), nullable=False),
    Column("spec", JSONB, nullable=False),
    UniqueConstraint("tenant_id", "id"),
    UniqueConstraint("tenant_id", "project_id", "digest"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
)

tool_versions = Table(
    "tool_versions",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("name", String(200), nullable=False),
    Column("version", Integer, nullable=False),
    Column("schema_digest", String(64), nullable=False),
    Column("input_schema", JSONB, nullable=False),
    Column("output_schema", JSONB, nullable=False),
    Column("risk_tier", String(8), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    UniqueConstraint("tenant_id", "project_id", "name", "version"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
)

runs = Table(
    "runs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("agent_version_id", String(64), nullable=False),
    Column("state", String(32), nullable=False),
    Column("state_version", Integer, nullable=False),
    Column("input", JSONB, nullable=False),
    Column("result", JSONB),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
    ForeignKeyConstraint(
        ["tenant_id", "agent_version_id"], ["agent_versions.tenant_id", "agent_versions.id"]
    ),
)

run_steps = Table(
    "run_steps",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("ordinal", Integer, nullable=False),
    Column("kind", String(32), nullable=False),
    Column("state", String(32), nullable=False),
    Column("input", JSONB, nullable=False),
    Column("output", JSONB),
    UniqueConstraint("tenant_id", "id"),
    UniqueConstraint("tenant_id", "run_id", "ordinal"),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
)

run_events = Table(
    "run_events",
    metadata,
    Column("tenant_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("sequence", BigInteger, nullable=False),
    Column("type", String(100), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("occurred_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    PrimaryKeyConstraint("tenant_id", "run_id", "sequence"),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
)

work_items = Table(
    "work_items",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("step_id", String(64), nullable=False),
    Column("status", String(32), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
    ForeignKeyConstraint(["tenant_id", "step_id"], ["run_steps.tenant_id", "run_steps.id"]),
)

idempotency_records = Table(
    "idempotency_records",
    metadata,
    Column("tenant_id", String(64), nullable=False),
    Column("scope", String(100), nullable=False),
    Column("key", String(200), nullable=False),
    Column("request_hash", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    PrimaryKeyConstraint("tenant_id", "scope", "key"),
    ForeignKeyConstraint(
        ["tenant_id", "run_id"],
        ["runs.tenant_id", "runs.id"],
        deferrable=True,
        initially="DEFERRED",
    ),
)

model_calls = Table(
    "model_calls",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("step_id", String(64), nullable=False),
    Column("model_route", String(200), nullable=False),
    Column("response", JSONB, nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
    ForeignKeyConstraint(["tenant_id", "step_id"], ["run_steps.tenant_id", "run_steps.id"]),
)

tool_calls = Table(
    "tool_calls",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("step_id", String(64), nullable=False),
    Column("tool_version_id", String(64), nullable=False),
    Column("arguments", JSONB, nullable=False),
    Column("result", JSONB, nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
    ForeignKeyConstraint(["tenant_id", "step_id"], ["run_steps.tenant_id", "run_steps.id"]),
    ForeignKeyConstraint(
        ["tenant_id", "tool_version_id"], ["tool_versions.tenant_id", "tool_versions.id"]
    ),
)

usage_entries = Table(
    "usage_entries",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("source", String(32), nullable=False),
    Column("quantity", BigInteger, nullable=False),
    Column("unit", String(32), nullable=False),
    UniqueConstraint("tenant_id", "id"),
    ForeignKeyConstraint(["tenant_id", "project_id"], ["projects.tenant_id", "projects.id"]),
    ForeignKeyConstraint(["tenant_id", "run_id"], ["runs.tenant_id", "runs.id"]),
)

Index("ix_work_items_ready", work_items.c.status, work_items.c.available_at, work_items.c.id)
Index("ix_runs_scope_created", runs.c.tenant_id, runs.c.project_id, runs.c.created_at)
```

- [ ] **Step 5: Alembic async environment 구성과 migration 생성**

Set `target_metadata` in `migrations/env.py` to the metadata above, then run:

Run: `uv run alembic revision --autogenerate -m "phase 1 runtime" --rev-id 0001`

Expected: `migrations/versions/0001_phase_1_runtime.py` creates exactly the 15 tables asserted by the test.

- [ ] **Step 6: migration 검증**

Run: `uv run pytest tests/integration/test_migrations.py -q`

Expected: `1 passed` against PostgreSQL 17.

- [ ] **Step 7: commit**

```bash
git add alembic.ini migrations src/agent_platform/adapters/postgres/tables.py tests/integration
git commit -m "feat: add phase one PostgreSQL schema"
```

### Task 6: Database Unit of Work와 Run repository 구현

**Files:**
- Create: `src/agent_platform/adapters/postgres/database.py`
- Create: `src/agent_platform/adapters/postgres/repositories.py`
- Create: `src/agent_platform/application/errors.py`
- Create: `src/agent_platform/application/ports.py`
- Modify: `tests/integration/conftest.py`
- Create: `tests/integration/test_run_repository.py`

- [ ] **Step 1: 원자적 접수 실패 rollback 테스트 작성**

```python
# tests/integration/test_run_repository.py
import pytest
from sqlalchemy import func, select

from agent_platform.adapters.postgres.tables import (
    idempotency_records,
    run_events,
    run_steps,
    runs,
    work_items,
)


@pytest.mark.asyncio
async def test_acceptance_rolls_back_all_rows_on_failure(
    repository, accept_kwargs, async_engine
) -> None:
    def fail_after_step() -> None:
        raise RuntimeError("injected failure")

    repository.after_step_insert = fail_after_step
    with pytest.raises(RuntimeError, match="injected failure"):
        await repository.accept_run(**accept_kwargs)

    async with async_engine.connect() as connection:
        counts = []
        for table in (idempotency_records, runs, run_steps, run_events, work_items):
            counts.append(await connection.scalar(select(func.count()).select_from(table)))
    assert counts == [0, 0, 0, 0, 0]
```

Extend `tests/integration/conftest.py` with a per-test active Tenant, Project, Principal,
Membership, and Agent Version seed. Its `accept_kwargs` fixture supplies the exact
`RunRepository.accept_run()` keyword arguments and deletes the seeded rows after each test.

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/integration/test_run_repository.py -q`

Expected: FAIL because database and repository adapters do not exist.

- [ ] **Step 3: engine factory와 Unit of Work port 구현**

```python
# src/agent_platform/application/errors.py
from collections.abc import Mapping
from typing import Any


class ApplicationError(Exception):
    code = "APPLICATION_ERROR"
    retryable = False

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = dict(details or {})


class IdempotencyConflict(ApplicationError):
    code = "IDEMPOTENCY_CONFLICT"


class ExecutionScopeNotFound(ApplicationError):
    code = "EXECUTION_SCOPE_NOT_FOUND"
```

```python
# src/agent_platform/application/ports.py
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class AcceptedRun:
    run_id: str
    project_id: str
    state: str
    duplicate: bool


@dataclass(frozen=True, slots=True)
class CreateRunCommand:
    tenant_id: str
    project_id: str
    principal_id: str
    agent_version_id: str
    input: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PrincipalContext:
    tenant_id: str
    project_id: str
    principal_id: str


@dataclass(frozen=True, slots=True)
class ClaimedWork:
    id: str
    tenant_id: str
    run_id: str
    step_id: str


class Transaction(Protocol):
    async def __aenter__(self) -> "Transaction": ...
    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> AbstractAsyncContextManager[Any]: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class IdGenerator(Protocol):
    def new(self, prefix: str) -> str: ...


class IdentityVerifier(Protocol):
    async def verify(self, bearer_token: str) -> PrincipalContext: ...


class RunRepository(Protocol):
    async def accept_run(
        self,
        *,
        command: CreateRunCommand,
        idempotency_key: str,
        request_hash: str,
        run_id: str,
        step_id: str,
        work_item_id: str,
        now: datetime,
    ) -> AcceptedRun: ...
```

```python
# src/agent_platform/adapters/postgres/database.py
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


@asynccontextmanager
async def unit_of_work(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    async with engine.begin() as connection:
        yield connection
```

- [ ] **Step 4: repository의 acceptance transaction 구현**

In `repositories.py`, implement `PostgresRunRepository.accept_run()` matching the
`RunRepository` protocol. Inside one `engine.begin()` block it must:

1. Select an active Tenant, Project, Principal, and Membership plus the requested Agent Version using every row's `tenant_id` and the Project relationship. Missing or inactive scope raises `ExecutionScopeNotFound` without disclosing which row failed.
2. Insert the idempotency row with `ON CONFLICT DO NOTHING RETURNING run_id`.
3. On conflict, lock and read the existing idempotency row. A matching digest returns the existing Run with `duplicate=True`; a different digest raises `IdempotencyConflict`.
4. On a new key, insert `runs`, the first `run_steps`, sequence 1 `run_events`, and `work_items` through the same `AsyncConnection` and return `duplicate=False` only after commit.

Add an injected callback used only by the test after the Step insert; raising from it must roll back the transaction.

The first event values are fixed:

```python
event = {
    "tenant_id": tenant_id,
    "run_id": run_id,
    "sequence": 1,
    "type": "RUN_ACCEPTED",
    "payload": {"agent_version_id": agent_version_id},
}
```

- [ ] **Step 5: integration test 실행**

Run: `uv run pytest tests/integration/test_run_repository.py -q`

Expected: rollback test passes and all five row counts remain zero.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/application/errors.py src/agent_platform/application/ports.py src/agent_platform/adapters/postgres tests/integration/test_run_repository.py
git commit -m "feat: add transactional run repository"
```

### Task 7: idempotent Run 접수 service 구현

**Files:**
- Modify: `src/agent_platform/application/errors.py`
- Create: `src/agent_platform/application/run_service.py`
- Create: `tests/integration/test_run_acceptance.py`

- [ ] **Step 1: 동일 key와 충돌 key 테스트 작성**

```python
# tests/integration/test_run_acceptance.py
from dataclasses import replace

import pytest

from agent_platform.application.errors import IdempotencyConflict


@pytest.mark.asyncio
async def test_same_key_and_payload_returns_same_run(run_service, run_command) -> None:
    first = await run_service.accept(run_command, "key-1")
    second = await run_service.accept(run_command, "key-1")
    assert first.run_id == second.run_id
    assert second.duplicate is True


@pytest.mark.asyncio
async def test_same_key_with_different_payload_conflicts(run_service, run_command) -> None:
    await run_service.accept(run_command, "key-1")
    changed = replace(
        run_command,
        input={
            **run_command.input,
            "candidate_model_ref": "mock://candidate-2",
        },
    )
    with pytest.raises(IdempotencyConflict):
        await run_service.accept(changed, "key-1")
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/integration/test_run_acceptance.py -q`

Expected: FAIL because `RunService` does not exist.

- [ ] **Step 3: acceptance algorithm 구현**

Implement `RunService.accept()` with this exact application flow:

1. Build a canonical mapping from `project_id`, `agent_version_id`, and `input`, then calculate its digest. Tenant and Principal come from verified identity context and are not accepted in the HTTP body.
2. Generate candidate Run, Step, and Work Item IDs.
3. Delegate once to `RunRepository.accept_run()`.

The repository retains the atomic transaction defined in Task 6: it validates Tenant, Project
membership, Agent Version, and Project ownership; claims the idempotency key; creates the Run
aggregate; and returns an existing Run only for a matching digest.

```python
# src/agent_platform/application/run_service.py
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import IdempotencyConflict
from agent_platform.application.ports import (
    AcceptedRun,
    Clock,
    CreateRunCommand,
    IdGenerator,
    RunRepository,
)


class RunService:
    def __init__(
        self,
        repository: RunRepository,
        id_generator: IdGenerator,
        clock: Clock,
    ) -> None:
        self._repository = repository
        self._id_generator = id_generator
        self._clock = clock

    async def accept(self, command: CreateRunCommand, idempotency_key: str) -> AcceptedRun:
        request_hash = canonical_digest(
            {
                "project_id": command.project_id,
                "agent_version_id": command.agent_version_id,
                "input": command.input,
            }
        )
        return await self._repository.accept_run(
            command=command,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            run_id=self._id_generator.new("run"),
            step_id=self._id_generator.new("step"),
            work_item_id=self._id_generator.new("work"),
            now=self._clock.now(),
        )
```

- [ ] **Step 4: 동시성 검증 추가**

Add a test that calls `accept()` 100 times concurrently with the same key and payload. Assert one distinct Run ID and one row in each of `runs`, `run_steps`, and `work_items`.

- [ ] **Step 5: acceptance integration test 실행**

Run: `uv run pytest tests/integration/test_run_acceptance.py -q`

Expected: three tests pass, including the 100-request concurrency case.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/application/errors.py src/agent_platform/application/run_service.py tests/integration/test_run_acceptance.py
git commit -m "feat: accept runs idempotently"
```

### Task 8: mock Model·Tool adapter와 Runtime Kernel 구현

**Files:**
- Create: `src/agent_platform/adapters/models/mock.py`
- Create: `src/agent_platform/adapters/tools/mock_evaluation.py`
- Create: `src/agent_platform/application/runtime_kernel.py`
- Create: `tests/unit/application/test_runtime_kernel.py`
- Create: `tests/contract/test_mock_gateways.py`

- [ ] **Step 1: 한 cycle 결과 테스트 작성**

```python
# tests/unit/application/test_runtime_kernel.py
import pytest


@pytest.mark.asyncio
async def test_kernel_completes_mock_evaluation(kernel, claimed_work) -> None:
    result = await kernel.execute(claimed_work)
    assert result.state == "COMPLETED"
    assert result.output["decision"] == "EVALUATED"
    assert result.output["quality_score"] == 0.86
    assert result.tool_calls == 1
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/unit/application/test_runtime_kernel.py -q`

Expected: FAIL because kernel and adapters do not exist.

- [ ] **Step 3: Model·Tool Protocol과 mock 구현**

Add these protocols to `application/ports.py`:

```python
class ModelGateway(Protocol):
    async def decide(
        self, *, input: dict[str, Any], allowed_tools: tuple[str, ...]
    ) -> dict[str, Any]: ...


class ToolGateway(Protocol):
    async def execute(self, *, tool_version: str, arguments: dict[str, Any]) -> dict[str, Any]: ...
```

```python
# src/agent_platform/adapters/models/mock.py
class MockModelGateway:
    async def decide(
        self, *, input: dict[str, object], allowed_tools: tuple[str, ...]
    ) -> dict[str, object]:
        tool = "evaluation.run_suite:v1"
        if tool not in allowed_tools:
            raise ValueError("required mock tool is not allowed")
        return {
            "tool_version": tool,
            "arguments": {
                "candidate_model_ref": input["candidate_model_ref"],
                "evaluation_suite_ref": input["evaluation_suite_ref"],
            },
        }
```

```python
# src/agent_platform/adapters/tools/mock_evaluation.py
from pydantic import BaseModel, ConfigDict


class EvaluationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_model_ref: str
    evaluation_suite_ref: str


class MockEvaluationTool:
    async def execute(
        self, *, tool_version: str, arguments: dict[str, object]
    ) -> dict[str, object]:
        if tool_version != "evaluation.run_suite:v1":
            raise ValueError("unsupported tool version")
        payload = EvaluationInput.model_validate(arguments)
        return {
            "decision": "EVALUATED",
            "candidate_model_ref": payload.candidate_model_ref,
            "evaluation_suite_ref": payload.evaluation_suite_ref,
            "quality_score": 0.86,
            "safety_score": 0.99,
        }
```

- [ ] **Step 4: Kernel의 transaction·I/O 순서 구현**

Implement `RuntimeKernel.execute()` in this order:

1. Transaction A: load the already claimed Work Item, move Run `QUEUED → RUNNING → WAITING_MODEL`, append events, commit.
2. No transaction: call `ModelGateway.decide()`.
3. Transaction B: persist model call, complete the `MODEL_CALL` Step, create a `TOOL_CALL` Step, move Run `WAITING_MODEL → RUNNING → WAITING_TOOL`, append events, commit.
4. No transaction: validate and call `ToolGateway.execute()`.
5. Transaction C: persist tool call and usage, complete the `TOOL_CALL` Step, move Run `WAITING_TOOL → RUNNING → COMPLETED`, store the Run result and `RUN_COMPLETED` event, mark the Work Item `DONE`, then commit.

The kernel must never hold `AsyncConnection` while awaiting a Model or Tool adapter.

- [ ] **Step 5: contract와 kernel test 실행**

Run: `uv run pytest tests/contract/test_mock_gateways.py tests/unit/application/test_runtime_kernel.py -q`

Expected: mock schema tests and one-cycle test pass.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/application src/agent_platform/adapters/models src/agent_platform/adapters/tools tests/contract tests/unit/application
git commit -m "feat: execute mock model and tool cycle"
```

### Task 9: FastAPI Run API 구현

**Files:**
- Create: `src/agent_platform/api/schemas.py`
- Create: `src/agent_platform/api/errors.py`
- Create: `src/agent_platform/api/dependencies.py`
- Create: `src/agent_platform/api/routes/runs.py`
- Create: `src/agent_platform/api/app.py`
- Create: `tests/contract/test_run_api.py`

- [ ] **Step 1: durable acceptance와 conflict API 테스트 작성**

```python
# tests/contract/test_run_api.py
import pytest


@pytest.mark.asyncio
async def test_create_run_returns_202(client) -> None:
    response = await client.post(
        "/v1/runs",
        headers={
            "Authorization": "Bearer test-token",
            "Idempotency-Key": "request-1",
        },
        json={
            "agent_version_id": "ai-model-release-agent:v1",
            "input": {
                "candidate_model_ref": "mock://candidate-17",
                "evaluation_suite_ref": "mock://release-gate-v3",
            },
        },
    )
    assert response.status_code == 202
    assert response.json()["state"] == "QUEUED"


@pytest.mark.asyncio
async def test_reused_key_with_different_payload_returns_409(client) -> None:
    first = {
        "agent_version_id": "ai-model-release-agent:v1",
        "input": {
            "candidate_model_ref": "mock://candidate-17",
            "evaluation_suite_ref": "mock://release-gate-v3",
        },
    }
    headers = {
        "Authorization": "Bearer test-token",
        "Idempotency-Key": "request-1",
    }
    await client.post("/v1/runs", headers=headers, json=first)
    first["input"]["candidate_model_ref"] = "mock://candidate-18"
    response = await client.post("/v1/runs", headers=headers, json=first)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/contract/test_run_api.py -q`

Expected: FAIL because the FastAPI app does not exist.

- [ ] **Step 3: strict HTTP schema와 routes 구현**

```python
# src/agent_platform/api/schemas.py
from typing import Any

from pydantic import BaseModel, ConfigDict


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_version_id: str
    input: dict[str, Any]


class RunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    project_id: str
    state: str
    result: dict[str, Any] | None = None
```

Implement routes:

- `POST /v1/runs`: require `Idempotency-Key`, return 202 only after acceptance transaction commit.
- `GET /v1/runs/{run_id}`: derive Tenant/Project from authenticated principal and return 404 for inaccessible IDs.
- `GET /v1/runs/{run_id}/events?after_sequence=N`: return events ordered by sequence.
- `GET /v1/runs/{run_id}/events/stream`: emit `text/event-stream`, accept `Last-Event-ID`, and resume at the next sequence. Send a comment heartbeat during idle periods, stop after a terminal event, and stop immediately when the client disconnects.
- `GET /health/live`: process liveness only.
- `GET /health/ready`: verify a short PostgreSQL `SELECT 1`.

`dependencies.py` resolves a bearer token through an `IdentityVerifier` port and returns
`PrincipalContext`. The route constructs `CreateRunCommand` by combining that context with the
validated body. Tests inject a deterministic verifier; the default app fails closed with
`IDENTITY_PROVIDER_NOT_CONFIGURED` until an external identity adapter is configured. Tenant,
Project, and Principal IDs must never be accepted from request headers, query parameters, or body.

Add contract cases for a missing bearer token (`401`), an unconfigured verifier (`503`), and a
body containing `tenant_id`, `project_id`, or `principal_id` (`422`). The inaccessible Run case
must use a valid identity from a different Project and still return `404`.

- [ ] **Step 4: error envelope 구현**

All application errors use this response shape:

```json
{
  "error": {
    "code": "IDEMPOTENCY_CONFLICT",
    "message": "idempotency key was already used with a different request"
  }
}
```

Do not expose SQL text, stack traces, provider payloads, Tenant existence, or Project existence.

- [ ] **Step 5: API tests 실행**

Run: `uv run pytest tests/contract/test_run_api.py -q`

Expected: 202, duplicate, 409, inaccessible Run, ordered event query, SSE resume, and readiness tests all pass.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/api tests/contract/test_run_api.py
git commit -m "feat: expose durable run API"
```

### Task 10: 단일 asyncio worker 구현

**Files:**
- Create: `src/agent_platform/worker/poller.py`
- Create: `src/agent_platform/worker/main.py`
- Create: `tests/integration/test_worker_poller.py`

- [ ] **Step 1: 한 항목 claim 테스트 작성**

```python
# tests/integration/test_worker_poller.py
import pytest


@pytest.mark.asyncio
async def test_poll_once_claims_and_completes_one_item(poller, seeded_run) -> None:
    processed = await poller.poll_once()
    assert processed is True
    run = await seeded_run.reload()
    assert run.state == "COMPLETED"
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/integration/test_worker_poller.py -q`

Expected: FAIL because worker modules do not exist.

- [ ] **Step 3: poller 구현**

`poll_once()` uses one short claim transaction:

```sql
SELECT id, tenant_id, run_id, step_id
FROM work_items
WHERE status = 'READY' AND available_at <= now()
ORDER BY available_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

It updates the selected row to `PROCESSING`, commits, and then calls `RuntimeKernel.execute()`. With no row it returns `False` without sleeping.

- [ ] **Step 4: worker main loop 구현**

```python
# src/agent_platform/worker/main.py
import asyncio

from agent_platform.settings import Settings
from agent_platform.worker.poller import build_poller


async def run() -> None:
    settings = Settings()  # pyright: ignore[reportCallIssue]
    poller = build_poller(settings)
    while True:
        processed = await poller.poll_once()
        if not processed:
            await asyncio.sleep(settings.worker_poll_interval_seconds)


if __name__ == "__main__":
    asyncio.run(run())
```

- [ ] **Step 5: worker integration test 실행**

Run: `uv run pytest tests/integration/test_worker_poller.py -q`

Expected: one Work Item is processed and the Run becomes `COMPLETED`.

- [ ] **Step 6: commit**

```bash
git add src/agent_platform/worker tests/integration/test_worker_poller.py
git commit -m "feat: add single runtime worker"
```

### Task 11: AI Model Release seed와 end-to-end 검증

**Files:**
- Create: `examples/ai-model-release/agent-version.json`
- Create: `examples/ai-model-release/evaluation-tool-version.json`
- Create: `tests/e2e/test_model_release_cycle.py`
- Modify: `README.md`

- [ ] **Step 1: end-to-end test 작성**

```python
# tests/e2e/test_model_release_cycle.py
import asyncio

import pytest


@pytest.mark.asyncio
async def test_model_release_vertical_slice(client, poller) -> None:
    response = await client.post(
        "/v1/runs",
        headers={
            "Authorization": "Bearer test-token",
            "Idempotency-Key": "model-release-e2e-1",
        },
        json={
            "agent_version_id": "ai-model-release-agent:v1",
            "input": {
                "candidate_model_ref": "mock://candidate-17",
                "evaluation_suite_ref": "mock://release-gate-v3",
            },
        },
    )
    assert response.status_code == 202
    run_id = response.json()["run_id"]

    assert await poller.poll_once() is True

    for _ in range(20):
        current = await client.get(
            f"/v1/runs/{run_id}", headers={"Authorization": "Bearer test-token"}
        )
        if current.json()["state"] == "COMPLETED":
            break
        await asyncio.sleep(0.05)

    assert current.json()["result"]["decision"] == "EVALUATED"
    events = await client.get(
        f"/v1/runs/{run_id}/events?after_sequence=0",
        headers={"Authorization": "Bearer test-token"},
    )
    sequences = [event["sequence"] for event in events.json()["items"]]
    assert sequences == list(range(1, len(sequences) + 1))
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/e2e/test_model_release_cycle.py -q`

Expected: FAIL because example registry fixtures are not installed.

- [ ] **Step 3: immutable example fixtures 작성**

`examples/ai-model-release/agent-version.json`:

```json
{
  "instructions_ref": "content://ai-model-release-agent/v1",
  "model_route": "mock/release-planner-v1",
  "tools": ["evaluation.run_suite:v1"],
  "execution_policy": {
    "max_steps": 2,
    "deadline_seconds": 60,
    "max_input_tokens": 4096,
    "max_output_tokens": 1024,
    "max_cost_usd": "0.00"
  }
}
```

`examples/ai-model-release/evaluation-tool-version.json`:

```json
{
  "name": "evaluation.run_suite",
  "version": 1,
  "input_schema": {
    "type": "object",
    "properties": {
      "candidate_model_ref": {"type": "string"},
      "evaluation_suite_ref": {"type": "string"}
    },
    "required": ["candidate_model_ref", "evaluation_suite_ref"],
    "additionalProperties": false
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "decision": {"const": "EVALUATED"},
      "quality_score": {"type": "number", "minimum": 0, "maximum": 1},
      "safety_score": {"type": "number", "minimum": 0, "maximum": 1}
    },
    "required": ["decision", "quality_score", "safety_score"],
    "additionalProperties": false
  },
  "risk_tier": "T1",
  "connection_kind": "mock"
}
```

Both files must validate as `AgentVersionSpec` and `ToolVersionSpec`. The Agent allows exactly
`evaluation.run_suite:v1`; both Tool JSON Schemas reject extra properties.

The seed fixture inserts one Tenant, one Project, one active Principal, one active Membership,
one mock Connection, one Agent Version, and one Tool Version before the test.

- [ ] **Step 4: README 실행 방법 갱신**

Add these commands to `README.md`:

```bash
cp .env.example .env
docker compose up -d postgres
uv sync --frozen
uv run alembic upgrade head
uv run uvicorn agent_platform.api.app:create_app --factory
uv run python -m agent_platform.worker.main
```

Do not describe Phase 2 lease/retry behavior as implemented.

- [ ] **Step 5: complete Phase 0·1 verification 실행**

Run:

```bash
uv sync --frozen
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest -q
```

Expected: all commands exit 0, no xfail, no skipped PostgreSQL integration test, and the end-to-end test reaches `COMPLETED`.

- [ ] **Step 6: schema downgrade/upgrade 검증**

Run: `uv run alembic downgrade base && uv run alembic upgrade head && uv run pytest tests/integration/test_migrations.py -q`

Expected: downgrade and upgrade exit 0 and all 15 Phase 1 tables exist again.

- [ ] **Step 7: final commit**

```bash
git add README.md examples/ai-model-release tests/e2e
git commit -m "test: verify model release vertical slice"
```

## 최종 인수 체크리스트

- [ ] `git status --short` is empty.
- [ ] `uv sync --frozen` succeeds from a fresh virtual environment.
- [ ] `uv run ruff format --check .` succeeds.
- [ ] `uv run ruff check .` succeeds.
- [ ] `uv run pyright` succeeds in strict mode.
- [ ] `uv run pytest -q` succeeds with no skipped PostgreSQL test.
- [ ] Alembic downgrade and upgrade both succeed.
- [ ] Domain has no FastAPI, Pydantic, SQLAlchemy, or Psycopg imports.
- [ ] Same idempotency key and payload return one Run ID under 100 concurrent calls.
- [ ] Same key with different payload returns `409 IDEMPOTENCY_CONFLICT`.
- [ ] API acceptance transaction creates Run, first Step, first Event, and Work Item atomically.
- [ ] Worker performs exactly one mock Tool call and stores a terminal result.
- [ ] Event sequence starts at 1 and has no gap.
- [ ] Unauthorized Tenant and Project Run lookup returns no object information.
- [ ] README describes only Phase 0·1 behavior that is actually verified.
