"""Core read-query columns. Frozen Alembic migrations own DDL and constraints.

Do not call metadata.create_all: this intentionally does not duplicate the
foreign keys, policies, triggers or grants of the versioned schema.
"""

from typing import Any

from sqlalchemy import BigInteger, Column, DateTime, MetaData, String, Table
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()


def _table(name: str, strings: str, jsons: str = "", integers: str = "", dates: str = "") -> Table:
    columns: list[Column[Any]] = [Column(item, String) for item in strings.split()]
    columns.extend(Column(item, JSONB) for item in jsons.split())
    columns.extend(Column(item, BigInteger) for item in integers.split())
    columns.extend(Column(item, DateTime(timezone=True)) for item in dates.split())
    return Table(name, metadata, *columns)


tenants = _table("tenants", "id status")
projects = _table("projects", "id tenant_id name status")
principals = _table("principals", "id tenant_id issuer subject type status")
project_memberships = _table(
    "project_memberships", "tenant_id project_id principal_id role_set_id status"
)
connections = _table("connections", "id tenant_id project_id kind config_ref credential_ref status")
agent_definitions = _table("agent_definitions", "id tenant_id project_id name")
tool_definitions = _table("tool_definitions", "id tenant_id project_id name")
agent_versions = _table(
    "agent_versions", "id tenant_id project_id definition_id digest", "spec", "version"
)
tool_versions = _table(
    "tool_versions",
    "id tenant_id project_id definition_id name schema_digest risk_tier connection_kind",
    "input_schema output_schema",
    "version",
)
runs = _table(
    "runs",
    "id tenant_id project_id principal_id agent_version_id state cancellation_outcome",
    "input result error",
    "state_version cancel_epoch",
    "created_at updated_at",
)
run_steps = _table(
    "run_steps", "id tenant_id project_id run_id kind state", "input output", "ordinal"
)
run_events = _table(
    "run_events",
    "tenant_id project_id run_id type actor",
    "payload",
    "sequence schema_version",
    "occurred_at",
)
work_items = _table(
    "work_items",
    "id tenant_id project_id run_id step_id status worker_id",
    integers="lease_token attempt_count",
    dates="available_at lease_expires_at",
)
run_attempts = _table(
    "run_attempts",
    "id tenant_id project_id run_id step_id work_id worker_id status error_code",
    integers="attempt_no lease_token",
    dates="started_at finished_at",
)
checkpoints = _table(
    "checkpoints",
    "id tenant_id project_id run_id step_id work_id attempt_id agent_version_id "
    "step_kind result_ref",
    integers="schema_version",
    dates="created_at",
)
dead_letter_items = _table(
    "dead_letter_items",
    "id tenant_id project_id run_id step_id work_id attempt_id reason_code",
    dates="created_at",
)
dead_letter_redrives = _table(
    "dead_letter_redrives",
    "id tenant_id project_id source_run_id source_dead_letter_id new_run_id "
    "principal_id idempotency_key reason",
    dates="created_at",
)
idempotency_records = _table(
    "idempotency_records", "tenant_id project_id principal_id key request_hash run_id"
)
model_calls = _table(
    "model_calls", "id tenant_id project_id run_id step_id model_route status", "response"
)
tool_calls = _table(
    "tool_calls",
    "id tenant_id project_id run_id step_id tool_version_id status",
    "arguments result",
)
usage_entries = _table(
    "usage_entries", "id tenant_id project_id run_id step_id source unit", integers="quantity"
)
tool_effects = _table(
    "tool_effects",
    "id tenant_id project_id run_id step_id tool_version_id tool_version idempotency_key "
    "request_hash status dispatch_token dispatch_attempt_id",
    "arguments result",
    dates="created_at updated_at",
)
mock_provider_results = _table(
    "mock_provider_results",
    "idempotency_key tenant_id project_id request_hash",
    "result",
    dates="created_at",
)
