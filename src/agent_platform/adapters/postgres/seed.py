"""Explicit local-example installation with an administrator connection."""

import asyncio
import json
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_platform.application.digest import canonical_digest
from agent_platform.application.ports import PrincipalContext, SeedResult
from agent_platform.contracts.agents import AgentVersionSpec
from agent_platform.contracts.tools import ToolVersionSpec


def _load_examples() -> tuple[dict[str, Any], dict[str, Any]]:
    examples = Path(__file__).resolve().parents[4] / "examples" / "ai-model-release"
    agent = AgentVersionSpec.model_validate_json(
        (examples / "agent-version.json").read_text()
    ).model_dump(mode="json")
    tool = ToolVersionSpec.model_validate_json(
        (examples / "evaluation-tool-version.json").read_text()
    ).model_dump(mode="json")
    return agent, tool


async def seed_example(
    engine: AsyncEngine,
    tenant_id: str = "demo",
    project_id: str = "demo-project",
    principal_id: str = "demo-user",
) -> SeedResult:
    agent, tool = await asyncio.to_thread(_load_examples)
    # Global primary IDs must remain distinct even when fixtures reuse display names.
    if tenant_id != "demo":
        project_id = canonical_digest({"tenant": tenant_id, "project": project_id})[:48]
        principal_id = canonical_digest({"tenant": tenant_id, "principal": principal_id})[:48]
    identity = {"tenant": tenant_id, "project": project_id}
    agent_definition = canonical_digest({**identity, "agent": "evaluation-agent"})[:48]
    tool_definition = canonical_digest({**identity, "tool": tool["name"]})[:48]
    agent_id = canonical_digest({"definition": agent_definition, "version": 1})[:48]
    tool_id = canonical_digest({"definition": tool_definition, "version": 1})[:48]
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO tenants(id,status) VALUES (:tenant,'ACTIVE') ON CONFLICT DO NOTHING"),
            identity,
        )
        await conn.execute(
            text("""
            INSERT INTO projects(id,tenant_id,name,status)
            VALUES (:project,:tenant,'Demo project','ACTIVE')
            ON CONFLICT DO NOTHING
        """),
            identity,
        )
        await conn.execute(
            text("""
            INSERT INTO principals(id,tenant_id,issuer,subject,type,status)
            VALUES (:principal,:tenant,'local-demo',:principal,'USER','ACTIVE')
            ON CONFLICT DO NOTHING
        """),
            {**identity, "principal": principal_id},
        )
        await conn.execute(
            text("""
            INSERT INTO project_memberships(tenant_id,project_id,principal_id,role_set_id,status)
            VALUES (:tenant,:project,:principal,'owner','ACTIVE') ON CONFLICT DO NOTHING
        """),
            {**identity, "principal": principal_id},
        )
        for relation, definition, name in (
            ("agent_definitions", agent_definition, "evaluation-agent"),
            ("tool_definitions", tool_definition, str(tool["name"])),
        ):
            await conn.execute(
                text(
                    f"INSERT INTO {relation}(id,tenant_id,project_id,name) "
                    "VALUES (:id,:tenant,:project,:name) ON CONFLICT DO NOTHING"
                ),
                {**identity, "id": definition, "name": name},
            )
        await conn.execute(
            text("""
            INSERT INTO agent_versions(id,tenant_id,project_id,definition_id,version,digest,spec)
            VALUES (:id,:tenant,:project,:definition,1,:digest,CAST(:spec AS jsonb))
            ON CONFLICT DO NOTHING
        """),
            {
                **identity,
                "id": agent_id,
                "definition": agent_definition,
                "digest": canonical_digest(agent),
                "spec": json.dumps(agent),
            },
        )
        await conn.execute(
            text("""
            INSERT INTO tool_versions
                                     (id,tenant_id,project_id,definition_id,name,version,schema_digest,
                                      input_schema,output_schema,risk_tier,connection_kind)
            VALUES (:id,:tenant,:project,:definition,:name,1,:digest,CAST(:input AS jsonb),
                    CAST(:output AS jsonb),:risk,:connection) ON CONFLICT DO NOTHING
        """),
            {
                **identity,
                "id": tool_id,
                "definition": tool_definition,
                "name": tool["name"],
                "digest": canonical_digest(tool),
                "input": json.dumps(tool["input_schema"]),
                "output": json.dumps(tool["output_schema"]),
                "risk": tool["risk_tier"],
                "connection": tool["connection_kind"],
            },
        )
        stored = (
            await conn.execute(
                text("SELECT spec FROM agent_versions WHERE id=:id"), {"id": agent_id}
            )
        ).scalar_one()
        if stored != agent:
            raise ValueError("Existing example version differs; publish a new version instead")
        stored_tool = (
            await conn.execute(
                text("SELECT schema_digest FROM tool_versions WHERE id=:id"), {"id": tool_id}
            )
        ).scalar_one()
        if stored_tool != canonical_digest(tool):
            raise ValueError("Existing tool version differs; publish a new version instead")
    return SeedResult(PrincipalContext(tenant_id, principal_id), project_id, agent_id)
