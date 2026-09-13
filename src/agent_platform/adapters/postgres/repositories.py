"""Step-sized durable work with explicit transaction boundaries and bound SQL.

Only mock adapters are supported in Phase 1. No lease recovery or automatic retry
is implied by a PROCESSING row. Each method owns its short database transaction.
"""

import json
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from agent_platform.adapters.postgres.database import set_context, unit_of_work
from agent_platform.application.digest import canonical_digest
from agent_platform.application.errors import (
    ExecutionScopeNotFound,
    IdempotencyConflict,
    InvalidInput,
    PolicyDenied,
    RuntimeConflict,
)
from agent_platform.application.ports import (
    AcceptedRun,
    ClaimedWork,
    CreateRunCommand,
    EventRecord,
    PrincipalContext,
    RunRecord,
)
from agent_platform.contracts.validation import validate_payload
from agent_platform.domain.runs import enforce_transition


def _json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"))


def _record(row: RowMapping) -> RunRecord:
    return RunRecord(**{name: row[name] for name in RunRecord.__dataclass_fields__})


async def _scope(connection: AsyncConnection, tenant: str, project: str, principal: str) -> bool:
    result = await connection.execute(
        text("""
        SELECT 1 FROM project_memberships m
        JOIN tenants t ON t.id=m.tenant_id AND t.status='ACTIVE'
        JOIN projects p ON p.id=m.project_id AND p.tenant_id=m.tenant_id AND p.status='ACTIVE'
        JOIN principals i ON i.id=m.principal_id AND i.tenant_id=m.tenant_id AND i.status='ACTIVE'
        WHERE m.tenant_id=:tenant AND m.project_id=:project AND m.principal_id=:principal
          AND m.status='ACTIVE' AND m.role_set_id IN ('owner','operator')
    """),
        {"tenant": tenant, "project": project, "principal": principal},
    )
    return result.first() is not None


async def _event(
    connection: AsyncConnection,
    run: RowMapping,
    target: str,
    event_type: str,
    actor: str,
    payload: dict[str, Any] | None = None,
) -> RowMapping:
    enforce_transition(str(run["state"]), target)
    updated = (
        (
            await connection.execute(
                text("""
        UPDATE runs SET state=:state,state_version=state_version+1,updated_at=now()
        WHERE id=:id AND tenant_id=:tenant AND state_version=:version RETURNING *
    """),
                {
                    "state": target,
                    "id": run["id"],
                    "tenant": run["tenant_id"],
                    "version": run["state_version"],
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if updated is None:
        raise RuntimeConflict("Run changed while recording transition")
    await connection.execute(
        text("""
        INSERT INTO run_events
          (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
        VALUES (:tenant,:project,:run,:sequence,:type,1,:actor,CAST(:payload AS jsonb))
    """),
        {
            "tenant": run["tenant_id"],
            "project": run["project_id"],
            "run": run["id"],
            "sequence": updated["state_version"],
            "type": event_type,
            "actor": actor,
            "payload": _json({"state": target, **(payload or {})}),
        },
    )
    return updated


class PostgresRunRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def check_health(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def accept_run(self, *, command: CreateRunCommand, idempotency_key: str) -> AcceptedRun:
        if not idempotency_key or len(idempotency_key) > 200:
            raise InvalidInput("Idempotency key must contain 1 to 200 characters")
        principal = command.principal
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            agent = (
                (
                    await conn.execute(
                        text("""
                SELECT * FROM agent_versions WHERE tenant_id=:tenant AND id=:id
            """),
                        {"tenant": principal.tenant_id, "id": command.agent_version_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if agent is None or not await _scope(
                conn, principal.tenant_id, str(agent["project_id"]), principal.principal_id
            ):
                raise ExecutionScopeNotFound()
            spec = cast(dict[str, Any], agent["spec"])
            if any(
                spec.get(field) is not None
                for field in ("execution_policy", "approval_policy", "compensation_policy")
            ):
                raise PolicyDenied("Agent policies are not supported in Phase 1")
            if spec.get("model_route") != "mock/release-planner-v1":
                raise PolicyDenied("Phase 1 permits the registered mock model only")
            for reference in spec["tools"]:
                name, separator, version = str(reference).rpartition(":v")
                if not separator or not version.isdigit():
                    raise PolicyDenied("Invalid registered tool version")
                registered = (
                    await conn.execute(
                        text("""
                    SELECT 1 FROM tool_versions WHERE tenant_id=:tenant AND project_id=:project
                     AND name=:name AND version=:version
                     AND connection_kind='mock' AND risk_tier='T1'
                """),
                        {
                            "tenant": principal.tenant_id,
                            "project": agent["project_id"],
                            "name": name,
                            "version": int(version),
                        },
                    )
                ).first()
                if registered is None:
                    raise PolicyDenied("Phase 1 permits registered T1 mock tools only")
            validate_payload(command.input, spec["input_schema"])
            digest = canonical_digest(
                {"agent_version_id": command.agent_version_id, "input": command.input}
            )
            run_id = uuid4().hex
            scope = {
                "tenant": principal.tenant_id,
                "project": agent["project_id"],
                "principal": principal.principal_id,
                "key": idempotency_key,
            }
            inserted = (
                await conn.execute(
                    text("""
                INSERT INTO idempotency_records
                    (tenant_id,project_id,principal_id,key,request_hash,run_id)
                VALUES (:tenant,:project,:principal,:key,:digest,:run)
                ON CONFLICT (tenant_id,project_id,principal_id,key) DO NOTHING RETURNING run_id
            """),
                    {**scope, "digest": digest, "run": run_id},
                )
            ).first()
            if inserted is None:
                previous = (
                    (
                        await conn.execute(
                            text("""
                    SELECT request_hash,run_id FROM idempotency_records
                    WHERE tenant_id=:tenant AND project_id=:project AND principal_id=:principal
                      AND key=:key
                """),
                            scope,
                        )
                    )
                    .mappings()
                    .one()
                )
                if previous["request_hash"] != digest:
                    raise IdempotencyConflict("Idempotency key was used with a different request")
                run = (
                    (
                        await conn.execute(
                            text("SELECT * FROM runs WHERE id=:id"), {"id": previous["run_id"]}
                        )
                    )
                    .mappings()
                    .one()
                )
                return AcceptedRun(_record(run), duplicate=True)
            run = (
                (
                    await conn.execute(
                        text("""
                INSERT INTO runs (id,tenant_id,project_id,principal_id,agent_version_id,
                                  state,state_version,input)
                VALUES (:id,:tenant,:project,:principal,:agent,'QUEUED',1,CAST(:input AS jsonb))
                RETURNING *
            """),
                        {
                            "id": run_id,
                            **scope,
                            "agent": command.agent_version_id,
                            "input": _json(command.input),
                        },
                    )
                )
                .mappings()
                .one()
            )
            step_id = uuid4().hex
            await self._create_step(conn, run, step_id, 1, "MODEL_CALL", command.input)
            await conn.execute(
                text("""
                INSERT INTO run_events
                  (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
                VALUES (:tenant,:project,:run,1,'RUN_ACCEPTED',1,:principal,CAST(:payload AS jsonb))
            """),
                {
                    **scope,
                    "run": run_id,
                    "payload": _json(
                        {
                            "state": "QUEUED",
                            "agent_version_id": command.agent_version_id,
                            "principal_id": principal.principal_id,
                        }
                    ),
                },
            )
            return AcceptedRun(_record(run), duplicate=False)

    async def get_run(self, principal: PrincipalContext, run_id: str) -> RunRecord:
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            return _record(await self._authorized_run(conn, principal, run_id))

    async def list_events(
        self, principal: PrincipalContext, run_id: str, after_sequence: int = 0, limit: int = 100
    ) -> list[EventRecord]:
        if after_sequence < 0 or not 1 <= limit <= 1000:
            raise InvalidInput("Invalid event cursor or page size")
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            await self._authorized_run(conn, principal, run_id)
            rows = (
                await conn.execute(
                    text("""
                SELECT sequence,type,schema_version,actor,payload,occurred_at FROM run_events
                WHERE tenant_id=:tenant AND run_id=:run AND sequence>:after
                ORDER BY sequence LIMIT :limit
            """),
                    {
                        "tenant": principal.tenant_id,
                        "run": run_id,
                        "after": after_sequence,
                        "limit": limit,
                    },
                )
            ).mappings()
            return [EventRecord(**dict(row)) for row in rows]

    async def _authorized_run(
        self, conn: AsyncConnection, principal: PrincipalContext, run_id: str
    ) -> RowMapping:
        run = (
            (
                await conn.execute(
                    text("SELECT * FROM runs WHERE id=:id AND tenant_id=:tenant"),
                    {"id": run_id, "tenant": principal.tenant_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if run is None or not await _scope(
            conn, principal.tenant_id, str(run["project_id"]), principal.principal_id
        ):
            raise ExecutionScopeNotFound()
        return run

    async def claim_work(self) -> ClaimedWork | None:
        while True:
            work, skipped = await self._claim_once()
            if not skipped:
                return work

    async def _claim_once(self) -> tuple[ClaimedWork | None, bool]:
        async with self.engine.begin() as conn:
            claim = (
                (await conn.execute(text("SELECT * FROM public.claim_runtime_work()")))
                .mappings()
                .one_or_none()
            )
            if claim is None:
                return None, False
            await set_context(conn, str(claim["scope_tenant"]))
            row = (
                (
                    await conn.execute(
                        text("""
                SELECT w.id,w.tenant_id,w.project_id,w.run_id,w.step_id,s.kind,s.input,
                       r.principal_id,a.spec AS agent_spec
                FROM work_items w JOIN run_steps s ON s.id=w.step_id
                JOIN runs r ON r.id=w.run_id JOIN agent_versions a ON a.id=r.agent_version_id
                WHERE w.id=:id AND w.status='PROCESSING' AND s.state='READY'
            """),
                        {"id": claim["work_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise RuntimeConflict("Claimed work has inconsistent step state")
            await set_context(conn, str(row["tenant_id"]), str(row["principal_id"]))
            work = ClaimedWork(**dict(row))
            run = (
                (
                    await conn.execute(
                        text("SELECT * FROM runs WHERE id=:run FOR UPDATE"), {"run": work.run_id}
                    )
                )
                .mappings()
                .one()
            )
            if not await _scope(conn, work.tenant_id, work.project_id, work.principal_id):
                await self._close_failure(
                    conn, work, run, "POLICY_DENIED", "Execution permission revoked", False
                )
                return None, True
            run = await _event(
                conn, run, "RUNNING", "WORK_CLAIMED", "worker", {"step_id": work.step_id}
            )
            await conn.execute(
                text("UPDATE run_steps SET state='RUNNING' WHERE id=:id"), {"id": work.step_id}
            )
            if work.kind == "MODEL_CALL":
                await conn.execute(
                    text("""
                    INSERT INTO model_calls
                      (id,tenant_id,project_id,run_id,step_id,model_route,status)
                    VALUES (:id,:tenant,:project,:run,:step,:route,'DISPATCHED')
                """),
                    {
                        "id": uuid4().hex,
                        "tenant": work.tenant_id,
                        "project": work.project_id,
                        "run": work.run_id,
                        "step": work.step_id,
                        "route": work.agent_spec["model_route"],
                    },
                )
                await _event(
                    conn,
                    run,
                    "WAITING_MODEL",
                    "MODEL_DISPATCHED",
                    "worker",
                    {"step_id": work.step_id},
                )
            elif work.kind == "TOOL_CALL":
                tool = (
                    (
                        await conn.execute(
                            text("""
                    SELECT t.* FROM tool_calls c JOIN tool_versions t ON t.id=c.tool_version_id
                    WHERE c.step_id=:step AND c.status='PENDING'
                """),
                            {"step": work.step_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if tool is None:
                    raise RuntimeConflict("Tool intent missing")
                await conn.execute(
                    text("UPDATE tool_calls SET status='DISPATCHED' WHERE step_id=:id"),
                    {"id": work.step_id},
                )
                work = ClaimedWork(
                    **dict(row), tool_version_id=str(tool["id"]), tool_spec=dict(tool)
                )
            else:
                raise RuntimeConflict("Unsupported step kind")
            return work, False

    async def _active(
        self, conn: AsyncConnection, work: ClaimedWork, expected: str | None
    ) -> RowMapping:
        run = (
            (
                await conn.execute(
                    text("""
            SELECT r.* FROM runs r JOIN work_items w ON w.run_id=r.id
            JOIN run_steps s ON s.id=w.step_id
            WHERE r.id=:run AND r.tenant_id=:tenant AND r.project_id=:project
              AND r.principal_id=:principal AND w.id=:work AND w.step_id=:step
              AND w.status='PROCESSING' AND s.state='RUNNING'
            FOR UPDATE OF r,w,s
        """),
                    {
                        "run": work.run_id,
                        "tenant": work.tenant_id,
                        "project": work.project_id,
                        "principal": work.principal_id,
                        "work": work.id,
                        "step": work.step_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if run is None or run["state"] not in {"RUNNING", "WAITING_MODEL"}:
            raise RuntimeConflict("Work is no longer active")
        if expected is not None and run["state"] != expected:
            raise RuntimeConflict("Work is in an unexpected state")
        return run

    async def complete_model(self, work: ClaimedWork, decision: dict[str, Any]) -> None:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, "WAITING_MODEL")
            reference = decision.get("tool_version")
            arguments = decision.get("arguments")
            if not isinstance(reference, str) or not isinstance(arguments, dict):
                raise InvalidInput("Model response must contain a tool and arguments")
            if reference not in work.agent_spec["tools"]:
                raise PolicyDenied("Model selected an unregistered tool")
            name, separator, version_text = reference.rpartition(":v")
            if not separator or not version_text.isdigit():
                raise InvalidInput("Invalid tool version")
            tool = (
                (
                    await conn.execute(
                        text("""
                SELECT * FROM tool_versions WHERE tenant_id=:tenant AND project_id=:project
                  AND name=:name AND version=:version
            """),
                        {
                            "tenant": work.tenant_id,
                            "project": work.project_id,
                            "name": name,
                            "version": int(version_text),
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if tool is None:
                raise PolicyDenied("Tool version is not registered in the run project")
            if tool["connection_kind"] != "mock":
                raise PolicyDenied("Phase 1 permits mock tools only")
            validate_payload(
                cast(dict[str, Any], arguments), cast(dict[str, Any], tool["input_schema"])
            )
            await conn.execute(
                text("""
                UPDATE model_calls SET response=CAST(:response AS jsonb),status='SUCCEEDED'
                WHERE step_id=:step AND status='DISPATCHED'
            """),
                {"response": _json(decision), "step": work.step_id},
            )
            await self._finish_step(conn, work, decision)
            await self._usage(conn, work, "MODEL")
            run = await _event(
                conn, run, "RUNNING", "MODEL_RECORDED", "worker", {"step_id": work.step_id}
            )
            step_id = uuid4().hex
            await self._create_step(
                conn, run, step_id, 2, "TOOL_CALL", cast(dict[str, Any], arguments)
            )
            await conn.execute(
                text("""
                INSERT INTO tool_calls
                  (id,tenant_id,project_id,run_id,step_id,tool_version_id,status,arguments)
                VALUES (:id,:tenant,:project,:run,:step,:tool,'PENDING',CAST(:arguments AS jsonb))
            """),
                {
                    "id": uuid4().hex,
                    "tenant": work.tenant_id,
                    "project": work.project_id,
                    "run": work.run_id,
                    "step": step_id,
                    "tool": tool["id"],
                    "arguments": _json(arguments),
                },
            )
            run = await _event(
                conn, run, "WAITING_TOOL", "TOOL_PROPOSED", "worker", {"step_id": step_id}
            )
            await _event(conn, run, "QUEUED", "TOOL_AUTO_SCHEDULED", "worker", {"step_id": step_id})

    async def complete_tool(self, work: ClaimedWork, result: dict[str, Any]) -> None:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, "RUNNING")
            tool = (
                (
                    await conn.execute(
                        text("""
                SELECT t.output_schema FROM tool_calls c
                JOIN tool_versions t ON t.id=c.tool_version_id
                WHERE c.step_id=:step AND c.status='DISPATCHED'
            """),
                        {"step": work.step_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if tool is None or work.kind != "TOOL_CALL":
                raise RuntimeConflict("Dispatched tool call missing")
            validate_payload(result, cast(dict[str, Any], tool["output_schema"]))
            await conn.execute(
                text("""
                UPDATE tool_calls SET result=CAST(:result AS jsonb),status='SUCCEEDED'
                WHERE step_id=:step
            """),
                {"result": _json(result), "step": work.step_id},
            )
            await self._finish_step(conn, work, result)
            await self._usage(conn, work, "TOOL")
            pending = (
                await conn.execute(
                    text("""
                SELECT count(*) FROM run_steps WHERE run_id=:run
                AND state NOT IN ('SUCCEEDED','SKIPPED')
            """),
                    {"run": work.run_id},
                )
            ).scalar_one()
            if pending:
                raise RuntimeConflict("Run still has unfinished steps")
            await conn.execute(
                text("UPDATE runs SET result=CAST(:result AS jsonb) WHERE id=:run"),
                {"result": _json(result), "run": work.run_id},
            )
            await _event(
                conn, run, "COMPLETED", "RUN_COMPLETED", "worker", {"step_id": work.step_id}
            )

    async def fail_work(
        self, work: ClaimedWork, code: str, message: str, *, timed_out: bool = False
    ) -> None:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, None)
            await self._close_failure(conn, work, run, code, message, timed_out)

    async def _close_failure(
        self,
        conn: AsyncConnection,
        work: ClaimedWork,
        run: RowMapping,
        code: str,
        message: str,
        timed_out: bool,
    ) -> None:
        await conn.execute(
            text("""
            UPDATE run_steps SET state=CASE WHEN id=:step THEN 'FAILED' ELSE 'SKIPPED' END
            WHERE run_id=:run AND state IN ('READY','PENDING','RUNNING')
        """),
            {"step": work.step_id, "run": work.run_id},
        )
        await conn.execute(
            text("""
            UPDATE work_items SET status='FAILED'
            WHERE run_id=:run AND status IN ('READY','PROCESSING')
        """),
            {"run": work.run_id},
        )
        for relation in ("model_calls", "tool_calls"):
            await conn.execute(
                text(
                    f"UPDATE {relation} SET status='FAILED' "
                    "WHERE run_id=:run AND status IN ('PENDING','DISPATCHED')"
                ),
                {"run": work.run_id},
            )
        error = {"code": code, "message": message}
        await conn.execute(
            text("UPDATE runs SET error=CAST(:error AS jsonb) WHERE id=:run"),
            {"error": _json(error), "run": work.run_id},
        )
        target = "TIMED_OUT" if timed_out else "FAILED"
        await _event(
            conn, run, target, "RUN_TIMED_OUT" if timed_out else "RUN_FAILED", "worker", error
        )

    async def _create_step(
        self,
        conn: AsyncConnection,
        run: RowMapping,
        step_id: str,
        ordinal: int,
        kind: str,
        payload: dict[str, Any],
    ) -> None:
        params = {
            "id": step_id,
            "tenant": run["tenant_id"],
            "project": run["project_id"],
            "run": run["id"],
            "ordinal": ordinal,
            "kind": kind,
            "input": _json(payload),
        }
        await conn.execute(
            text("""
            INSERT INTO run_steps (id,tenant_id,project_id,run_id,ordinal,kind,state,input)
            VALUES (:id,:tenant,:project,:run,:ordinal,:kind,'READY',CAST(:input AS jsonb))
        """),
            params,
        )
        await conn.execute(
            text("""
            INSERT INTO work_items (id,tenant_id,project_id,run_id,step_id,status)
            VALUES (:work,:tenant,:project,:run,:id,'READY')
        """),
            {**params, "work": uuid4().hex},
        )

    async def _finish_step(
        self, conn: AsyncConnection, work: ClaimedWork, output: dict[str, Any]
    ) -> None:
        await conn.execute(
            text(
                "UPDATE run_steps SET state='SUCCEEDED',output=CAST(:output AS jsonb) WHERE id=:id"
            ),
            {"id": work.step_id, "output": _json(output)},
        )
        await conn.execute(
            text("UPDATE work_items SET status='DONE' WHERE id=:id"), {"id": work.id}
        )

    async def _usage(self, conn: AsyncConnection, work: ClaimedWork, source: str) -> None:
        await conn.execute(
            text("""
            INSERT INTO usage_entries (id,tenant_id,project_id,run_id,step_id,source,quantity,unit)
            VALUES (:id,:tenant,:project,:run,:step,:source,1,'call')
        """),
            {
                "id": uuid4().hex,
                "tenant": work.tenant_id,
                "project": work.project_id,
                "run": work.run_id,
                "step": work.step_id,
                "source": source,
            },
        )
