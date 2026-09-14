"""Step-sized durable work with explicit transaction boundaries and bound SQL.

Leased attempts fence stale workers. Only registered deterministic mocks may replay.
Lock ordering is always work item, run, then step; each operation is a short transaction.
"""

import json
import math
import random
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
    DeadLetterRecord,
    EffectDispatch,
    EventRecord,
    PrincipalContext,
    RunRecord,
)
from agent_platform.contracts.validation import validate_payload
from agent_platform.domain.runs import TERMINAL_STATES, enforce_transition


def _json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"))


def _record(row: RowMapping) -> RunRecord:
    return RunRecord(**{name: row[name] for name in RunRecord.__dataclass_fields__ if name in row})


class _WorkSetChanged(Exception):
    """Restart a short transaction if Model completion inserted the next Work."""


def _dispatch(row: RowMapping) -> EffectDispatch:
    return EffectDispatch(**{name: row[name] for name in EffectDispatch.__dataclass_fields__})


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
    target: str | None,
    event_type: str,
    actor: str,
    payload: dict[str, Any] | None = None,
) -> RowMapping:
    if target is None:
        # Facts such as dispatch need a durable sequence without inventing a
        # Run transition (RUNNING -> RUNNING is deliberately not a legal edge).
        target = str(run["state"])
    else:
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
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30,
        max_attempts: int = 3,
        retry_base_seconds: float = 1,
    ) -> None:
        if not math.isfinite(lease_seconds) or not 0 < lease_seconds <= 3600:
            raise ValueError("lease_seconds must be finite and between 0 and 3600")
        if max_attempts < 1 or not math.isfinite(retry_base_seconds) or retry_base_seconds <= 0:
            raise ValueError("Positive max_attempts and finite retry_base_seconds required")
        if worker_id is not None and not 1 <= len(worker_id) <= 200:
            raise ValueError("worker_id must contain 1 to 200 characters")
        self.engine = engine
        self.worker_id = worker_id if worker_id is not None else uuid4().hex
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.retry_base_seconds = retry_base_seconds

    async def check_health(self) -> None:
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def accept_run(self, *, command: CreateRunCommand, idempotency_key: str) -> AcceptedRun:
        if not idempotency_key or len(idempotency_key) > 200:
            raise InvalidInput("Idempotency key must contain 1 to 200 characters")
        principal = command.principal
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            agent = await self._validate_acceptance(conn, command)
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
            run = await self._create_run(conn, command, agent, run_id)
            return AcceptedRun(_record(run), duplicate=False)

    async def _validate_acceptance(
        self, conn: AsyncConnection, command: CreateRunCommand
    ) -> RowMapping:
        principal = command.principal
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
                  AND name=:name AND version=:version AND connection_kind='mock' AND risk_tier='T1'
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
        return agent

    async def _create_run(
        self, conn: AsyncConnection, command: CreateRunCommand, agent: RowMapping, run_id: str
    ) -> RowMapping:
        scope = {
            "tenant": command.principal.tenant_id,
            "project": agent["project_id"],
            "principal": command.principal.principal_id,
        }
        run = (
            (
                await conn.execute(
                    text("""
            INSERT INTO runs(id,tenant_id,project_id,principal_id,agent_version_id,
                             state,state_version,input)
            VALUES(:id,:tenant,:project,:principal,:agent,'QUEUED',1,CAST(:input AS jsonb))
            RETURNING *
        """),
                    {
                        **scope,
                        "id": run_id,
                        "agent": command.agent_version_id,
                        "input": _json(command.input),
                    },
                )
            )
            .mappings()
            .one()
        )
        await self._create_step(conn, run, uuid4().hex, 1, "MODEL_CALL", command.input)
        await conn.execute(
            text("""
            INSERT INTO run_events
              (tenant_id,project_id,run_id,sequence,type,schema_version,actor,payload)
            VALUES(:tenant,:project,:run,1,'RUN_ACCEPTED',1,:principal,CAST(:payload AS jsonb))
        """),
            {
                **scope,
                "run": run_id,
                "payload": _json(
                    {
                        "state": "QUEUED",
                        "agent_version_id": command.agent_version_id,
                        "principal_id": command.principal.principal_id,
                    }
                ),
            },
        )
        return run

    async def list_dead_letters(
        self, principal: PrincipalContext, run_id: str, after_id: str = "", limit: int = 100
    ) -> list[DeadLetterRecord]:
        if len(after_id) > 64 or not 1 <= limit <= 100:
            raise InvalidInput("Invalid dead-letter cursor or page size")
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            await self._authorized_run(conn, principal, run_id)
            rows = (
                (
                    await conn.execute(
                        text("""
                SELECT d.id,d.run_id,d.step_id,d.attempt_id,d.reason_code,d.created_at,
                  r.new_run_id AS redriven_run_id FROM dead_letter_items d
                LEFT JOIN dead_letter_redrives r ON r.source_dead_letter_id=d.id
                  AND r.tenant_id=d.tenant_id AND r.project_id=d.project_id
                WHERE d.tenant_id=:tenant AND d.run_id=:run AND d.id>:after
                ORDER BY d.id LIMIT :limit
            """),
                        {
                            "tenant": principal.tenant_id,
                            "run": run_id,
                            "after": after_id,
                            "limit": limit,
                        },
                    )
                )
                .mappings()
                .all()
            )
            return [DeadLetterRecord(**dict(row)) for row in rows]

    async def redrive_dead_letter(
        self,
        principal: PrincipalContext,
        run_id: str,
        item_id: str,
        *,
        idempotency_key: str,
        reason: str,
    ) -> AcceptedRun:
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise InvalidInput("Idempotency key must contain 1 to 200 characters")
        if reason not in {"WORKER_RECOVERED", "TRANSIENT_FAILURE_RESOLVED"}:
            raise InvalidInput("Unsupported redrive reason")
        while True:
            try:
                async with unit_of_work(
                    self.engine, principal.tenant_id, principal.principal_id
                ) as conn:
                    source = await self._locked_run(conn, principal, run_id)
                    item = (
                        (
                            await conn.execute(
                                text("""
                        SELECT d.*,w.status AS work_status FROM dead_letter_items d
                        JOIN work_items w ON w.id=d.work_id AND w.run_id=d.run_id
                        WHERE d.tenant_id=:tenant AND d.run_id=:run AND d.id=:item
                    """),
                                {"tenant": principal.tenant_id, "run": run_id, "item": item_id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if item is None:
                        raise ExecutionScopeNotFound()
                    previous = (
                        (
                            await conn.execute(
                                text("""
                        SELECT * FROM dead_letter_redrives WHERE source_dead_letter_id=:item
                    """),
                                {"item": item_id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if previous is not None:
                        if (
                            previous["principal_id"] != principal.principal_id
                            or previous["idempotency_key"] != idempotency_key
                            or previous["reason"] != reason
                        ):
                            raise IdempotencyConflict("Dead letter already has a redrive")
                        child = await self._authorized_run(
                            conn, principal, str(previous["new_run_id"])
                        )
                        return AcceptedRun(_record(child), duplicate=True)
                    await self._validate_redrive(conn, source, item)
                    command = CreateRunCommand(
                        principal,
                        str(source["agent_version_id"]),
                        cast(dict[str, Any], source["input"]),
                    )
                    agent = await self._validate_acceptance(conn, command)
                    child = await self._create_run(conn, command, agent, uuid4().hex)
                    link = (
                        await conn.execute(
                            text("""
                        INSERT INTO dead_letter_redrives
                          (id,tenant_id,project_id,source_run_id,source_dead_letter_id,new_run_id,
                           principal_id,idempotency_key,reason)
                        VALUES(:id,:tenant,:project,:source,:item,:child,:principal,:key,:reason)
                        ON CONFLICT DO NOTHING RETURNING id
                    """),
                            {
                                "id": uuid4().hex,
                                "tenant": principal.tenant_id,
                                "project": source["project_id"],
                                "source": run_id,
                                "item": item_id,
                                "child": child["id"],
                                "principal": principal.principal_id,
                                "key": idempotency_key,
                                "reason": reason,
                            },
                        )
                    ).first()
                    if link is None:
                        # The DLQ lock serializes identical-source requests. A conflict
                        # here means this scoped key belongs to another source operation.
                        # Raising rolls back the provisional child and its initial Work.
                        raise IdempotencyConflict("Redrive key was used for another request")
                    payload = {
                        "source_run_id": run_id,
                        "source_dead_letter_id": item_id,
                        "new_run_id": child["id"],
                        "reason": reason,
                    }
                    await _event(
                        conn, source, None, "DEAD_LETTER_REDRIVEN", principal.principal_id, payload
                    )
                    child = await _event(
                        conn, child, None, "RUN_REDRIVEN", principal.principal_id, payload
                    )
                    return AcceptedRun(_record(child), duplicate=False)
            except _WorkSetChanged:
                continue

    async def _validate_redrive(
        self, conn: AsyncConnection, source: RowMapping, item: RowMapping
    ) -> None:
        error = cast(dict[str, Any], source["error"] or {})
        if (
            source["state"] != "FAILED"
            or source["cancel_epoch"]
            or error.get("code") != "RETRY_EXHAUSTED"
            or item["reason_code"] != "RETRY_EXHAUSTED"
            or item["work_status"] != "FAILED"
        ):
            raise RuntimeConflict("Dead letter is not eligible for redrive")
        unsafe = (
            await conn.execute(
                text("""
            SELECT 1 WHERE EXISTS(SELECT 1 FROM work_items WHERE run_id=:run
              AND status IN ('READY','PROCESSING','OUTCOME_UNKNOWN'))
            OR EXISTS(SELECT 1 FROM tool_effects WHERE run_id=:run
              AND (dispatch_token IS NOT NULL OR status IN
                ('DISPATCHED','SUCCEEDED','OUTCOME_UNKNOWN')))
            OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=:run
              AND (status IN ('DISPATCHED','SUCCEEDED','OUTCOME_UNKNOWN') OR result IS NOT NULL))
            OR EXISTS(SELECT 1 FROM run_events WHERE run_id=:run AND type='TOOL_DISPATCHED')
            OR EXISTS(SELECT 1 FROM model_calls WHERE run_id=:run
              AND model_route<>'mock/release-planner-v1')
        """),
                {"run": source["id"]},
            )
        ).first()
        if unsafe is not None:
            raise RuntimeConflict("Run has active work or an unsafe execution history")

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
                (
                    await conn.execute(
                        text("SELECT * FROM public.claim_runtime_work(:owner,:seconds)"),
                        {"owner": self.worker_id, "seconds": self.lease_seconds},
                    )
                )
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
                       r.principal_id,r.agent_version_id,a.spec AS agent_spec,
                       w.worker_id,w.lease_token,w.attempt_count AS attempt_no
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
            work = ClaimedWork(**dict(row), attempt_id=uuid4().hex)
            run = (
                (
                    await conn.execute(
                        text("SELECT * FROM runs WHERE id=:run FOR UPDATE"), {"run": work.run_id}
                    )
                )
                .mappings()
                .one()
            )
            await conn.execute(
                text("""
                INSERT INTO run_attempts
                  (id,tenant_id,project_id,run_id,step_id,work_id,attempt_no,worker_id,
                   lease_token,status)
                VALUES (:attempt,:tenant,:project,:run,:step,:work,:attempt_no,:owner,
                        :token,'RUNNING')
                """),
                self._lease_params(work),
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
                    ON CONFLICT (tenant_id,project_id,run_id,step_id)
                    DO UPDATE SET status='DISPATCHED'
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
                work = ClaimedWork(
                    **dict(row),
                    attempt_id=work.attempt_id,
                    tool_version_id=str(tool["id"]),
                    tool_spec=dict(tool),
                )
            else:
                raise RuntimeConflict("Unsupported step kind")
            return work, False

    async def _active(
        self, conn: AsyncConnection, work: ClaimedWork, expected: str | None
    ) -> RowMapping:
        # Acquire the work lock first, then evaluate expiry in a separate statement:
        # a lease can expire while this transaction is waiting for that lock.
        await conn.execute(
            text("SELECT id FROM work_items WHERE id=:work FOR UPDATE"), {"work": work.id}
        )
        active = (
            await conn.execute(
                text("""
            SELECT 1 FROM work_items w JOIN run_attempts a ON a.work_id=w.id
            WHERE w.id=:work AND w.tenant_id=:tenant AND w.project_id=:project
              AND w.run_id=:run AND w.step_id=:step AND w.status='PROCESSING'
              AND w.worker_id=:owner AND w.lease_token=:token
              AND w.lease_expires_at>clock_timestamp()
              AND a.id=:attempt AND a.status='RUNNING' AND a.lease_token=w.lease_token
        """),
                self._lease_params(work),
            )
        ).first()
        if active is None:
            raise RuntimeConflict("Work lease is no longer active")
        run = (
            (
                await conn.execute(
                    text("""
            SELECT r.* FROM runs r JOIN work_items w ON w.run_id=r.id
            JOIN run_steps s ON s.id=w.step_id
            WHERE r.id=:run AND r.tenant_id=:tenant AND r.project_id=:project
              AND r.principal_id=:principal AND w.id=:work AND w.step_id=:step
              AND w.status='PROCESSING' AND s.state='RUNNING'
            FOR UPDATE OF r
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
        if run is None or run["state"] not in {"RUNNING", "WAITING_MODEL"} or run["cancel_epoch"]:
            raise RuntimeConflict("Work is no longer active")
        if expected is not None and run["state"] != expected:
            raise RuntimeConflict("Work is in an unexpected state")
        await conn.execute(
            text("SELECT id FROM run_steps WHERE id=:step FOR UPDATE"), {"step": work.step_id}
        )
        return run

    @staticmethod
    def _lease_params(work: ClaimedWork) -> dict[str, Any]:
        return {
            "work": work.id,
            "tenant": work.tenant_id,
            "project": work.project_id,
            "run": work.run_id,
            "step": work.step_id,
            "owner": work.worker_id,
            "token": work.lease_token,
            "attempt": work.attempt_id,
            "attempt_no": work.attempt_no,
        }

    async def heartbeat(self, work: ClaimedWork) -> bool:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            await conn.execute(
                text("SELECT id FROM work_items WHERE id=:work FOR UPDATE"), {"work": work.id}
            )
            updated = await conn.execute(
                text("""
                UPDATE work_items
                SET lease_expires_at=clock_timestamp()+:seconds*interval '1 second'
                WHERE id=:work AND tenant_id=:tenant AND project_id=:project AND run_id=:run
                  AND step_id=:step AND worker_id=:owner AND lease_token=:token
                  AND status='PROCESSING' AND lease_expires_at>clock_timestamp()
                  AND EXISTS(SELECT 1 FROM run_attempts a WHERE a.id=:attempt
                    AND a.work_id=:work AND a.status='RUNNING' AND a.lease_token=:token)
                RETURNING id
            """),
                {**self._lease_params(work), "seconds": self.lease_seconds},
            )
            return updated.first() is not None

    async def recover_expired(self, limit: int = 100) -> int:
        if not 1 <= limit <= 1000:
            raise InvalidInput("Recovery limit must be between 1 and 1000")
        recovered = 0
        # One work lock per transaction avoids holding one run lock while waiting
        # for another Work, including when concurrent workers reap different runs.
        for _ in range(limit):
            async with self.engine.begin() as conn:
                claim = (
                    (await conn.execute(text("SELECT * FROM public.expired_runtime_work(1)")))
                    .mappings()
                    .one_or_none()
                )
                if claim is None:
                    break
                await set_context(conn, str(claim["scope_tenant"]))
                row = (
                    (
                        await conn.execute(
                            text("""
                    SELECT w.id,w.tenant_id,w.project_id,w.run_id,w.step_id,s.kind,s.input,
                      r.principal_id,r.agent_version_id,a.spec AS agent_spec,
                      w.worker_id,w.lease_token,w.attempt_count AS attempt_no,x.id AS attempt_id
                    FROM work_items w JOIN run_steps s ON s.id=w.step_id
                    JOIN runs r ON r.id=w.run_id JOIN agent_versions a ON a.id=r.agent_version_id
                    JOIN run_attempts x ON x.work_id=w.id AND x.lease_token=w.lease_token
                    WHERE w.id=:work AND w.status='PROCESSING'
                      AND w.lease_expires_at<=clock_timestamp() AND x.status='RUNNING'
                """),
                            {"work": claim["work_id"]},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise RuntimeConflict("Expired work has inconsistent attempt state")
                work = ClaimedWork(**dict(row))
                await set_context(conn, work.tenant_id, work.principal_id)
                run = (
                    (
                        await conn.execute(
                            text("SELECT * FROM runs WHERE id=:run FOR UPDATE"),
                            {"run": work.run_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                await conn.execute(
                    text("SELECT id FROM run_steps WHERE id=:step FOR UPDATE"),
                    {"step": work.step_id},
                )
                if run["state"] not in {"RUNNING", "WAITING_MODEL"}:
                    raise RuntimeConflict("Expired work has inconsistent run state")
                await self._reschedule(
                    conn, work, run, "LEASE_EXPIRED", "Worker lease expired", expired=True
                )
                recovered += 1
        return recovered

    async def retry_work(self, work: ClaimedWork, code: str, message: str) -> None:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, None)
            await self._reschedule(conn, work, run, code, message, expired=False)

    async def _effect_for_work(self, conn: AsyncConnection, work: ClaimedWork) -> RowMapping:
        effect = (
            (
                await conn.execute(
                    text("""
            SELECT * FROM tool_effects WHERE tenant_id=:tenant AND project_id=:project
              AND run_id=:run AND step_id=:step FOR UPDATE
        """),
                    self._lease_params(work),
                )
            )
            .mappings()
            .one_or_none()
        )
        if effect is None or work.kind != "TOOL_CALL":
            raise RuntimeConflict("Tool effect missing")
        return effect

    async def begin_tool_dispatch(self, work: ClaimedWork) -> EffectDispatch:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, "RUNNING")
            if not await _scope(conn, work.tenant_id, work.project_id, work.principal_id):
                raise PolicyDenied("Execution permission revoked")
            effect = await self._effect_for_work(conn, work)
            if effect["status"] not in {"PREPARED", "DISPATCHED"}:
                raise RuntimeConflict("Effect cannot be dispatched")
            if effect["status"] == "DISPATCHED":
                if effect["dispatch_attempt_id"] == work.attempt_id:
                    raise RuntimeConflict("Attempt already dispatched")
                if effect["tool_version"] != "evaluation.run_suite:v1":
                    raise RuntimeConflict("Unknown effect cannot be blindly replayed")
            if (
                work.tool_version_id != effect["tool_version_id"]
                or work.input != effect["arguments"]
            ):
                raise RuntimeConflict("Tool effect intent changed")
            digest = canonical_digest(
                {"tool_version": effect["tool_version"], "arguments": effect["arguments"]}
            )
            if digest != effect["request_hash"]:
                raise RuntimeConflict("Tool effect digest mismatch")
            live = (
                await conn.execute(
                    text("""
                SELECT 1 FROM work_items WHERE id=:work AND status='PROCESSING'
                  AND lease_expires_at>clock_timestamp() AND lease_token=:token
            """),
                    self._lease_params(work),
                )
            ).first()
            if live is None:
                raise RuntimeConflict("Work lease expired before dispatch")
            effect = (
                (
                    await conn.execute(
                        text("""
                UPDATE tool_effects SET status='DISPATCHED',dispatch_token=:dispatch,
                  dispatch_attempt_id=:attempt,updated_at=clock_timestamp()
                WHERE id=:id RETURNING *
            """),
                        {"id": effect["id"], "dispatch": uuid4().hex, "attempt": work.attempt_id},
                    )
                )
                .mappings()
                .one()
            )
            await conn.execute(
                text("UPDATE tool_calls SET status='DISPATCHED' WHERE step_id=:step"),
                {"step": work.step_id},
            )
            await _event(
                conn,
                run,
                None,
                "TOOL_DISPATCHED",
                "worker",
                {
                    "effect_id": effect["id"],
                    "attempt_id": work.attempt_id,
                    "dispatch_token": effect["dispatch_token"],
                },
            )
            return _dispatch(effect)

    async def mark_tool_unknown(self, work: ClaimedWork) -> None:
        async with unit_of_work(self.engine, work.tenant_id, work.principal_id) as conn:
            run = await self._active(conn, work, "RUNNING")
            effect = await self._effect_for_work(conn, work)
            if effect["status"] != "DISPATCHED" or effect["dispatch_attempt_id"] != work.attempt_id:
                raise RuntimeConflict("Effect is not dispatched by this attempt")
            await self._end_lease(conn, work, "OUTCOME_UNKNOWN", "OUTCOME_UNKNOWN")
            await self._unknown_effect(conn, work, run)

    async def _unknown_effect(
        self, conn: AsyncConnection, work: ClaimedWork, run: RowMapping
    ) -> None:
        for relation in ("tool_effects", "tool_calls"):
            await conn.execute(
                text(f"UPDATE {relation} SET status='OUTCOME_UNKNOWN' WHERE step_id=:step"),
                {"step": work.step_id},
            )
        await conn.execute(
            text("UPDATE run_steps SET state='OUTCOME_UNKNOWN' WHERE id=:step"),
            {"step": work.step_id},
        )
        await conn.execute(
            text("""
            INSERT INTO dead_letter_items
              (id,tenant_id,project_id,run_id,step_id,work_id,attempt_id,reason_code)
            VALUES (:id,:tenant,:project,:run,:step,:work,:attempt,'OUTCOME_UNKNOWN')
            ON CONFLICT(work_id) DO NOTHING
        """),
            {**self._lease_params(work), "id": uuid4().hex},
        )
        await conn.execute(
            text("UPDATE runs SET error=CAST(:error AS jsonb) WHERE id=:run"),
            {
                "run": work.run_id,
                "error": _json(
                    {
                        "code": "OUTCOME_UNKNOWN",
                        "message": "Tool outcome requires provider reconciliation",
                    }
                ),
            },
        )
        await _event(
            conn, run, "OUTCOME_UNKNOWN", "RUN_OUTCOME_UNKNOWN", "worker", {"step_id": work.step_id}
        )

    async def _locked_run(
        self, conn: AsyncConnection, principal: PrincipalContext, run_id: str
    ) -> RowMapping:
        await self._authorized_run(conn, principal, run_id)
        locked_ids = (
            (
                await conn.execute(
                    text("""
            SELECT id FROM work_items WHERE run_id=:run ORDER BY id FOR UPDATE
        """),
                    {"run": run_id},
                )
            )
            .scalars()
            .all()
        )
        run = (
            (
                await conn.execute(
                    text("SELECT * FROM runs WHERE id=:run FOR UPDATE"), {"run": run_id}
                )
            )
            .mappings()
            .one()
        )
        current_ids = (
            (
                await conn.execute(
                    text("""
            SELECT id FROM work_items WHERE run_id=:run ORDER BY id
        """),
                    {"run": run_id},
                )
            )
            .scalars()
            .all()
        )
        if current_ids != locked_ids:
            # Do not acquire new Work while holding Run: a claimant may hold it
            # and wait on our Run. Roll back, then acquire the entire new set.
            raise _WorkSetChanged()
        if not await _scope(
            conn, principal.tenant_id, str(run["project_id"]), principal.principal_id
        ):
            raise ExecutionScopeNotFound()
        await conn.execute(
            text("SELECT id FROM run_steps WHERE run_id=:run ORDER BY id FOR UPDATE"),
            {"run": run_id},
        )
        await conn.execute(
            text("SELECT id FROM tool_effects WHERE run_id=:run ORDER BY id FOR UPDATE"),
            {"run": run_id},
        )
        return run

    async def cancel_run(self, principal: PrincipalContext, run_id: str) -> RunRecord:
        while True:
            try:
                async with unit_of_work(
                    self.engine, principal.tenant_id, principal.principal_id
                ) as conn:
                    run = await self._locked_run(conn, principal, run_id)
                    if run["state"] in TERMINAL_STATES or run["cancel_epoch"]:
                        return _record(run)
                    effects = (
                        (
                            await conn.execute(
                                text("SELECT * FROM tool_effects WHERE run_id=:run"),
                                {"run": run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    uncertain = any(
                        e["status"] in {"DISPATCHED", "OUTCOME_UNKNOWN"} for e in effects
                    )
                    target = "OUTCOME_UNKNOWN" if uncertain else "CANCELLED"
                    run = (
                        (
                            await conn.execute(
                                text("""
                        UPDATE runs SET cancel_epoch=cancel_epoch+1,cancellation_outcome=:outcome
                        WHERE id=:run RETURNING *
                    """),
                                {
                                    "run": run_id,
                                    "outcome": "OUTCOME_UNKNOWN" if uncertain else "NO_EFFECT",
                                },
                            )
                        )
                        .mappings()
                        .one()
                    )
                    await conn.execute(
                        text("""
                        UPDATE work_items SET status=:target,worker_id=NULL,lease_expires_at=NULL,
                          lease_token=lease_token+1 WHERE run_id=:run AND status IN
                          ('READY','PROCESSING','OUTCOME_UNKNOWN')
                    """),
                        {"run": run_id, "target": target},
                    )
                    await conn.execute(
                        text("""
                        UPDATE run_attempts SET status=:status,finished_at=clock_timestamp(),
                          error_code='CANCEL_REQUESTED' WHERE run_id=:run AND status='RUNNING'
                    """),
                        {"run": run_id, "status": "OUTCOME_UNKNOWN" if uncertain else "ABANDONED"},
                    )
                    await conn.execute(
                        text("""
                        UPDATE run_steps SET state=:target WHERE run_id=:run AND state IN
                          ('PENDING','READY','RUNNING','OUTCOME_UNKNOWN')
                    """),
                        {"run": run_id, "target": target},
                    )
                    await conn.execute(
                        text("""
                        UPDATE tool_effects SET status=CASE WHEN status='PREPARED' THEN 'CANCELLED'
                          ELSE 'OUTCOME_UNKNOWN' END,updated_at=clock_timestamp()
                        WHERE run_id=:run AND status IN ('PREPARED','DISPATCHED','OUTCOME_UNKNOWN')
                    """),
                        {"run": run_id},
                    )
                    for relation in ("model_calls", "tool_calls"):
                        await conn.execute(
                            text(
                                f"UPDATE {relation} SET status=:target WHERE run_id=:run "
                                "AND status IN ('PENDING','DISPATCHED','OUTCOME_UNKNOWN')"
                            ),
                            {"run": run_id, "target": target},
                        )
                    run = await _event(
                        conn,
                        run,
                        "CANCEL_REQUESTED",
                        "RUN_CANCEL_REQUESTED",
                        principal.principal_id,
                    )
                    run = await _event(
                        conn,
                        run,
                        target,
                        "RUN_OUTCOME_UNKNOWN" if uncertain else "RUN_CANCELLED",
                        principal.principal_id,
                    )
                    return _record(run)
            except _WorkSetChanged:
                continue

    async def pending_effect(
        self, principal: PrincipalContext, run_id: str
    ) -> EffectDispatch | None:
        async with unit_of_work(self.engine, principal.tenant_id, principal.principal_id) as conn:
            run = await self._authorized_run(conn, principal, run_id)
            if run["state"] != "OUTCOME_UNKNOWN":
                return None
            effect = (
                (
                    await conn.execute(
                        text("""
                SELECT * FROM tool_effects WHERE run_id=:run AND status='OUTCOME_UNKNOWN'
            """),
                        {"run": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            return _dispatch(effect) if effect is not None else None

    async def reconcile_effect(
        self,
        principal: PrincipalContext,
        run_id: str,
        effect: EffectDispatch,
        result: dict[str, Any],
    ) -> RunRecord:
        while True:
            try:
                async with unit_of_work(
                    self.engine, principal.tenant_id, principal.principal_id
                ) as conn:
                    run = await self._locked_run(conn, principal, run_id)
                    stored = (
                        (
                            await conn.execute(
                                text("SELECT * FROM tool_effects WHERE run_id=:run AND id=:id"),
                                {"run": run_id, "id": effect.id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if stored is None or _dispatch(stored) != effect:
                        raise RuntimeConflict("Effect snapshot changed")
                    if stored["status"] == "SUCCEEDED" and run["state"] in TERMINAL_STATES:
                        return _record(run)
                    if run["state"] != "OUTCOME_UNKNOWN" or stored["status"] != "OUTCOME_UNKNOWN":
                        raise RuntimeConflict("Effect is not awaiting reconciliation")
                    if effect.tool_version != "evaluation.run_suite:v1":
                        raise PolicyDenied("Provider reconciliation is not supported")
                    await conn.execute(
                        text("SELECT set_config('app.project_id',:project,true)"),
                        {"project": effect.project_id},
                    )
                    provider = (
                        (
                            await conn.execute(
                                text("""
                        SELECT * FROM mock_provider_results WHERE idempotency_key=:key
                          AND tenant_id=:tenant AND project_id=:project
                    """),
                                {
                                    "key": effect.idempotency_key,
                                    "tenant": effect.tenant_id,
                                    "project": effect.project_id,
                                },
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if (
                        provider is None
                        or provider["request_hash"] != stored["request_hash"]
                        or provider["result"] != result
                    ):
                        raise RuntimeConflict("Stored provider result does not confirm this effect")
                    schemas = (
                        (
                            await conn.execute(
                                text("""
                        SELECT t.output_schema,a.spec FROM tool_versions t
                        JOIN agent_versions a ON a.id=:agent WHERE t.id=:tool
                    """),
                                {
                                    "agent": run["agent_version_id"],
                                    "tool": stored["tool_version_id"],
                                },
                            )
                        )
                        .mappings()
                        .one()
                    )
                    validate_payload(result, cast(dict[str, Any], schemas["output_schema"]))
                    for relation in ("tool_effects", "tool_calls"):
                        await conn.execute(
                            text(
                                f"UPDATE {relation} SET status='SUCCEEDED',"
                                "result=CAST(:result AS jsonb) "
                                "WHERE step_id=:step"
                            ),
                            {"step": stored["step_id"], "result": _json(result)},
                        )
                    await conn.execute(
                        text(
                            "UPDATE run_steps SET state='SUCCEEDED',output=CAST(:result AS jsonb) "
                            "WHERE id=:step"
                        ),
                        {"step": stored["step_id"], "result": _json(result)},
                    )
                    await conn.execute(
                        text("UPDATE work_items SET status='DONE' WHERE step_id=:step"),
                        {"step": stored["step_id"]},
                    )
                    await conn.execute(
                        text("""
                        INSERT INTO checkpoints(id,tenant_id,project_id,run_id,step_id,work_id,
                          attempt_id,agent_version_id,step_kind,result_ref)
                        SELECT :id,e.tenant_id,e.project_id,e.run_id,e.step_id,a.work_id,a.id,
                          :agent,'TOOL_CALL',:ref FROM tool_effects e JOIN run_attempts a
                          ON a.id=e.dispatch_attempt_id WHERE e.id=:effect
                    """),
                        {
                            "id": uuid4().hex,
                            "agent": run["agent_version_id"],
                            "effect": effect.id,
                            "ref": f"run_steps/{stored['step_id']}/output",
                        },
                    )
                    await conn.execute(
                        text("""
                        INSERT INTO usage_entries
                          (id,tenant_id,project_id,run_id,step_id,source,quantity,unit)
                        VALUES(:id,:tenant,:project,:run,:step,'TOOL',1,'call')
                    """),
                        {
                            "id": uuid4().hex,
                            "tenant": run["tenant_id"],
                            "project": run["project_id"],
                            "run": run_id,
                            "step": stored["step_id"],
                        },
                    )
                    run = (
                        (
                            await conn.execute(
                                text("""
                        UPDATE runs SET result=CAST(:result AS jsonb),error=NULL,
                          cancellation_outcome=CASE WHEN cancel_epoch>0
                            THEN 'EFFECT_SUCCEEDED' ELSE NULL END
                        WHERE id=:run RETURNING *
                    """),
                                {"result": _json(result), "run": run_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    target = "CANCELLED" if run["cancel_epoch"] else "COMPLETED"
                    return _record(
                        await _event(
                            conn,
                            run,
                            target,
                            "EFFECT_RECONCILED",
                            principal.principal_id,
                            {"effect_id": effect.id, "effect_outcome": "SUCCEEDED"},
                        )
                    )
            except _WorkSetChanged:
                continue

    async def _reschedule(
        self,
        conn: AsyncConnection,
        work: ClaimedWork,
        run: RowMapping,
        code: str,
        message: str,
        *,
        expired: bool,
    ) -> None:
        safe = False
        dispatched = False
        if work.kind == "MODEL_CALL":
            safe = (
                await conn.execute(
                    text("""
                SELECT 1 FROM model_calls c JOIN runs r ON r.id=c.run_id
                JOIN agent_versions a ON a.id=r.agent_version_id
                WHERE c.step_id=:step AND c.model_route='mock/release-planner-v1'
                  AND a.spec->>'model_route'='mock/release-planner-v1'
            """),
                    {"step": work.step_id},
                )
            ).first() is not None
        elif work.kind == "TOOL_CALL":
            effect = await self._effect_for_work(conn, work)
            dispatched = effect["status"] == "DISPATCHED"
            safe = (
                await conn.execute(
                    text("""
                SELECT 1 FROM tool_calls c JOIN tool_versions t ON t.id=c.tool_version_id
                WHERE c.step_id=:step AND t.connection_kind='mock' AND t.risk_tier='T1'
                  AND t.name='evaluation.run_suite' AND t.version=1
            """),
                    {"step": work.step_id},
                )
            ).first() is not None
            safe = safe and effect["tool_version"] == "evaluation.run_suite:v1"
        exhausted = work.attempt_no >= self.max_attempts
        if dispatched and (not expired or exhausted):
            # A reported provider error or exhausted replay cannot prove failure.
            safe = False
        target = "OUTCOME_UNKNOWN" if not safe else "FAILED" if exhausted else "QUEUED"
        work_status = "OUTCOME_UNKNOWN" if not safe else "FAILED" if exhausted else "READY"
        # Equal jitter keeps retries delayed while preserving a strict 60s cap.
        ceiling = min(60.0, self.retry_base_seconds * 2 ** min(work.attempt_no - 1, 20))
        delay = random.uniform(ceiling / 2, ceiling)
        comparison = "<=" if expired else ">"
        changed = await conn.execute(
            text(f"""
            UPDATE work_items SET status=:status,worker_id=NULL,lease_expires_at=NULL,
              available_at=clock_timestamp()+:delay*interval '1 second'
            WHERE id=:work AND status='PROCESSING' AND worker_id=:owner AND lease_token=:token
              AND lease_expires_at {comparison} clock_timestamp() RETURNING id
        """),
            {**self._lease_params(work), "status": work_status, "delay": delay},
        )
        if changed.first() is None:
            raise RuntimeConflict("Work lease changed during recovery")
        attempt_status = "ABANDONED" if expired else "RETRY_SCHEDULED"
        if not safe:
            attempt_status = "OUTCOME_UNKNOWN"
        elif exhausted and not expired:
            attempt_status = "FAILED"
        await conn.execute(
            text("""
            UPDATE run_attempts SET status=:status,finished_at=clock_timestamp(),error_code=:code
            WHERE id=:attempt AND status='RUNNING'
        """),
            {"status": attempt_status, "code": code, "attempt": work.attempt_id},
        )
        await conn.execute(
            text("UPDATE run_steps SET state=:state WHERE id=:step"),
            {"state": "READY" if target == "QUEUED" else work_status, "step": work.step_id},
        )
        for relation in ("model_calls", "tool_calls"):
            await conn.execute(
                text(f"UPDATE {relation} SET status=:status WHERE step_id=:step"),
                {"status": "PENDING" if target == "QUEUED" else work_status, "step": work.step_id},
            )
        if work.kind == "TOOL_CALL" and target != "QUEUED":
            await conn.execute(
                text("UPDATE tool_effects SET status=:status WHERE step_id=:step"),
                {"status": "OUTCOME_UNKNOWN" if dispatched else "CANCELLED", "step": work.step_id},
            )
        error = {
            "code": "OUTCOME_UNKNOWN" if not safe else "RETRY_EXHAUSTED" if exhausted else code,
            "message": message,
        }
        if target != "QUEUED":
            await conn.execute(
                text("UPDATE runs SET error=CAST(:error AS jsonb) WHERE id=:run"),
                {"error": _json(error), "run": work.run_id},
            )
            await conn.execute(
                text("""
                INSERT INTO dead_letter_items
                  (id,tenant_id,project_id,run_id,step_id,work_id,attempt_id,reason_code)
                VALUES (:id,:tenant,:project,:run,:step,:work,:attempt,:reason)
            """),
                {**self._lease_params(work), "id": uuid4().hex, "reason": error["code"]},
            )
        event_type = "WORK_RECOVERED" if expired else "WORK_RETRY_SCHEDULED"
        if target != "QUEUED":
            event_type = "RUN_OUTCOME_UNKNOWN" if not safe else "RUN_FAILED"
        await _event(
            conn,
            run,
            target,
            event_type,
            "reaper" if expired else "worker",
            {
                "step_id": work.step_id,
                "attempt_id": work.attempt_id,
                "attempt_no": work.attempt_no,
                **error,
            },
        )

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
            await conn.execute(
                text("""
                INSERT INTO tool_effects
                  (id,tenant_id,project_id,run_id,step_id,tool_version_id,tool_version,
                   idempotency_key,request_hash,arguments,status)
                VALUES (:step,:tenant,:project,:run,:step,:tool,:reference,:step,
                  :digest,CAST(:arguments AS jsonb),'PREPARED')
                """),
                {
                    "step": step_id,
                    "tenant": work.tenant_id,
                    "project": work.project_id,
                    "run": work.run_id,
                    "tool": tool["id"],
                    "reference": reference,
                    "digest": canonical_digest({"tool_version": reference, "arguments": arguments}),
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
            effect = await self._effect_for_work(conn, work)
            if effect["status"] != "DISPATCHED" or effect["dispatch_attempt_id"] != work.attempt_id:
                raise RuntimeConflict("Effect is not dispatched by this attempt")
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
                text(
                    "UPDATE tool_effects SET status='SUCCEEDED',result=CAST(:result AS jsonb), "
                    "updated_at=clock_timestamp() WHERE id=:id"
                ),
                {"id": effect["id"], "result": _json(result)},
            )
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
        if work.kind == "TOOL_CALL":
            effect = await self._effect_for_work(conn, work)
            if effect["status"] == "DISPATCHED":
                await self._end_lease(conn, work, "OUTCOME_UNKNOWN", "OUTCOME_UNKNOWN")
                await self._unknown_effect(conn, work, run)
                return
            await conn.execute(
                text("UPDATE tool_effects SET status='CANCELLED' WHERE id=:id"),
                {"id": effect["id"]},
            )
        await self._end_lease(conn, work, "FAILED", "FAILED")
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
        await self._end_lease(conn, work, "DONE", "SUCCEEDED")
        await conn.execute(
            text("""
            INSERT INTO checkpoints
              (id,tenant_id,project_id,run_id,step_id,work_id,attempt_id,agent_version_id,
               step_kind,result_ref)
            SELECT :id,:tenant,:project,r.id,:step,:work,:attempt,r.agent_version_id,
              :kind,:ref FROM runs r WHERE r.id=:run
        """),
            {
                **self._lease_params(work),
                "id": uuid4().hex,
                "kind": work.kind,
                "ref": f"run_steps/{work.step_id}/output",
            },
        )

    async def _end_lease(
        self, conn: AsyncConnection, work: ClaimedWork, status: str, attempt_status: str
    ) -> None:
        updated = await conn.execute(
            text("""
            UPDATE work_items SET status=:status,worker_id=NULL,lease_expires_at=NULL
            WHERE id=:work AND status='PROCESSING' AND worker_id=:owner
              AND lease_token=:token AND lease_expires_at>clock_timestamp() RETURNING id
        """),
            {**self._lease_params(work), "status": status},
        )
        if updated.first() is None:
            raise RuntimeConflict("Work lease expired before recording outcome")
        attempt = await conn.execute(
            text("""
            UPDATE run_attempts SET status=:status,finished_at=clock_timestamp()
            WHERE id=:attempt AND work_id=:work AND lease_token=:token
              AND status='RUNNING' RETURNING id
        """),
            {**self._lease_params(work), "status": attempt_status},
        )
        if attempt.first() is None:
            raise RuntimeConflict("Attempt is no longer active")

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
