"""FastAPI composition root, bounded ingress and resumable event streaming."""

import asyncio
import json
import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Annotated, Any, Literal, cast

from fastapi import Body, Depends, FastAPI, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from agent_platform.adapters.identity import StaticTokenVerifier
from agent_platform.application.errors import (
    ApplicationError,
    AuthenticationRequired,
    ExecutionScopeNotFound,
    IdempotencyConflict,
    IdentityProviderNotConfigured,
    InvalidInput,
    ObservationUnavailable,
    PolicyDenied,
    ProviderUnavailable,
    RuntimeConflict,
)
from agent_platform.application.observability import ObservationRepository
from agent_platform.application.ports import (
    CreateRunCommand,
    IdentityVerifier,
    PrincipalContext,
    RunRecord,
    RunRepository,
    ToolGateway,
)
from agent_platform.application.run_service import RunService
from agent_platform.settings import Settings

TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "REJECTED", "TIMED_OUT", "CANCELLED"})


def error_response(code: str, message: str, status_code: int) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message, "retryable": False}},
        status_code=status_code,
    )


class BoundedBodyMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int = 65536) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.max_bytes:
                await error_response("BODY_TOO_LARGE", "Request body exceeds limit", 413)(
                    scope, receive, send
                )
                return
            if not message.get("more_body", False):
                break
        delivered = False

        async def bounded_receive() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    agent_version_id: str = Field(min_length=1, max_length=200)
    input: dict[str, Any]


class EmptyMutationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RedriveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: Literal["WORKER_RECOVERED", "TRANSIENT_FAILURE_RESOLVED"]


class EvaluationCandidateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_state_version: int = Field(ge=1, strict=True)
    expected_state: Literal["COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "REJECTED"]


def public_run(run: RunRecord) -> dict[str, Any]:
    return {
        "run_id": run.id,
        "project_id": run.project_id,
        "agent_version_id": run.agent_version_id,
        "state": run.state,
        "state_version": run.state_version,
        "input": run.input,
        "result": run.result,
        "error": run.error,
        "created_at": run.created_at,
        "cancel_epoch": run.cancel_epoch,
        "cancellation_outcome": run.cancellation_outcome,
    }


async def authenticate(request: Request) -> PrincipalContext:
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or len(token) > 8192:
        raise AuthenticationRequired()
    verifier = cast(IdentityVerifier | None, request.app.state.identity_verifier)
    if verifier is None:
        raise IdentityProviderNotConfigured()
    try:
        return await verifier.verify(token)
    except AuthenticationRequired:
        raise
    except Exception as error:
        raise IdentityProviderNotConfigured() from error


def repository_for(request: Request) -> RunRepository:
    return cast(RunRepository, request.app.state.repository)


def observations_for(request: Request) -> ObservationRepository:
    observations = cast(ObservationRepository | None, request.app.state.observations)
    if observations is None:
        raise ObservationUnavailable()
    return observations


Principal = Annotated[PrincipalContext, Depends(authenticate)]
Repository = Annotated[RunRepository, Depends(repository_for)]
Observations = Annotated[ObservationRepository, Depends(observations_for)]


def create_app(
    settings: Settings | None = None,
    repository: RunRepository | None = None,
    identity_verifier: IdentityVerifier | None = None,
    tool_gateway: ToolGateway | None = None,
    observations: ObservationRepository | None = None,
) -> FastAPI:
    configuration = settings or Settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        engine: AsyncEngine | None = None
        if repository is None:
            from agent_platform.adapters.postgres.database import create_engine
            from agent_platform.adapters.postgres.repositories import PostgresRunRepository

            engine = create_engine(configuration.database_url)
            application.state.repository = PostgresRunRepository(engine)
            if observations is None:
                from agent_platform.adapters.postgres.observations import (
                    PostgresObservationRepository,
                )

                application.state.observations = PostgresObservationRepository(engine)
            if tool_gateway is None:
                from agent_platform.adapters.tools.persistent_mock import (
                    PersistentMockEvaluationTool,
                )

                application.state.tool_gateway = PersistentMockEvaluationTool(engine)
        try:
            yield
        finally:
            if engine is not None:
                await engine.dispose()

    app = FastAPI(title="Agent Runtime Platform", lifespan=lifespan)
    app.add_middleware(BoundedBodyMiddleware)
    app.state.repository = repository
    app.state.identity_verifier = identity_verifier
    app.state.tool_gateway = tool_gateway
    app.state.observations = observations
    if identity_verifier is None and configuration.development_mode:
        secret = configuration.development_token
        if secret is not None and secret.get_secret_value():
            app.state.identity_verifier = StaticTokenVerifier(
                {
                    secret.get_secret_value(): PrincipalContext(
                        configuration.development_tenant_id, configuration.development_principal_id
                    )
                }
            )

    @app.exception_handler(ApplicationError)
    async def application_error_handler(_request: Request, exc: ApplicationError) -> JSONResponse:
        error_mapping: dict[type[ApplicationError], tuple[int, str]] = {
            AuthenticationRequired: (401, "Authentication required"),
            IdentityProviderNotConfigured: (
                503,
                "Identity provider is not configured or unavailable",
            ),
            ExecutionScopeNotFound: (404, "Resource not found"),
            IdempotencyConflict: (409, "Idempotency key conflicts with an earlier request"),
            RuntimeConflict: (409, "Execution state conflicts with this request"),
            InvalidInput: (422, "Request payload is invalid"),
            PolicyDenied: (403, "Operation is not permitted"),
            ProviderUnavailable: (503, "Provider result lookup is unavailable"),
            ObservationUnavailable: (503, "Observation storage is unavailable"),
        }
        status_code, message = error_mapping.get(type(exc), (500, "Request could not be processed"))
        return error_response(exc.code, message, status_code)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, _exc: RequestValidationError
    ) -> JSONResponse:
        return error_response("CLIENT_INVALID", "Request payload or parameters are invalid", 422)

    @app.exception_handler(Exception)
    async def unknown_error_handler(_request: Request, _exc: Exception) -> JSONResponse:
        return error_response("INTERNAL_ERROR", "Request could not be processed", 500)

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(repo: Repository) -> JSONResponse:
        try:
            await repo.check_health()
        except Exception:
            return error_response("DATABASE_UNAVAILABLE", "Database is unavailable", 503)
        return JSONResponse({"status": "ready"})

    @app.post("/v1/runs", status_code=202)
    async def create_run(
        body: CreateRunRequest,
        principal: Principal,
        repo: Repository,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    ) -> dict[str, Any]:
        accepted = await RunService(repo).create(
            CreateRunCommand(principal, body.agent_version_id, body.input), idempotency_key
        )
        return {
            "run_id": accepted.run.id,
            "project_id": accepted.run.project_id,
            "state": accepted.run.state,
            "duplicate": accepted.duplicate,
        }

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: str, principal: Principal, repo: Repository) -> dict[str, Any]:
        run = await repo.get_run(principal, run_id)
        return public_run(run)

    @app.post("/v1/runs/{run_id}/cancel", status_code=202)
    async def cancel_run(
        run_id: str,
        principal: Principal,
        repo: Repository,
        body: Annotated[EmptyMutationRequest | None, Body()] = None,
    ) -> dict[str, Any]:
        return public_run(await repo.cancel_run(principal, run_id))

    @app.get("/v1/runs/{run_id}/trace")
    async def get_trace(run_id: str, principal: Principal, store: Observations) -> dict[str, Any]:
        return await store.get_trace(principal, run_id)

    @app.post("/v1/runs/{run_id}/evaluation-candidates", status_code=201)
    async def create_evaluation_candidate(
        run_id: str,
        body: EvaluationCandidateRequest,
        principal: Principal,
        store: Observations,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    ) -> JSONResponse:
        if not idempotency_key.strip():
            raise InvalidInput("Invalid idempotency key")
        accepted = await store.create_evaluation_candidate(
            principal,
            run_id,
            source_state_version=body.source_state_version,
            expected_state=body.expected_state,
            idempotency_key=idempotency_key,
        )
        return JSONResponse(
            jsonable_encoder(asdict(accepted)), status_code=200 if accepted.duplicate else 201
        )

    @app.get("/v1/evaluation-candidates/{candidate_id}")
    async def get_evaluation_candidate(
        candidate_id: str, principal: Principal, store: Observations
    ) -> dict[str, Any]:
        # Use the same timestamp representation as create/duplicate responses.
        return cast(
            dict[str, Any],
            jsonable_encoder(asdict(await store.get_evaluation_candidate(principal, candidate_id))),
        )

    @app.get("/v1/runs/{run_id}/dead-letters")
    async def list_dead_letters(
        run_id: str,
        principal: Principal,
        repo: Repository,
        after_id: Annotated[str, Query(max_length=64)] = "",
        limit: Annotated[int, Query(ge=1, le=100)] = 100,
    ) -> dict[str, Any]:
        items = await repo.list_dead_letters(principal, run_id, after_id, limit)
        return {"items": [asdict(item) for item in items]}

    @app.post("/v1/runs/{run_id}/dead-letters/{item_id}/redrive", status_code=202)
    async def redrive_dead_letter(
        run_id: str,
        item_id: str,
        body: RedriveRequest,
        principal: Principal,
        repo: Repository,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    ) -> dict[str, Any]:
        if not idempotency_key.strip():
            raise InvalidInput("Invalid idempotency key")
        accepted = await repo.redrive_dead_letter(
            principal, run_id, item_id, idempotency_key=idempotency_key, reason=body.reason
        )
        return {
            "run_id": accepted.run.id,
            "source_run_id": run_id,
            "dead_letter_id": item_id,
            "project_id": accepted.run.project_id,
            "state": accepted.run.state,
            "duplicate": accepted.duplicate,
        }

    @app.post("/v1/runs/{run_id}/reconcile")
    async def reconcile_run(
        request: Request,
        run_id: str,
        principal: Principal,
        repo: Repository,
        body: Annotated[EmptyMutationRequest | None, Body()] = None,
    ) -> dict[str, Any]:
        effect = await repo.pending_effect(principal, run_id)
        if effect is None:
            return public_run(await repo.get_run(principal, run_id))
        provider = cast(ToolGateway | None, request.app.state.tool_gateway)
        if provider is None:
            raise ProviderUnavailable()
        try:
            async with asyncio.timeout(configuration.operation_timeout_seconds):
                result = await provider.lookup(effect)
        except Exception:
            raise ProviderUnavailable() from None
        if result is None:
            # Absence is not proof that an in-flight request cannot still succeed.
            return public_run(await repo.get_run(principal, run_id))
        return public_run(await repo.reconcile_effect(principal, run_id, effect, result))

    @app.get("/v1/runs/{run_id}/events")
    async def get_events(
        run_id: str,
        principal: Principal,
        repo: Repository,
        after_sequence: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> dict[str, Any]:
        events = await repo.list_events(principal, run_id, after_sequence, limit)
        return {"items": [asdict(event) for event in events]}

    @app.get("/v1/runs/{run_id}/events/stream")
    async def stream_events(
        request: Request,
        run_id: str,
        principal: Principal,
        repo: Repository,
        last_event_id: Annotated[int | None, Header(ge=0)] = None,
        after_sequence: Annotated[int, Query(ge=0)] = 0,
    ) -> StreamingResponse:
        await repo.get_run(principal, run_id)

        async def stream() -> AsyncIterator[str]:
            cursor = last_event_id if last_event_id is not None else after_sequence
            last_heartbeat = time.monotonic()
            while not await request.is_disconnected():
                try:
                    current_principal = await authenticate(request)
                    run = await repo.get_run(current_principal, run_id)
                    events = await repo.list_events(
                        current_principal, run_id, after_sequence=cursor, limit=100
                    )
                except ApplicationError:
                    yield 'event: error\ndata: {"code":"STREAM_ACCESS_DENIED"}\n\n'
                    return
                except Exception:
                    yield 'event: error\ndata: {"code":"STREAM_UNAVAILABLE"}\n\n'
                    return
                for event in events:
                    cursor = event.sequence
                    payload = json.dumps(jsonable_encoder(asdict(event)), ensure_ascii=False)
                    yield f"id: {event.sequence}\nevent: {event.type}\ndata: {payload}\n\n"
                if run.state in TERMINAL_STATES and len(events) < 100:
                    return
                if len(events) == 100:
                    continue
                if time.monotonic() - last_heartbeat >= configuration.sse_heartbeat_seconds:
                    yield ": heartbeat\n\n"
                    last_heartbeat = time.monotonic()
                await asyncio.sleep(configuration.sse_poll_interval_seconds)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    return app
