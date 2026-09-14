import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_platform.adapters.models.mock import MockModelGateway
from agent_platform.adapters.postgres.repositories import PostgresRunRepository
from agent_platform.adapters.postgres.seed import seed_example
from agent_platform.adapters.telemetry.otel import OtelRuntimeTelemetry
from agent_platform.adapters.tools.persistent_mock import PersistentMockEvaluationTool
from agent_platform.application.errors import RetryableGatewayError
from agent_platform.application.ports import CreateRunCommand
from agent_platform.application.runtime_kernel import RuntimeKernel
from agent_platform.settings import Settings
from agent_platform.worker import main
from agent_platform.worker.poller import WorkerPoller

INPUT = {
    "candidate_model_ref": "mock://SECRET_candidate",
    "evaluation_suite_ref": "mock://SECRET_suite",
}


@pytest.fixture
def receiver(request):
    bodies = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(request.param)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/traces", bodies
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)


@pytest.mark.asyncio
@pytest.mark.parametrize("receiver", [200, 503], indirect=True)
async def test_real_worker_postgres_to_local_otlp_preserves_results(
    admin_engine, runtime_engine, runtime_url, receiver, monkeypatch, caplog
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await repository.accept_run(
        command=CreateRunCommand(seed.principal, seed.agent_version_id, INPUT),
        idempotency_key="telemetry-e2e",
    )
    endpoint, bodies = receiver
    instances = []

    def capture(**kwargs):
        telemetry = OtelRuntimeTelemetry(**kwargs)
        instances.append(telemetry)
        return telemetry

    monkeypatch.setattr(main, "OtelRuntimeTelemetry", capture)
    await main.run_worker(
        Settings(
            _env_file=None,
            database_url=runtime_url,
            telemetry_enabled=True,
            telemetry_endpoint=endpoint,
        ),
        drain=True,
    )
    run = await repository.get_run(seed.principal, accepted.run.id)
    assert run.state == "COMPLETED"
    assert run.result["candidate_model_ref"] == INPUT["candidate_model_ref"]
    assert len(bodies) == 4
    spans = []
    for body in bodies:
        assert b"SECRET" not in body
        decoded = ExportTraceServiceRequest()
        decoded.ParseFromString(body)
        spans.extend(decoded.resource_spans[0].scope_spans[0].spans)
    roots = {span.span_id: span for span in spans if not span.parent_span_id}
    assert len(roots) == 2
    assert len({span.trace_id for span in roots.values()}) == 2
    assert sorted(span.name for span in spans) == [
        "runtime.attempt",
        "runtime.attempt",
        "runtime.model",
        "runtime.tool",
    ]
    for span in spans:
        attrs = {attr.key: attr.value.string_value for attr in span.attributes}
        assert attrs["runtime.run.id"] == run.id
        assert attrs["runtime.agent_version.id"] == seed.agent_version_id
        assert attrs["runtime.outcome"] == ("RETURNED" if span.parent_span_id else "RECORDED")
        assert not span.events
        if span.parent_span_id:
            assert span.trace_id == roots[span.parent_span_id].trace_id
    counts = instances[0].snapshot()
    assert counts["exported"] + counts["failed"] == 4
    assert counts["in_flight"] == counts["queue_depth"] == 0
    assert "SECRET" not in caplog.text


class BlockedExporter(SpanExporter):
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()

    def export(self, spans):
        self.entered.set()
        self.release.wait(5)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


@pytest.mark.asyncio
async def test_blocked_exporter_and_full_queue_do_not_block_durable_tool_completion(
    admin_engine, runtime_engine
):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine)
    accepted = await repository.accept_run(
        command=CreateRunCommand(seed.principal, seed.agent_version_id, INPUT),
        idempotency_key="blocked-exporter",
    )
    exporter = BlockedExporter()
    telemetry = OtelRuntimeTelemetry(
        exporter=exporter,
        queue_capacity=1,
        shutdown_timeout_seconds=0.01,
    )
    poller = WorkerPoller(
        repository,
        RuntimeKernel(
            repository,
            MockModelGateway(),
            PersistentMockEvaluationTool(runtime_engine),
            telemetry=telemetry,
        ),
    )
    try:
        assert await asyncio.wait_for(poller.poll_once(), 2)
        assert await asyncio.to_thread(exporter.entered.wait, 1)
        assert await asyncio.wait_for(poller.poll_once(), 2)
        assert (await repository.get_run(seed.principal, accepted.run.id)).state == "COMPLETED"
        assert telemetry.snapshot()["in_flight"] == 1
        assert telemetry.snapshot()["dropped"] >= 1
        telemetry.shutdown()
        assert telemetry.snapshot()["queue_depth"] == 0
    finally:
        exporter.release.set()
        telemetry.shutdown()


@pytest.mark.asyncio
async def test_exhausted_retry_span_never_claims_retry_was_scheduled(admin_engine, runtime_engine):
    seed = await seed_example(admin_engine)
    repository = PostgresRunRepository(runtime_engine, max_attempts=1)
    accepted = await repository.accept_run(
        command=CreateRunCommand(seed.principal, seed.agent_version_id, INPUT),
        idempotency_key="exhausted-trace",
    )

    class UnavailableModel:
        async def decide(self, **kwargs):
            raise RetryableGatewayError("SECRET_provider")

    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)
    poller = WorkerPoller(
        repository,
        RuntimeKernel(
            repository,
            UnavailableModel(),
            PersistentMockEvaluationTool(runtime_engine),
            telemetry=telemetry,
        ),
    )
    try:
        assert await poller.poll_once()
        run = await repository.get_run(seed.principal, accepted.run.id)
        assert run.state == "FAILED"
        assert run.error["code"] == "RETRY_EXHAUSTED"
        assert telemetry.force_flush(1)
        root = next(s for s in exporter.get_finished_spans() if s.parent is None)
        assert root.attributes["runtime.outcome"] == "RETRY_HANDLED"
    finally:
        telemetry.shutdown()
