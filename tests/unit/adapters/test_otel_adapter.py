import asyncio
import threading
import time
from dataclasses import replace
from uuid import uuid4

import pytest
from opentelemetry import baggage, context, metrics, trace
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_platform.adapters.telemetry.otel import OtelRuntimeTelemetry
from agent_platform.application.ports import ClaimedWork


def work():
    return ClaimedWork(
        id=str(uuid4()),
        tenant_id="tenant-canary",
        project_id="project-canary",
        principal_id="principal-canary",
        run_id=str(uuid4()),
        step_id=str(uuid4()),
        kind="TOOL_CALL",
        input={"secret": "input-canary"},
        agent_spec={"secret": "spec-canary"},
        tool_version_id=str(uuid4()),
        tool_spec={"secret": "tool-canary"},
        attempt_id=str(uuid4()),
        worker_id="worker-canary",
        lease_token=987654,
    )


def test_actual_sdk_hierarchy_isolated_roots_and_allowlisted_metadata(monkeypatch):
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "secret=env-canary")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "service-canary")
    monkeypatch.setenv("OTEL_TRACES_SAMPLER", "always_off")
    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)
    claimed = work()
    ambient = context.attach(baggage.set_baggage("secret", "baggage-canary"))
    try:
        attempt = telemetry.start("attempt", claimed)
        child = telemetry.start("tool", claimed)
        assert baggage.get_baggage("secret") is None
        child.finish("RETURNED")
        attempt.finish("RECORDED")
        second = telemetry.start("attempt", replace(claimed, attempt_id=str(uuid4())))
        second.finish("ERROR")
        assert baggage.get_baggage("secret") == "baggage-canary"
        assert telemetry.force_flush(1)
        spans = exporter.get_finished_spans()
        assert len(spans) == 3
        tool, first, next_attempt = spans
        assert first.parent is None and next_attempt.parent is None
        assert tool.parent.span_id == first.context.span_id
        assert tool.context.trace_id == first.context.trace_id
        assert first.context.trace_id != next_attempt.context.trace_id
        assert tool.name == "runtime.tool"
        assert tool.end_time >= tool.start_time
        assert tool.attributes["runtime.run.id"] == claimed.run_id
        assert len(tool.attributes["runtime.tenant.hash"]) == 64
        assert next_attempt.status.status_code == trace.StatusCode.ERROR
        assert next_attempt.status.description is None
        assert all(not span.events for span in spans)
        rendered = str([(s.attributes, s.resource.attributes, s.events) for s in spans])
        for canary in (
            "input-canary",
            "spec-canary",
            "tool-canary",
            "worker-canary",
            "tenant-canary",
            "project-canary",
            "principal-canary",
            "env-canary",
            "service-canary",
            "baggage-canary",
        ):
            assert canary not in rendered
        assert telemetry.snapshot()["exported"] == 3
    finally:
        context.detach(ambient)
        telemetry.shutdown()


class BlockingExporter(SpanExporter):
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()

    def export(self, spans):
        self.entered.set()
        self.release.wait(5)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


def test_queue_saturation_and_shutdown_never_wait_for_hung_exporter():
    exporter = BlockingExporter()
    telemetry = OtelRuntimeTelemetry(
        exporter=exporter, queue_capacity=2, shutdown_timeout_seconds=0.03
    )
    try:
        telemetry.start("attempt", work()).finish("RECORDED")
        assert exporter.entered.wait(1)
        started = time.monotonic()
        for _ in range(8):
            telemetry.start("attempt", work()).finish("RECORDED")
        assert time.monotonic() - started < 0.2
        assert telemetry.snapshot()["queue_depth"] == 2
        assert telemetry.snapshot()["dropped"] == 6
        assert not telemetry.force_flush(0.01)
        started = time.monotonic()
        telemetry.shutdown()
        assert time.monotonic() - started < 0.2
        counts = telemetry.snapshot()
        assert counts["queue_depth"] == 0
        assert counts["dropped"] == 8
        assert counts["in_flight"] == 1
    finally:
        exporter.release.set()
        telemetry.shutdown()


class FailingExporter(SpanExporter):
    def export(self, spans):
        raise RuntimeError("exporter-secret-canary")

    def shutdown(self):
        raise RuntimeError("shutdown-secret-canary")


def test_export_failure_is_counted_without_logging_payload(caplog):
    telemetry = OtelRuntimeTelemetry(exporter=FailingExporter())
    telemetry.start("attempt", work()).finish("FAILED")
    assert telemetry.force_flush(1)
    telemetry.shutdown()
    assert telemetry.snapshot()["failed"] == 1
    assert "canary" not in caplog.text


def test_unknown_operation_and_outcome_never_export_free_text():
    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)
    try:
        with pytest.raises(ValueError, match="operation"):
            telemetry.start("secret-operation", work())
        handle = telemetry.start("attempt", work())
        handle.finish("secret-outcome")
        handle.finish("FAILED")
        assert telemetry.force_flush(1)
        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].attributes["runtime.outcome"] == "ERROR"
        assert "secret" not in str(spans[0].attributes)
    finally:
        telemetry.shutdown()


@pytest.mark.asyncio
async def test_concurrent_attempt_contexts_do_not_cross_parent_boundaries():
    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)

    async def execute(claimed):
        attempt = telemetry.start("attempt", claimed)
        await asyncio.sleep(0)
        child = telemetry.start("model", claimed)
        await asyncio.sleep(0)
        child.finish("RETURNED")
        attempt.finish("RECORDED")

    try:
        await asyncio.gather(*(execute(work()) for _ in range(20)))
        assert telemetry.force_flush(1)
        spans = exporter.get_finished_spans()
        roots = {s.context.span_id: s for s in spans if s.parent is None}
        assert len(roots) == 20
        assert len({s.context.trace_id for s in roots.values()}) == 20
        for child in (s for s in spans if s.parent is not None):
            parent = roots[child.parent.span_id]
            assert child.attributes["runtime.attempt.id"] == parent.attributes["runtime.attempt.id"]
        assert trace.get_current_span().get_span_context().is_valid is False
    finally:
        telemetry.shutdown()


def test_content_addressed_version_ids_are_preserved_and_free_text_ids_are_hashed():
    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)
    try:
        claimed = replace(
            work(),
            run_id="f" * 32,
            agent_version_id="a" * 48,
            tool_version_id="b" * 64,
            step_id="step-secret-canary",
        )
        telemetry.start("attempt", claimed).finish("RECORDED")
        assert telemetry.force_flush(1)
        attributes = exporter.get_finished_spans()[0].attributes
        assert attributes["runtime.run.id"] == "f" * 32
        assert attributes["runtime.agent_version.id"] == "a" * 48
        assert attributes["runtime.tool_version.id"] == "b" * 64
        assert "runtime.step.id" not in attributes
        assert len(attributes["runtime.step.hash"]) == 64
        assert "canary" not in str(attributes)
    finally:
        telemetry.shutdown()


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_invalid_queue_capacity_is_rejected(capacity):
    with pytest.raises(ValueError, match="capacity"):
        OtelRuntimeTelemetry(queue_capacity=capacity)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_timeout_is_rejected(timeout):
    with pytest.raises(ValueError, match="timeout"):
        OtelRuntimeTelemetry(export_timeout_seconds=timeout)


def test_shutdown_rejects_new_work_and_repeated_shutdown_is_safe():
    exporter = InMemorySpanExporter()
    telemetry = OtelRuntimeTelemetry(exporter=exporter)
    telemetry.start("attempt", work()).finish("RECORDED")
    telemetry.shutdown()
    telemetry.shutdown()
    telemetry.start("attempt", work()).finish("RECORDED")
    assert telemetry.snapshot()["ended"] == 1
    assert telemetry.snapshot()["exported"] == 1


@pytest.mark.parametrize("enabled", ["true", "  TRUE ", "false"])
def test_sdk_internal_metrics_never_consults_global_meter_provider(monkeypatch, enabled):
    monkeypatch.setenv("OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED", enabled)
    consulted = []

    def global_provider():
        consulted.append(True)
        return metrics.NoOpMeterProvider()

    monkeypatch.setattr(metrics, "get_meter_provider", global_provider)
    telemetry = OtelRuntimeTelemetry(exporter=InMemorySpanExporter())
    try:
        telemetry.start("attempt", work()).finish("RECORDED")
        assert telemetry.force_flush(1)
        assert not consulted
        assert telemetry.snapshot()["exported"] == 1
    finally:
        telemetry.shutdown()


@pytest.mark.parametrize("value", ["", "1", "invalid-metric-secret-canary"])
def test_invalid_sdk_metrics_env_fails_before_sdk_logging_or_export_thread(
    monkeypatch,
    caplog,
    value,
):
    monkeypatch.setenv("OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED", value)
    telemetry = None
    try:
        with pytest.raises(ValueError, match=r"^invalid telemetry internal metrics setting$"):
            telemetry = OtelRuntimeTelemetry(exporter=InMemorySpanExporter())
        assert not caplog.records
    finally:
        if telemetry is not None:
            telemetry.shutdown()


def test_sdk_initialization_failure_does_not_start_an_exporter_thread(monkeypatch):
    from agent_platform.adapters.telemetry import otel

    started = []
    monkeypatch.setattr(threading.Thread, "start", lambda self: started.append(self))

    def broken_tracer(*args, **kwargs):
        raise RuntimeError("sdk-unavailable")

    monkeypatch.setattr(otel.TracerProvider, "get_tracer", broken_tracer)
    with pytest.raises(RuntimeError, match="sdk-unavailable"):
        OtelRuntimeTelemetry(exporter=InMemorySpanExporter())
    assert not started
