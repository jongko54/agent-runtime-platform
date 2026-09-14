"""Bounded, best-effort tracing with an explicit loopback-only OTLP boundary.

No global provider, resource detectors, environment headers, HTTP proxies or retries.
One daemon owns transport; even an injected exporter that never returns cannot hold
up execution or shutdown. In-flight data cannot be recovered from such an exporter.
"""

import hashlib
import http.client
import ipaddress
import math
import os
import re
import threading
import time
from collections import deque
from collections.abc import Sequence
from contextvars import ContextVar, Token
from urllib.parse import urlsplit
from uuid import UUID

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanLimits, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from agent_platform.application.ports import ClaimedWork
from agent_platform.application.telemetry import NoopSpan, SpanHandle

_OPERATIONS = frozenset({"attempt", "model", "tool"})
_OUTCOMES = frozenset(
    {
        "RETURNED",
        "RECORDED",
        "FAILED",
        "TIMED_OUT",
        "RETRY_HANDLED",
        "OUTCOME_UNKNOWN",
        "POLICY_DENIED",
        "CLIENT_INVALID",
        "CONFLICT",
        "CANCELLED",
        "ERROR",
    }
)
_ERROR_OUTCOMES = _OUTCOMES - {"RETURNED", "RECORDED", "CANCELLED"}


def validate_endpoint(endpoint: str) -> tuple[str, int]:
    """Accept literal loopback IPs only; never perform DNS resolution."""
    try:
        parts = urlsplit(endpoint)
        host = parts.hostname or ""
        port = parts.port if parts.port is not None else 80
        address = ipaddress.ip_address(host)
        if (
            parts.scheme != "http"
            or not address.is_loopback
            or "%" in host
            or (isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None)
            or parts.username is not None
            or parts.password is not None
            or parts.path != "/v1/traces"
            or parts.query
            or parts.fragment
            or "?" in endpoint
            or "#" in endpoint
            or any(c.isspace() for c in endpoint)
            or not 1 <= port <= 65535
        ):
            raise ValueError
    except ValueError:
        raise ValueError("telemetry endpoint must be an HTTP loopback IP /v1/traces URL") from None
    return host, port


def _positive_timeout(value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError("telemetry timeout must be positive and finite")


class LoopbackOTLPExporter(SpanExporter):
    """Single POST, no redirect/retry, no logging of collector-controlled content."""

    def __init__(self, endpoint: str, timeout_seconds: float = 1):
        self._host, self._port = validate_endpoint(endpoint)
        _positive_timeout(timeout_seconds)
        self._timeout = timeout_seconds

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        connection = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        try:
            body = encode_spans(spans).SerializeToString()
            connection.request(
                "POST",
                "/v1/traces",
                body=body,
                headers={
                    "Content-Type": "application/x-protobuf",
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            if response.status != 200:
                return SpanExportResult.FAILURE
            payload = response.read(65537)
            if len(payload) > 65536:
                return SpanExportResult.FAILURE
            result = ExportTraceServiceResponse()
            result.ParseFromString(payload)
            if result.partial_success.rejected_spans != 0:
                return SpanExportResult.FAILURE
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE
        finally:
            connection.close()

    def shutdown(self) -> None:
        pass


class _BoundedProcessor(SpanProcessor):
    def __init__(self, exporter: SpanExporter, capacity: int, shutdown_timeout: float):
        self._exporter = exporter
        self._capacity = capacity
        self._shutdown_timeout = shutdown_timeout
        self._condition = threading.Condition()
        self._queue: deque[ReadableSpan] = deque()
        self._accepting = True
        self._stopped = False
        self._counts = dict.fromkeys(
            [
                "ended",
                "enqueued",
                "exported",
                "failed",
                "dropped",
                "in_flight",
                "instrumentation_errors",
            ],
            0,
        )
        self._thread = threading.Thread(target=self._run, name="runtime-otel-export", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def on_end(self, span: ReadableSpan) -> None:
        with self._condition:
            self._counts["ended"] += 1
            if not self._accepting or len(self._queue) >= self._capacity:
                self._counts["dropped"] += 1
                return
            self._queue.append(span)
            self._counts["enqueued"] += 1
            self._condition.notify_all()

    def instrumentation_error(self) -> None:
        with self._condition:
            self._counts["instrumentation_errors"] += 1

    def snapshot(self) -> dict[str, int]:
        with self._condition:
            return {**self._counts, "queue_depth": len(self._queue)}

    def _run(self) -> None:
        try:
            while True:
                with self._condition:
                    self._condition.wait_for(lambda: bool(self._queue) or self._stopped)
                    if self._stopped:
                        return
                    span = self._queue.popleft()
                    self._counts["in_flight"] = 1
                try:
                    success = self._exporter.export((span,)) == SpanExportResult.SUCCESS
                except Exception:
                    success = False
                with self._condition:
                    self._counts["exported" if success else "failed"] += 1
                    self._counts["in_flight"] = 0
                    self._condition.notify_all()
        finally:
            try:
                self._exporter.shutdown()
            except Exception:
                self.instrumentation_error()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        deadline = time.monotonic() + max(0, timeout_millis) / 1000
        with self._condition:
            while self._queue or self._counts["in_flight"]:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def shutdown(self) -> None:
        deadline = time.monotonic() + self._shutdown_timeout
        with self._condition:
            if self._stopped:
                return
            self._accepting = False
        self.force_flush(int(max(0, deadline - time.monotonic()) * 1000))
        with self._condition:
            self._stopped = True
            self._counts["dropped"] += len(self._queue)
            self._queue.clear()
            self._condition.notify_all()
        self._thread.join(max(0, deadline - time.monotonic()))


class _Handle:
    def __init__(
        self,
        span: trace.Span,
        active: ContextVar[Context | None],
        active_token: Token[Context | None],
        context_token: Token[Context],
        processor: _BoundedProcessor,
    ):
        self._span = span
        self._active = active
        self._active_token = active_token
        self._context_token = context_token
        self._processor = processor
        self._finished = False

    def finish(self, outcome: str) -> None:
        if self._finished:
            return
        self._finished = True
        safe_outcome = outcome if outcome in _OUTCOMES else "ERROR"
        try:
            self._span.set_attribute("runtime.outcome", safe_outcome)
            if safe_outcome in _ERROR_OUTCOMES:
                self._span.set_status(trace.StatusCode.ERROR)
            self._span.end()
        except Exception:
            self._processor.instrumentation_error()
        finally:
            context.detach(self._context_token)
            self._active.reset(self._active_token)


class OtelRuntimeTelemetry:
    def __init__(
        self,
        *,
        endpoint: str = "http://127.0.0.1:4318/v1/traces",
        queue_capacity: int = 2048,
        export_timeout_seconds: float = 1,
        shutdown_timeout_seconds: float = 2,
        exporter: SpanExporter | None = None,
    ):
        # SDK 1.44's boolean parser logs invalid environment values verbatim.
        # Reject them before constructing SDK objects or starting a transport thread.
        internal_metrics = os.environ.get("OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED")
        if internal_metrics is not None and internal_metrics.strip().lower() not in {
            "true",
            "false",
        }:
            raise ValueError("invalid telemetry internal metrics setting")
        validate_endpoint(endpoint)
        if type(queue_capacity) is not int or queue_capacity < 1:
            raise ValueError("telemetry queue capacity must be a positive integer")
        _positive_timeout(export_timeout_seconds)
        _positive_timeout(shutdown_timeout_seconds)
        self._provider = TracerProvider(
            sampler=ALWAYS_ON,
            # Explicitly avoid the SDK consulting a global meter provider even
            # when its internal-metrics environment flag enables synchronous adds.
            meter_provider=NoOpMeterProvider(),
            resource=Resource(
                {
                    "service.name": "agent-runtime-worker",
                    "runtime.telemetry.mapping_version": "1",
                }
            ),
            shutdown_on_exit=False,
            span_limits=SpanLimits(
                max_attributes=16,
                max_events=0,
                max_links=0,
                max_span_attributes=16,
                max_event_attributes=0,
                max_link_attributes=0,
                max_attribute_length=128,
                max_span_attribute_length=128,
            ),
        )
        self._tracer = self._provider.get_tracer("agent_platform.runtime", "1")
        self._processor = _BoundedProcessor(
            exporter
            if exporter is not None
            else LoopbackOTLPExporter(endpoint, export_timeout_seconds),
            queue_capacity,
            shutdown_timeout_seconds,
        )
        self._provider.add_span_processor(self._processor)
        self._active: ContextVar[Context | None] = ContextVar("runtime_otel_context", default=None)
        self._closed = False
        self._processor.start()

    def start(self, operation: str, work: ClaimedWork) -> SpanHandle:
        if operation not in _OPERATIONS:
            raise ValueError("unsupported telemetry operation")
        if self._closed:
            return NoopSpan()
        attributes: dict[str, str | int] = {
            "runtime.operation": operation,
            "runtime.attempt.number": work.attempt_no,
            "runtime.tenant.hash": hashlib.sha256(work.tenant_id.encode()).hexdigest(),
            "runtime.project.hash": hashlib.sha256(work.project_id.encode()).hexdigest(),
        }
        for field, value in (
            ("run", work.run_id),
            ("step", work.step_id),
            ("attempt", work.attempt_id),
            ("tool_version", work.tool_version_id),
            ("agent_version", work.agent_version_id),
        ):
            if value:
                if re.fullmatch(r"(?:[a-f0-9]{32}|[a-f0-9]{48}|[a-f0-9]{64})", value):
                    attributes[f"runtime.{field}.id"] = value
                    continue
                try:
                    attributes[f"runtime.{field}.id"] = str(UUID(value))
                except ValueError:
                    attributes[f"runtime.{field}.hash"] = hashlib.sha256(value.encode()).hexdigest()
        parent = Context() if operation == "attempt" else (self._active.get() or Context())
        span = self._tracer.start_span(
            f"runtime.{operation}", context=parent, attributes=attributes
        )
        isolated = trace.set_span_in_context(span, Context())
        return _Handle(
            span,
            self._active,
            self._active.set(isolated),
            context.attach(isolated),
            self._processor,
        )

    def snapshot(self) -> dict[str, int]:
        return self._processor.snapshot()

    def force_flush(self, timeout_seconds: float = 1) -> bool:
        _positive_timeout(timeout_seconds)
        return self._processor.force_flush(int(timeout_seconds * 1000))

    def shutdown(self) -> None:
        if not self._closed:
            self._closed = True
            self._provider.shutdown()
