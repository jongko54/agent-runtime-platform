import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from test_otel_adapter import work

from agent_platform.adapters.telemetry.otel import OtelRuntimeTelemetry, validate_endpoint


@contextmanager
def collector(status=200, response=b"", extra_headers=None, delay=0):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers["Content-Length"])
            requests.append((self.path, dict(self.headers), self.rfile.read(length)))
            if delay:
                time.sleep(delay)
            self.send_response(status)
            self.send_header("Content-Length", str(len(response)))
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/traces", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)


def test_real_otlp_protobuf_ignores_env_headers_proxy_and_resource(monkeypatch):
    for name, value in {
        "OTEL_EXPORTER_OTLP_HEADERS": "Authorization=header-canary",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "secret=trace-header-canary",
        "OTEL_RESOURCE_ATTRIBUTES": "secret=resource-canary",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://external-canary.invalid",
        "HTTP_PROXY": "http://proxy-canary.invalid:99",
        "HTTPS_PROXY": "http://proxy-canary.invalid:99",
        "ALL_PROXY": "http://proxy-canary.invalid:99",
        "NO_PROXY": "",
    }.items():
        monkeypatch.setenv(name, value)
    with collector() as (endpoint, requests):
        telemetry = OtelRuntimeTelemetry(endpoint=endpoint)
        try:
            telemetry.start("attempt", work()).finish("RECORDED")
            assert telemetry.force_flush(1)
            assert telemetry.snapshot()["exported"] == 1
            assert len(requests) == 1
            path, headers, body = requests[0]
            assert path == "/v1/traces"
            assert headers["Content-Type"] == "application/x-protobuf"
            assert "Authorization" not in headers
            decoded = ExportTraceServiceRequest()
            decoded.ParseFromString(body)
            assert len(decoded.resource_spans[0].scope_spans[0].spans) == 1
            assert b"canary" not in body
        finally:
            telemetry.shutdown()


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1/v1/traces",
        "http://localhost/v1/traces",
        "http://example.com/v1/traces",
        "http://192.168.1.1/v1/traces",
        "http://169.254.169.254/v1/traces",
        "http://user:secret@127.0.0.1/v1/traces",
        "http://127.0.0.1/v1/traces?",
        "http://127.0.0.1/v1/traces?secret=value",
        "http://127.0.0.1/v1/traces#",
        "http://127.0.0.1/other",
        "http://127.0.0.1:99999/v1/traces",
        "http://127.0.0.1:0/v1/traces",
        "http://[::1%lo0]/v1/traces",
        "http://[::ffff:127.0.0.1]/v1/traces",
        "\nhttp://127.0.0.1/v1/traces",
    ],
)
def test_rejects_unsafe_endpoints_before_transport(endpoint):
    with pytest.raises(ValueError, match="loopback"):
        OtelRuntimeTelemetry(endpoint=endpoint)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:4318/v1/traces",
        "http://127.0.0.2/v1/traces",
        "http://[::1]:4318/v1/traces",
    ],
)
def test_literal_loopback_ipv4_and_ipv6_are_accepted(endpoint):
    assert validate_endpoint(endpoint)


def test_redirect_is_failure_without_following_location(caplog):
    with collector() as (target, destination):
        with collector(302, b"secret-response-canary", {"Location": target}) as (
            endpoint,
            requests,
        ):
            telemetry = OtelRuntimeTelemetry(endpoint=endpoint)
            try:
                telemetry.start("attempt", work()).finish("RECORDED")
                assert telemetry.force_flush(1)
                assert len(requests) == 1
                assert not destination
                assert telemetry.snapshot()["failed"] == 1
                assert "canary" not in caplog.text
            finally:
                telemetry.shutdown()


partial = ExportTraceServiceResponse()
partial.partial_success.rejected_spans = 1
partial.partial_success.error_message = "collector-secret-canary"


@pytest.mark.parametrize(
    ("status", "response"),
    [
        (200, partial.SerializeToString()),
        (200, b"not-protobuf-secret-canary"),
        (200, b"x" * 65537),
        (500, b"collector-secret-canary"),
    ],
)
def test_collector_failure_partial_and_invalid_response_are_counted_without_content(
    status,
    response,
    caplog,
):
    with collector(status, response) as (endpoint, requests):
        telemetry = OtelRuntimeTelemetry(endpoint=endpoint)
        try:
            telemetry.start("attempt", work()).finish("RECORDED")
            assert telemetry.force_flush(1)
            assert telemetry.snapshot()["failed"] == 1
            assert len(requests) == 1
            assert "canary" not in caplog.text
        finally:
            telemetry.shutdown()


def test_slow_collector_times_out_off_the_execution_thread():
    with collector(delay=0.2) as (endpoint, _):
        telemetry = OtelRuntimeTelemetry(endpoint=endpoint, export_timeout_seconds=0.03)
        try:
            started = time.monotonic()
            telemetry.start("attempt", work()).finish("RECORDED")
            assert time.monotonic() - started < 0.1
            assert telemetry.force_flush(1)
            assert telemetry.snapshot()["failed"] == 1
        finally:
            telemetry.shutdown()
