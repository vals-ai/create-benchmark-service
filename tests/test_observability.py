"""Focused proof for benchmark-service Sentry telemetry and trace propagation."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock

import opentelemetry.propagate as propagate
import pytest
import sentry_sdk
import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sentry_sdk.consts import INSTRUMENTER
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.opentelemetry import SentryPropagator, SentrySpanProcessor
from sentry_sdk.integrations.stdlib import StdlibIntegration
from sentry_sdk.transport import Transport

from benchmark_service import __version__ as framework_version
from benchmark_service import app as app_module, observability
from benchmark_service import client as client_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.client import BenchmarkServiceClient, SandboxRecoveryAttempt
from benchmark_service.observability import (
    RUN_ID_HEADER,
    TASK_ID_HEADER,
    correlation_scope,
    init_sentry,
    request_headers,
)
from benchmark_service.schemas import EvaluateResponseRequest
from tests.conftest import StubBenchmark


class _CaptureTransport(Transport):
    def __init__(self, options: dict[str, Any] | None = None) -> None:
        super().__init__(options)
        self.events: list[dict[str, Any]] = []
        self.transactions: list[dict[str, Any]] = []
        self.init_calls = 0

    def capture_envelope(self, envelope: Envelope) -> None:
        for item in envelope.items:
            event = item.get_event()
            if event is not None:
                self.events.append(dict(event))
            transaction = item.get_transaction_event()
            if transaction is not None:
                self.transactions.append(dict(transaction))


class _FailedStatusError(Exception):
    status_code = 500


class _FailingWebSocketBenchmark(StubBenchmark):
    def get_service_version(self) -> str:
        return "websocket-service-4.5.6"

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        raise RuntimeError("websocket evaluation failed")


class _VersionedBenchmark(StubBenchmark):
    def get_service_version(self) -> str:
        return "service-hook-1.2.3"


class _OtherVersionedBenchmark(StubBenchmark):
    def get_service_version(self) -> str:
        return "other-service-4.5.6"


_INCOMING_TRACE_ID = "0123456789abcdef0123456789abcdef"
_INCOMING_CLIENT_SPAN_ID = "0123456789abcdef"
_INCOMING_SENTRY_TRACE = f"{_INCOMING_TRACE_ID}-{_INCOMING_CLIENT_SPAN_ID}-1"


@asynccontextmanager
async def _serve_loopback(app: FastAPI) -> AsyncIterator[str]:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 0))
    server_socket.listen()
    server_socket.setblocking(False)
    port = server_socket.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="critical", access_log=False)
    )
    server_task = asyncio.create_task(server.serve(sockets=[server_socket]))

    try:
        deadline = asyncio.get_running_loop().time() + 5
        while not server.started:
            if server_task.done():
                server_task.result()
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("Uvicorn did not start within 5 seconds")
            await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5)
        finally:
            server_socket.close()


@pytest.fixture
def configured_sentry(monkeypatch: pytest.MonkeyPatch) -> Iterator[_CaptureTransport]:
    transport = _CaptureTransport()
    real_init = sentry_sdk.init
    monkeypatch.setattr(
        observability.sentry_sdk,
        "is_initialized",
        lambda: transport.init_calls > 0,
    )

    def init_with_transport(*args: Any, **kwargs: Any) -> Any:
        transport.init_calls += 1
        kwargs["transport"] = transport
        kwargs["disabled_integrations"] = [StdlibIntegration]
        return real_init(*args, **kwargs)

    monkeypatch.setenv("AUTH_DISABLED", "true")
    monkeypatch.setenv("SERVICE_NAME", "swebench")
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "dev")
    monkeypatch.setenv("SENTRY_RELEASE", "abc123")
    monkeypatch.setattr(observability.sentry_sdk, "init", init_with_transport)
    yield transport
    sentry_sdk.flush()
    sentry_sdk.get_client().close()
    real_init(dsn=None, disabled_integrations=[StdlibIntegration])




def _composite_propagator() -> CompositePropagator:
    return CompositePropagator(
        [TraceContextTextMapPropagator(), W3CBaggagePropagator(), SentryPropagator()]
    )


@pytest.fixture
def otel_tracer(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[trace.Tracer, InMemorySpanExporter, TracerProvider]]:
    real_init = sentry_sdk.init
    real_init(
        dsn="https://public@example.com/1",
        transport=_CaptureTransport,
        instrumenter=INSTRUMENTER.OTEL,
        traces_sample_rate=1.0,
        disabled_integrations=[StdlibIntegration],
    )
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SentrySpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("benchmark-service-test")
    monkeypatch.setattr(observability, "_tracer", tracer)
    previous_propagator = propagate.get_global_textmap()
    propagate.set_global_textmap(_composite_propagator())
    try:
        yield tracer, exporter, provider
    finally:
        propagate.set_global_textmap(previous_propagator)
        try:
            provider.shutdown()
            sentry_sdk.flush()
            sentry_sdk.get_client().close()
        finally:
            real_init(dsn=None, disabled_integrations=[StdlibIntegration])






def test_app_without_sentry_dsn_preserves_health(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.setenv("AUTH_DISABLED", "true")
    init = Mock()
    monkeypatch.setattr(observability.sentry_sdk, "init", init)

    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    init.assert_not_called()


def test_init_sentry_uses_process_configuration_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "dev")
    monkeypatch.setenv("SENTRY_RELEASE", "abc123")
    is_initialized = Mock(side_effect=[False, True])
    init = Mock()
    monkeypatch.setattr(observability.sentry_sdk, "is_initialized", is_initialized)
    monkeypatch.setattr(observability.sentry_sdk, "init", init)

    assert init_sentry() is True
    assert init_sentry() is True

    assert init.call_count == 1
    options = init.call_args.kwargs
    assert options["dsn"] == "https://public@example.com/1"
    assert options["environment"] == "dev"
    assert options["release"] == "abc123"
    assert options["traces_sample_rate"] == 1.0


def test_lifespan_failure_before_service_creation_uses_package_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )
    monkeypatch.setattr(
        app_module,
        "require_supported_auth_config",
        Mock(side_effect=RuntimeError("startup configuration failed")),
    )
    app = BenchmarkServiceApp(_VersionedBenchmark)

    with sentry_sdk.isolation_scope():
        sentry_sdk.set_tag("service.version", "caller-service-8.8.8")
        with pytest.raises(RuntimeError, match="startup configuration failed"):
            with TestClient(app):
                pass
        sentry_sdk.flush()

        assert len(configured_sentry.events) == 1
        tags = configured_sentry.events[0]["tags"]
        assert tags["service.name"] == "swebench"
        assert tags["framework.version"] == framework_version
        assert tags["service.version"] == "package-1.2.3"

        sentry_sdk.capture_message("caller scope after lifespan failure")
        sentry_sdk.flush()
        assert configured_sentry.events[1]["tags"]["service.version"] == "caller-service-8.8.8"


def test_lifespan_failure_after_service_creation_uses_runtime_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )

    def fail_after_service_creation(_service: _VersionedBenchmark) -> Any:
        raise RuntimeError("post-create startup failed")

    monkeypatch.setattr(
        _VersionedBenchmark,
        "eval_mode",
        property(fail_after_service_creation),
    )
    app = BenchmarkServiceApp(_VersionedBenchmark)

    with pytest.raises(RuntimeError, match="post-create startup failed"):
        with TestClient(app):
            pass
    sentry_sdk.flush()

    assert len(configured_sentry.events) == 1
    tags = configured_sentry.events[0]["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "service-hook-1.2.3"


def test_versionless_app_does_not_inherit_other_app_service_version(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", None)),
    )
    monkeypatch.setenv("SERVICE_NAME", "versioned-service")
    versioned_app = BenchmarkServiceApp(_VersionedBenchmark)
    monkeypatch.setenv("SERVICE_NAME", "versionless-service")
    versionless_app = BenchmarkServiceApp(StubBenchmark)

    async def fail() -> None:
        raise RuntimeError("app failed")

    versioned_app.add_api_route("/fail", fail, methods=["GET"])
    versionless_app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(versioned_app, raise_server_exceptions=False) as client:
        versioned_response = client.get("/fail")
    with TestClient(versionless_app, raise_server_exceptions=False) as client:
        versionless_response = client.get("/fail")
    sentry_sdk.flush()

    assert (versioned_response.status_code, versionless_response.status_code) == (
        500,
        500,
    )
    assert len(configured_sentry.events) == 2
    versioned_tags = configured_sentry.events[0]["tags"]
    versionless_tags = configured_sentry.events[1]["tags"]
    assert versioned_tags["service.version"] == "service-hook-1.2.3"
    assert versionless_tags["service.name"] == "versionless-service"
    assert versionless_tags["framework.version"] == framework_version
    assert "service.version" not in versionless_tags


async def test_concurrent_correlation_scopes_restore_their_caller() -> None:
    barrier = asyncio.Barrier(2)

    async def headers_for(run_id: str, task_id: str) -> dict[str, str]:
        with correlation_scope(run_id=run_id, task_id=task_id):
            await barrier.wait()
            inner_headers = request_headers({})
            assert (inner_headers[RUN_ID_HEADER], inner_headers[TASK_ID_HEADER]) == (run_id, task_id)
        restored = request_headers({})
        assert (restored[RUN_ID_HEADER], restored[TASK_ID_HEADER]) == ("outer-run", "outer-task")
        return inner_headers

    with correlation_scope(run_id="outer-run", task_id="outer-task"):
        first, second = await asyncio.gather(
            headers_for("run-a", "task-a"),
            headers_for("run-b", "task-b"),
        )
        restored = request_headers({})

    uncorrelated = request_headers({})
    assert (first[RUN_ID_HEADER], first[TASK_ID_HEADER]) == ("run-a", "task-a")
    assert (second[RUN_ID_HEADER], second[TASK_ID_HEADER]) == ("run-b", "task-b")
    assert (restored[RUN_ID_HEADER], restored[TASK_ID_HEADER]) == ("outer-run", "outer-task")
    assert RUN_ID_HEADER not in uncorrelated
    assert TASK_ID_HEADER not in uncorrelated

@pytest.mark.parametrize("error_type", [RuntimeError, _FailedStatusError])
def test_http_exceptions_are_captured_once_with_request_identity(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
    error_type: type[Exception],
) -> None:
    app = BenchmarkServiceApp(StubBenchmark)

    async def fail() -> None:
        raise error_type("request failed")

    app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(
            "/fail",
            headers={
                RUN_ID_HEADER: "run-1",
                TASK_ID_HEADER: "task-1",
                "sentry-trace": _INCOMING_SENTRY_TRACE,
            },
        )
    sentry_sdk.flush()

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    assert len(configured_sentry.events) == 1
    event = configured_sentry.events[0]
    assert event["exception"]["values"][-1]["type"] == error_type.__name__
    assert event["tags"]["run_id"] == "run-1"
    assert event["tags"]["task_id"] == "task-1"
    assert event["tags"]["service.name"] == "swebench"
    assert event["environment"] == "dev"
    assert event["release"] == "abc123"
    transaction = next(
        item
        for item in configured_sentry.transactions
        if item["contexts"]["trace"].get("parent_span_id") == _INCOMING_CLIENT_SPAN_ID
    )
    assert transaction["contexts"]["trace"]["trace_id"] == _INCOMING_TRACE_ID


def test_request_identity_uses_runtime_service_version_override(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setattr(
        app_module,
        "_get_service_metadata",
        Mock(return_value=("benchmark-package", "package-1.2.3")),
    )
    app = BenchmarkServiceApp(_VersionedBenchmark)

    async def fail() -> None:
        raise RuntimeError("versioned request failed")

    app.add_api_route("/fail-versioned", fail, methods=["GET"])
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/fail-versioned")
    sentry_sdk.flush()

    assert response.status_code == 500
    assert len(configured_sentry.events) == 1
    event = configured_sentry.events[0]
    tags = event["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "service-hook-1.2.3"
    assert event["release"] == "abc123"


def test_multiple_apps_keep_request_identity_and_single_client_initialization(
    monkeypatch: pytest.MonkeyPatch,
    configured_sentry: _CaptureTransport,
) -> None:
    monkeypatch.setenv("SERVICE_NAME", "first-service")
    first_app = BenchmarkServiceApp(_VersionedBenchmark)
    monkeypatch.setenv("SERVICE_NAME", "second-service")
    second_app = BenchmarkServiceApp(_OtherVersionedBenchmark)

    async def fail() -> None:
        raise RuntimeError("app failed")

    first_app.add_api_route("/fail", fail, methods=["GET"])
    second_app.add_api_route("/fail", fail, methods=["GET"])
    with TestClient(first_app, raise_server_exceptions=False) as client:
        first_response = client.get("/fail")
    with TestClient(second_app, raise_server_exceptions=False) as client:
        second_response = client.get("/fail")
    sentry_sdk.flush()

    assert (first_response.status_code, second_response.status_code) == (500, 500)
    assert len(configured_sentry.events) == 2
    assert [
        (
            event["tags"]["service.name"],
            event["tags"]["framework.version"],
            event["tags"]["service.version"],
        )
        for event in configured_sentry.events
    ] == [
        ("first-service", framework_version, "service-hook-1.2.3"),
        ("second-service", framework_version, "other-service-4.5.6"),
    ]
    assert configured_sentry.init_calls == 1


def test_websocket_exception_is_captured_once_with_request_identity(
    configured_sentry: _CaptureTransport,
) -> None:
    app = BenchmarkServiceApp(_FailingWebSocketBenchmark)

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/evaluate-response",
            headers={RUN_ID_HEADER: "run-2", "sentry-trace": _INCOMING_SENTRY_TRACE},
        ) as websocket:
            websocket.send_json({"task_id": "task-2", "response": "answer", "dataset": "default"})
            response = websocket.receive_json()
    sentry_sdk.flush()

    assert response["type"] == "error"
    assert "websocket evaluation failed" in response["data"]
    assert len(configured_sentry.events) == 1
    tags = configured_sentry.events[0]["tags"]
    assert tags["service.name"] == "swebench"
    assert tags["framework.version"] == framework_version
    assert tags["service.version"] == "websocket-service-4.5.6"
    assert tags["run_id"] == "run-2"
    assert tags["task_id"] == "task-2"
    assert tags["dataset"] == "default"
    transaction = next(
        item
        for item in configured_sentry.transactions
        if item["contexts"]["trace"].get("parent_span_id") == _INCOMING_CLIENT_SPAN_ID
    )
    assert transaction["contexts"]["trace"]["trace_id"] == _INCOMING_TRACE_ID


async def test_http_client_transport_span_injects_trace_and_dynamic_identity(
    otel_tracer: tuple[trace.Tracer, InMemorySpanExporter, TracerProvider],
) -> None:
    tracer, exporter, provider = otel_tracer
    app = FastAPI()
    observed_headers: dict[str, str] = {}

    async def health(request: Request) -> dict[str, str]:
        observed_headers.update(request.headers)
        return {"status": "ok"}
    app.add_api_route("/health", health, methods=["GET"])

    instrumentor = HTTPXClientInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    try:
        async with _serve_loopback(app) as url:
            async with BenchmarkServiceClient(url, {"Authorization": "Bearer test"}) as client:

                async def operation(_attempt: SandboxRecoveryAttempt) -> Any:
                    return await client.health_check()

                with tracer.start_as_current_span("caller") as caller_span:
                    caller_context = caller_span.get_span_context()
                    result = await client.run_with_sandbox_recovery("task-3", "run-3", operation)
    finally:
        instrumentor.uninstrument()

    client_spans = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.CLIENT]
    assert len(client_spans) == 1
    client_span = client_spans[0]
    assert client_span.name == "GET"
    assert result.status == "ok"
    assert client_span.context is not None
    assert client_span.context.trace_id == caller_context.trace_id
    assert client_span.context.span_id != caller_context.span_id
    assert observed_headers[RUN_ID_HEADER.lower()] == "run-3"
    assert observed_headers[TASK_ID_HEADER.lower()] == "task-3"
    assert observed_headers["traceparent"].split("-")[:3] == [
        "00",
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]
    assert observed_headers["sentry-trace"].split("-")[:2] == [
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]


async def test_websocket_handshake_uses_its_client_span_and_dynamic_identity(
    otel_tracer: tuple[trace.Tracer, InMemorySpanExporter, TracerProvider],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracer, exporter, _provider = otel_tracer
    monkeypatch.setattr(client_module.trace, "get_tracer", lambda _name: tracer)
    app = FastAPI()
    observed_headers: dict[str, str] = {}

    async def evaluate_response(websocket: WebSocket) -> None:
        observed_headers.update(websocket.headers)
        await websocket.accept()
        request = await websocket.receive_json()
        await websocket.send_json(
            {
                "type": "result",
                "data": {
                    "task_id": request["task_id"],
                    "state": request["eval_resume_state"],
                },
            }
        )
        await websocket.close()
    app.add_api_websocket_route("/ws/evaluate-response", evaluate_response)

    async with _serve_loopback(app) as url:
        async with BenchmarkServiceClient(url, {"Authorization": "Bearer test"}) as client:

            async def operation(_attempt: SandboxRecoveryAttempt) -> Any:
                return await client.resume_evaluation(
                    "task-1",
                    {"artifact_prefix": "s3://bucket/run"},
                )

            with tracer.start_as_current_span("caller") as caller_span:
                caller_context = caller_span.get_span_context()
                result = await client.run_with_sandbox_recovery("task-1", "run-4", operation)

    client_spans = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.CLIENT]
    assert len(client_spans) == 1
    client_span = client_spans[0]
    assert client_span.name == "WEBSOCKET /ws/evaluate-response"
    recovery_spans = [
        span
        for span in exporter.get_finished_spans()
        if span.name == "benchmark_service.recovery"
    ]
    assert len(recovery_spans) == 1
    recovery_span = recovery_spans[0]
    assert recovery_span.context is not None
    assert recovery_span.parent is not None
    assert recovery_span.parent.span_id == caller_context.span_id
    assert client_span.parent is not None
    assert client_span.parent.span_id == recovery_span.context.span_id
    assert recovery_span.attributes == {
        "benchmark_service.recovery.outcome": "success",
        "benchmark_service.recovery.trigger": "none",
        "benchmark_service.recovery.attempt_count": 1,
        "benchmark_service.recovery.retry_count": 0,
    }
    assert client_span.attributes == {
        "benchmark_service.websocket.outcome": "result",
        "benchmark_service.websocket.message_chunk_count": 0,
        "benchmark_service.websocket.resume_state_chunk_count": 0,
    }
    assert result == {"task_id": "task-1", "state": {"artifact_prefix": "s3://bucket/run"}}
    assert client_span.context is not None
    assert client_span.context.trace_id == caller_context.trace_id
    assert client_span.context.span_id != caller_context.span_id
    assert observed_headers[RUN_ID_HEADER.lower()] == "run-4"
    assert observed_headers[TASK_ID_HEADER.lower()] == "task-1"
    assert observed_headers["traceparent"].split("-")[:3] == [
        "00",
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]
    assert observed_headers["sentry-trace"].split("-")[:2] == [
        f"{caller_context.trace_id:032x}",
        f"{client_span.context.span_id:016x}",
    ]
