"""Focused proof for benchmark-service Sentry telemetry and trace propagation."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock, call

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
from sentry_sdk.transport import Transport

from benchmark_service import observability
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
    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        raise RuntimeError("websocket evaluation failed")


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

    def init_with_transport(*args: Any, **kwargs: Any) -> Any:
        kwargs["transport"] = transport
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
    real_init(dsn=None)


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
            real_init(dsn=None)





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


def test_init_sentry_uses_deployment_identity_and_full_sampling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.com/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "dev")
    monkeypatch.setenv("SENTRY_RELEASE", "abc123")
    init = Mock()
    scope = Mock()
    monkeypatch.setattr(observability.sentry_sdk, "init", init)
    monkeypatch.setattr(observability.sentry_sdk, "get_global_scope", Mock(return_value=scope))

    enabled = init_sentry(service_name="swebench", framework_version="0.38.0", service_version="1.2.3")

    assert enabled is True
    options = init.call_args.kwargs
    assert options["dsn"] == "https://public@example.com/1"
    assert options["environment"] == "dev"
    assert options["release"] == "abc123"
    assert options["traces_sample_rate"] == 1.0
    scope.set_tag.assert_has_calls(
        [
            call("service.name", "swebench"),
            call("framework.version", "0.38.0"),
            call("service.version", "1.2.3"),
        ]
    )


async def test_concurrent_correlation_scopes_restore_their_caller() -> None:
    barrier = asyncio.Barrier(2)

    async def headers_for(run_id: str, task_id: str) -> dict[str, str]:
        with correlation_scope(run_id=run_id, task_id=task_id):
            await barrier.wait()
            inner_headers = request_headers()
            assert (inner_headers[RUN_ID_HEADER], inner_headers[TASK_ID_HEADER]) == (run_id, task_id)
        restored = request_headers()
        assert (restored[RUN_ID_HEADER], restored[TASK_ID_HEADER]) == ("outer-run", "outer-task")
        return inner_headers

    with correlation_scope(run_id="outer-run", task_id="outer-task"):
        first, second = await asyncio.gather(
            headers_for("run-a", "task-a"),
            headers_for("run-b", "task-b"),
        )
        restored = request_headers()

    uncorrelated = request_headers()
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

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        observed_headers.update(request.headers)
        return {"status": "ok"}

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
) -> None:
    tracer, exporter, _provider = otel_tracer
    app = FastAPI()
    observed_headers: dict[str, str] = {}

    @app.websocket("/ws/evaluate-response")
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
