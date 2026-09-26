"""Sentry telemetry and request correlation for benchmark services."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

try:
    import sentry_sdk
except ModuleNotFoundError as exc:
    if exc.name != "sentry_sdk":
        raise
    sentry_sdk = None

try:
    from opentelemetry import trace
    from opentelemetry.propagate import inject
    from opentelemetry.trace import SpanKind
except ModuleNotFoundError as exc:
    if exc.name != "opentelemetry":
        raise
    trace = None
    inject = None
    SpanKind = None

SENTRY_DSN_ENV = "SENTRY_DSN"
SENTRY_ENVIRONMENT_ENV = "SENTRY_ENVIRONMENT"
SENTRY_RELEASE_ENV = "SENTRY_RELEASE"
RUN_ID_HEADER = "X-Vals-Run-ID"
TASK_ID_HEADER = "X-Vals-Task-ID"

_run_id: ContextVar[str | None] = ContextVar("benchmark_service_run_id", default=None)
_task_id: ContextVar[str | None] = ContextVar("benchmark_service_task_id", default=None)
_tracer = trace.get_tracer("benchmark_service.client") if trace is not None else None


def init_sentry() -> bool:
    """Initialize the process-wide Sentry client once when a DSN is configured."""
    dsn = os.getenv(SENTRY_DSN_ENV)
    if not dsn:
        return False
    if sentry_sdk is None:
        raise ModuleNotFoundError("Sentry requires the telemetry extra: uv add 'create-benchmark-service[telemetry]'")

    from sentry_sdk.integrations.logging import LoggingIntegration

    if not sentry_sdk.is_initialized():
        sentry_sdk.init(
            dsn=dsn,
            environment=os.getenv(SENTRY_ENVIRONMENT_ENV),
            release=os.getenv(SENTRY_RELEASE_ENV),
            traces_sample_rate=0.1,
            integrations=[LoggingIntegration(level=None, event_level=None, sentry_logs_level=None)],
        )
    return True


@contextmanager
def isolation_scope() -> Iterator[None]:
    """Isolate service telemetry when Sentry is installed."""
    if sentry_sdk is None:
        yield
        return
    with sentry_sdk.isolation_scope():
        yield


@contextmanager
def correlation_scope(*, run_id: str | None = None, task_id: str | None = None) -> Iterator[None]:
    """Bind caller correlation values for nested client requests."""
    run_token = _run_id.set(run_id) if run_id is not None else None
    task_token = _task_id.set(task_id) if task_id is not None else None
    try:
        yield
    finally:
        if task_token is not None:
            _task_id.reset(task_token)
        if run_token is not None:
            _run_id.reset(run_token)


def request_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Copy headers, inject the current trace context, and add dynamic identity."""
    request_headers = dict(headers)
    if inject is not None:
        inject(request_headers)
    run_id = _run_id.get()
    task_id = _task_id.get()
    if run_id is not None:
        request_headers[RUN_ID_HEADER] = run_id
    if task_id is not None:
        request_headers[TASK_ID_HEADER] = task_id
    return request_headers


@contextmanager
def websocket_request_span(operation: str, headers: Mapping[str, str]) -> Iterator[dict[str, str]]:
    """Create the explicit WebSocket client span and inject its request headers."""
    if _tracer is None or SpanKind is None:
        yield request_headers(headers)
        return
    with _tracer.start_as_current_span(operation, kind=SpanKind.CLIENT):
        yield request_headers(headers)


def bind_service_context(*, service_name: str, framework_version: str, service_version: str | None) -> None:
    """Attach app-owned service identity to the current native Sentry scope."""
    if sentry_sdk is None:
        return
    sentry_sdk.set_tag("service.name", service_name)
    sentry_sdk.set_tag("framework.version", framework_version)
    if service_version is not None:
        sentry_sdk.set_tag("service.version", service_version)


def bind_request_context(
    headers: Mapping[str, str],
    *,
    run_id: str | None = None,
    task_id: str | None = None,
    dataset: str | None = None,
    sandbox_id: str | None = None,
) -> None:
    """Attach benchmark request identity to the current native Sentry scope."""
    if sentry_sdk is None:
        return
    request_run_id = run_id if run_id is not None else headers.get(RUN_ID_HEADER)
    request_task_id = task_id if task_id is not None else headers.get(TASK_ID_HEADER)
    if request_run_id is not None:
        sentry_sdk.set_tag("run_id", request_run_id)
    if request_task_id is not None:
        sentry_sdk.set_tag("task_id", request_task_id)
    if dataset is not None:
        sentry_sdk.set_tag("dataset", dataset)
    if sandbox_id is not None:
        sentry_sdk.set_tag("sandbox_id", sandbox_id)


def capture_http_exception(exc: Exception) -> None:
    """Capture errors not already reported by Starlette's failed-status handler."""
    if sentry_sdk is None:
        return
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and 500 <= status_code <= 599:
        return
    sentry_sdk.capture_exception(exc)


def capture_exception(exc: Exception) -> None:
    """Capture an exception consumed by a benchmark-service transport."""
    if sentry_sdk is not None:
        sentry_sdk.capture_exception(exc)
