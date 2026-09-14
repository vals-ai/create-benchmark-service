"""Sentry telemetry and request correlation for benchmark services."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

import sentry_sdk
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind
from sentry_sdk.integrations.logging import LoggingIntegration

SENTRY_DSN_ENV = "SENTRY_DSN"
SENTRY_ENVIRONMENT_ENV = "SENTRY_ENVIRONMENT"
SENTRY_RELEASE_ENV = "SENTRY_RELEASE"
RUN_ID_HEADER = "X-Vals-Run-ID"
TASK_ID_HEADER = "X-Vals-Task-ID"

_run_id: ContextVar[str | None] = ContextVar("benchmark_service_run_id", default=None)
_task_id: ContextVar[str | None] = ContextVar("benchmark_service_task_id", default=None)
_tracer = trace.get_tracer("benchmark_service.client")


def init_sentry() -> bool:
    """Initialize the process-wide Sentry client once when a DSN is configured."""
    dsn = os.getenv(SENTRY_DSN_ENV)
    if not dsn:
        return False

    if not sentry_sdk.is_initialized():
        sentry_sdk.init(
            dsn=dsn,
            environment=os.getenv(SENTRY_ENVIRONMENT_ENV),
            release=os.getenv(SENTRY_RELEASE_ENV),
            traces_sample_rate=1.0,
            integrations=[LoggingIntegration(level=None, event_level=None, sentry_logs_level=None)],
        )
    return True


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
    inject(request_headers)
    run_id = _run_id.get()
    task_id = _task_id.get()
    if run_id is not None:
        request_headers[RUN_ID_HEADER] = run_id
    if task_id is not None:
        request_headers[TASK_ID_HEADER] = task_id
    return request_headers


@contextmanager
def websocket_request_span(
    operation: str,
    headers: Mapping[str, str],
) -> Iterator[tuple[trace.Span, dict[str, str]]]:
    """Create the explicit WebSocket client span and inject its request headers."""
    with _tracer.start_as_current_span(operation, kind=SpanKind.CLIENT) as span:
        yield span, request_headers(headers)


def bind_service_context(*, service_name: str, framework_version: str, service_version: str | None) -> None:
    """Attach app-owned service identity to the current native Sentry scope."""
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
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and 500 <= status_code <= 599:
        return
    sentry_sdk.capture_exception(exc)


def capture_exception(exc: Exception) -> None:
    """Capture an exception consumed by a benchmark-service transport."""
    sentry_sdk.capture_exception(exc)
