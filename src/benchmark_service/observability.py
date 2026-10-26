"""Trace propagation and request correlation for benchmark clients."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind

RUN_ID_HEADER = "X-Vals-Run-ID"
TASK_ID_HEADER = "X-Vals-Task-ID"
ATTEMPT_ID_HEADER = "X-Vals-Attempt-ID"
EMAIL_HEADER = "X-Vals-Email"

_run_id: ContextVar[str | None] = ContextVar("benchmark_service_run_id", default=None)
_task_id: ContextVar[str | None] = ContextVar("benchmark_service_task_id", default=None)
_attempt_id: ContextVar[str | None] = ContextVar("benchmark_service_attempt_id", default=None)
_email: ContextVar[str | None] = ContextVar("benchmark_service_email", default=None)
_tracer = trace.get_tracer("benchmark_service.client")


@contextmanager
def correlation_scope(
    *,
    run_id: str | None = None,
    task_id: str | None = None,
    attempt_id: str | None = None,
    email: str | None = None,
) -> Iterator[None]:
    """Bind caller correlation values for nested client requests.

    A scope that names a run also names its attempt and email (possibly none), so a
    nested request for another run never inherits the enclosing ones.
    """
    run_token = _run_id.set(run_id) if run_id is not None else None
    task_token = _task_id.set(task_id) if task_id is not None else None
    attempt_token = _attempt_id.set(attempt_id) if run_id is not None or attempt_id is not None else None
    email_token = _email.set(email) if run_id is not None or email is not None else None
    try:
        yield
    finally:
        if email_token is not None:
            _email.reset(email_token)
        if attempt_token is not None:
            _attempt_id.reset(attempt_token)
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
    attempt_id = _attempt_id.get()
    email = _email.get()
    if run_id is not None:
        request_headers[RUN_ID_HEADER] = run_id
    if task_id is not None:
        request_headers[TASK_ID_HEADER] = task_id
    if attempt_id is not None:
        request_headers[ATTEMPT_ID_HEADER] = attempt_id
    if email is not None:
        request_headers[EMAIL_HEADER] = email
    return request_headers


@contextmanager
def websocket_request_span(operation: str, headers: Mapping[str, str]) -> Iterator[dict[str, str]]:
    """Create the explicit WebSocket client span and inject its request headers."""
    with _tracer.start_as_current_span(operation, kind=SpanKind.CLIENT):
        yield request_headers(headers)
