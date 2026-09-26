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

_run_id: ContextVar[str | None] = ContextVar("benchmark_service_run_id", default=None)
_task_id: ContextVar[str | None] = ContextVar("benchmark_service_task_id", default=None)
_tracer = trace.get_tracer("benchmark_service.client")


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
def websocket_request_span(operation: str, headers: Mapping[str, str]) -> Iterator[dict[str, str]]:
    """Create the explicit WebSocket client span and inject its request headers."""
    with _tracer.start_as_current_span(operation, kind=SpanKind.CLIENT):
        yield request_headers(headers)
