"""Sentry integration loaded when a service enables error reporting."""

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from benchmark_service.observability import RUN_ID_HEADER, TASK_ID_HEADER

SENTRY_DSN_ENV = "SENTRY_DSN"
SENTRY_ENVIRONMENT_ENV = "SENTRY_ENVIRONMENT"
SENTRY_RELEASE_ENV = "SENTRY_RELEASE"


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
            traces_sample_rate=0.1,
            integrations=[LoggingIntegration(level=None, event_level=None, sentry_logs_level=None)],
        )
    return True


@contextmanager
def isolation_scope() -> Iterator[None]:
    """Isolate telemetry for a service with Sentry enabled."""
    with sentry_sdk.isolation_scope():
        yield


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
