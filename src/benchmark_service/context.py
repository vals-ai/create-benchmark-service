"""Request-scoped access to the sandbox provider serving the current call.

``evaluate_instance`` receives the sandbox to grade but not the provider that
owns it, which a benchmark needs when grading must happen somewhere the agent
could not reach. Exposing it here rather than widening that abstract signature
keeps every existing implementation a valid override::

    provider = current_sandbox_provider()
    if provider is None:
        raise RuntimeError("this benchmark grades in a second sandbox")
    verifier = await provider.create_sandbox(...)
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from benchmark_service.sandbox import SandboxProvider

_current_sandbox_provider: ContextVar[SandboxProvider | None] = ContextVar(
    "benchmark_service_sandbox_provider", default=None
)


def current_sandbox_provider() -> SandboxProvider | None:
    """Return the provider serving the in-flight request, or None outside one."""
    return _current_sandbox_provider.get()


@contextmanager
def sandbox_provider_scope(provider: SandboxProvider | None) -> Iterator[None]:
    """Bind a provider for the duration of one request."""
    token = _current_sandbox_provider.set(provider)
    try:
        yield
    finally:
        _current_sandbox_provider.reset(token)
