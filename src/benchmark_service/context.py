"""Request-scoped access to the sandbox provider and tenant serving the current call.

``evaluate_instance`` receives the sandbox to grade but not the provider that
owns it, which a benchmark needs when grading must happen somewhere the agent
could not reach. Exposing it here rather than widening that abstract signature
keeps every existing implementation a valid override::

    provider = current_sandbox_provider()
    if provider is None:
        raise RuntimeError("this benchmark grades in a second sandbox")
    verifier = await provider.create_sandbox(...)

The authenticated tenant is bound the same way, so request-scoped storage such
as ``benchmark_service.eval_resume`` can keep each tenant's objects apart.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import Context, ContextVar, copy_context

from typing import TYPE_CHECKING

from benchmark_service.sandbox import SandboxProvider
from benchmark_service.schemas import NativeToolContext, ToolContext

if TYPE_CHECKING:
    from benchmark_service.schemas import ChildEgress

_current_sandbox_provider: ContextVar[SandboxProvider | None] = ContextVar(
    "benchmark_service_sandbox_provider", default=None
)

_current_tenant: ContextVar[str | None] = ContextVar("benchmark_service_tenant", default=None)
_current_tool_context: ContextVar[ToolContext] = ContextVar(
    "benchmark_service_tools", default=NativeToolContext(mode="native")
)

_current_child_egress: ContextVar[Mapping[str, ChildEgress]] = ContextVar("benchmark_service_child_egress", default={})

_current_grading_sandbox_id: ContextVar[str | None] = ContextVar("benchmark_service_grading_sandbox_id", default=None)


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


def current_tenant() -> str | None:
    """Return the authenticated tenant of the in-flight request, or None outside one."""
    return _current_tenant.get()


def current_tool_context() -> ToolContext:
    """Return the task tool interface selected by the caller's agent contract."""
    return _current_tool_context.get()


@contextmanager
def tool_context_scope(context: ToolContext) -> Iterator[None]:
    token = _current_tool_context.set(context)
    try:
        yield
    finally:
        _current_tool_context.reset(token)


@contextmanager
def tenant_scope(tenant: str | None) -> Iterator[None]:
    """Bind the authenticated tenant for the duration of one request."""
    token = _current_tenant.set(tenant)
    try:
        yield
    finally:
        _current_tenant.reset(token)


def current_child_egress(profile: str) -> ChildEgress:
    """Return the in-flight request's proxy access for a child profile.

    Raises LookupError when the caller sent none, so a box the task declared isn't created unproxied.
    """
    try:
        return _current_child_egress.get()[profile]
    except KeyError:
        raise LookupError(f"The caller sent no egress proxy access for child profile {profile!r}") from None


@contextmanager
def child_egress_scope(child_egress: Mapping[str, ChildEgress]) -> Iterator[None]:
    """Bind the request's child egress for the duration of one request."""
    token = _current_child_egress.set(child_egress)
    try:
        yield
    finally:
        _current_child_egress.reset(token)


def current_grading_sandbox_id() -> str | None:
    """Return the id of the sandbox ``/v1`` grading created for the in-flight evaluation, or None."""
    return _current_grading_sandbox_id.get()


def grading_sandbox_context(sandbox_id: str) -> Context:
    """Copy the current context with ``sandbox_id`` bound as the ``/v1`` grading sandbox."""
    context = copy_context()
    context.run(_current_grading_sandbox_id.set, sandbox_id)
    return context
