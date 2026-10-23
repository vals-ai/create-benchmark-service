"""Request-scoped hook a sandbox provider calls after it creates a sandbox."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

logger = logging.getLogger(__name__)

SandboxCreatedHook = Callable[[str, str], Awaitable[None]]

_sandbox_created_hook: ContextVar[SandboxCreatedHook | None] = ContextVar(
    "benchmark_service_sandbox_created_hook", default=None
)


@contextmanager
def sandbox_created_scope(hook: SandboxCreatedHook | None) -> Iterator[None]:
    """Bind ``hook(provider, sandbox_id)`` for the duration of one request."""
    token = _sandbox_created_hook.set(hook)
    try:
        yield
    finally:
        _sandbox_created_hook.reset(token)


async def notify_sandbox_created(provider: str, sandbox_id: str) -> None:
    hook = _sandbox_created_hook.get()
    if hook is None:
        return
    try:
        await hook(provider, sandbox_id)
    except Exception:
        logger.warning("sandbox_created hook failed for %s sandbox %s", provider, sandbox_id, exc_info=True)
