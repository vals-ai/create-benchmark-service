"""Optional authentication for the reusable service framework."""

import os
from collections.abc import Mapping

UNAUTHENTICATED_TENANT_SENTINEL = "unauthenticated"
AUTH_REQUIRED_ENV = "AUTH_REQUIRED"


def is_auth_required() -> bool:
    """Return whether this deployment explicitly requires authentication."""
    return os.environ.get(AUTH_REQUIRED_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


async def resolve_caller_tenant(headers: Mapping[str, str]) -> str | None:
    """Allow local callers unless the deployment requires authentication.

    Deployments implement authentication by overriding ``resolve_tenant`` on
    their benchmark service.
    """
    if not is_auth_required():
        return UNAUTHENTICATED_TENANT_SENTINEL
    return None


async def check_benchmark_service_auth(headers: Mapping[str, str]) -> bool:
    """Allow unauthenticated requests unless authentication is enabled."""
    return await resolve_caller_tenant(headers) is not None
