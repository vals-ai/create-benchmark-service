"""Local-development authentication for the reusable service framework."""

import os
from collections.abc import Mapping

UNAUTHENTICATED_TENANT_SENTINEL = "_unauthenticated"
AUTH_DISABLED_ENV = "AUTH_DISABLED"


async def resolve_caller_tenant(headers: Mapping[str, str]) -> str | None:
    """Reject callers unless local development explicitly disables authentication.

    Deployments implement authentication by overriding ``resolve_tenant`` on
    their benchmark service.
    """
    if os.environ.get(AUTH_DISABLED_ENV, "").lower() in {"1", "true", "yes", "on"}:
        return UNAUTHENTICATED_TENANT_SENTINEL
    return None


async def check_benchmark_service_auth(headers: Mapping[str, str]) -> bool:
    """Check whether local development explicitly allows this request."""
    return await resolve_caller_tenant(headers) is not None
