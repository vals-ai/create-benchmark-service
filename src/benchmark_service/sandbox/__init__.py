from collections.abc import Mapping
from typing import Any

from benchmark_service.sandbox.abstract import (
    ExecResult,
    InvalidSandboxConfigurationError,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxFile,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderType,
    SandboxQuery,
    SandboxResources,
    SandboxSourceType,
)
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider

_PROVIDER_REGISTRY: dict[SandboxProviderType, type[SandboxProvider]] = {
    SandboxProviderType.DAYTONA: DaytonaSandboxProvider,
}


async def create_provider(
    headers: Mapping[str, str],
    provider_type: SandboxProviderType | None = None,
    **kwargs: Any,
) -> SandboxProvider:
    if provider_type is None:
        provider_type = SandboxProviderType(headers.get("x-sandbox-provider", "daytona"))
    provider_cls = _PROVIDER_REGISTRY.get(provider_type)
    if provider_cls is None:
        raise ValueError(f"Unsupported sandbox provider: {provider_type}")
    return await provider_cls.from_headers(headers, **kwargs)


__all__ = [
    "DaytonaSandbox",
    "DaytonaSandboxProvider",
    "ExecResult",
    "InvalidSandboxConfigurationError",
    "Sandbox",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxFile",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxProviderType",
    "SandboxQuery",
    "SandboxResources",
    "SandboxSourceType",
    "create_provider",
]
