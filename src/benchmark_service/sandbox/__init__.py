from __future__ import annotations

import os
from collections.abc import Mapping

from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider
from benchmark_service.sandbox.types import (
    DaytonaBackendConfig,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
    SandboxBackendConfig,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
)

_PROVIDER = os.environ.get("SANDBOX_PROVIDER", "daytona")


def sandbox_config_from_headers(headers: Mapping[str, str]) -> SandboxBackendConfig:
    match _PROVIDER:
        case "daytona":
            api_key = headers.get("x-api-key")
            api_url = headers.get("x-api-url")
            target = headers.get("x-target")
            if not api_key or not api_url or not target:
                raise MissingSandboxConfigError("Missing required headers: x-api-key, x-api-url, x-target")
            return DaytonaBackendConfig(api_key=api_key, api_url=api_url, target=target)
        case _:
            raise ValueError(f"Unknown sandbox provider: {_PROVIDER}")


def create_provider(config: SandboxBackendConfig) -> SandboxProvider:
    return config.create_provider()


__all__ = [
    "DaytonaBackendConfig",
    "DaytonaSandbox",
    "DaytonaSandboxProvider",
    "ExecResult",
    "ImageSource",
    "MissingSandboxConfigError",
    "Resources",
    "Sandbox",
    "SandboxBackendConfig",
    "SandboxCommandError",
    "SandboxConnectionError",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxQuery",
    "SandboxSource",
    "SnapshotSource",
    "create_provider",
    "sandbox_config_from_headers",
]
