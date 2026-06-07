from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from pydantic import Field, TypeAdapter

from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
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

SandboxProviderConfig = Annotated[DaytonaProviderConfig | ModalProviderConfig, Field(discriminator="type")]

_provider_config_adapter: TypeAdapter[SandboxProviderConfig] = TypeAdapter(SandboxProviderConfig)
_provider_config_from_headers = {
    "daytona": DaytonaProviderConfig.from_headers,
    "modal": ModalProviderConfig.from_headers,
}


def sandbox_provider_config_from_mapping(data: Mapping[str, Any]) -> SandboxProviderConfig:
    return _provider_config_adapter.validate_python(data)


def sandbox_config_from_headers(headers: Mapping[str, str]) -> SandboxProviderConfig:
    provider = headers.get("x-sandbox-provider", "daytona")
    config_from_headers = _provider_config_from_headers.get(provider)
    if config_from_headers is None:
        raise ValueError(f"Unknown sandbox provider: {provider}")
    return config_from_headers(headers)


__all__ = [
    "DaytonaProviderConfig",
    "ExecResult",
    "ImageSource",
    "MissingSandboxConfigError",
    "ModalProviderConfig",
    "Resources",
    "Sandbox",
    "SandboxCommandError",
    "SandboxConnectionError",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxProviderConfig",
    "SandboxQuery",
    "SandboxSource",
    "SnapshotSource",
    "sandbox_config_from_headers",
    "sandbox_provider_config_from_mapping",
]
