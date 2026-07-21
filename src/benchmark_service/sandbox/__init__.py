from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from pydantic import Field, TypeAdapter

from benchmark_service.sandbox.compose import ComposeSandbox
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.kubernetes.config import KubernetesProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ComposeSource,
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

SandboxProviderConfig = Annotated[
    DaytonaProviderConfig | KubernetesProviderConfig | ModalProviderConfig,
    Field(discriminator="type"),
]

_provider_config_adapter: TypeAdapter[SandboxProviderConfig] = TypeAdapter(SandboxProviderConfig)


def sandbox_provider_config_from_mapping(data: Mapping[str, Any]) -> SandboxProviderConfig:
    return _provider_config_adapter.validate_python(data)


__all__ = [
    "ComposeSource",
    "ComposeSandbox",
    "DaytonaProviderConfig",
    "ExecResult",
    "ImageSource",
    "KubernetesProviderConfig",
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
    "sandbox_provider_config_from_mapping",
]
