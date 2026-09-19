from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from pydantic import Field, TypeAdapter

from benchmark_service.sandbox.compose import ComposeSandbox
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ComposeSource,
    ControlledWorkload,
    ControlledWorkloadResult,
    ControlledWorkloadUnsupportedError,
    ExecResult,
    GenerationContainment,
    ImageSource,
    MissingSandboxConfigError,
    ResourceCapacity,
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxCapacityDomain,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    LINUX_PID_NAMESPACE_V1,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
)

SandboxProviderConfig = Annotated[DaytonaProviderConfig | ModalProviderConfig, Field(discriminator="type")]

_provider_config_adapter: TypeAdapter[SandboxProviderConfig] = TypeAdapter(SandboxProviderConfig)


def sandbox_provider_config_from_mapping(data: Mapping[str, Any]) -> SandboxProviderConfig:
    return _provider_config_adapter.validate_python(data)


__all__ = [
    "ComposeSource",
    "ControlledWorkload",
    "ControlledWorkloadResult",
    "ControlledWorkloadUnsupportedError",
    "ComposeSandbox",
    "DaytonaProviderConfig",
    "ExecResult",
    "GenerationContainment",
    "ImageSource",
    "LINUX_PID_NAMESPACE_V1",
    "MissingSandboxConfigError",
    "ModalProviderConfig",
    "ResourceCapacity",
    "Resources",
    "Sandbox",
    "SandboxCapacity",
    "SandboxCapacityDomain",
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
    "TargetedSnapshotSource",
    "VolumeMount",
    "sandbox_provider_config_from_mapping",
]
