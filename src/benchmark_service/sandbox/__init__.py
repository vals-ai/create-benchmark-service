from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, get_args

from pydantic import Field, TypeAdapter

from benchmark_service.sandbox.compose import ComposeSandbox
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.daytona_v2 import DaytonaV2ProviderConfig
from benchmark_service.sandbox.local.docker import DockerProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ComposeSource,
    DockerfileSource,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    ResourceCapacity,
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxBuildError,
    SandboxCapacityDomain,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxRejectedError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxReservationDemand,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
)

SandboxProviderConfig = Annotated[
    DaytonaProviderConfig | DaytonaV2ProviderConfig | ModalProviderConfig | DockerProviderConfig,
    Field(discriminator="type"),
]

_provider_config_adapter: TypeAdapter[SandboxProviderConfig] = TypeAdapter(SandboxProviderConfig)


def sandbox_provider_config_from_mapping(data: Mapping[str, Any]) -> SandboxProviderConfig:
    return _provider_config_adapter.validate_python(data)


def priced_provider_types() -> list[str]:
    """Provider types whose sandboxes price themselves through ``Sandbox.cost_usd``."""
    configs = get_args(get_args(SandboxProviderConfig)[0])
    return [config.model_fields["type"].default for config in configs if getattr(config, "prices_sandboxes", False)]


__all__ = [
    "ComposeSource",
    "ComposeSandbox",
    "DaytonaProviderConfig",
    "DaytonaV2ProviderConfig",
    "DockerProviderConfig",
    "DockerfileSource",
    "ExecResult",
    "ImageSource",
    "MissingSandboxConfigError",
    "ModalProviderConfig",
    "ResourceCapacity",
    "Resources",
    "Sandbox",
    "SandboxBuildError",
    "SandboxCapacity",
    "SandboxCapacityDomain",
    "SandboxCommandError",
    "SandboxConnectionError",
    "SandboxRejectedError",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxProviderConfig",
    "SandboxQuery",
    "SandboxReservationDemand",
    "SandboxSource",
    "SnapshotSource",
    "TargetedSnapshotSource",
    "VolumeMount",
    "priced_provider_types",
    "sandbox_provider_config_from_mapping",
]
