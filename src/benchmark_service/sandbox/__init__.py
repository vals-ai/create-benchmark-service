from collections.abc import Mapping

from benchmark_service.sandbox.abstract import (
    ExecResult,
    ImageSandboxCreateRequest,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxFile,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderType,
    SandboxQuery,
    SandboxResources,
    SnapshotSandboxCreateRequest,
)
from benchmark_service.sandbox.daytona import DaytonaSandbox, DaytonaSandboxProvider
from benchmark_service.sandbox.modal import ModalSandbox, ModalSandboxProvider


async def create_provider(headers: Mapping[str, str]) -> SandboxProvider:
    match SandboxProviderType(headers.get("x-sandbox-provider", "daytona")):
        case SandboxProviderType.DAYTONA:
            return await DaytonaSandboxProvider.from_headers(headers)
        case SandboxProviderType.MODAL:
            return await ModalSandboxProvider.from_headers(headers)


__all__ = [
    "DaytonaSandbox",
    "DaytonaSandboxProvider",
    "ExecResult",
    "ImageSandboxCreateRequest",
    "ModalSandbox",
    "ModalSandboxProvider",
    "Sandbox",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxFile",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxProviderType",
    "SandboxQuery",
    "SandboxResources",
    "SnapshotSandboxCreateRequest",
    "create_provider",
]
