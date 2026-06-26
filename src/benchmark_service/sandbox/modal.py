from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from typing import Literal

from pydantic import BaseModel

from benchmark_service.sandbox.types import (
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxProvider,
    SandboxQuery,
)


class ModalProviderConfig(BaseModel):
    type: Literal["modal"] = "modal"

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> "ModalProviderConfig":
        return cls()

    def create_provider(self) -> SandboxProvider:
        return ModalSandboxProvider(self)


class ModalSandboxProvider(SandboxProvider):
    def __init__(self, config: ModalProviderConfig) -> None:
        self._config = config

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        raise SandboxError("Modal sandbox provider is not implemented")

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        raise SandboxError("Modal sandbox provider is not implemented")

    async def delete_sandbox(self, instance_id: str) -> None:
        raise SandboxError("Modal sandbox provider is not implemented")

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        raise SandboxError("Modal sandbox provider is not implemented")
        yield

    async def close(self) -> None:
        pass
