from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping

from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCreateRequest,
    SandboxQuery,
)


class KubernetesRuntimeDriver(ABC):
    """Runtime-specific operations used by the Kubernetes sandbox provider."""

    @abstractmethod
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox: ...

    @abstractmethod
    async def get_sandbox(self, instance_id: str) -> Sandbox: ...

    @abstractmethod
    async def delete_sandbox(self, instance_id: str) -> None: ...

    @abstractmethod
    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]: ...

    @abstractmethod
    async def exec(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    def command(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]: ...

    @abstractmethod
    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        content: bytes,
    ) -> None: ...

    @abstractmethod
    async def download_file(self, instance_id: str, remote_path: str) -> bytes: ...

    async def stream_download(
        self,
        instance_id: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        yield await self.download_file(instance_id, remote_path)

    @abstractmethod
    async def modify_egress_rules(
        self,
        instance_id: str,
        allowed_addresses: list[str],
    ) -> None: ...

    @abstractmethod
    async def clear_egress_rules(self, instance_id: str) -> None: ...

    async def close(self) -> None:
        pass
