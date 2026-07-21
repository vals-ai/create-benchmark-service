from __future__ import annotations

from collections.abc import AsyncGenerator

from benchmark_service.sandbox.kubernetes.runtime import KubernetesRuntimeDriver
from benchmark_service.sandbox.types import (
    Sandbox,
    SandboxCreateRequest,
    SandboxProvider,
    SandboxQuery,
)


class KubernetesSandboxProvider(SandboxProvider):
    """Sandbox provider that delegates Kubernetes work to a runtime driver."""

    def __init__(self, driver: KubernetesRuntimeDriver) -> None:
        self._driver = driver

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        return await self._driver.create_sandbox(request)

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        return await self._driver.get_sandbox(instance_id)

    async def delete_sandbox(self, instance_id: str) -> None:
        await self._driver.delete_sandbox(instance_id)

    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        return self._driver.list_sandboxes(query)

    async def close(self) -> None:
        await self._driver.close()
