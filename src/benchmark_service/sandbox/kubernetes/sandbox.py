from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping

from benchmark_service.sandbox.kubernetes.runtime import KubernetesRuntimeDriver
from benchmark_service.sandbox.types import ExecResult, Sandbox, validate_command_env


class KubernetesSandbox(Sandbox):
    """Runtime-neutral handle for one Kubernetes sandbox."""

    def __init__(
        self,
        instance_id: str,
        name: str,
        state: str,
        driver: KubernetesRuntimeDriver,
    ) -> None:
        self._instance_id = instance_id
        self._name = name
        self._state = state
        self._driver = driver

    @property
    def id(self) -> str:
        return self._instance_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> str:
        return self._state

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        return await self._driver.exec(
            self.id,
            command,
            cwd=cwd,
            timeout=timeout,
        )

    def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars) if env_vars is not None else None
        return self._driver.command(
            self.id,
            command,
            cwd=cwd,
            timeout=timeout,
            env_vars=env,
        )

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self._driver.upload_file(self.id, remote_path, content)

    async def download_file(self, remote_path: str) -> bytes:
        return await self._driver.download_file(self.id, remote_path)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        async for chunk in self._driver.stream_download(self.id, remote_path):
            yield chunk

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._driver.modify_egress_rules(self.id, allowed_addresses)

    async def clear_egress_rules(self) -> None:
        await self._driver.clear_egress_rules(self.id)
