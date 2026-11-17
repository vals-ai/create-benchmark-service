from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from datetime import datetime
from typing import Any

from benchmark_service.sandbox.types import ExecResult, Sandbox, SandboxCommandError, validate_command_env


class CommandEnvSandbox(Sandbox):
    """Runs every command of a wrapped sandbox with caller env vars; a command's own ``env_vars`` win."""

    def __init__(self, inner: Sandbox, env_vars: Mapping[str, str]) -> None:
        self._inner = inner
        self._env = validate_command_env(env_vars)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    @property
    def id(self) -> str:
        return self._inner.id

    @property
    def name(self) -> str:
        return self._inner.name

    @property
    def state(self) -> str:
        return self._inner.state

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        return self._inner.provider_metadata

    @property
    def has_ended(self) -> bool:
        return self._inner.has_ended

    def cost_usd(self, ended_at: datetime) -> float | None:
        return self._inner.cost_usd(ended_at)

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        try:
            async for chunk in self.command(command, cwd=cwd, timeout=timeout):
                output.append(chunk)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(output))
        return ExecResult(exit_code=0, output="".join(output))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = self._env | validate_command_env(env_vars)
        async for chunk in self._inner.command(command, cwd=cwd, timeout=timeout, env_vars=env):
            yield chunk

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self._inner.upload_file(remote_path, content)

    async def download_file(self, remote_path: str) -> bytes:
        return await self._inner.download_file(remote_path)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        async for chunk in self._inner.stream_download(remote_path):
            yield chunk

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._inner.modify_egress_rules(allowed_addresses)

    async def block_all_egress(self) -> None:
        await self._inner.block_all_egress()

    async def clear_egress_rules(self) -> None:
        await self._inner.clear_egress_rules()
