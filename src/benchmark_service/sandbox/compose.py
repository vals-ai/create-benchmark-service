from __future__ import annotations

import shlex
import uuid
from collections.abc import AsyncGenerator

from benchmark_service.sandbox.types import ComposeSource, ExecResult, Sandbox, SandboxError


class HarborComposeSandbox(Sandbox):
    def __init__(self, outer: Sandbox, source: ComposeSource) -> None:
        self._outer = outer
        self._service = source.service

    @property
    def id(self) -> str:
        return self._outer.id

    @property
    def name(self) -> str:
        return self._outer.name

    @property
    def state(self) -> str:
        return self._outer.state

    def _compose_command(self, parts: list[str]) -> str:
        return shlex.join(["docker", "compose", *parts])

    def _exec_command(self, command: str, cwd: str | None) -> str:
        parts = ["exec", "-T"]
        if cwd:
            parts.extend(["-w", cwd])
        parts.extend([self._service, "bash", "-lc", command])
        return self._compose_command(parts)

    def _temp_path(self, prefix: str, remote_path: str) -> str:
        name = remote_path.rstrip("/").rsplit("/", 1)[-1] or "file"
        return f"/tmp/{prefix}-{uuid.uuid4().hex}-{name}"

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        return await self._outer.exec(self._exec_command(command, cwd), timeout=timeout)

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[str, None]:
        async for chunk in self._outer.command(self._exec_command(command, cwd), timeout=timeout):
            yield chunk

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        temp = self._temp_path("compose-upload", remote_path)
        try:
            await self._outer.upload_file(temp, content)
            result = await self._outer.exec(
                self._compose_command(["cp", temp, f"{self._service}:{remote_path}"]),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"docker compose cp failed: {result.output}")
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def download_file(self, remote_path: str) -> bytes:
        temp = self._temp_path("compose-download", remote_path)
        try:
            result = await self._outer.exec(
                self._compose_command(["cp", f"{self._service}:{remote_path}", temp]),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"docker compose cp failed: {result.output}")
            return await self._outer.download_file(temp)
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)
