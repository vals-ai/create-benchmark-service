from __future__ import annotations

import shlex
import uuid
from collections.abc import AsyncGenerator, Mapping

from benchmark_service.sandbox.types import (
    ComposeSource,
    ExecResult,
    Sandbox,
    SandboxError,
    validate_command_env,
)

_COMPOSE_EXEC_ENV_ARGS = r"$(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/-e \1/p')"


class ComposeSandbox(Sandbox):
    def __init__(self, outer: Sandbox, source: ComposeSource) -> None:
        self._outer = outer
        self._service = source.service
        self._compose_command_prefix = source.compose_command
        self.labels = outer.labels
        self.created_at = outer.created_at

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
        return f"{self._compose_command_prefix} {shlex.join(parts)}"

    def _compose_exec_command(self, parts: list[str]) -> str:
        return f"{self._compose_command_prefix} exec {_COMPOSE_EXEC_ENV_ARGS} {shlex.join(parts)}"

    def _exec_command(self, command: str, cwd: str | None) -> str:
        parts = ["-T"]
        if cwd:
            parts.extend(["-w", cwd])
        parts.extend([self._service, "sh", "-lc", command])
        return self._compose_exec_command(parts)

    def _container_lookup(self) -> str:
        return f"container_id=$({self._compose_command(['ps', '-q', self._service])})"

    def _temp_path(self, prefix: str, remote_path: str) -> str:
        name = remote_path.rstrip("/").rsplit("/", 1)[-1] or "file"
        return f"/var/tmp/{prefix}-{uuid.uuid4().hex}-{name}"

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
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars) if env_vars is not None else None
        async for chunk in self._outer.command(self._exec_command(command, cwd), timeout=timeout, env_vars=env):
            yield chunk

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        temp = self._temp_path("compose-upload", remote_path)
        try:
            await self._outer.upload_file(temp, content)
            parent = remote_path.rstrip("/").rsplit("/", 1)[0] or "."
            make_parent = shlex.quote(f"mkdir -p {shlex.quote(parent)}")
            write_file = shlex.quote(f"cat > {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {make_parent}; "
                    f"cat {shlex.quote(temp)} | docker exec -i \"$container_id\" sh -lc {write_file}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose upload failed: {result.output}")
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def download_file(self, remote_path: str) -> bytes:
        temp = self._temp_path("compose-download", remote_path)
        try:
            read_file = shlex.quote(f"cat {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {read_file} > {shlex.quote(temp)}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose download failed: {result.output}")
            return await self._outer.download_file(temp)
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        temp = self._temp_path("compose-download", remote_path)
        try:
            read_file = shlex.quote(f"cat {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {read_file} > {shlex.quote(temp)}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose download failed: {result.output}")
            async for chunk in self._outer.stream_download(temp):
                yield chunk
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._outer.modify_egress_rules(allowed_addresses)

    async def clear_egress_rules(self) -> None:
        await self._outer.clear_egress_rules()
