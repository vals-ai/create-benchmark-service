from __future__ import annotations

import asyncio
import shlex
import tempfile
import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from pathlib import Path

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    DaytonaNotFoundError,
    Resources as DaytonaResources,
)
from daytona.common.errors import DaytonaError
from daytona.handle.async_pty_handle import AsyncPtyHandle

from benchmark_service.sandbox.types import (
    DaytonaBackendConfig,
    ExecResult,
    ImageSource,
    Sandbox,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSource,
)

_SANDBOX_AUTOSTOP_INTERVAL_MINUTES = 10 * 60
_SANDBOX_CREATE_TIMEOUT_SECONDS = 360


class DaytonaSandbox(Sandbox):
    def __init__(self, inner: AsyncSandbox) -> None:
        self._inner = inner
        self.id = inner.id
        self.name = inner.name
        self.state = str(inner.state)

    @property
    def inner(self) -> AsyncSandbox:
        return self._inner

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ExecResult:
        full_command = _command(command, cwd, timeout)
        if on_output:
            return await self._exec_pty(full_command, on_output)

        try:
            result = await self._inner.process.exec(full_command)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

        assert result.exit_code is not None
        return ExecResult(exit_code=result.exit_code, stdout=result.result or "")

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        try:
            await self._inner.fs.upload_file(content, remote_path)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def download_file(self, remote_path: str) -> bytes:
        with tempfile.NamedTemporaryFile() as temp_file:
            try:
                await self._inner.fs.download_file(remote_path, temp_file.name)
            except DaytonaError as exc:
                raise SandboxError(str(exc)) from exc
            return Path(temp_file.name).read_bytes()

    async def _exec_pty(self, command: str, on_output: Callable[[str], None]) -> ExecResult:
        session_id = f"{self.id}:exec-{uuid.uuid4().hex}"
        status_path = f"/tmp/.benchmark-service/{uuid.uuid4().hex}.status"
        stdout: list[str] = []
        handle: AsyncPtyHandle | None = None

        def on_data(data: bytes) -> None:
            text = data.decode("utf-8", errors="replace")
            stdout.append(text)
            on_output(text)

        try:
            handle = await self._inner.process.create_pty_session(
                id=session_id,
                on_data=on_data,
                envs={"TERM": "dumb", "LANG": "C.UTF-8"},
            )
            await handle.send_input("stty -echo\n")
            await handle.send_input(f"mkdir -p /tmp/.benchmark-service; {command}; echo $? > {status_path}; exit\n")
            with suppress(Exception):
                await handle.wait()

            while True:
                result = await self._inner.process.exec(f"test -e {shlex.quote(status_path)}")
                assert result.exit_code is not None
                if result.exit_code == 0:
                    break
                await asyncio.sleep(1)

            result = await self._inner.process.exec(f"cat {shlex.quote(status_path)}")
            assert result.exit_code == 0
            assert result.result
            return ExecResult(exit_code=int(result.result.strip()), stdout="".join(stdout))
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc
        finally:
            if handle:
                with suppress(Exception):
                    await handle.disconnect()
            with suppress(Exception):
                await self._inner.process.kill_pty_session(session_id)
            with suppress(Exception):
                await self._inner.process.exec(f"rm -f {shlex.quote(status_path)}")


class DaytonaSandboxProvider(SandboxProvider):
    def __init__(self, config: DaytonaBackendConfig) -> None:
        self._daytona = AsyncDaytona(
            config=DaytonaConfig(
                api_key=config.api_key,
                api_url=config.api_url,
                target=config.target,
                connection_pool_maxsize=None,
            )
        )

    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        resources = DaytonaResources(
            cpu=request.resources.cpu,
            memory=request.resources.memory_gb,
            disk=request.resources.disk_gb,
        )

        match request.source:
            case ImageSource(image=image):
                inner = await self._daytona.create(
                    CreateSandboxFromImageParams(
                        auto_stop_interval=_SANDBOX_AUTOSTOP_INTERVAL_MINUTES,
                        auto_delete_interval=0,
                        name=request.name,
                        labels=request.labels,
                        image=image,
                        network_block_all=False,
                        resources=resources,
                        env_vars=request.env_vars,
                    ),
                    timeout=_SANDBOX_CREATE_TIMEOUT_SECONDS,
                )
            case SnapshotSource(snapshot=snapshot):
                inner = await self._daytona.create(
                    CreateSandboxFromSnapshotParams(
                        auto_stop_interval=_SANDBOX_AUTOSTOP_INTERVAL_MINUTES,
                        auto_delete_interval=0,
                        name=request.name,
                        labels=request.labels,
                        snapshot=snapshot,
                        language="python",
                        network_block_all=False,
                        env_vars=request.env_vars,
                    ),
                    timeout=_SANDBOX_CREATE_TIMEOUT_SECONDS,
                )

        return DaytonaSandbox(inner)

    async def get_sandbox(self, instance_id: str) -> DaytonaSandbox:
        try:
            return DaytonaSandbox(await self._daytona.get(instance_id))
        except DaytonaNotFoundError as exc:
            raise SandboxNotFoundError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            sandbox = await self._daytona.get(instance_id)
            await self._daytona.delete(sandbox)
        except DaytonaNotFoundError:
            return
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[DaytonaSandbox, None]:
        page = 1
        total_pages = 1
        while page <= total_pages:
            try:
                sandboxes = await self._daytona.list(labels=query.labels, limit=query.page_size, page=page)
            except DaytonaError as exc:
                raise SandboxError(str(exc)) from exc

            total_pages = sandboxes.total_pages
            if not sandboxes.items:
                return

            for sandbox in sandboxes.items:
                yield DaytonaSandbox(sandbox)

            page = int(sandboxes.page) + 1

    async def close(self) -> None:
        await self._daytona.close()


def _command(command: str, cwd: str | None, timeout: float | None) -> str:
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    if timeout is not None:
        command = f"timeout {int(timeout)} {command}"
    return command
