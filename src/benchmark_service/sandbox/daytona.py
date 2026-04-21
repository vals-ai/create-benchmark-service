from __future__ import annotations

import asyncio
import shlex
import tempfile
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path
from typing import Self

from daytona import (
    AsyncDaytona,
    AsyncSandbox as DaytonaAsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    DaytonaNotFoundError,
    FileUpload,
    Resources,
    SandboxState,
)
from daytona.common.errors import DaytonaError
from daytona.handle.async_pty_handle import AsyncPtyHandle

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
    SnapshotSandboxCreateRequest,
)


_TIMEOUT_EXIT_CODE = 124
_SUCCESS_EXIT_CODE = 0
_DEAD_SANDBOX_STATES = (SandboxState.DESTROYING, SandboxState.DESTROYED, SandboxState.STOPPED)


def _command(command: str, cwd: str | None, env: Mapping[str, str] | None, timeout: float | None) -> str:
    parts: list[str] = []
    if cwd:
        parts.append(f"cd {shlex.quote(cwd)}")
    if env:
        parts.append(" ".join(f"{key}={shlex.quote(value)}" for key, value in env.items()))
    parts.append(command)
    shell_command = " && ".join(parts)
    if timeout is not None:
        shell_command = f"timeout {int(timeout)} {shell_command}"
    return shell_command


class DaytonaSandbox(Sandbox):
    def __init__(self, provider: DaytonaSandboxProvider, inner: DaytonaAsyncSandbox) -> None:
        super().__init__(provider=provider, id=inner.id, name=inner.name)
        self._inner = inner

    @property
    def inner(self) -> DaytonaAsyncSandbox:
        return self._inner

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        shell_command = _command(command, cwd, env, timeout)
        if on_stdout or on_stderr:
            on_output = on_stdout or on_stderr
            assert on_output
            return await self._exec_pty(shell_command, on_output)

        result = await self._process_exec(shell_command)
        return ExecResult(exit_code=result.exit_code, stdout=result.result or "")

    async def _process_exec(self, command: str):
        try:
            return await self._inner.process.exec(command)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def _exec_pty(self, command: str, on_output: Callable[[str], None]) -> ExecResult:
        pty_id = uuid.uuid4().hex
        session_id = f"{self.id}:pty-{pty_id}"
        status_path = f"/tmp/.benchmark-service/{pty_id}.status"
        stdout: list[str] = []
        handle: AsyncPtyHandle | None = None

        def on_data(data: bytes) -> None:
            text = data.decode("utf-8", errors="replace")
            stdout.append(text)
            on_output(text)

        try:
            handle = await self._inner.process.create_pty_session(
                id=session_id, on_data=on_data, envs={"TERM": "dumb", "LANG": "C.UTF-8"}
            )
            await handle.send_input("stty -echo\n")
            await handle.send_input(f"mkdir -p /tmp/.benchmark-service && {command}; echo $? > {status_path}; exit\n")
            try:
                await handle.wait()
            except Exception:
                pass

            while (await self._process_exec(f"test -e {status_path}")).exit_code != _SUCCESS_EXIT_CODE:
                await self._check_health()
                await asyncio.sleep(1)

            result = await self._process_exec(f"cat {status_path}")
            assert result.exit_code == _SUCCESS_EXIT_CODE
            assert result.result
            return ExecResult(exit_code=int(result.result.strip()), stdout="".join(stdout))
        finally:
            if handle:
                try:
                    await handle.disconnect()
                except Exception:
                    pass
            try:
                await self._inner.process.kill_pty_session(session_id)
            except Exception:
                pass
            try:
                await self._process_exec(f"rm -f {status_path}")
            except Exception:
                pass

    async def _check_health(self) -> None:
        try:
            await self._inner.refresh_data()
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc
        if self._inner.state in _DEAD_SANDBOX_STATES:
            raise SandboxError(f"Sandbox {self.name} stopped during command execution")

    async def upload_file(self, file: SandboxFile) -> None:
        try:
            await self._inner.fs.upload_file(file.content, file.remote_path)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def upload_local_file(self, local_path: Path, remote_path: str) -> None:
        try:
            await self._inner.fs.upload_file(str(local_path), remote_path)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def upload_files(self, files: list[SandboxFile]) -> None:
        uploads = [FileUpload(source=file.content, destination=file.remote_path) for file in files]
        try:
            await self._inner.fs.upload_files(uploads)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def download_file(self, remote_path: str) -> bytes:
        with tempfile.NamedTemporaryFile() as temp_file:
            try:
                await self._inner.fs.download_file(remote_path, temp_file.name)
            except DaytonaError as exc:
                raise SandboxError(str(exc)) from exc
            return Path(temp_file.name).read_bytes()

    async def wait_until_ready(self) -> None:
        try:
            await self._inner.wait_for_sandbox_start(timeout=0)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def wait_until_stopped(self) -> None:
        try:
            await self._inner.wait_for_sandbox_stop(timeout=0)
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc


class DaytonaSandboxProvider(SandboxProvider):
    provider_type = SandboxProviderType.DAYTONA

    def __init__(self, daytona: AsyncDaytona) -> None:
        self._daytona = daytona

    @classmethod
    async def from_headers(cls, headers: Mapping[str, str]) -> Self:
        api_key = headers.get("x-api-key")
        api_url = headers.get("x-api-url")
        target = headers.get("x-target")
        assert api_key
        assert api_url
        assert target
        return cls(AsyncDaytona(config=DaytonaConfig(api_key=api_key, api_url=api_url, target=target)))

    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaSandbox:
        resources = None
        if request.resources:
            resources = Resources(
                cpu=request.resources.cpu, memory=request.resources.memory, disk=request.resources.disk
            )

        match request:
            case ImageSandboxCreateRequest():
                params = CreateSandboxFromImageParams(
                    image=request.image,
                    name=request.name,
                    resources=resources,
                    env_vars=request.env_vars,
                    labels=request.labels,
                    auto_delete_interval=request.auto_delete_interval,
                )
                inner = await self._daytona.create(params)
            case SnapshotSandboxCreateRequest():
                params = CreateSandboxFromSnapshotParams(
                    snapshot=request.snapshot,
                    name=request.name,
                    env_vars=request.env_vars,
                    labels=request.labels,
                    auto_delete_interval=request.auto_delete_interval,
                )
                inner = await self._daytona.create(params)
        return DaytonaSandbox(self, inner)

    async def get_sandbox(self, id: str) -> DaytonaSandbox:
        try:
            return DaytonaSandbox(self, await self._daytona.get(id))
        except DaytonaNotFoundError as exc:
            raise SandboxNotFoundError(str(exc)) from exc

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        assert isinstance(sandbox, DaytonaSandbox)
        try:
            await self._daytona.delete(sandbox.inner)
        except DaytonaNotFoundError:
            return
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[DaytonaSandbox]:
        async def iter_sandboxes() -> AsyncIterator[DaytonaSandbox]:
            q = query or SandboxQuery()
            page = await self._daytona.list(labels=q.labels or None, limit=q.limit)
            for inner in page.items:
                yield DaytonaSandbox(self, inner)

        return iter_sandboxes()

    async def close(self) -> None:
        await self._daytona.close()
