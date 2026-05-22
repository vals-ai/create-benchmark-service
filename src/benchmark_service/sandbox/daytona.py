from __future__ import annotations

import asyncio
import shlex
import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    DaytonaNotFoundError,
    Resources as DaytonaResources,
)
from daytona.common.errors import DaytonaConnectionError, DaytonaError
from daytona.handle.async_pty_handle import AsyncPtyHandle

from benchmark_service.sandbox.types import (
    DaytonaBackendConfig,
    ExecResult,
    ImageSource,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSource,
)

_PTY_STATUS_CHECK_ATTEMPTS = 30


class DaytonaSandbox(Sandbox):
    def __init__(self, sandbox: AsyncSandbox) -> None:
        self._sandbox = sandbox

    @property
    def inner(self) -> AsyncSandbox:
        return self._sandbox

    @property
    def id(self) -> str:
        return self._sandbox.id

    @property
    def name(self) -> str:
        return self._sandbox.name

    @property
    def state(self) -> str:
        return str(self._sandbox.state)

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        full_command = _command(command, cwd, timeout)
        try:
            result = await self._sandbox.process.exec(full_command)
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

        return ExecResult(exit_code=result.exit_code, output=result.result)

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[str, None]:
        output: asyncio.Queue[str] = asyncio.Queue()
        exec_task = asyncio.create_task(self._exec_pty(_command(command, cwd, timeout), output.put_nowait))

        while not exec_task.done():
            try:
                yield await asyncio.wait_for(output.get(), timeout=0.1)
            except TimeoutError:
                continue

        while not output.empty():
            yield output.get_nowait()

        result = await exec_task
        if result.exit_code != 0:
            raise SandboxCommandError(result.exit_code)

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        try:
            await self._sandbox.fs.upload_file(content, remote_path)
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def download_file(self, remote_path: str) -> bytes:
        try:
            stream = await self._sandbox.fs.download_file_stream(remote_path)
            return b"".join([chunk async for chunk in stream])
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

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
            handle = await self._sandbox.process.create_pty_session(
                id=session_id,
                on_data=on_data,
                envs={"TERM": "dumb", "LANG": "C.UTF-8"},
            )
            await handle.send_input("stty -echo\n")
            await handle.send_input(f"mkdir -p /tmp/.benchmark-service; {command}; echo $? > {status_path}; exit\n")
            with suppress(Exception):
                await handle.wait()

            for _ in range(_PTY_STATUS_CHECK_ATTEMPTS):
                result = await self._sandbox.process.exec(f"test -e {shlex.quote(status_path)}")
                if result.exit_code == 0:
                    break
                await asyncio.sleep(1)
            else:
                raise SandboxConnectionError("Daytona PTY closed before writing exit code")

            result = await self._sandbox.process.exec(f"cat {shlex.quote(status_path)}")
            if result.exit_code != 0 or not result.result:
                raise SandboxError("Failed to read Daytona PTY exit code")
            return ExecResult(exit_code=int(result.result.strip()), output="".join(stdout))
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc
        finally:
            if handle:
                with suppress(Exception):
                    await handle.disconnect()
            with suppress(Exception):
                await self._sandbox.process.kill_pty_session(session_id)
            with suppress(Exception):
                await self._sandbox.process.exec(f"rm -f {shlex.quote(status_path)}")


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
            cpu=request.resources.vcpu,
            memory=request.resources.memory,
            disk=request.resources.disk,
        )

        match request.source:
            case ImageSource(image=image):
                inner = await self._daytona.create(
                    CreateSandboxFromImageParams(
                        auto_stop_interval=request.auto_stop_interval,
                        auto_delete_interval=0,
                        name=request.name,
                        labels=request.labels,
                        image=image,
                        network_block_all=False,
                        resources=resources,
                        env_vars=request.env_vars,
                    ),
                    timeout=request.create_timeout,
                )
            case SnapshotSource(snapshot=snapshot):
                inner = await self._daytona.create(
                    CreateSandboxFromSnapshotParams(
                        auto_stop_interval=request.auto_stop_interval,
                        auto_delete_interval=0,
                        name=request.name,
                        labels=request.labels,
                        snapshot=snapshot,
                        language="python",
                        network_block_all=False,
                        env_vars=request.env_vars,
                    ),
                    timeout=request.create_timeout,
                )

        return DaytonaSandbox(inner)

    async def get_sandbox(self, instance_id: str) -> DaytonaSandbox:
        try:
            return DaytonaSandbox(await self._daytona.get(instance_id))
        except DaytonaNotFoundError as exc:
            raise SandboxNotFoundError(str(exc)) from exc
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            sandbox = await self._daytona.get(instance_id)
            await self._daytona.delete(sandbox)
        except DaytonaNotFoundError:
            return
        except DaytonaConnectionError as exc:
            raise SandboxConnectionError(str(exc)) from exc
        except DaytonaError as exc:
            raise SandboxError(str(exc)) from exc

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[DaytonaSandbox, None]:
        page = 1
        total_pages = 1
        while page <= total_pages:
            try:
                sandboxes = await self._daytona.list(labels=query.labels, limit=query.page_size, page=page)
            except DaytonaConnectionError as exc:
                raise SandboxConnectionError(str(exc)) from exc
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
    if timeout is not None:
        command = f"timeout {timeout:g} {command}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return command
