"""Image-based sandboxes on a shared local Docker daemon."""

from __future__ import annotations

import asyncio
import codecs
import io
import logging
import math
import os
import shlex
import tarfile
from collections.abc import AsyncGenerator, Generator, Mapping
from contextlib import aclosing, contextmanager
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from uuid import uuid4

from aiodocker import Docker
from aiodocker.containers import DockerContainer
from aiodocker.exceptions import DockerError
from aiodocker.types import JSONObject
from aiohttp import ClientError
from pydantic import AliasPath, BaseModel, Field

from benchmark_service.sandbox.types import (
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
    validate_command_env,
)

logger = logging.getLogger(__name__)
_MANAGED_LABEL = "io.vals.cbs.managed"
_NAME_LABEL = "io.vals.cbs.name"


class DockerProviderConfig(BaseModel):
    type: Literal["docker"] = "docker"

    def create_provider(self) -> SandboxProvider:
        if os.environ.get("CBS_DOCKER_ENABLED", "").lower() != "true":
            raise SandboxError("Docker sandbox access requires CBS_DOCKER_ENABLED=true on this process")
        return DockerSandboxProvider()


class _ContainerInfo(BaseModel):
    id: str = Field(alias="Id")
    created: datetime = Field(alias="Created")
    state: str = Field(validation_alias=AliasPath("State", "Status"))
    labels: dict[str, str] | None = Field(validation_alias=AliasPath("Config", "Labels"))


@contextmanager
def _docker_errors() -> Generator[None]:
    try:
        yield
    except DockerError as error:
        if error.status == 404:
            raise SandboxNotFoundError(str(error.message)) from error
        if error.status >= 500:
            raise SandboxConnectionError(str(error.message)) from error
        raise SandboxError(str(error.message)) from error
    except (ClientError, OSError) as error:
        raise SandboxConnectionError("Local Docker connection failed") from error


async def _finish_cleanup(task: asyncio.Task[None]) -> None:
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


class DockerSandbox(Sandbox):
    def __init__(self, container: DockerContainer, info: _ContainerInfo) -> None:
        self._container = container
        self._info = info
        self.labels = info.labels
        self.created_at = info.created

    @property
    def id(self) -> str:
        return self._info.id

    @property
    def name(self) -> str:
        return (self._info.labels or {}).get(_NAME_LABEL, self.id)

    @property
    def state(self) -> str:
        return self._info.state

    async def _cleanup_command(self, pid_file: str, *, terminate: bool) -> None:
        async with asyncio.timeout(15):
            try:
                if terminate:
                    command = (
                        f"if test -f {pid_file}; then pid=$(cat {pid_file}); "
                        'kill -TERM -"$pid" 2>/dev/null; sleep 0.2; '
                        'kill -KILL -"$pid" 2>/dev/null; fi; '
                        f"rm -f {pid_file}"
                    )
                else:
                    command = f"rm -f {pid_file}"
                cleanup = await self._container.exec(["/bin/sh", "-c", command])
                async with cleanup.start() as stream:
                    while await stream.read_out() is not None:
                        pass
            except DockerError as error:
                if error.status != 404:
                    raise

    async def _command_bytes(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[bytes]:
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise SandboxError("Docker command timeout must be a positive finite number")
        deadline = asyncio.get_running_loop().time() + timeout if timeout is not None else None
        environment = validate_command_env(env_vars)
        pid_file = f"/tmp/cbs-command-{uuid4().hex}.pid"
        script = f"echo $$ > {pid_file}; exec /bin/sh -c {shlex.quote(command)} 2>&1"
        with _docker_errors():
            execution = None
            finished = False
            try:
                async with asyncio.timeout_at(deadline):
                    execution = await self._container.exec(
                        ["setsid", "--wait", "/bin/sh", "-c", script],
                        environment=environment,
                        workdir=cwd,
                    )
                stream = execution.start()
                try:
                    while True:
                        async with asyncio.timeout_at(deadline):
                            message = await stream.read_out()
                        if message is None:
                            break
                        yield message.data
                finally:
                    await stream.close()
                async with asyncio.timeout_at(deadline):
                    info = await execution.inspect()
                    while info.get("Running"):
                        await asyncio.sleep(0.01)
                        info = await execution.inspect()
                finished = True
                exit_code = info.get("ExitCode")
                if not isinstance(exit_code, int):
                    raise SandboxError("Docker command finished without an exit code")
                if exit_code:
                    raise SandboxCommandError(exit_code)
            except TimeoutError:
                raise SandboxCommandError(124) from None
            finally:
                if execution is not None:
                    await _finish_cleanup(asyncio.create_task(self._cleanup_command(pid_file, terminate=not finished)))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str]:
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        iterator = self._command_bytes(command, cwd=cwd, timeout=timeout, env_vars=env_vars)
        async with aclosing(iterator):
            try:
                async for chunk in iterator:
                    if text := decoder.decode(chunk):
                        yield text
                if text := decoder.decode(b"", final=True):
                    yield text
            except SandboxCommandError:
                if text := decoder.decode(b"", final=True):
                    yield text
                raise

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        try:
            async for chunk in self.command(command, cwd=cwd, timeout=timeout):
                output.append(chunk)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(output))
        return ExecResult(exit_code=0, output="".join(output))

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        path = PurePosixPath(remote_path)
        result = await self.exec(f"mkdir -p {shlex.quote(str(path.parent))}")
        if result.exit_code:
            raise SandboxCommandError(result.exit_code)

        def archive() -> bytes:
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w") as tar:
                info = tarfile.TarInfo(path.name)
                info.size = len(content)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(content))
            return output.getvalue()

        with _docker_errors():
            await self._container.put_archive(str(path.parent), await asyncio.to_thread(archive))  # pyright: ignore[reportUnknownMemberType]

    async def download_file(self, remote_path: str) -> bytes:
        return b"".join([chunk async for chunk in self.stream_download(remote_path)])

    def stream_download(self, remote_path: str) -> AsyncGenerator[bytes]:
        path = PurePosixPath(remote_path)
        return self._command_bytes(f"cat -- {shlex.quote(str(path))}")


class DockerSandboxProvider(SandboxProvider):
    def __init__(self) -> None:
        self._docker = Docker(url=os.environ.get("DOCKER_HOST"))

    async def _get_container(self, instance_id: str) -> tuple[DockerContainer, _ContainerInfo]:
        container = self._docker.containers.container(instance_id)  # pyright: ignore[reportUnknownMemberType]
        info = _ContainerInfo.model_validate(await container.show())  # pyright: ignore[reportUnknownMemberType]
        if not info.labels or info.labels.get(_MANAGED_LABEL) != "true":
            raise SandboxNotFoundError("Docker container is not managed by this provider")
        return container, info

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        with _docker_errors():
            container, info = await self._get_container(instance_id)
            return DockerSandbox(container, info)

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        if not isinstance(request.source, ImageSource):
            raise SandboxError("Local Docker supports image sources only; snapshots and Compose are not supported")
        if request.resources.gpu or request.volumes or request.sandbox_secrets:
            raise SandboxError("Local Docker does not support GPUs, persistent volumes, or provider-managed secrets")
        name = f"cbs-{uuid4().hex}"
        labels = {**request.labels, _MANAGED_LABEL: "true", _NAME_LABEL: request.name}
        config: JSONObject = {
            "Image": request.source.image,
            "Entrypoint": ["/bin/sh", "-c"],
            "Cmd": ["trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"],
            "Env": [f"{key}={value}" for key, value in request.env_vars.items()],
            "Labels": labels,
            "HostConfig": {
                "NanoCpus": request.resources.vcpu * 1_000_000_000,
                "Memory": request.resources.memory * 1024**3,
                "NetworkMode": "none" if request.network_block_all else "bridge",
                "SecurityOpt": ["no-new-privileges:true"],
            },
        }
        with _docker_errors():
            try:
                async with asyncio.timeout(request.create_timeout):
                    container = await self._docker.containers.run(config, name=name)  # pyright: ignore[reportUnknownMemberType]
                    return await self.get_sandbox(container.id)
            except BaseException as error:

                async def cleanup() -> None:
                    try:
                        async with asyncio.timeout(15):
                            await self.delete_sandbox(name)
                    except SandboxNotFoundError:
                        pass
                    except (SandboxError, TimeoutError):
                        logger.exception("Failed to clean up Docker sandbox after creation failed")

                await _finish_cleanup(asyncio.create_task(cleanup()))
                if isinstance(error, TimeoutError):
                    raise SandboxError("Docker sandbox creation timed out") from error
                raise

    async def delete_sandbox(self, instance_id: str) -> None:
        with _docker_errors():
            container, _ = await self._get_container(instance_id)
            await container.delete(force=True, v=True)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox]:
        labels = {**query.labels, _MANAGED_LABEL: "true"}
        with _docker_errors():
            containers = await self._docker.containers.list(  # pyright: ignore[reportUnknownMemberType]
                all=True, filters={"label": [f"{k}={v}" for k, v in labels.items()]}
            )
            for container in containers:
                try:
                    sandbox = await self.get_sandbox(container.id)
                except SandboxNotFoundError:
                    continue
                if query.created_at_lte is not None and sandbox.created_at is not None:
                    if sandbox.created_at > query.created_at_lte:
                        continue
                yield sandbox

    async def close(self) -> None:
        await self._docker.close()
