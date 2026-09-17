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
from contextlib import contextmanager
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from uuid import uuid4

from aiodocker import Docker
from aiodocker.containers import DockerContainer
from aiodocker.exceptions import DockerError
from aiodocker.execs import Exec
from aiodocker.types import JSONObject
from aiohttp import ClientError
from pydantic import BaseModel, Field, field_validator

from benchmark_service.blocking import run_blocking
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
_INSTALLATION_LABEL = "io.vals.cbs.installation"
_NAME_LABEL = "io.vals.cbs.name"


class DockerProviderConfig(BaseModel):
    type: Literal["docker"] = "docker"
    docker_endpoint: str = "unix:///var/run/docker.sock"
    installation_id: str = Field(default="valkyrie-local", pattern=r"^[a-zA-Z0-9_.-]{1,64}$")
    platform: Literal["linux/arm64", "linux/amd64"] | None = None

    @field_validator("docker_endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        if not value.startswith("unix:///") or "\x00" in value:
            raise ValueError("Local Docker requires an absolute Unix socket endpoint")
        return value

    @classmethod
    def from_env(cls) -> DockerProviderConfig:
        return cls.model_validate(
            {
                "docker_endpoint": os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock"),
                "installation_id": os.environ.get("CBS_DOCKER_INSTALLATION", "valkyrie-local"),
                "platform": os.environ.get("CBS_DOCKER_PLATFORM"),
            }
        )

    def create_provider(self) -> SandboxProvider:
        if os.environ.get("CBS_DOCKER_ENABLED", "").lower() != "true":
            raise SandboxError("Docker sandbox access requires CBS_DOCKER_ENABLED=true on this process")
        return DockerSandboxProvider(self)


class _ContainerConfig(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict, alias="Labels")


class _ContainerState(BaseModel):
    status: str = Field(alias="Status")


class _ContainerInfo(BaseModel):
    id: str = Field(alias="Id")
    created: datetime = Field(alias="Created")
    state: _ContainerState = Field(alias="State")
    config: _ContainerConfig = Field(alias="Config")


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


def _remote_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or "\x00" in value or path == PurePosixPath("/"):
        raise SandboxError("Docker file paths must be absolute file paths without traversal")
    return path


class DockerSandbox(Sandbox):
    def __init__(self, container: DockerContainer, info: _ContainerInfo) -> None:
        self._container = container
        self._info = info
        self.labels = info.config.labels
        self.created_at = info.created

    @property
    def id(self) -> str:
        return self._info.id

    @property
    def name(self) -> str:
        return self._info.config.labels.get(_NAME_LABEL, self.id)

    @property
    def state(self) -> str:
        return self._info.state.status

    async def _cleanup_command(self, execution: Exec, pid_file: str) -> None:
        async with asyncio.timeout(15):
            with _docker_errors():
                try:
                    info = await execution.inspect()
                    if info.get("Running"):
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
        environment = validate_command_env(env_vars)
        pid_file = f"/tmp/cbs-command-{uuid4().hex}.pid"
        script = f"echo $$ > {pid_file}; exec /bin/sh -c {shlex.quote(command)} 2>&1"
        with _docker_errors():
            execution = await self._container.exec(
                ["setsid", "--wait", "/bin/sh", "-c", script],
                environment=environment,
                workdir=cwd,
            )
            try:
                async with asyncio.timeout(timeout):
                    async with execution.start() as stream:
                        while (message := await stream.read_out()) is not None:
                            yield message.data
                    info = await execution.inspect()
                    while info.get("Running"):
                        await asyncio.sleep(0.01)
                        info = await execution.inspect()
                exit_code = info.get("ExitCode")
                if not isinstance(exit_code, int):
                    raise SandboxError("Docker command finished without an exit code")
                if exit_code:
                    raise SandboxCommandError(exit_code)
            except TimeoutError:
                raise SandboxCommandError(124) from None
            finally:
                await _finish_cleanup(asyncio.create_task(self._cleanup_command(execution, pid_file)))

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
        try:
            async for chunk in iterator:
                if text := decoder.decode(chunk):
                    yield text
            if text := decoder.decode(b"", final=True):
                yield text
        finally:
            await iterator.aclose()

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        try:
            async for chunk in self.command(command, cwd=cwd, timeout=timeout):
                output.append(chunk)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(output))
        return ExecResult(exit_code=0, output="".join(output))

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        path = _remote_path(remote_path)
        result = await self.exec(f"mkdir -p {shlex.quote(str(path.parent))}")
        if result.exit_code:
            raise SandboxCommandError(result.exit_code)

        def archive() -> bytes:
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w") as tar:
                info = tarfile.TarInfo(path.name)
                info.size = len(content)
                info.mode = 0o600
                tar.addfile(info, io.BytesIO(content))
            return output.getvalue()

        with _docker_errors():
            await self._container.put_archive(str(path.parent), await run_blocking(archive))  # pyright: ignore[reportUnknownMemberType]

    async def download_file(self, remote_path: str) -> bytes:
        return b"".join([chunk async for chunk in self.stream_download(remote_path)])

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes]:
        path = _remote_path(remote_path)
        iterator = self._command_bytes(f"cat -- {shlex.quote(str(path))}")
        try:
            async for chunk in iterator:
                yield chunk
        finally:
            await iterator.aclose()


class DockerSandboxProvider(SandboxProvider):
    def __init__(self, config: DockerProviderConfig) -> None:
        self.config = config
        self._docker = Docker(url=config.docker_endpoint)

    async def _get_container(self, instance_id: str) -> tuple[DockerContainer, _ContainerInfo]:
        container = self._docker.containers.container(instance_id)  # pyright: ignore[reportUnknownMemberType]
        info = _ContainerInfo.model_validate(await container.show())  # pyright: ignore[reportUnknownMemberType]
        if info.config.labels.get(_INSTALLATION_LABEL) != self.config.installation_id:
            raise SandboxNotFoundError("Docker container does not belong to this installation")
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
        if request.auto_stop_interval != 0:
            raise SandboxError("Local Docker requires auto_stop_interval=0; the caller must delete its sandboxes")
        validate_command_env(request.env_vars)
        name = f"cbs-{self.config.installation_id}-{uuid4().hex}"
        labels = {**request.labels, _INSTALLATION_LABEL: self.config.installation_id, _NAME_LABEL: request.name}
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
                    try:
                        await self._docker.images.inspect(request.source.image)
                    except DockerError as error:
                        if error.status != 404:
                            raise
                        await self._docker.images.pull(request.source.image, platform=self.config.platform)
                    if self.config.platform is not None:
                        image = await self._docker.images.inspect(request.source.image)
                        if f"{image.get('Os')}/{image.get('Architecture')}" != self.config.platform:
                            raise SandboxError(f"Docker image must match platform {self.config.platform}")
                    container = await self._docker.containers.create(config, name=name)
                    await container.start()  # pyright: ignore[reportUnknownMemberType]
                    sandbox = await self.get_sandbox(container.id)
                    probe = await sandbox.exec("true", timeout=10)
                    if probe.exit_code:
                        raise SandboxError("Docker images must provide /bin/sh and setsid")
                    return sandbox
            except BaseException as error:

                async def cleanup() -> None:
                    try:
                        await self.delete_sandbox(name)
                    except SandboxNotFoundError:
                        pass
                    except SandboxError:
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
        if query.labels.get(_INSTALLATION_LABEL, self.config.installation_id) != self.config.installation_id:
            return
        labels = {**query.labels, _INSTALLATION_LABEL: self.config.installation_id}
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
