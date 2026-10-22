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
from collections import deque
from collections.abc import AsyncGenerator, Generator, Mapping
from contextlib import aclosing, contextmanager
from datetime import datetime
from pathlib import PurePosixPath
from typing import Awaitable, Callable, Literal
from uuid import uuid4

from aiodocker import Docker
from aiodocker.containers import DockerContainer
from aiodocker.exceptions import DockerError
from aiodocker.execs import Exec
from aiodocker.stream import Stream
from aiodocker.types import JSONObject
from aiohttp import ClientError
from pydantic import AliasPath, BaseModel, Field

from benchmark_service.sandbox._process_group import (
    cleanup_command, episode_owner_command, owner_command, probe_command, stop_command,
)
from benchmark_service.sandbox.types import (
    ControlledWorkload,
    ControlledWorkloadResult,
    ExecResult,
    GenerationContainment,
    ImageSource,
    LINUX_PROCESS_GROUP_V1,
    MAX_SANDBOX_LIFETIME_SECONDS,
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
_PULL_TIMEOUT_S = 1800
_CONTROLLED_TAIL_BYTES = 64 * 1024


class DockerProviderConfig(BaseModel):
    type: Literal["docker"] = "docker"

    def create_provider(self) -> SandboxProvider:
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

    @property
    def generation_containment(self) -> GenerationContainment:
        return LINUX_PROCESS_GROUP_V1

    async def probe_generation_containment(self) -> None:
        marker = f"/tmp/.cbs-probe-{uuid4().hex}"
        result = await self.exec(probe_command(marker))
        if result.exit_code:
            raise SandboxError(f"Docker container does not support process groups: {result.output}")

    def _process_group_workload(
        self, command: str, episode_script: str | None, *, cwd: str | None,
        env_vars: Mapping[str, str] | None
    ) -> ControlledWorkload:
        return _DockerControlledWorkload(
            self._container, self._raise_if_finished, command, cwd,
            validate_command_env(env_vars), episode_script
        )

    async def _raise_if_finished(self) -> None:
        # Docker reports a killed command's exit before it marks the container stopped.
        for _ in range(10):
            info = _ContainerInfo.model_validate(await self._container.show())  # pyright: ignore[reportUnknownMemberType]
            if info.state != "running":
                raise SandboxNotFoundError(f"Docker container is {info.state}")
            await asyncio.sleep(0.1)

    async def _cleanup_command(self, pid_file: str, *, terminate: bool) -> None:
        try:
            async with asyncio.timeout(15):
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
        except (DockerError, ClientError, OSError) as error:
            if not isinstance(error, DockerError) or error.status not in (404, 409):
                logger.warning("Failed to clean up Docker command", exc_info=True)

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
                if exit_code == 137:
                    # Stopping a container kills its commands with SIGKILL.
                    await self._raise_if_finished()
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
        if not path.is_absolute():
            # Docker extracts archives relative to /, but commands and downloads resolve from the working directory.
            path = PurePosixPath((await self.exec("pwd")).output.strip()) / path
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



class _DockerControlledWorkload(ControlledWorkload):
    def __init__(
        self,
        container: DockerContainer,
        raise_if_finished: Callable[[], Awaitable[None]],
        command: str,
        cwd: str | None,
        env_vars: dict[str, str],
        episode_script: str | None,
    ) -> None:
        self._container = container
        self._raise_if_finished = raise_if_finished
        self._command = command
        self._cwd = cwd
        self._env_vars = env_vars
        self._episode_script = episode_script
        self._marker = f"/tmp/.cbs-controlled-{uuid4().hex}"
        self._started = asyncio.Event()
        self._status_done = asyncio.Event()
        self._execution: Exec | None = None
        self._output: asyncio.Queue[str | None] = asyncio.Queue()
        self._drain_task: asyncio.Task[None] | None = None
        self._tail: deque[str] = deque()
        self._tail_bytes = 0
        self._stop_task: asyncio.Task[tuple[float, int | None]] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._result_task = asyncio.create_task(self._run())

    def _accept(self, text: str) -> None:
        if not text:
            return
        self._output.put_nowait(text)
        self._tail.append(text)
        self._tail_bytes += len(text.encode("utf-8"))
        while self._tail_bytes > _CONTROLLED_TAIL_BYTES:
            excess = self._tail_bytes - _CONTROLLED_TAIL_BYTES
            first = self._tail[0].encode("utf-8")
            if len(first) <= excess:
                self._tail_bytes -= len(first)
                self._tail.popleft()
            else:
                self._tail[0] = first[excess:].decode("utf-8", errors="ignore")
                self._tail_bytes -= len(first) - len(self._tail[0].encode("utf-8"))

    async def _drain(self, stream: Stream) -> None:
        try:
            with _docker_errors():
                decoders: dict[int, codecs.IncrementalDecoder] = {}
                while (message := await stream.read_out()) is not None:
                    decoder = decoders.get(message.stream)
                    if decoder is None:
                        decoder = decoders[message.stream] = codecs.getincrementaldecoder("utf-8")(errors="replace")
                    self._accept(decoder.decode(message.data))
                for decoder in decoders.values():
                    self._accept(decoder.decode(b"", final=True))
        finally:
            self._output.put_nowait(None)

    async def _status(self, execution: Exec) -> int:
        info = await execution.inspect()
        while info.get("Running"):
            await asyncio.sleep(0.05)
            info = await execution.inspect()
        exit_code = info.get("ExitCode")
        if not isinstance(exit_code, int):
            raise SandboxError("Docker controlled command finished without an exit code")
        if exit_code == 137:
            await self._raise_if_finished()
        self._status_done.set()
        return exit_code

    async def _run(self) -> ControlledWorkloadResult:
        script = (
            episode_owner_command(self._command, self._marker, self._episode_script)
            if self._episode_script is not None
            else owner_command(self._command, self._marker, "/bin/sh -c")
        )
        with _docker_errors():
            try:
                execution = await self._container.exec(
                    ["/bin/sh", "-c", script],
                    environment=self._env_vars,
                    workdir=self._cwd,
                )
                stream = execution.start()
                async with stream:
                    self._execution = execution
                    self._started.set()
                    drain = self._drain_task = asyncio.create_task(self._drain(stream))
                    status = asyncio.create_task(self._status(execution))
                    try:
                        done, _ = await asyncio.wait((status, drain), return_when=asyncio.FIRST_COMPLETED)
                        if drain in done:
                            await drain
                        exit_code = await status
                        absence_confirmed_at, foreground_status = await self._ensure_stopped()
                        await drain
                        assert self._cleanup_task is not None
                        await self._cleanup_task
                        if foreground_status is None and exit_code == 0:
                            raise SandboxError("Docker workload exited without a foreground status")
                        result = ExecResult(
                            exit_code=foreground_status if foreground_status is not None else exit_code,
                            output="".join(self._tail),
                        )
                        return ControlledWorkloadResult(result, absence_confirmed_at)
                    except Exception:
                        status_failed = status.done() and not status.cancelled() and status.exception() is not None
                        drain_failed = drain.done() and not drain.cancelled() and drain.exception() is not None
                        if not status.done():
                            status.cancel()
                        await asyncio.gather(status, return_exceptions=True)
                        if status_failed and not drain_failed:
                            if not drain.done():
                                self._drain_task = None
                            raise
                        await self._ensure_stopped()
                        await drain
                        raise
                    finally:
                        if not drain.done():
                            drain.cancel()
                            await asyncio.gather(drain, return_exceptions=True)
            finally:
                self._started.set()
                if self._drain_task is None:
                    self._output.put_nowait(None)

    async def _control_exec(self, command: str) -> ExecResult:
        with _docker_errors():
            execution = await self._container.exec(["/bin/sh", "-c", command])
            output: list[bytes] = []
            async with execution.start() as stream:
                while (message := await stream.read_out()) is not None:
                    output.append(message.data)
            info = await execution.inspect()
            while info.get("Running"):
                await asyncio.sleep(0.01)
                info = await execution.inspect()
            exit_code = info.get("ExitCode")
            if not isinstance(exit_code, int):
                raise SandboxError("Docker control command finished without an exit code")
            return ExecResult(exit_code=exit_code, output=b"".join(output).decode(errors="replace"))

    async def _stop(self) -> tuple[float, int | None]:
        await self._started.wait()
        if self._execution is None:
            await self._result_task
            raise SandboxError("Docker controlled command did not start")
        command = stop_command(self._marker, episode_owner=self._episode_script is not None)
        while True:
            stopped = await self._control_exec(command)
            if stopped.exit_code == 0:
                break
            if stopped.exit_code != 75:
                raise SandboxError(f"Docker process group did not stop: {stopped.output}")
            if self._status_done.is_set():
                raise SandboxError("Docker workload finished without a process-group marker")
            await asyncio.sleep(0.05)
        self._cleanup_task = asyncio.create_task(self._cleanup())
        return asyncio.get_running_loop().time(), int(stopped.output.strip()) if stopped.output.strip() else None

    async def _cleanup(self) -> None:
        if self._drain_task is None:
            await asyncio.gather(self._result_task, return_exceptions=True)
        else:
            await asyncio.gather(self._drain_task, return_exceptions=True)
        removed = await self._control_exec(cleanup_command(self._marker))
        if removed.exit_code != 0:
            raise SandboxError(f"Docker process-group control cleanup failed: {removed.output}")

    async def _ensure_stopped(self) -> tuple[float, int | None]:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        return await asyncio.shield(self._stop_task)

    async def output(self) -> AsyncGenerator[str, None]:
        while (chunk := await self._output.get()) is not None:
            yield chunk
        if self._drain_task is None:
            await asyncio.shield(self._result_task)
        else:
            await asyncio.shield(self._drain_task)

    async def wait(self) -> ControlledWorkloadResult:
        return await asyncio.shield(self._result_task)

    async def kill(self) -> None:
        await self._ensure_stopped()


def _build_container_config(request: SandboxCreateRequest) -> JSONObject:
    """Validate Docker support and build the container creation settings."""
    if not isinstance(request.source, ImageSource):
        raise SandboxError("Local Docker supports image sources only; snapshots and Compose are not supported")
    if request.resources.gpu or request.volumes or request.sandbox_secrets:
        raise SandboxError("Local Docker does not support GPUs, persistent volumes, or provider-managed secrets")
    labels = {**request.labels, _MANAGED_LABEL: "true", _NAME_LABEL: request.name}
    return {
        "Image": request.source.image,
        "Entrypoint": ["/bin/sh", "-c"],
        "Cmd": [f"trap 'exit 0' TERM INT; sleep {MAX_SANDBOX_LIFETIME_SECONDS} & wait $!"],
        "Env": [f"{key}={value}" for key, value in request.env_vars.items()],
        "Labels": labels,
        "HostConfig": {
            "NanoCpus": request.resources.vcpu * 1_000_000_000,
            "Memory": request.resources.memory * 1024**3,
            "NetworkMode": "none" if request.network_block_all else "bridge",
            "SecurityOpt": ["no-new-privileges:true"],
        },
    }


class DockerSandboxProvider(SandboxProvider):
    def __init__(self) -> None:
        self._docker_host = os.environ.get("DOCKER_HOST")
        self._docker = Docker(url=self._docker_host)

    async def _get_container(self, instance_id: str) -> tuple[DockerContainer, _ContainerInfo]:
        container = self._docker.containers.container(instance_id)  # pyright: ignore[reportUnknownMemberType]
        info = _ContainerInfo.model_validate(await container.show())  # pyright: ignore[reportUnknownMemberType]
        if not info.labels or info.labels.get(_MANAGED_LABEL) != "true":
            raise SandboxNotFoundError("Docker container is not managed by this provider")
        return self._docker.containers.container(info.id), info  # pyright: ignore[reportUnknownMemberType]

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        with _docker_errors():
            container, info = await self._get_container(instance_id)
            return DockerSandbox(container, info)

    async def _ensure_image(self, image: str) -> None:
        with _docker_errors():
            try:
                await self._docker.images.inspect(image)
                return
            except DockerError as error:
                if error.status != 404:
                    raise

        # The docker CLI applies the host's registry logins and credential helpers, which aiodocker does not read.
        # DOCKER_CONTEXT overrides DOCKER_HOST in the CLI, unlike the explicit URL passed to aiodocker.
        # Pin pulls to the same host selected when this provider was created, even if the environment changes.
        env = os.environ.copy()
        host_args: list[str] = []
        if self._docker_host:
            env.pop("DOCKER_CONTEXT", None)
            host_args = ["--host", self._docker_host]
        try:
            process = await asyncio.create_subprocess_exec(
                "docker",
                *host_args,
                "pull",
                "--quiet",
                image,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except OSError as error:
            raise SandboxError("Pulling a missing Docker image requires the docker CLI on PATH") from error
        try:
            async with asyncio.timeout(_PULL_TIMEOUT_S):
                output, _ = await process.communicate()
        except TimeoutError as error:
            raise SandboxError(f"Timed out pulling Docker image {image}") from error
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

        if process.returncode != 0:
            raise SandboxError(f"Failed to pull Docker image {image}: {output.decode(errors='replace').strip()}")

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        config = _build_container_config(request)
        # create_timeout bounds container creation, so a large first pull must not run inside it.
        await self._ensure_image(str(config["Image"]))
        name = f"cbs-{uuid4().hex}"
        run = asyncio.create_task(self._docker.containers.run(config, name=name))  # pyright: ignore[reportUnknownMemberType]
        with _docker_errors():
            try:
                async with asyncio.timeout(request.create_timeout):
                    container = await asyncio.shield(run)
                    sandbox = await self.get_sandbox(container.id)
                    # Commands run under setsid --wait, which BusyBox images such as Alpine lack.
                    check = await sandbox.exec("true")
                    if check.exit_code:
                        raise SandboxError(
                            f"Docker image {config['Image']} cannot run commands; it needs /bin/sh and "
                            f"setsid --wait (util-linux): {check.output.strip()}"
                        )
                    return sandbox
            except BaseException as error:
                await _finish_cleanup(asyncio.create_task(self._cleanup_failed_creation(name, run)))
                if isinstance(error, TimeoutError):
                    raise SandboxError("Docker sandbox creation timed out") from error
                raise

    async def _cleanup_failed_creation(self, name: str, run: asyncio.Task[DockerContainer]) -> None:
        # Docker keeps creating after the client gives up, so let an in-flight create finish before deleting by name.
        was_running = not run.done()
        try:
            async with asyncio.timeout(15):
                await asyncio.wait({run})
        except TimeoutError:
            run.cancel()
        if was_running and run.done() and not run.cancelled() and (error := run.exception()) is not None:
            logger.warning("Docker sandbox creation failed after its deadline", exc_info=error)
        try:
            async with asyncio.timeout(15):
                await self.delete_sandbox(name)
        except (SandboxError, TimeoutError):
            logger.exception("Failed to clean up Docker sandbox after creation failed")

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            with _docker_errors():
                container, _ = await self._get_container(instance_id)
                await container.delete(force=True, v=True)
        except SandboxNotFoundError:
            return

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
