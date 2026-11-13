from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import re
import shlex
import socket
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Mapping
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import modal.experimental as modal_experimental

from modal import App, Client, Image, SandboxSnapshot, Volume
from modal import Sandbox as ModalSdkSandbox
from modal.exception import ConflictError as ModalConflictError
from modal.exception import ConnectionError as ModalConnectionError
from modal.exception import Error as ModalError
from modal.exception import ImageBuildError as ModalImageBuildError
from modal.exception import InvalidError as ModalInvalidError
from modal.exception import NotFoundError as ModalNotFoundError
from modal.exception import ResourceExhaustedError as ModalResourceExhaustedError
from grpclib import Status
from grpclib.exceptions import GRPCError, ProtocolError, StreamTerminatedError
from pydantic import BaseModel
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.gpu import modal_gpu_type
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ExecResult,
    ImageSource,
    MAX_SANDBOX_LIFETIME_SECONDS,
    MissingSandboxConfigError,
    Sandbox,
    SandboxBuildError,
    SandboxCommandError,
    SandboxConnectionError,
    CHECKPOINT_TIMEOUT_SECONDS,
    Checkpoint,
    CheckpointKind,
    SandboxCapabilities,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    files_checkpoint_source,
    VolumeMount,
    resolve_volume_subpath,
    validate_command_env,
)

# Modal sandboxes must belong to an app; all benchmark sandboxes share one.
_APP_NAME = "benchmark-service"
# Modal's default sandbox timeout is 5 minutes; benchmark tasks run for hours.
_MAX_LIFETIME_SECONDS = min(MAX_SANDBOX_LIFETIME_SECONDS, 24 * 60 * 60)
_ALLOW_ALL_CIDRS = ("0.0.0.0/0",)
_RESOLVE_IN_SANDBOX = (
    'for host in "$@"; do getent ahostsv4 "$host" 2>/dev/null | awk \'{print $1}\'; done; '
    "awk '/^nameserver/ {print $2}' /etc/resolv.conf 2>/dev/null"
)
_COMMAND_STATUS_POLL_SECONDS = 10
_CONTROLLED_OUTPUT_TAIL_BYTES = 64 * 1024
_OUTPUT_DRAIN_SECONDS = 5
_INVALID_NAME_CHARS = re.compile(r"[^a-zA-Z0-9_.-]")
logger = logging.getLogger(__name__)
# Late allocations outlive request-scoped providers.
_BACKGROUND_TASKS: set[asyncio.Task[None]] = set()


def _background_finished(task: asyncio.Task[None]) -> None:
    _BACKGROUND_TASKS.discard(task)
    try:
        task.result()
    except BaseException:
        logger.exception("Modal background task failed: %s", task.get_name())


def _start_background(action: Coroutine[Any, Any, None], name: str) -> asyncio.Task[None]:
    task = asyncio.create_task(action, name=name)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_background_finished)
    return task


async def _close_client(client: Client, cleanup: tuple[asyncio.Task[None], ...]) -> None:
    try:
        await asyncio.gather(*cleanup, return_exceptions=True)
    finally:
        await client._close.aio()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]


_PROVIDER_RETRY = retry(
    retry=retry_if_exception_type(SandboxConnectionError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)


class ModalProviderConfig(BaseModel):
    type: Literal["modal"] = "modal"
    runtime: Literal["gvisor", "vm"] = "gvisor"
    # Modal's memory snapshots are early preview and must be enabled when a sandbox is created.
    memory_snapshots: bool = False
    MODAL_TOKEN_ID: str
    MODAL_TOKEN_SECRET: str

    @classmethod
    def from_env(cls) -> "ModalProviderConfig":
        token_id = os.environ.get("MODAL_TOKEN_ID")
        token_secret = os.environ.get("MODAL_TOKEN_SECRET")
        missing = [
            name
            for name, value in (
                ("MODAL_TOKEN_ID", token_id),
                ("MODAL_TOKEN_SECRET", token_secret),
            )
            if not value
        ]
        if missing:
            raise MissingSandboxConfigError(f"Missing required environment variables: {', '.join(missing)}")
        return cls.model_validate(
            {
                "runtime": os.environ.get("MODAL_RUNTIME", "gvisor"),
                "memory_snapshots": os.environ.get("MODAL_MEMORY_SNAPSHOTS", "false"),
                "MODAL_TOKEN_ID": token_id,
                "MODAL_TOKEN_SECRET": token_secret,
            }
        )

    def create_provider(self) -> SandboxProvider:
        return ModalSandboxProvider(self)


# Modal re-raises these raw grpclib and socket errors once its own transport retries run out.
_TRANSPORT_ERRORS = (StreamTerminatedError, ProtocolError, OSError)
_PROVIDER_ERRORS = (ModalError, GRPCError, *_TRANSPORT_ERRORS)
_TRANSIENT_GRPC_STATUSES = (Status.UNAVAILABLE, Status.DEADLINE_EXCEEDED)


def _is_transport_error(exc: Exception) -> bool:
    return isinstance(exc, _TRANSPORT_ERRORS) or (
        isinstance(exc, GRPCError) and not isinstance(exc, ModalError) and exc.status in _TRANSIENT_GRPC_STATUSES
    )


def _sandbox_error(exc: Exception) -> SandboxError:
    if _is_transport_error(exc):
        return SandboxConnectionError(f"{type(exc).__name__}: {exc}")
    if isinstance(exc, ModalImageBuildError):
        return SandboxBuildError(f"Modal image build failed: {exc}", str(exc))
    if isinstance(exc, ModalNotFoundError):
        return SandboxNotFoundError(str(exc))
    if isinstance(exc, ModalConflictError) and "shutting down" in str(exc).lower():
        return SandboxNotFoundError(str(exc))
    if isinstance(exc, ModalConnectionError):
        return SandboxConnectionError(str(exc))
    return SandboxError(str(exc))


def _is_missing_volume_error(exc: ModalNotFoundError) -> bool:
    message = str(exc).lower()
    return "volume" in message and ("not found" in message or "does not exist" in message)


def _command(command: str, cwd: str | None, timeout: float | None) -> str:
    # Match Daytona semantics: timeout exits 124, stderr merges into stdout.
    if timeout is not None:
        command = f"timeout {timeout:g} sh -c {shlex.quote(command)}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return f"{{ {command} ; }} 2>&1"


class ModalSandbox(Sandbox):
    def __init__(
        self,
        sandbox: ModalSdkSandbox,
        name: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        self._sandbox = sandbox
        self._name = name
        self.labels = dict(labels) if labels is not None else None

    @property
    def id(self) -> str:
        return self._sandbox.object_id

    @property
    def name(self) -> str:
        return self._name or self._sandbox.object_id

    @property
    def state(self) -> str:
        # Modal does not expose a cached lifecycle state on the sandbox handle.
        return "unknown"

    @_PROVIDER_RETRY
    async def _raise_if_finished(self, *, attempts: int = 1, wait_seconds: float = 0) -> None:
        try:
            for attempt in range(attempts):
                poll_result = await self._sandbox.poll.aio()
                if poll_result is not None:
                    raise SandboxNotFoundError(f"Sandbox not found: id={self.id}, poll_result={poll_result}.")
                if attempt < attempts - 1:
                    await asyncio.sleep(wait_seconds)
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        await self._raise_if_finished()
        process = await self._start_process(command, cwd=cwd, timeout=timeout)
        try:
            output = "".join([str(chunk) async for chunk in process.stdout])
            exit_code = await process.wait.aio()
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        if exit_code != 0:
            await self._raise_if_finished(attempts=6, wait_seconds=0.5)
        return ExecResult(exit_code=exit_code, output=output)

    @_PROVIDER_RETRY
    async def _start_process(
        self,
        command: str,
        *,
        cwd: str | None,
        timeout: float | None,
        env_vars: dict[str, str] | None = None,
    ) -> Any:
        modal_env: dict[str, str | None] | None = None
        if env_vars is not None:
            modal_env = {name: value for name, value in env_vars.items()}
        try:
            return await self._sandbox.exec.aio(
                "/bin/sh",
                "-lc",
                _command(command, cwd, timeout),
                env=modal_env,
                text=True,
            )
        except _PROVIDER_ERRORS as exc:
            if _is_transport_error(exc):
                # The command may have started before its reply was lost, so a retry could run it twice.
                raise SandboxError(f"Modal exec start lost its reply: {type(exc).__name__}: {exc}") from exc
            raise _sandbox_error(exc) from exc

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars) if env_vars is not None else None
        await self._raise_if_finished()
        process = await self._start_process(command, cwd=cwd, timeout=timeout, env_vars=env)
        try:
            async for chunk in self._stream_command(process):
                yield chunk
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def _stream_command(self, process: Any) -> AsyncGenerator[str, None]:
        output_iterator = process.stdout.__aiter__()
        next_chunk_task = asyncio.create_task(anext(output_iterator))
        wait_task: asyncio.Task[Any] | None = asyncio.create_task(process.wait.aio())
        exit_code: int | None = None
        drain_deadline: float | None = None
        loop = asyncio.get_running_loop()

        try:
            while True:
                completed = await self._wait_for_command_progress(next_chunk_task, wait_task, drain_deadline)
                if completed is None:
                    break

                if wait_task is not None and wait_task in completed:
                    exit_code = wait_task.result()
                    wait_task = None
                    drain_deadline = loop.time() + _COMMAND_STATUS_POLL_SECONDS

                if next_chunk_task in completed:
                    try:
                        chunk = next_chunk_task.result()
                    except StopAsyncIteration:
                        if wait_task is not None:
                            exit_code = await wait_task
                        break

                    yield str(chunk)
                    if wait_task is None:
                        drain_deadline = loop.time() + _COMMAND_STATUS_POLL_SECONDS
                    next_chunk_task = asyncio.create_task(anext(output_iterator))
                    continue

                if drain_deadline is not None:
                    continue
                await self._raise_if_finished()
        finally:
            await self._cancel_command_tasks(next_chunk_task, wait_task)

        if exit_code is None:
            raise RuntimeError("Modal command completed without an exit code")
        if exit_code != 0:
            await self._raise_if_finished(attempts=6, wait_seconds=0.5)
            raise SandboxCommandError(exit_code)

    async def _wait_for_command_progress(
        self,
        next_chunk_task: asyncio.Task[Any],
        wait_task: asyncio.Task[Any] | None,
        drain_deadline: float | None,
    ) -> set[asyncio.Task[Any]] | None:
        tasks: set[asyncio.Task[Any]] = {next_chunk_task}
        if wait_task is not None:
            tasks.add(wait_task)

        poll_timeout = _COMMAND_STATUS_POLL_SECONDS
        if drain_deadline is not None:
            remaining = drain_deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            poll_timeout = min(poll_timeout, remaining)

        completed, _ = await asyncio.wait(
            tasks,
            timeout=poll_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        return completed

    @staticmethod
    async def _cancel_command_tasks(
        next_chunk_task: asyncio.Task[Any],
        wait_task: asyncio.Task[Any] | None,
    ) -> None:
        tasks_to_cancel = [next_chunk_task]
        if wait_task is not None:
            tasks_to_cancel.append(wait_task)
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    @_PROVIDER_RETRY
    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self._raise_if_finished()
        try:
            await cast(Awaitable[None], self._sandbox.filesystem.write_bytes.aio(content, remote_path))
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def download_file(self, remote_path: str) -> bytes:
        await self._raise_if_finished()
        try:
            content = await cast(Awaitable[bytes], self._sandbox.filesystem.read_bytes.aio(remote_path))
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        return bytes(content)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        await self._raise_if_finished()
        process = await self._start_download_process(remote_path)
        try:
            async for chunk in process.stdout:
                yield bytes(chunk)
            exit_code = await process.wait.aio()
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        if exit_code != 0:
            await self._raise_if_finished(attempts=6, wait_seconds=0.5)
            raise SandboxError(f"File download failed: path={remote_path}, exit_code={exit_code}.")

    @_PROVIDER_RETRY
    async def _start_download_process(self, remote_path: str) -> Any:
        try:
            return await self._sandbox.exec.aio("cat", "--", remote_path, text=False)
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def _set_outbound_network_policy(self, cidrs: list[str], domains: list[str]) -> None:
        try:
            set_policy = cast(Any, self._sandbox)._experimental_set_outbound_network_policy
            await cast(
                Awaitable[None],
                set_policy.aio(outbound_cidr_allowlist=cidrs, outbound_domain_allowlist=domains),
            )
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def _host_cidrs(self, hosts: list[str]) -> list[str]:
        """The IPv4 addresses the hosts resolve to here and in the sandbox, and the sandbox's DNS servers."""
        if not hosts:
            return []
        result = await self.exec(f"sh -c {shlex.quote(_RESOLVE_IN_SANDBOX)} _ " + " ".join(map(shlex.quote, hosts)))
        local = [address for host in hosts for address in await _resolve_ipv4(host)]
        return [f"{address}/32" for address in [*result.stdout.split(), *local] if _is_ipv4(address)]

    async def checkpoint(self, kind: CheckpointKind, name: str) -> Checkpoint:
        try:
            if kind == CheckpointKind.FILES:
                saved = await self._sandbox.snapshot_filesystem.aio(timeout=CHECKPOINT_TIMEOUT_SECONDS)
            else:
                # Memory snapshots are an early-preview private API.
                saved = await self._sandbox._experimental_snapshot.aio()  # pyright: ignore[reportPrivateUsage]
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        return Checkpoint(provider="modal", kind=kind, ref=saved.object_id)

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        cidrs, hosts = resolve_allowed_addresses(allowed_addresses)
        # No address list can express a wildcard, so those stay hostname rules on Modal's domain allowlist.
        wildcards = [host for host in hosts if host.startswith("*.")]
        exact = [host for host in hosts if host not in wildcards]
        await self._set_outbound_network_policy(
            list(dict.fromkeys([*cidrs, *await self._host_cidrs(exact)])), wildcards
        )

    async def block_all_egress(self) -> None:
        await self._set_outbound_network_policy([], [])

    async def clear_egress_rules(self) -> None:
        await self._set_outbound_network_policy(list(_ALLOW_ALL_CIDRS), [])


def _is_ipv4(value: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(value), ipaddress.IPv4Address)
    except ValueError:
        return False


async def _resolve_ipv4(host: str) -> list[str]:
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except OSError:
        return []
    return [str(info[4][0]) for info in infos]


class ModalSandboxProvider(SandboxProvider):
    def __init__(self, config: ModalProviderConfig) -> None:
        self._config = config
        self._client: Client | None = None
        self._app: App | None = None
        self._cleanup_tasks: set[asyncio.Task[None]] = set()

    async def _connect(self) -> tuple[Client, App]:
        if self._client is None or self._app is None:
            try:
                client = await Client.from_credentials.aio(self._config.MODAL_TOKEN_ID, self._config.MODAL_TOKEN_SECRET)
                self._app = await App.lookup.aio(
                    _APP_NAME,
                    client=client,
                    create_if_missing=True,
                )
                self._client = client
            except _PROVIDER_ERRORS as exc:
                raise _sandbox_error(exc) from exc
        return self._client, self._app

    def _resolve_volumes(
        self,
        mounts: list[VolumeMount],
        client: Client,
        labels: Mapping[str, str],
    ) -> dict[str, Volume]:
        """Look up each named volume, keyed by the path it mounts at.

        create_if_missing is off by default and deliberately not inferred: a
        benchmark whose fixture volume is missing should fail loudly at creation
        rather than start a sandbox with an empty directory where its data
        should be, which surfaces much later as a confusing benchmark failure.
        """
        resolved: dict[str, Volume] = {}
        for mount in mounts:
            subpath = resolve_volume_subpath(mount, labels)
            try:
                volume = Volume.from_name(
                    mount.name,
                    create_if_missing=mount.create_if_missing,
                    client=client,
                )
                if mount.read_only or subpath is not None:
                    volume = volume.with_mount_options(
                        read_only=mount.read_only,
                        sub_path=subpath,
                    )
            except ModalNotFoundError as exc:
                raise SandboxError(
                    f"Modal volume {mount.name!r} does not exist (mount {mount.mount_path}); "
                    "create it or set create_if_missing"
                ) from exc
            except _PROVIDER_ERRORS as exc:
                raise _sandbox_error(exc) from exc
            resolved[mount.mount_path] = volume
        return resolved

    def _resolve_image(self, source: SandboxSource, client: Client) -> Image:
        # A snapshot is a Modal filesystem snapshot, referenced by its Image id.
        match source:
            case ImageSource(image=image):
                return Image.from_registry(image)  # pyright: ignore[reportUnknownMemberType]
            case SnapshotSource(snapshot=snapshot):
                return Image.from_id(snapshot, client=client)
            case DockerfileSource(dockerfile=dockerfile):
                if not Path(dockerfile).is_file():
                    raise SandboxError(f"Cannot read the Dockerfile {dockerfile}")
                return Image.from_dockerfile(dockerfile, context_dir=str(PurePosixPath(dockerfile).parent))  # pyright: ignore[reportUnknownMemberType]
            case _:
                raise SandboxError(f"Modal sandbox provider does not support source type: {source.type}")

    @_PROVIDER_RETRY
    async def _fetch_labels(self, sandbox: ModalSdkSandbox) -> dict[str, str]:
        try:
            return await cast(Any, sandbox.get_tags).aio()
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    @property
    def capabilities(self) -> SandboxCapabilities:
        if self._config.memory_snapshots:
            return SandboxCapabilities(checkpoints=frozenset(CheckpointKind))
        return SandboxCapabilities(checkpoints=frozenset({CheckpointKind.FILES}))

    async def delete_checkpoint(self, checkpoint: Checkpoint) -> None:
        if checkpoint.provider == "modal" and checkpoint.kind == CheckpointKind.MEMORY:
            return  # Modal has no API to delete memory snapshots.
        image_id = files_checkpoint_source(checkpoint, "modal").snapshot
        client, _ = await self._connect()
        try:
            # modal.experimental has no stubs, so the .aio variant is untyped.
            await cast(Any, modal_experimental.image_delete).aio(image_id, client=client)
        except ModalNotFoundError:
            return
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        if isinstance(request.source, Checkpoint):
            if request.source.provider == "modal" and request.source.kind == CheckpointKind.MEMORY:
                return await self._restore_memory(request, request.source.ref)
            request = request.model_copy(update={"source": files_checkpoint_source(request.source, "modal")})
        if request.sandbox_secrets:
            raise SandboxError("Modal sandbox provider does not support sandbox_secrets")

        client, app = await self._connect()
        # Keep the full UUID within Modal's 64-character name limit.
        prefix = _INVALID_NAME_CHARS.sub("_", request.name)[:31] or "sandbox"
        modal_name = f"{prefix}-{uuid.uuid4().hex}"
        image = self._resolve_image(request.source, client)
        allow_all_egress = not request.network_block_all
        gpu: str | None = None
        if request.resources.gpu:
            gpu_type = modal_gpu_type(request.resources.acceptable_gpu_types)
            if not gpu_type:
                raise SandboxError("Modal sandbox provider requires gpu_type when gpu is requested")
            gpu = f"{gpu_type}:{request.resources.gpu}"
        runtime = "gvisor" if gpu else self._config.runtime
        create_kwargs: dict[str, Any] = {
            "app": app,
            "name": modal_name,
            "image": image,
            "env": dict(request.env_vars),
            "tags": request.labels,
            "cpu": (request.resources.vcpu, request.resources.vcpu),
            "memory": request.resources.memory * 1024,
            "gpu": gpu,
            "idle_timeout": request.auto_stop_interval * 60 if request.auto_stop_interval else None,
            "timeout": _MAX_LIFETIME_SECONDS,
            "block_network": request.network_block_all,
            "outbound_cidr_allowlist": list(_ALLOW_ALL_CIDRS) if allow_all_egress else None,
            "client": client,
            "runtime": runtime,
            # Keep this flag: nested Docker's overlay2 failed without it on T4.
            "experimental_options": {"enable_docker": True} if runtime == "gvisor" else None,
            "_experimental_enable_snapshot": self._config.memory_snapshots,
        }
        if request.volumes:
            create_kwargs["volumes"] = self._resolve_volumes(request.volumes, client, request.labels)

        # One SDK call preserves Modal's transport idempotency key.
        creation = asyncio.create_task(ModalSdkSandbox.create.aio(**create_kwargs))  # pyright: ignore[reportUnknownMemberType]
        try:
            async with asyncio.timeout(request.provider_timeout):
                inner = await asyncio.shield(creation)
        except (TimeoutError, asyncio.CancelledError) as exc:
            # Retain ownership until the late allocation can be terminated.
            cleanup = _start_background(self._cleanup_create(creation), f"Modal cleanup {request.name}")
            self._cleanup_tasks.add(cleanup)
            cleanup.add_done_callback(self._cleanup_tasks.discard)
            if isinstance(exc, asyncio.CancelledError):
                raise
            if isinstance(request.source, DockerfileSource):
                raise SandboxBuildError(
                    f"Timed out building {request.source.dockerfile} after {request.provider_timeout:g}s"
                ) from exc
            raise SandboxError(f"Failed to create Modal sandbox within {request.provider_timeout}s") from exc
        except ModalResourceExhaustedError as exc:
            raise SandboxError(f"Modal sandbox resources exhausted: {exc}") from exc
        except ModalNotFoundError as exc:
            if request.volumes and _is_missing_volume_error(exc):
                raise SandboxError(f"Modal volume mount failed: {exc}") from exc
            raise _sandbox_error(exc) from exc
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        return ModalSandbox(inner, name=request.name, labels=request.labels)

    async def _restore_memory(self, request: SandboxCreateRequest, snapshot_id: str) -> Sandbox:
        """Restore keeps the snapshot's image, resources and network policy; only the name and tags are set."""
        if request.network_block_all:
            raise SandboxError(
                "A memory checkpoint resumes under its saved network policy, before network_block_all could apply"
            )
        client, _ = await self._connect()
        prefix = _INVALID_NAME_CHARS.sub("_", request.name)[:31] or "sandbox"
        snapshot = SandboxSnapshot.from_id(snapshot_id, client)
        restore = cast(
            Callable[..., Coroutine[Any, Any, ModalSdkSandbox]],
            ModalSdkSandbox._experimental_from_snapshot.aio,  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
        )
        creation = asyncio.create_task(restore(snapshot, client, name=f"{prefix}-{uuid.uuid4().hex}"))
        try:
            async with asyncio.timeout(request.provider_timeout):
                inner = await asyncio.shield(creation)
        except (TimeoutError, asyncio.CancelledError) as exc:
            cleanup = _start_background(self._cleanup_create(creation), f"Modal cleanup {request.name}")
            self._cleanup_tasks.add(cleanup)
            cleanup.add_done_callback(self._cleanup_tasks.discard)
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise SandboxError(f"Failed to restore Modal sandbox within {request.provider_timeout}s") from exc
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        try:
            await inner.set_tags.aio(request.labels)
        except BaseException:
            with suppress(*_PROVIDER_ERRORS):
                await inner.terminate.aio()
            raise
        return ModalSandbox(inner, name=request.name, labels=request.labels)

    async def _cleanup_create(self, creation: asyncio.Task[ModalSdkSandbox]) -> None:
        try:
            inner = await creation
            await inner.terminate.aio(wait=True)
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def get_sandbox(self, instance_id: str) -> Sandbox:
        client, _ = await self._connect()
        try:
            inner = await ModalSdkSandbox.from_id.aio(instance_id, client=client)
            if await inner.poll.aio() is not None:
                raise SandboxNotFoundError(f"Sandbox not found: id={instance_id}.")
        except ModalInvalidError as exc:
            raise SandboxNotFoundError(f"Sandbox not found: id={instance_id}.") from exc
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc
        return ModalSandbox(inner, labels=await self._fetch_labels(inner))

    @_PROVIDER_RETRY
    async def delete_sandbox(self, instance_id: str) -> None:
        client, _ = await self._connect()
        try:
            inner = await ModalSdkSandbox.from_id.aio(instance_id, client=client)
            await inner.terminate.aio(wait=True)
        except ModalNotFoundError:
            return
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        for inner in await self._list_sandboxes(query):
            yield ModalSandbox(inner, labels=await self._fetch_labels(inner))

    @_PROVIDER_RETRY
    async def _list_sandboxes(self, query: SandboxQuery) -> list[ModalSdkSandbox]:
        if query.created_at_lte is not None:
            raise SandboxError("Modal sandbox provider does not support filtering by creation time")

        client, app = await self._connect()
        try:
            sandboxes: list[ModalSdkSandbox] = []
            async for inner in ModalSdkSandbox.list.aio(app_id=app.app_id, tags=query.labels or None, client=client):
                if await inner.poll.aio() is None:
                    sandboxes.append(inner)
            return sandboxes
        except _PROVIDER_ERRORS as exc:
            raise _sandbox_error(exc) from exc

    async def close(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        self._app = None
        cleanup = tuple(self._cleanup_tasks)
        self._cleanup_tasks.clear()
        closing = _start_background(_close_client(client, cleanup), "Modal client close")
        if not cleanup:
            await asyncio.shield(closing)
