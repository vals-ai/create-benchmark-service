from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shlex
from collections.abc import AsyncGenerator, Awaitable, Mapping
from typing import Any, Literal, cast

from modal import App, Client, Image, Volume
from modal import Sandbox as ModalSdkSandbox
from modal.exception import ConflictError as ModalConflictError
from modal.exception import ConnectionError as ModalConnectionError
from modal.exception import Error as ModalError
from modal.exception import InvalidError as ModalInvalidError
from modal.exception import NotFoundError as ModalNotFoundError
from pydantic import BaseModel
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    VolumeMount,
    resolve_volume_subpath,
    validate_command_env,
)

# Modal sandboxes must belong to an app; all benchmark sandboxes share one.
_APP_NAME = "benchmark-service"
# Modal's default sandbox timeout is 5 minutes; benchmark tasks run for hours.
_MAX_LIFETIME_SECONDS = 24 * 60 * 60
_ALLOW_ALL_CIDRS = ("0.0.0.0/0",)
_ALLOW_ALL_DOMAINS = ("*",)
_COMMAND_STATUS_POLL_SECONDS = 10
_MAX_NAME_LENGTH = 64
_INVALID_NAME_CHARS = re.compile(r"[^a-zA-Z0-9_.-]")
_APP_ID_PATTERN = re.compile(r"^ap-[a-zA-Z0-9]{22}$")


_PROVIDER_RETRY = retry(
    retry=retry_if_exception_type(SandboxConnectionError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)


class ModalProviderConfig(BaseModel):
    type: Literal["modal"] = "modal"
    runtime: Literal["gvisor", "vm"] = "gvisor"
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
                "MODAL_TOKEN_ID": token_id,
                "MODAL_TOKEN_SECRET": token_secret,
            }
        )

    def create_provider(self) -> SandboxProvider:
        return ModalSandboxProvider(self)


def _sandbox_error(exc: ModalError) -> SandboxError:
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
        command = f"timeout {timeout:g} {command}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return f"{{ {command} ; }} 2>&1"


def _modal_sandbox_name(name: str) -> str:
    """Return the deterministic Modal SDK name for a requested sandbox name.

    Arguments
    - name: Requested sandbox name from the benchmark task.

    Returns
    A Modal-safe name for SDK lookup/create calls; callers should keep the original name on Sandbox.
    """
    modal_name = _INVALID_NAME_CHARS.sub("_", name) or "sandbox"
    if _APP_ID_PATTERN.fullmatch(modal_name):
        modal_name = f"sandbox-{modal_name}"
    if len(modal_name) <= _MAX_NAME_LENGTH:
        return modal_name
    digest = hashlib.sha1(name.encode()).hexdigest()[:8]
    return f"{modal_name[:55]}-{digest}"


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
        except ModalError as exc:
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
        except ModalError as exc:
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
        except ModalError as exc:
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
        except ModalError as exc:
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
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    @_PROVIDER_RETRY
    async def download_file(self, remote_path: str) -> bytes:
        await self._raise_if_finished()
        try:
            content = await cast(Awaitable[bytes], self._sandbox.filesystem.read_bytes.aio(remote_path))
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        return bytes(content)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        await self._raise_if_finished()
        process = await self._start_download_process(remote_path)
        try:
            async for chunk in process.stdout:
                yield bytes(chunk)
            exit_code = await process.wait.aio()
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        if exit_code != 0:
            await self._raise_if_finished(attempts=6, wait_seconds=0.5)
            raise SandboxError(f"File download failed: path={remote_path}, exit_code={exit_code}.")

    @_PROVIDER_RETRY
    async def _start_download_process(self, remote_path: str) -> Any:
        try:
            return await self._sandbox.exec.aio("cat", "--", remote_path, text=False)
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def _set_outbound_network_policy(self, cidrs: list[str], domains: list[str]) -> None:
        try:
            set_policy = cast(Any, self._sandbox)._experimental_set_outbound_network_policy
            await cast(
                Awaitable[None],
                set_policy.aio(
                    outbound_cidr_allowlist=cidrs,
                    outbound_domain_allowlist=domains,
                ),
            )
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def _resolve_domain_cidrs(self, domains: list[str]) -> list[str]:
        domains_to_resolve = [domain for domain in domains if domain != "*" and not domain.startswith("*.")]
        if not domains_to_resolve:
            return []

        script = f"""
import socket

for domain in {domains_to_resolve!r}:
    try:
        infos = socket.getaddrinfo(domain, None, family=socket.AF_INET, type=socket.SOCK_STREAM)
    except OSError:
        continue
    for info in infos:
        print(info[4][0])
"""
        result = await self.exec(f"python -c {shlex.quote(script)}")
        if result.exit_code != 0:
            return []

        addresses = [f"{line.strip()}/32" for line in result.stdout.splitlines() if line.strip()]
        return list(dict.fromkeys(addresses))

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        cidrs, domains = resolve_allowed_addresses(allowed_addresses)
        cidrs = list(dict.fromkeys([*cidrs, *await self._resolve_domain_cidrs(domains)]))
        await self._set_outbound_network_policy(cidrs, domains)

    async def clear_egress_rules(self) -> None:
        await self._set_outbound_network_policy(list(_ALLOW_ALL_CIDRS), list(_ALLOW_ALL_DOMAINS))


class ModalSandboxProvider(SandboxProvider):
    def __init__(self, config: ModalProviderConfig) -> None:
        self._config = config
        self._client: Client | None = None
        self._app: App | None = None

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
            except ModalError as exc:
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
            except ModalError as exc:
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
            case _:
                raise SandboxError(f"Modal sandbox provider does not support source type: {source.type}")

    @_PROVIDER_RETRY
    async def _fetch_labels(self, sandbox: ModalSdkSandbox) -> dict[str, str]:
        try:
            return await cast(Any, sandbox.get_tags).aio()
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def _find_reusable_sandbox(self, name: str, client: Client) -> ModalSdkSandbox | None:
        # Match Daytona: task retry/resume reuses a still-running sandbox by
        # name (Modal names are unique per app, so a blind create would raise).
        try:
            inner = await ModalSdkSandbox.from_name.aio(
                _APP_NAME,
                name,
                client=client,
            )
        except ModalNotFoundError:
            return None
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        try:
            still_running = await inner.poll.aio() is None
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        return inner if still_running else None

    @_PROVIDER_RETRY
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        if request.sandbox_secrets:
            raise SandboxError("Modal sandbox provider does not support sandbox_secrets")

        client, app = await self._connect()
        modal_name = _modal_sandbox_name(request.name)
        existing = await self._find_reusable_sandbox(modal_name, client)
        if existing is not None:
            return ModalSandbox(existing, name=request.name, labels=request.labels)
        image = self._resolve_image(request.source, client)
        allow_all_egress = not request.network_block_all
        gpu: str | None = None
        if request.resources.gpu:
            if not request.resources.gpu_type:
                raise SandboxError("Modal sandbox provider requires gpu_type when gpu is requested")
            gpu = f"{request.resources.gpu_type}:{request.resources.gpu}"
        create_kwargs: dict[str, Any] = {
            "app": app,
            "name": modal_name,
            "image": image,
            "env": dict(request.env_vars),
            "tags": request.labels,
            "cpu": float(request.resources.vcpu),
            "memory": request.resources.memory * 1024,
            "gpu": gpu,
            "idle_timeout": request.auto_stop_interval * 60 if request.auto_stop_interval else None,
            "timeout": _MAX_LIFETIME_SECONDS,
            "block_network": request.network_block_all,
            "outbound_cidr_allowlist": list(_ALLOW_ALL_CIDRS) if allow_all_egress else None,
            "outbound_domain_allowlist": list(_ALLOW_ALL_DOMAINS) if allow_all_egress else None,
            "client": client,
            "experimental_options": (
                {"vm_runtime": True} if self._config.runtime == "vm" else {"enable_docker": True}
            ),
        }
        if request.volumes:
            create_kwargs["volumes"] = self._resolve_volumes(request.volumes, client, request.labels)

        try:
            # No entrypoint args: an argless Modal sandbox idles until timeout.
            inner = await asyncio.wait_for(
                ModalSdkSandbox.create.aio(**create_kwargs),  # pyright: ignore[reportUnknownMemberType]
                timeout=request.create_timeout,
            )
        except TimeoutError as exc:
            raise SandboxError(f"Failed to create Modal sandbox within {request.create_timeout}s") from exc
        except ModalNotFoundError as exc:
            if request.volumes and _is_missing_volume_error(exc):
                raise SandboxError(f"Modal volume mount failed: {exc}") from exc
            raise _sandbox_error(exc) from exc
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        return ModalSandbox(inner, name=request.name, labels=request.labels)

    @_PROVIDER_RETRY
    async def get_sandbox(self, instance_id: str) -> Sandbox:
        client, _ = await self._connect()
        try:
            inner = await ModalSdkSandbox.from_id.aio(instance_id, client=client)
            if await inner.poll.aio() is not None:
                raise SandboxNotFoundError(f"Sandbox not found: id={instance_id}.")
        except ModalInvalidError as exc:
            raise SandboxNotFoundError(f"Sandbox not found: id={instance_id}.") from exc
        except ModalError as exc:
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
        except ModalError as exc:
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
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def close(self) -> None:
        if self._client is not None:
            await self._client._close.aio()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]
            self._client = None
            self._app = None
