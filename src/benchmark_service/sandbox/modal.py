from __future__ import annotations

import asyncio
import shlex
from collections.abc import AsyncGenerator, Awaitable
from typing import Any, Literal, cast

from modal import App, Client, Image
from modal import Sandbox as ModalSdkSandbox
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
)

# Modal sandboxes must belong to an app; all benchmark sandboxes share one.
_APP_NAME = "benchmark-service"
# Modal's default sandbox timeout is 5 minutes; benchmark tasks run for hours.
_MAX_LIFETIME_SECONDS = 24 * 60 * 60
_ALLOW_ALL_CIDRS = ("0.0.0.0/0",)
_ALLOW_ALL_DOMAINS = ("*",)


_PROVIDER_RETRY = retry(
    retry=retry_if_exception_type(SandboxConnectionError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)


class ModalProviderConfig(BaseModel):
    type: Literal["modal"] = "modal"
    MODAL_TOKEN_ID: str
    MODAL_TOKEN_SECRET: str

    def create_provider(self) -> SandboxProvider:
        return ModalSandboxProvider(self)


def _sandbox_error(exc: ModalError) -> SandboxError:
    if isinstance(exc, ModalNotFoundError):
        return SandboxNotFoundError(str(exc))
    if isinstance(exc, ModalConnectionError):
        return SandboxConnectionError(str(exc))
    return SandboxError(str(exc))


def _command(command: str, cwd: str | None, timeout: float | None) -> str:
    # Match Daytona semantics: timeout exits 124, stderr merges into stdout.
    if timeout is not None:
        command = f"timeout {timeout:g} {command}"
    if cwd:
        command = f"cd {shlex.quote(cwd)} && {command}"
    return f"{{ {command} ; }} 2>&1"


class ModalSandbox(Sandbox):
    def __init__(self, sandbox: ModalSdkSandbox, name: str | None = None) -> None:
        self._sandbox = sandbox
        self._name = name

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

    async def _raise_if_finished(self, *, attempts: int = 1, wait_seconds: float = 0) -> None:
        try:
            for attempt in range(attempts):
                if await self._sandbox.poll.aio() is not None:
                    raise SandboxNotFoundError(f"Sandbox not found: id={self.id}.")
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
    async def _start_process(self, command: str, *, cwd: str | None, timeout: float | None) -> Any:
        try:
            return await self._sandbox.exec.aio("/bin/sh", "-lc", _command(command, cwd, timeout), text=True)
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> AsyncGenerator[str, None]:
        await self._raise_if_finished()
        process = await self._start_process(command, cwd=cwd, timeout=timeout)

        try:
            async for chunk in process.stdout:
                yield str(chunk)
            exit_code = await process.wait.aio()
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

        if exit_code != 0:
            await self._raise_if_finished(attempts=6, wait_seconds=0.5)
            raise SandboxCommandError(exit_code)

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

    def _resolve_image(self, source: SandboxSource, client: Client) -> Image:
        # A snapshot is a Modal filesystem snapshot, referenced by its Image id.
        match source:
            case ImageSource(image=image):
                return Image.from_registry(image)  # pyright: ignore[reportUnknownMemberType]
            case SnapshotSource(snapshot=snapshot):
                return Image.from_id(snapshot, client=client)
            case _:
                raise SandboxError(f"Modal sandbox provider does not support source type: {source.type}")

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
        client, app = await self._connect()
        existing = await self._find_reusable_sandbox(request.name, client)
        if existing is not None:
            return ModalSandbox(existing, name=request.name)
        image = self._resolve_image(request.source, client)
        create_kwargs: dict[str, Any] = {
            "app": app,
            "name": request.name,
            "image": image,
            "env": dict(request.env_vars),
            "tags": request.labels,
            "cpu": float(request.resources.vcpu),
            "memory": request.resources.memory * 1024,
            "idle_timeout": request.auto_stop_interval * 60 if request.auto_stop_interval else None,
            "timeout": _MAX_LIFETIME_SECONDS,
            "block_network": False,
            "outbound_cidr_allowlist": list(_ALLOW_ALL_CIDRS),
            "outbound_domain_allowlist": list(_ALLOW_ALL_DOMAINS),
            "client": client,
            # Nested Docker always on, matching Daytona; no disk parameter exists.
            "experimental_options": {"enable_docker": True},
        }

        try:
            # No entrypoint args: an argless Modal sandbox idles until timeout.
            inner = await asyncio.wait_for(
                ModalSdkSandbox.create.aio(**create_kwargs),  # pyright: ignore[reportUnknownMemberType]
                timeout=request.create_timeout,
            )
        except TimeoutError as exc:
            raise SandboxError(f"Failed to create Modal sandbox within {request.create_timeout}s") from exc
        except ModalError as exc:
            raise _sandbox_error(exc) from exc
        return ModalSandbox(inner, name=request.name)

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
        return ModalSandbox(inner)

    @_PROVIDER_RETRY
    async def delete_sandbox(self, instance_id: str) -> None:
        client, _ = await self._connect()
        try:
            inner = await ModalSdkSandbox.from_id.aio(instance_id, client=client)
            await inner.terminate.aio()
        except ModalNotFoundError:
            return
        except ModalError as exc:
            raise _sandbox_error(exc) from exc

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        for inner in await self._list_sandboxes(query):
            yield ModalSandbox(inner)

    @_PROVIDER_RETRY
    async def _list_sandboxes(self, query: SandboxQuery) -> list[ModalSdkSandbox]:
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
