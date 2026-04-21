from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path
from typing import Any, Self

from modal import App, Client, Image, Probe, Sandbox as ModalSdkSandbox, Secret
from modal.exception import Error as ModalError
from modal.exception import NotFoundError

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

_APP_NAME = "benchmark-service"
_KEEPALIVE = ("/bin/sh", "-lc", "while true; do sleep 3600; done")


class ModalSandbox(Sandbox):
    def __init__(self, provider: ModalSandboxProvider, inner: ModalSdkSandbox) -> None:
        super().__init__(provider=provider, id=inner.object_id, name=inner.object_id)
        self._inner = inner

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        try:
            process = await self._inner.exec.aio(
                "sh",
                "-lc",
                command,
                workdir=cwd,
                env=dict(env) if env else None,
                timeout=int(timeout) if timeout is not None else None,
                text=True,
            )
            stdout_task = asyncio.create_task(_read_stream(process.stdout, on_stdout))
            stderr_task = asyncio.create_task(_read_stream(process.stderr, on_stderr))
            exit_code = await process.wait.aio()
            return ExecResult(exit_code=exit_code, stdout=await stdout_task, stderr=await stderr_task)
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    async def upload_file(self, file: SandboxFile) -> None:
        try:
            await self._inner.filesystem.write_bytes.aio(file.content, file.remote_path)
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    async def upload_local_file(self, local_path: Path, remote_path: str) -> None:
        try:
            await self._inner.filesystem.copy_from_local.aio(local_path, remote_path)  # pyright: ignore[reportUnknownMemberType]
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    async def upload_files(self, files: list[SandboxFile]) -> None:
        for file in files:
            await self.upload_file(file)

    async def download_file(self, remote_path: str) -> bytes:
        try:
            return await self._inner.filesystem.read_bytes.aio(remote_path)
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    async def wait_until_ready(self) -> None:
        result = await self.exec("true")
        assert result.exit_code == 0

    async def wait_until_stopped(self) -> None:
        try:
            await self._inner.wait.aio(raise_on_termination=False)  # pyright: ignore[reportUnknownMemberType]
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc


class ModalSandboxProvider(SandboxProvider):
    provider_type = SandboxProviderType.MODAL

    def __init__(self, client: Client, app: App, registry_secret: Secret | None) -> None:
        self._client = client
        self._app = app
        self._registry_secret = registry_secret

    @classmethod
    async def from_headers(cls, headers: Mapping[str, str]) -> Self:
        token_id = headers.get("x-modal-token-id")
        token_secret = headers.get("x-modal-token-secret")
        client = (
            await Client.from_credentials.aio(token_id, token_secret)
            if token_id and token_secret
            else await Client.from_env.aio()  # pyright: ignore[reportUnknownMemberType]
        )

        registry_secret_name = headers.get("x-registry-secret-name")
        username = headers.get("x-registry-username")
        password = headers.get("x-registry-password")
        if registry_secret_name:
            registry_secret = Secret.from_name(
                registry_secret_name,
                required_keys=["REGISTRY_USERNAME", "REGISTRY_PASSWORD"],
                client=client,
            )
        elif username or password:
            assert username
            assert password
            registry_secret = Secret.from_dict({"REGISTRY_USERNAME": username, "REGISTRY_PASSWORD": password})
        else:
            registry_secret = None

        app = await App.lookup.aio(_APP_NAME, client=client, create_if_missing=True)
        return cls(client, app, registry_secret)

    async def create_sandbox(self, request: SandboxCreateRequest) -> ModalSandbox:
        match request:
            case ImageSandboxCreateRequest():
                image = Image.from_registry(request.image, secret=self._registry_secret)  # pyright: ignore[reportUnknownMemberType]
            case SnapshotSandboxCreateRequest():
                image = Image.from_id(request.snapshot, client=self._client)

        env_vars: dict[str, str | None] = dict(request.env_vars)
        env_vars.setdefault("DOCKER_STORAGE_DRIVER", "overlay2")
        env_vars.setdefault("DOCKER_EXTRA_ARGS", "--iptables=false --bridge=none --ip-forward=false --ip-masq=false")

        try:
            inner = await ModalSdkSandbox.create.aio(  # pyright: ignore[reportUnknownMemberType]
                *_KEEPALIVE,
                app=self._app,
                name=request.name,
                image=image,
                env=env_vars,
                cpu=float(request.resources.cpu) if request.resources else None,
                memory=request.resources.memory * 1024 if request.resources else None,
                idle_timeout=request.auto_delete_interval,
                block_network=request.network_blocked,
                timeout=request.creation_timeout,
                readiness_probe=Probe.with_exec("true"),
                experimental_options={"enable_docker": True},
                client=self._client,
            )
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc
        return ModalSandbox(self, inner)

    async def get_sandbox(self, id: str) -> ModalSandbox:
        try:
            if id.startswith("sb-"):
                inner = await ModalSdkSandbox.from_id.aio(id, client=self._client)
            else:
                inner = await ModalSdkSandbox.from_name.aio(_APP_NAME, id, client=self._client)
            return ModalSandbox(self, inner)
        except NotFoundError as exc:
            raise SandboxNotFoundError(str(exc)) from exc
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        try:
            inner = await ModalSdkSandbox.from_id.aio(sandbox.id, client=self._client)
            await inner.terminate.aio()
        except NotFoundError:
            return
        except ModalError as exc:
            raise SandboxError(str(exc)) from exc

    def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[ModalSandbox]:
        async def iter_sandboxes() -> AsyncIterator[ModalSandbox]:
            q = query or SandboxQuery()
            count = 0
            try:
                async for inner in ModalSdkSandbox.list.aio(
                    app_id=self._app.app_id, tags=q.labels or None, client=self._client
                ):
                    yield ModalSandbox(self, inner)
                    count += 1
                    if count >= q.limit:
                        return
            except ModalError as exc:
                raise SandboxError(str(exc)) from exc

        return iter_sandboxes()

    async def close(self) -> None:
        await self._client._close.aio()  # pyright: ignore[reportPrivateUsage, reportUnknownMemberType]


async def _read_stream(reader: Any, callback: Callable[[str], None] | None) -> str:
    chunks: list[str] = []
    async for chunk in reader:
        text = str(chunk)
        chunks.append(text)
        if callback:
            callback(text)
    return "".join(chunks)
