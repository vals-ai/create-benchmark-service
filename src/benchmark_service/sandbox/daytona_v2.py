"""Opt-in Daytona v2 provider (``api.daytona.com``) with fractional CPU.

Talks to the v2 REST API directly so the v0 SDK pin stays unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, field_validator

from benchmark_service.sandbox.types import (
    DockerfileSource,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
    SandboxBuildError,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    validate_command_env,
)

DEFAULT_DAYTONA_V2_API_URL = "https://api.daytona.com"
MIB_PER_GIB = 1024
VCPUS_PER_CORE = 2
_POLL_SECONDS = 1.0
_WAIT_MS = 30_000
_RUNNING = "running"
_FAILED_SANDBOX_STATUSES = frozenset({"failed", "lost", "stopped", "deleting", "deleted"})
_FAILED_IMAGE_STATUSES = frozenset({"failed", "error"})


class DaytonaV2ProviderConfig(BaseModel):
    type: Literal["daytona_v2"] = "daytona_v2"
    DAYTONA_V2_API_KEY: str
    DAYTONA_V2_API_URL: str = DEFAULT_DAYTONA_V2_API_URL
    DAYTONA_V2_REGION: str | None = Field(default=None, exclude_if=lambda value: value is None)
    DAYTONA_V2_SPOT: bool = False

    @field_validator("DAYTONA_V2_API_URL")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        return value.rstrip("/")

    @classmethod
    def from_env(cls) -> "DaytonaV2ProviderConfig":
        api_key = os.environ.get("DAYTONA_V2_API_KEY")
        if not api_key:
            raise MissingSandboxConfigError("Missing required environment variables: DAYTONA_V2_API_KEY")
        return cls(
            DAYTONA_V2_API_KEY=api_key,
            DAYTONA_V2_API_URL=os.environ.get("DAYTONA_V2_API_URL") or DEFAULT_DAYTONA_V2_API_URL,
            DAYTONA_V2_REGION=os.environ.get("DAYTONA_V2_REGION") or None,
            DAYTONA_V2_SPOT=(os.environ.get("DAYTONA_V2_SPOT") or "").lower() in ("1", "true", "yes"),
        )

    def create_provider(self) -> SandboxProvider:
        return DaytonaV2SandboxProvider(self)


def v2_resources(resources: Resources) -> dict[str, Any]:
    """Map CBS resources (logical vCPUs, GiB) to v2 create fields (cores, MiB); fractions pass through."""
    return {
        "cpu": resources.vcpu / VCPUS_PER_CORE,
        "memory_mib": resources.memory * MIB_PER_GIB,
        "disk_mib": resources.disk * MIB_PER_GIB,
    }


def _error(exc: httpx.HTTPError, what: str) -> SandboxError:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 404:
            return SandboxNotFoundError(f"{what}: not found")
        if status in (408, 429) or status >= 500:
            return SandboxConnectionError(f"{what}: Daytona v2 returned HTTP {status}")
        return SandboxError(f"{what}: Daytona v2 returned HTTP {status}: {exc.response.text[:500]}")
    return SandboxConnectionError(f"{what}: {exc}")


async def _send(client: httpx.AsyncClient, method: str, url: str, what: str, **kwargs: Any) -> httpx.Response:
    try:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise _error(exc, what) from exc
    return response


def _toolbox_url(sandbox: Mapping[str, Any]) -> str:
    sandbox_id = str(sandbox["id"])
    base = sandbox.get("toolbox_proxy_url")
    if not isinstance(base, str) or not base:
        raise SandboxError(f"Daytona v2 sandbox {sandbox_id} has no toolbox URL")
    base = base.rstrip("/")
    for placeholder in ("{sandbox_id}", "{sandboxId}"):
        if placeholder in base:
            return base.replace(placeholder, sandbox_id)
    return f"{base}/{sandbox_id}"


class DaytonaV2Sandbox(Sandbox):
    def __init__(self, data: Mapping[str, Any], client: httpx.AsyncClient) -> None:
        self._data = dict(data)
        self._client = client
        self.labels = self._data.get("labels")

    @property
    def id(self) -> str:
        return str(self._data["id"])

    @property
    def name(self) -> str:
        return str(self._data.get("name") or self.id)

    @property
    def state(self) -> str:
        return str(self._data.get("status") or "unknown")

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        return {key: str(self._data[key]) for key in ("region_id", "cpu", "memory_mib", "spot") if key in self._data}

    @property
    def _toolbox(self) -> str:
        return _toolbox_url(self._data)

    async def _start_process(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str] | None, wait_ms: int
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"kind": "exec", "shellCommand": command}
        if cwd:
            body["cwd"] = cwd
        if env:
            body["env"] = dict(env)
        if timeout is not None:
            body["timeout_ms"] = int(timeout * 1000)
        params = {"waitMs": wait_ms, "logs": "text"} if wait_ms else {}
        response = await _send(
            self._client, "POST", f"{self._toolbox}/processes", f"exec in {self.id}", json=body, params=params
        )
        return response.json()

    async def _process(self, process_id: str) -> dict[str, Any]:
        response = await _send(
            self._client,
            "GET",
            f"{self._toolbox}/processes/{quote(process_id, safe='')}",
            f"process {process_id} in {self.id}",
            params={"waitMs": _WAIT_MS},
        )
        return response.json()

    async def _log_frames(self, process_id: str, cursor: str | None) -> dict[str, Any]:
        params: dict[str, Any] = {"encoding": "text"}
        if cursor:
            params["cursor"] = cursor
        response = await _send(
            self._client,
            "GET",
            f"{self._toolbox}/processes/{quote(process_id, safe='')}/logs",
            f"logs of {process_id} in {self.id}",
            params=params,
        )
        return response.json()

    async def _stream(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str] | None
    ) -> AsyncGenerator[str | int, None]:
        """Yield output text, then the exit code as the final int."""
        process = await self._start_process(command, cwd, timeout, env, 0)
        process_id = str(process["id"])
        cursor: str | None = None
        while True:
            page = await self._log_frames(process_id, cursor)
            frames: list[dict[str, Any]] = page.get("frames") or []
            for frame in frames:
                if frame.get("channel") in ("stdout", "stderr", "pty"):
                    yield str(frame.get("data", ""))
            next_cursor = page.get("nextCursor")
            if page.get("eof"):
                break
            if not frames or next_cursor == cursor:
                await asyncio.sleep(_POLL_SECONDS)
            cursor = next_cursor or cursor
        while (exit_code := process.get("exitCode")) is None:
            process = await self._process(process_id)
        yield int(exit_code)

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        exit_code = -1
        async for item in self._stream(command, cwd, timeout, None):
            if isinstance(item, int):
                exit_code = item
            else:
                output.append(item)
        return ExecResult(exit_code=exit_code, output="".join(output))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars)
        async for item in self._stream(command, cwd, timeout, env):
            if isinstance(item, int):
                if item != 0:
                    raise SandboxCommandError(item)
            else:
                yield item

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await _send(
            self._client,
            "POST",
            f"{self._toolbox}/files/upload",
            f"upload {remote_path} to {self.id}",
            params={"path": remote_path},
            files={"file": (Path(remote_path).name or "file", content, "application/octet-stream")},
        )

    async def download_file(self, remote_path: str) -> bytes:
        response = await _send(
            self._client,
            "GET",
            f"{self._toolbox}/files/download",
            f"download {remote_path} from {self.id}",
            params={"path": remote_path},
        )
        return response.content


class DaytonaV2SandboxProvider(SandboxProvider):
    def __init__(self, config: DaytonaV2ProviderConfig, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._config = config
        self._client = httpx.AsyncClient(
            base_url=config.DAYTONA_V2_API_URL,
            transport=transport,
            headers={"Authorization": f"Bearer {config.DAYTONA_V2_API_KEY}"},
            timeout=httpx.Timeout(60.0, read=_WAIT_MS / 1000 + 30),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _image(self, source: SandboxSource, deadline: float) -> str:
        match source:
            case SnapshotSource(snapshot=snapshot):
                return snapshot
            case ImageSource(image=image):
                body: dict[str, Any] = {"source": {"type": "registry", "ref": image}}
            case DockerfileSource(dockerfile=dockerfile):
                body = {"dockerfile": Path(dockerfile).read_text()}
            case _:
                raise SandboxError(f"Daytona v2 provider does not support {type(source).__name__}")
        if self._config.DAYTONA_V2_REGION:
            body["region_id"] = self._config.DAYTONA_V2_REGION
        image = (await _send(self._client, "POST", "/images", "register image", json=body)).json()
        loop = asyncio.get_running_loop()
        while image.get("status") != "ready":
            if image.get("status") in _FAILED_IMAGE_STATUSES:
                raise SandboxBuildError(f"Daytona v2 image {image.get('id')} failed: {image.get('error')}")
            if loop.time() > deadline:
                raise SandboxBuildError(f"Daytona v2 image {image.get('id')} was not ready in time")
            await asyncio.sleep(_POLL_SECONDS)
            image = (
                await _send(self._client, "GET", f"/images/{quote(str(image['id']), safe='')}", "get image")
            ).json()
        return str(image["id"])

    def _create_body(self, request: SandboxCreateRequest, image: str) -> dict[str, Any]:
        unsupported = [
            name
            for name, used in (
                ("gpu", request.resources.gpu),
                ("volumes", request.volumes),
                ("sandbox_secrets", request.sandbox_secrets),
            )
            if used
        ]
        if unsupported:
            raise SandboxError(f"Daytona v2 provider does not support: {', '.join(unsupported)}")
        body: dict[str, Any] = {
            "image": image,
            "name": request.name,
            "labels": request.labels,
            "env": request.env_vars,
            **v2_resources(request.resources),
            "spot": self._config.DAYTONA_V2_SPOT,
            "create_timeout_seconds": request.create_timeout,
            "wait_seconds": 0,
        }
        if self._config.DAYTONA_V2_REGION:
            body["region_id"] = self._config.DAYTONA_V2_REGION
        if request.network_block_all:
            body["network_policy"] = {"block_all": True}
        if request.auto_stop_interval > 0:
            body["lifecycle_policy"] = {
                "idle_expiry_seconds": request.auto_stop_interval * 60,
                "on_idle_timeout": "delete",
            }
        return body

    async def create_sandbox(self, request: SandboxCreateRequest) -> DaytonaV2Sandbox:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + request.provider_timeout
        image = await self._image(request.source, deadline)
        data = (
            await _send(
                self._client,
                "POST",
                "/sandboxes",
                f"create {request.name}",
                json=self._create_body(request, image),
                headers={"Idempotency-Key": request.name},
            )
        ).json()
        while data.get("status") != _RUNNING:
            if data.get("status") in _FAILED_SANDBOX_STATUSES or loop.time() > deadline:
                await self.delete_sandbox(str(data["id"]))
                raise SandboxError(
                    f"Daytona v2 sandbox {data['id']} did not start: {data.get('status')} {data.get('error') or ''}".strip()
                )
            await asyncio.sleep(_POLL_SECONDS)
            data = await self._get(str(data["id"]))
        return DaytonaV2Sandbox(data, self._client)

    async def _get(self, instance_id: str) -> dict[str, Any]:
        response = await _send(
            self._client, "GET", f"/sandboxes/{quote(instance_id, safe='')}", f"get sandbox {instance_id}"
        )
        return response.json()

    async def get_sandbox(self, instance_id: str) -> DaytonaV2Sandbox:
        return DaytonaV2Sandbox(await self._get(instance_id), self._client)

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            await _send(
                self._client, "DELETE", f"/sandboxes/{quote(instance_id, safe='')}", f"delete sandbox {instance_id}"
            )
        except SandboxNotFoundError:
            return

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        if query.created_at_lte is not None:
            raise SandboxError("Daytona v2 provider does not filter sandboxes by creation time")
        params: dict[str, Any] = {"labels": json.dumps(query.labels), "limit": query.page_size}
        while True:
            page = (await _send(self._client, "GET", "/sandboxes/search", "search sandboxes", params=params)).json()
            items: list[dict[str, Any]] = page.get("sandboxes") or []
            for item in items:
                yield await self.get_sandbox(str(item["id"]))
            if not page.get("next_cursor"):
                return
            params["cursor"] = page["next_cursor"]
