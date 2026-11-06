"""Sailboxes over HTTPS, with credentials scoped to each provider instance."""

from __future__ import annotations

import asyncio
import base64
import codecs
import json
import math
import os
import shlex
import uuid
from collections import deque
from collections.abc import AsyncGenerator, Mapping
from datetime import datetime
from typing import Any, Literal
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from benchmark_service.sandbox._process_group import (
    cleanup_command,
    episode_owner_command,
    owner_command,
    probe_command,
    stop_command,
)
from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import (
    LINUX_PROCESS_GROUP_V1,
    MAX_SANDBOX_LIFETIME_SECONDS,
    ControlledWorkload,
    ControlledWorkloadResult,
    ExecResult,
    GenerationContainment,
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
    validate_command_env,
)

_LABELS_ENV = "CBS_SANDBOX_LABELS"
_MANAGED_ENV = "CBS_SANDBOX_MANAGED"
_TERMINAL_STATES = {"failed", "terminating", "terminated"}
_TAIL_BYTES = 64 * 1024
_LABELS_ADAPTER = TypeAdapter(dict[str, str])


class SailProviderConfig(BaseModel):
    type: Literal["sail"] = "sail"
    SAIL_API_KEY: str = Field(min_length=1, repr=False)
    SAIL_APP: str = Field(default="vals-benchmark-service", min_length=1)

    @classmethod
    def from_env(cls) -> SailProviderConfig:
        key = os.environ.get("SAIL_API_KEY")
        if not key:
            raise MissingSandboxConfigError("Missing required environment variable: SAIL_API_KEY")
        return cls(SAIL_API_KEY=key, SAIL_APP=os.environ.get("SAIL_APP") or "vals-benchmark-service")

    def create_provider(self) -> SandboxProvider:
        return SailSandboxProvider(self)


def _check_response(response: httpx.Response) -> None:
    if response.is_success:
        return
    # Provider response bodies may echo commands, environment variables, or credentials.
    message = f"Sail API returned HTTP {response.status_code}"
    if response.status_code == 404:
        raise SandboxNotFoundError(message)
    if response.status_code == 429 or response.status_code >= 500:
        raise SandboxConnectionError(message)
    raise SandboxError(message)


class SailSandbox(Sandbox):
    def __init__(self, provider: SailSandboxProvider, info: Mapping[str, Any]) -> None:
        self._provider = provider
        self._info = dict(info)
        raw_labels = info.get("env", {}).get(_LABELS_ENV, "{}")
        try:
            self.labels = _LABELS_ADAPTER.validate_json(raw_labels, strict=True)
        except ValidationError:
            raise SandboxError("Sail sandbox has invalid framework labels") from None
        created = info.get("created_at")
        self.created_at = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else None

    @property
    def id(self) -> str:
        return str(self._info["sailbox_id"])

    @property
    def name(self) -> str:
        return str(self._info.get("name") or self.id)

    @property
    def state(self) -> str:
        return str(self._info["status"])

    @property
    def generation_containment(self) -> GenerationContainment:
        return LINUX_PROCESS_GROUP_V1

    @property
    def _path(self) -> str:
        return f"/sailboxes/{quote(self.id, safe='')}"

    async def probe_generation_containment(self) -> None:
        result = await self.exec(probe_command(f"/tmp/.cbs-probe-{uuid.uuid4().hex}"))
        if result.exit_code:
            raise SandboxError("Sail sandbox does not support process-group containment")

    def _process_group_workload(
        self, command: str, episode_script: str | None, *, cwd: str | None, env_vars: Mapping[str, str] | None
    ) -> ControlledWorkload:
        return _SailControlledWorkload(self, command, cwd, validate_command_env(env_vars), episode_script)

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise SandboxError("Sail command timeout must be positive and finite")
        if cwd is not None:
            command = f"cd {shlex.quote(cwd)} && {command}"
        payload: dict[str, Any] = {
            "command": ["/bin/sh", "-c", command],
            "env": validate_command_env(env_vars),
            "idempotency_key": uuid.uuid4().hex,
        }
        if timeout is not None:
            payload["timeout"] = math.ceil(timeout)
        decoders = {name: codecs.getincrementaldecoder("utf-8")("replace") for name in ("stdout", "stderr")}
        try:
            # Never replay an exec after losing its stream: it may already have changed the sandbox.
            async with self._provider.client.stream("POST", f"{self._path}/exec", json=payload) as response:
                _check_response(response)
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    event = json.loads(line)
                    kind = event.get("type")
                    if kind in decoders:
                        text = decoders[kind].decode(base64.b64decode(event["data"], validate=True))
                        if text:
                            yield text
                    elif kind == "error":
                        raise SandboxError("Sail command stream reported an error")
                    elif kind == "exit":
                        for decoder in decoders.values():
                            text = decoder.decode(b"", final=True)
                            if text:
                                yield text
                        code = event.get("return_code")
                        if not isinstance(code, int):
                            raise SandboxError("Sail command ended without an exit code")
                        if code:
                            raise SandboxCommandError(code)
                        return
        except httpx.TransportError as error:
            raise SandboxConnectionError("Sail command connection was interrupted") from error
        raise SandboxConnectionError("Sail command stream ended before its exit event")

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        parts: list[str] = []
        try:
            async for part in self.command(command, cwd=cwd, timeout=timeout):
                parts.append(part)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(parts))
        return ExecResult(exit_code=0, output="".join(parts))

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self._provider.request(
            "PUT",
            f"{self._path}/files",
            params={"path": remote_path},
            content=content,
            headers={"Content-Type": "application/octet-stream"},
        )

    async def download_file(self, remote_path: str) -> bytes:
        try:
            return (await self._provider.request("GET", f"{self._path}/files", params={"path": remote_path})).content
        except SandboxNotFoundError:
            await self._provider.get_sandbox(self.id)
            raise FileNotFoundError(remote_path) from None

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        try:
            async with self._provider.client.stream(
                "GET", f"{self._path}/files", params={"path": remote_path}
            ) as response:
                _check_response(response)
                async for chunk in response.aiter_bytes():
                    yield chunk
        except httpx.TransportError as error:
            raise SandboxConnectionError("Sail file download was interrupted") from error

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        if "*" in allowed_addresses:
            await self.clear_egress_rules()
            return
        cidrs, domains = resolve_allowed_addresses(allowed_addresses)
        await self._provider.request(
            "PUT", f"{self._path}/egress-policy", json={"document": {"allowlist": [*cidrs, *domains]}}
        )

    async def block_all_egress(self) -> None:
        await self._provider.request("PUT", f"{self._path}/egress-policy", json={"document": {"allowlist": []}})

    async def clear_egress_rules(self) -> None:
        await self._provider.request("PUT", f"{self._path}/egress-policy", json={"document": {}})


class SailSandboxProvider(SandboxProvider):
    def __init__(self, config: SailProviderConfig) -> None:
        self._config = config
        self.client = httpx.AsyncClient(
            base_url="https://sailbox-api.sailresearch.com/v1",
            headers={"Authorization": f"Bearer {config.SAIL_API_KEY}"},
            timeout=httpx.Timeout(60, read=300),
        )
        self._app_id: str | None = None
        self._app_lock = asyncio.Lock()
        self._cleanup: set[asyncio.Task[None]] = set()

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = await self.client.request(method, path, **kwargs)
        except httpx.TransportError as error:
            raise SandboxConnectionError("Sail API connection failed") from error
        _check_response(response)
        return response

    async def _app(self) -> str:
        async with self._app_lock:
            if self._app_id is None:
                response = await self.request(
                    "POST",
                    "https://api.sailresearch.com/v1/apps/find",
                    json={"name": self._config.SAIL_APP, "mint_if_missing": True},
                )
                self._app_id = str(response.json()["id"])
        return self._app_id

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        if not isinstance(request.source, ImageSource):
            raise SandboxError("Sail currently supports prebuilt image sources only")
        if request.resources.gpu or request.volumes or request.sandbox_secrets:
            raise SandboxError("Sail currently does not support GPUs, volume mounts, or provider-managed secrets")
        if request.resources.vcpu > 64 or request.resources.memory > 512 or request.resources.disk > 1024:
            raise SandboxError("Sail request exceeds 64 vCPUs, 512 GiB RAM, or 1024 GiB disk")
        if set(request.env_vars) & {_LABELS_ENV, _MANAGED_ENV}:
            raise SandboxError("Sail request overrides reserved framework environment variables")
        image = request.source.image
        if image == "sail:debian":
            image_spec: dict[str, Any] = {"base": "BASE_IMAGE_DEBIAN"}
        elif image == "sail:devbox":
            image_spec = {"base": "BASE_IMAGE_DEVBOX"}
        else:
            first = image.split("/", 1)[0]
            if "/" not in image or ("." not in first and ":" not in first):
                image = f"docker.io/{image}" if "/" in image else f"docker.io/library/{image}"
            if image.split("/", 1)[0] not in {"docker.io", "ghcr.io", "public.ecr.aws", "quay.io"}:
                raise SandboxError("Sail image must use a supported public registry and be prebuilt in Sail")
            image_spec = {"oci": {"ref": image}}
        env = {**request.env_vars, _MANAGED_ENV: "1", _LABELS_ENV: json.dumps(request.labels, separators=(",", ":"))}
        if len(env[_LABELS_ENV]) > 4096:
            raise SandboxError("Sail framework labels exceed the 4096-byte environment limit")
        payload: dict[str, Any] = {
            "app_id": await self._app(),
            "name": request.name,
            "image": image_spec,
            "vcpu_count": request.resources.vcpu,
            "memory_limit_gib": max(16, request.resources.memory),
            "state_disk_limit_gib": max(8, request.resources.disk),
            "max_lifetime_seconds": MAX_SANDBOX_LIFETIME_SECONDS,
            "env": env,
            "auto_sleep": {"automatic": False},
            "egress_policy": {"allowlist": []} if request.network_block_all else {},
        }
        creation = asyncio.create_task(
            self.request(
                "POST",
                "/sailboxes",
                json=payload,
                headers={"Idempotency-Key": uuid.uuid4().hex},
            )
        )
        try:
            async with asyncio.timeout(request.create_timeout):
                response = await asyncio.shield(creation)
        except (TimeoutError, asyncio.CancelledError) as error:
            cleanup = asyncio.create_task(self._cleanup_late_creation(creation))
            self._cleanup.add(cleanup)
            if isinstance(error, asyncio.CancelledError):
                raise
            raise SandboxError("Sail sandbox creation timed out; late allocation cleanup is pending") from error
        info = response.json()
        sandbox_id = str(info["sailbox_id"])
        if info["status"] != "running":
            await self.delete_sandbox(sandbox_id)
            raise SandboxError("Sail sandbox failed to start")
        return SailSandbox(self, {**info, "name": request.name, "env": env})

    async def _cleanup_late_creation(self, creation: asyncio.Task[httpx.Response]) -> None:
        response = await creation
        await self.delete_sandbox(str(response.json()["sailbox_id"]))

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        info = (await self.request("GET", f"/sailboxes/{quote(instance_id, safe='')}")).json()
        if info["status"] in _TERMINAL_STATES:
            raise SandboxNotFoundError(f"Sail sandbox is {info['status']}")
        return SailSandbox(self, info)

    async def delete_sandbox(self, instance_id: str) -> None:
        try:
            await self.request("POST", f"/sailboxes/{quote(instance_id, safe='')}/terminate")
        except SandboxNotFoundError:
            pass

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        offset = 0
        while True:
            page = (
                await self.request(
                    "GET",
                    "/sailboxes",
                    params={
                        "app": await self._app(),
                        "limit": min(100, max(1, query.page_size)),
                        "offset": offset,
                        "exclude_status": ",".join(sorted(_TERMINAL_STATES)),
                    },
                )
            ).json()
            rows = page["data"]
            for info in rows:
                if info.get("env", {}).get(_MANAGED_ENV) != "1":
                    continue
                sandbox = SailSandbox(self, info)
                if any((sandbox.labels or {}).get(k) != v for k, v in query.labels.items()):
                    continue
                if query.created_at_lte is not None:
                    if sandbox.created_at is None or sandbox.created_at > query.created_at_lte:
                        continue
                yield sandbox
            if not page.get("has_more"):
                return
            if not rows:
                raise SandboxError("Sail returned an empty page with has_more=true")
            offset += len(rows)

    async def close(self) -> None:
        try:
            if self._cleanup:
                await asyncio.gather(*tuple(self._cleanup))
        finally:
            await self.client.aclose()


class _SailControlledWorkload(ControlledWorkload):
    def __init__(
        self,
        sandbox: SailSandbox,
        command: str,
        cwd: str | None,
        env_vars: dict[str, str],
        episode_script: str | None,
    ) -> None:
        self._sandbox = sandbox
        self._marker = f"/tmp/.cbs-controlled-{uuid.uuid4().hex}"
        self._episode = episode_script is not None
        command = f"cd {shlex.quote(cwd)} && {command}" if cwd else command
        owner = (
            episode_owner_command(command, self._marker, episode_script)
            if episode_script
            else owner_command(
                command,
                self._marker,
                "sh -c",
            )
        )
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._changed = asyncio.Event()
        self._tail: deque[bytes] = deque()
        self._tail_size = 0
        self._absent_at: float | None = None
        self._stop_task: asyncio.Task[tuple[float, int | None]] | None = None
        self._stream = asyncio.create_task(self._read(f"exec 2>&1; {owner}", env_vars))
        self._result = asyncio.create_task(self._finish())

    @property
    def agent_absence_confirmed_at(self) -> float | None:
        return self._absent_at

    async def _read(self, command: str, env: dict[str, str]) -> int:
        try:
            async for chunk in self._sandbox.command(command, env_vars=env):
                self._queue.put_nowait(chunk)
                encoded = chunk.encode()
                self._tail.append(encoded)
                self._tail_size += len(encoded)
                while self._tail_size > _TAIL_BYTES:
                    first = self._tail.popleft()
                    excess = self._tail_size - _TAIL_BYTES
                    self._tail_size -= len(first)
                    if len(first) > excess:
                        self._tail.appendleft(first[excess:])
                        self._tail_size += len(first) - excess
                self._changed.set()
        except SandboxCommandError as error:
            return error.exit_code
        finally:
            self._changed.set()
        return 0

    async def output(self) -> AsyncGenerator[str, None]:
        while True:
            if not self._queue.empty():
                yield self._queue.get_nowait()
            elif self._stream.done():
                await self._stream
                return
            else:
                self._changed.clear()
                await self._changed.wait()

    async def _stop(self) -> tuple[float, int | None]:
        while True:
            result = await self._sandbox.exec(stop_command(self._marker, episode_owner=self._episode))
            if result.exit_code == 0:
                self._absent_at = asyncio.get_running_loop().time()
                return self._absent_at, int(result.output.strip()) if result.output.strip() else None
            if result.exit_code != 75:
                raise SandboxError("Sail process-group termination could not be confirmed")
            if self._stream.done():
                await self._stream
                raise SandboxError("Sail workload ended without a process-group marker")
            await asyncio.sleep(0.05)

    async def _ensure_stopped(self) -> tuple[float, int | None]:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        return await asyncio.shield(self._stop_task)

    async def _finish(self) -> ControlledWorkloadResult:
        exit_code = await self._stream
        stopped_at, status = await self._ensure_stopped()
        if status is None and exit_code == 0:
            raise SandboxError("Sail workload ended without a foreground exit status")
        cleanup = await self._sandbox.exec(cleanup_command(self._marker))
        if cleanup.exit_code:
            raise SandboxError("Sail process-group control cleanup failed")
        return ControlledWorkloadResult(
            ExecResult(
                exit_code=status if status is not None else exit_code,
                output=b"".join(self._tail).decode("utf-8", errors="replace"),
            ),
            stopped_at,
        )

    async def wait(self) -> ControlledWorkloadResult:
        return await asyncio.shield(self._result)

    async def kill(self) -> None:
        await self._ensure_stopped()
