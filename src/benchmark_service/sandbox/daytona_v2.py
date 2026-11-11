"""Opt-in Daytona v2 provider (``api.daytona.com``) with fractional CPU.

Talks to the v2 REST API directly so the v0 SDK pin stays unchanged.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import signal
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import aclosing, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, field_validator

from benchmark_service.sandbox.created import notify_sandbox_created
from benchmark_service.sandbox.egress import resolve_allowed_addresses
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
_KILL_AFTER_MS = 5_000
_RUNNING = "running"
_FAILED_SANDBOX_STATUSES = frozenset({"failed", "lost", "stopped", "deleting", "deleted"})
_FAILED_IMAGE_STATUSES = frozenset({"failed", "error"})
_DELETED_SANDBOX_STATUSES = frozenset({"deleting", "deleted"})
_TIMEOUT_EXIT_CODE = 124
_POLICY_TIMEOUT_SECONDS = 60.0


class DaytonaV2ProviderConfig(BaseModel):
    type: Literal["daytona_v2"] = "daytona_v2"
    prices_sandboxes: ClassVar[bool] = True
    DAYTONA_V2_API_KEY: str
    DAYTONA_V2_API_URL: str = DEFAULT_DAYTONA_V2_API_URL
    DAYTONA_V2_REGION: str | None = Field(default=None, exclude_if=lambda value: value is None)
    DAYTONA_V2_SPOT: bool = False

    @field_validator("DAYTONA_V2_API_URL")
    @classmethod
    def _normalize_api_url(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError("DAYTONA_V2_API_URL must use https")
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
    """Map CBS resources (logical vCPUs, GiB) to v2 create fields (cores, MiB); fractions pass through.

    v2 boots a sandbox at the plan cap unless told otherwise, so memory is capped at the
    request and CPU at whole vCPUs, the `cores_max` step (0.5 cores), rounded up.
    """
    cores = resources.vcpu / VCPUS_PER_CORE
    memory_mib = resources.memory * MIB_PER_GIB
    return {
        "cpu": cores,
        "cores_max": math.ceil(resources.vcpu) / VCPUS_PER_CORE,
        "memory_mib": memory_mib,
        "memory_max_mib": memory_mib,
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


def _exit_code(process: Mapping[str, Any]) -> int | None:
    """The process's exit code; a timeout exits 124 like the other providers, a signal 128 + its number."""
    if process.get("exitCode") is not None:
        return int(process["exitCode"])
    if process.get("state") != "terminal":
        return None
    if process.get("reason") == "timed_out":
        return _TIMEOUT_EXIT_CODE
    name = str(process.get("signal") or "")
    signum = getattr(signal.Signals, name if name.startswith("SIG") else f"SIG{name}", None)
    return 128 + signum if signum is not None else -1


def _created_at(sandbox: Mapping[str, Any]) -> datetime | None:
    value = sandbox.get("created_at")
    if not isinstance(value, str):
        return None
    created_at = datetime.fromisoformat(value)
    return created_at.astimezone(UTC) if created_at.utcoffset() is not None else None


# On-demand AMD Zen 5 rates from the https://www.daytona.com/pricing calculator, billed per second while
# running. Ephemeral disk and the Linux licence are $0. The region multiplier is taken as 1.0: Daytona
# documents earth at 1.0, and the default region's multiplier needs an org key with read:billing to read.
ZEN5_CORE_USD_PER_HOUR = 0.235
MEMORY_GIB_USD_PER_HOUR = 0.0162
PREEMPTIBLE_MULTIPLIER = 0.7
_ENDED_STATUSES = frozenset({"stopped", "deleting", "deleted", "failed", "lost"})


class DaytonaV2Sandbox(Sandbox):
    def __init__(self, data: Mapping[str, Any], client: httpx.AsyncClient) -> None:
        self._data = dict(data)
        self._client = client
        self.labels = self._data.get("labels")
        self.created_at = _created_at(self._data)

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
    def has_ended(self) -> bool:
        return self.state in _ENDED_STATUSES

    def cost_usd(self, ended_at: datetime) -> float | None:
        cpu, memory_mib = self._data.get("cpu"), self._data.get("memory_mib")
        if self.created_at is None or cpu is None or memory_mib is None:
            return None
        per_hour = float(cpu) * ZEN5_CORE_USD_PER_HOUR + float(memory_mib) / 1024 * MEMORY_GIB_USD_PER_HOUR
        if self._data.get("spot") is True:
            per_hour *= PREEMPTIBLE_MULTIPLIER
        return per_hour * max((ended_at - self.created_at).total_seconds(), 0) / 3600

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
            body["timeoutMs"] = math.ceil(timeout * 1000)
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
        exit_code: int | None = None
        try:
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
            while (exit_code := _exit_code(process)) is None:
                process = await self._process(process_id)
            yield exit_code
        finally:
            if exit_code is None:
                await self._stop_process(process_id)

    async def _stop_process(self, process_id: str) -> None:
        """SIGTERM a process its caller cancelled or closed, escalating to SIGKILL."""
        with suppress(SandboxError):
            await _send(
                self._client,
                "POST",
                f"{self._toolbox}/processes/{quote(process_id, safe='')}/signals",
                f"stop {process_id} in {self.id}",
                json={"signal": "SIGTERM", "escalateAfterMs": _KILL_AFTER_MS, "escalateTo": "SIGKILL"},
            )

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        exit_code = -1
        async with aclosing(self._stream(command, cwd, timeout, None)) as stream:
            async for item in stream:
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
        async with aclosing(self._stream(command, cwd, timeout, env)) as stream:
            async for item in stream:
                if isinstance(item, int):
                    if item != 0:
                        raise SandboxCommandError(item)
                else:
                    yield item

    async def _set_network_policy(self, rules: Mapping[str, Any]) -> None:
        """Replace the sandbox's egress rules and wait until Daytona reports them applied."""
        url = f"/sandboxes/{quote(self.id, safe='')}/network-policy"
        what = f"network policy of {self.id}"
        current = await _send(self._client, "GET", url, what)
        policy = (
            await _send(self._client, "PUT", url, what, json=rules, headers={"If-Match": current.headers["etag"]})
        ).json()
        revision = policy["desired_revision"]
        deadline = asyncio.get_running_loop().time() + _POLICY_TIMEOUT_SECONDS
        while policy.get("status") != "applied" or (policy.get("applied_revision") or 0) < revision:
            if policy.get("error"):
                raise SandboxError(f"Daytona v2 could not apply the {what}: {policy['error']}")
            if asyncio.get_running_loop().time() > deadline:
                raise SandboxConnectionError(f"Daytona v2 did not apply the {what} in time")
            await asyncio.sleep(_POLL_SECONDS)
            policy = (await _send(self._client, "GET", url, what)).json()

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        cidrs, domains = resolve_allowed_addresses(allowed_addresses)
        await self._set_network_policy({"block_all": True, "allow_cidrs": cidrs, "allow_domains": domains or None})

    async def block_all_egress(self) -> None:
        await self._set_network_policy({"block_all": True, "allow_cidrs": [], "allow_domains": None})

    async def clear_egress_rules(self) -> None:
        await self._set_network_policy({"block_all": False, "allow_cidrs": [], "allow_domains": None})

    @asynccontextmanager
    async def _missing_path_is_error(self, remote_path: str) -> AsyncIterator[None]:
        """Raise SandboxNotFoundError on a toolbox 404 only if the sandbox is gone, so recovery skips missing files."""
        try:
            yield
        except SandboxNotFoundError as exc:
            await _send(self._client, "GET", f"/sandboxes/{quote(self.id, safe='')}", f"get sandbox {self.id}")
            raise SandboxError(f"{remote_path} not found in sandbox {self.id}") from exc

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        async with self._missing_path_is_error(remote_path):
            await _send(
                self._client,
                "POST",
                f"{self._toolbox}/files/upload",
                f"upload {remote_path} to {self.id}",
                params={"path": remote_path},
                files={"file": (Path(remote_path).name or "file", content, "application/octet-stream")},
            )

    async def download_file(self, remote_path: str) -> bytes:
        async with self._missing_path_is_error(remote_path):
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

    def _reject_unsupported(self, request: SandboxCreateRequest) -> None:
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

    def _create_body(self, request: SandboxCreateRequest, image: str) -> dict[str, Any]:
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
        self._reject_unsupported(request)
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
        sandbox_id = str(data["id"])
        try:
            await notify_sandbox_created("daytona_v2", sandbox_id)
            while data.get("status") != _RUNNING:
                if data.get("status") in _FAILED_SANDBOX_STATUSES or loop.time() > deadline:
                    detail = f"{data.get('status')} {data.get('error') or ''}".strip()
                    raise SandboxError(f"Daytona v2 sandbox {sandbox_id} did not start: {detail}")
                await asyncio.sleep(_POLL_SECONDS)
                data = await self._get(sandbox_id)
        except BaseException:
            with suppress(SandboxError):
                await self.delete_sandbox(sandbox_id)
            raise
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
        params: dict[str, Any] = {"labels": json.dumps(query.labels), "limit": query.page_size}
        if query.created_at_lte is not None:
            # The search end bound is exclusive.
            params["created_end_at"] = (query.created_at_lte + timedelta(microseconds=1)).isoformat()
        while True:
            page = (await _send(self._client, "GET", "/sandboxes/search", "search sandboxes", params=params)).json()
            items: list[dict[str, Any]] = page.get("sandboxes") or []
            for item in items:
                sandbox = await self.get_sandbox(str(item["id"]))
                if sandbox.state not in _DELETED_SANDBOX_STATUSES:
                    yield sandbox
            if not page.get("next_cursor"):
                return
            params["cursor"] = page["next_cursor"]
