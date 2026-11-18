"""Opt-in sandbox provider on AWS Lambda MicroVMs (Firecracker).

Each sandbox is one MicroVM started from an image that runs ``agent.py``. The agent serves
exec and file routes on port 8080 behind the MicroVM's authenticated HTTPS endpoint;
labels and environment travel in the run-hook payload because MicroVMs carry no tags.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import AsyncGenerator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import BaseModel

from benchmark_service.sandbox.lambda_microvm.image import (
    DEFAULT_BASE_DOCKERFILE,
    aws_client,
    aws_error_code,
    baseline_memory_mib,
    ensure_image,
    network_connector,
)
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ExecResult,
    ImageSource,
    MissingSandboxConfigError,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SnapshotSource,
    UnsupportedSandboxConfigError,
    validate_command_env,
)

APP_PORT = 8080
MAX_DURATION_SECONDS = 8 * 60 * 60
MAX_DISK_GIB = 32
_TOKEN_MINUTES = 60
_TOKEN_REFRESH_SECONDS = 5 * 60
_POLL_SECONDS = 1.0
_ENDED_STATES = frozenset({"TERMINATING", "TERMINATED"})
_IMAGE_ARN_MARKER = ":microvm-image:"


class LambdaMicrovmProviderConfig(BaseModel):
    type: Literal["lambda_microvm"] = "lambda_microvm"
    LAMBDA_MICROVM_REGION: str = "us-east-1"
    LAMBDA_MICROVM_IMAGE: str | None = None
    LAMBDA_MICROVM_EXECUTION_ROLE_ARN: str | None = None
    LAMBDA_MICROVM_BUILD_BUCKET: str | None = None
    LAMBDA_MICROVM_BUILD_ROLE_ARN: str | None = None
    LAMBDA_MICROVM_ARCHITECTURE: Literal["ARM_64", "X86_64"] = "ARM_64"
    LAMBDA_MICROVM_MAX_DURATION_SECONDS: int = MAX_DURATION_SECONDS

    @classmethod
    def from_env(cls) -> "LambdaMicrovmProviderConfig":
        values = {name: os.environ[name] for name in cls.model_fields if name.isupper() and os.environ.get(name)}
        config = cls.model_validate(values)
        if not config.LAMBDA_MICROVM_IMAGE and not config.can_build:
            raise MissingSandboxConfigError(
                "Set LAMBDA_MICROVM_IMAGE, or LAMBDA_MICROVM_BUILD_BUCKET and LAMBDA_MICROVM_BUILD_ROLE_ARN"
            )
        return config

    @property
    def can_build(self) -> bool:
        return bool(self.LAMBDA_MICROVM_BUILD_BUCKET and self.LAMBDA_MICROVM_BUILD_ROLE_ARN)

    def create_provider(self) -> SandboxProvider:
        return LambdaMicrovmSandboxProvider(self)


def _aws_error(error: Exception, what: str) -> SandboxError:
    code = aws_error_code(error)
    if code in ("ResourceNotFoundException", "NotFoundException"):
        return SandboxNotFoundError(f"{what}: not found")
    if code in ("ThrottlingException", "ServiceQuotaExceededException", "ServiceException", "InternalServerException"):
        return SandboxConnectionError(f"{what}: {code}")
    return SandboxError(f"{what}: {error}")


class LambdaMicrovmSandbox(Sandbox):
    def __init__(self, provider: LambdaMicrovmSandboxProvider, data: Mapping[str, Any]) -> None:
        self._provider = provider
        self.data = dict(data)
        self._name = self.id
        started = self.data.get("startedAt")
        self.created_at = started.astimezone(UTC) if isinstance(started, datetime) else None

    @property
    def id(self) -> str:
        return str(self.data["microvmId"])

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> str:
        return str(self.data.get("state") or "unknown").lower()

    @property
    def has_ended(self) -> bool:
        return str(self.data.get("state")) in _ENDED_STATES

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        keys = ("imageArn", "imageVersion", "endpoint", "stateReason")
        return {key: str(self.data[key]) for key in keys if self.data.get(key)}

    def _apply_info(self, info: Mapping[str, Any]) -> None:
        self._name = str(info.get("name") or self.id)
        labels: Mapping[str, Any] = info.get("labels") or {}
        self.labels = {str(key): str(value) for key, value in labels.items()}

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        url, headers = await self._provider.endpoint(self)
        try:
            response = await self._provider.http.request(method, f"{url}{path}", headers=headers, **kwargs)
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"MicroVM {self.id} {path}: {error}") from error
        _raise_for_status(response, self.id, path)
        return response

    async def info(self) -> Mapping[str, Any]:
        info = (await self._request("GET", "/info")).json()
        self._apply_info(info)
        return info

    def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        return self._command(command, cwd=cwd, timeout=timeout, env=validate_command_env(env_vars))

    async def _command(
        self, command: str, *, cwd: str | None, timeout: float | None, env: dict[str, str]
    ) -> AsyncGenerator[str, None]:
        url, headers = await self._provider.endpoint(self)
        body = {"command": command, "cwd": cwd, "timeout": timeout, "env": env}
        exit_code: int | None = None
        try:
            async with self._provider.http.stream(
                "POST", f"{url}/exec", headers=headers, json=body, timeout=httpx.Timeout(30.0, read=None)
            ) as response:
                if response.status_code >= 400:
                    await response.aread()
                    _raise_for_status(response, self.id, "/exec")
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    event = json.loads(line)
                    if "output" in event:
                        yield str(event["output"])
                    elif "exit_code" in event:
                        exit_code = int(event["exit_code"])
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"MicroVM {self.id} /exec: {error}") from error
        if exit_code is None:
            raise SandboxConnectionError(f"MicroVM {self.id} /exec ended without an exit code")
        if exit_code != 0:
            raise SandboxCommandError(exit_code)

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output: list[str] = []
        try:
            async for chunk in self.command(command, cwd=cwd, timeout=timeout):
                output.append(chunk)
        except SandboxCommandError as error:
            return ExecResult(exit_code=error.exit_code, output="".join(output))
        return ExecResult(exit_code=0, output="".join(output))

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self._request("PUT", "/files", params={"path": remote_path}, content=content)

    async def download_file(self, remote_path: str) -> bytes:
        return (await self._request("GET", "/files", params={"path": remote_path})).content


def _raise_for_status(response: httpx.Response, sandbox_id: str, path: str) -> None:
    status = response.status_code
    if status < 400:
        return
    detail = response.text[:500]
    if status in (408, 429) or status >= 500:
        raise SandboxConnectionError(f"MicroVM {sandbox_id} {path}: HTTP {status} {detail}")
    raise SandboxError(f"MicroVM {sandbox_id} {path}: HTTP {status} {detail}")


class LambdaMicrovmSandboxProvider(SandboxProvider):
    def __init__(
        self,
        config: LambdaMicrovmProviderConfig,
        *,
        microvms: Any = None,
        s3: Any = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._microvms = microvms
        self._s3 = s3
        self.http = http or httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        self._tokens: dict[str, tuple[str, str, float]] = {}

    def _client(self, service: str) -> Any:
        return aws_client(service, self.config.LAMBDA_MICROVM_REGION)

    @property
    def microvms(self) -> Any:
        if self._microvms is None:
            self._microvms = self._client("lambda-microvms")
        return self._microvms

    async def _aws(self, what: str, operation: str, **kwargs: Any) -> Any:
        try:
            return await asyncio.to_thread(getattr(self.microvms, operation), **kwargs)
        except Exception as error:
            if isinstance(error, SandboxError):
                raise
            raise _aws_error(error, what) from error

    def validate_sandbox_config(self, *, requires_nested_docker: bool, resources: Resources) -> None:
        if requires_nested_docker:
            raise UnsupportedSandboxConfigError("The lambda_microvm provider does not run nested Docker yet")
        if resources.gpu:
            raise UnsupportedSandboxConfigError("Lambda MicroVMs have no GPUs")
        if resources.disk > MAX_DISK_GIB:
            raise UnsupportedSandboxConfigError(f"Lambda MicroVMs offer at most {MAX_DISK_GIB} GiB of disk")
        try:
            baseline_memory_mib(resources.vcpu, resources.memory)
        except SandboxError as error:
            raise UnsupportedSandboxConfigError(str(error)) from error

    async def _image_arn(self, request: SandboxCreateRequest) -> str:
        source = request.source
        if isinstance(source, (ImageSource, SnapshotSource)):
            ref = source.image if isinstance(source, ImageSource) else source.snapshot
            if _IMAGE_ARN_MARKER in ref or isinstance(source, SnapshotSource):
                return ref
            return await self._build(f"FROM {ref}\n", None, request)
        if isinstance(source, DockerfileSource):
            path = Path(source.dockerfile)
            return await self._build(path.read_text(), path.parent, request)
        raise UnsupportedSandboxConfigError(f"lambda_microvm does not support {source.type} sources")

    async def _build(self, dockerfile: str, context_dir: Path | None, request: SandboxCreateRequest) -> str:
        config = self.config
        if not (config.LAMBDA_MICROVM_BUILD_BUCKET and config.LAMBDA_MICROVM_BUILD_ROLE_ARN):
            raise MissingSandboxConfigError(
                "Building MicroVM images needs LAMBDA_MICROVM_BUILD_BUCKET and LAMBDA_MICROVM_BUILD_ROLE_ARN"
            )
        if self._s3 is None:
            self._s3 = self._client("s3")
        return await asyncio.to_thread(
            ensure_image,
            self.microvms,
            self._s3,
            dockerfile=dockerfile,
            context_dir=context_dir,
            memory_mib=baseline_memory_mib(request.resources.vcpu, request.resources.memory),
            bucket=config.LAMBDA_MICROVM_BUILD_BUCKET,
            build_role_arn=config.LAMBDA_MICROVM_BUILD_ROLE_ARN,
            architecture=config.LAMBDA_MICROVM_ARCHITECTURE,
            timeout=float(request.provider_timeout),
        )

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        self.validate_sandbox_config(requires_nested_docker=request.requires_nested_docker, resources=request.resources)
        if request.volumes:
            raise UnsupportedSandboxConfigError("lambda_microvm does not support volumes yet")
        if request.sandbox_secrets:
            raise UnsupportedSandboxConfigError("lambda_microvm does not support provider-managed secrets yet")

        image_arn = await self._image_arn(request)
        region = self.config.LAMBDA_MICROVM_REGION
        payload = {
            "name": request.name,
            "labels": request.labels,
            "env_vars": request.env_vars,
            "created_at": datetime.now(UTC).isoformat(),
        }
        idle_seconds = request.auto_stop_interval * 60 or self.config.LAMBDA_MICROVM_MAX_DURATION_SECONDS
        kwargs: dict[str, Any] = {
            "imageIdentifier": image_arn,
            "runHookPayload": json.dumps(payload),
            "maximumDurationInSeconds": self.config.LAMBDA_MICROVM_MAX_DURATION_SECONDS,
            "idlePolicy": {
                "maxIdleDurationSeconds": min(idle_seconds, self.config.LAMBDA_MICROVM_MAX_DURATION_SECONDS),
                "suspendedDurationSeconds": 0,
                "autoResumeEnabled": False,
            },
            "ingressNetworkConnectors": [network_connector(region, "ALL_INGRESS")],
            "egressNetworkConnectors": []
            if request.network_block_all
            else [network_connector(region, "INTERNET_EGRESS")],
            "clientToken": str(uuid.uuid4()),
        }
        if self.config.LAMBDA_MICROVM_EXECUTION_ROLE_ARN:
            kwargs["executionRoleArn"] = self.config.LAMBDA_MICROVM_EXECUTION_ROLE_ARN
        data = await self._aws("run MicroVM", "run_microvm", **kwargs)
        sandbox = LambdaMicrovmSandbox(self, data)
        try:
            await self._wait_ready(sandbox, request.create_timeout)
        except BaseException:
            await asyncio.shield(self._terminate_quietly(sandbox.id))
            raise
        return sandbox

    async def _wait_ready(self, sandbox: LambdaMicrovmSandbox, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while sandbox.state != "running":
            if sandbox.has_ended:
                raise SandboxError(f"MicroVM {sandbox.id} ended while starting: {sandbox.provider_metadata}")
            if time.monotonic() > deadline:
                raise SandboxError(f"MicroVM {sandbox.id} did not start within {timeout}s")
            await asyncio.sleep(_POLL_SECONDS)
            sandbox.data = await self._aws("get MicroVM", "get_microvm", microvmIdentifier=sandbox.id)
        while True:
            try:
                await sandbox.info()
                return
            except SandboxConnectionError:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(_POLL_SECONDS)

    async def _terminate_quietly(self, sandbox_id: str) -> None:
        try:
            await self.delete_sandbox(sandbox_id)
        except SandboxError:
            pass

    async def endpoint(self, sandbox: LambdaMicrovmSandbox) -> tuple[str, dict[str, str]]:
        cached = self._tokens.get(sandbox.id)
        if cached is None or cached[2] - time.monotonic() < _TOKEN_REFRESH_SECONDS:
            response = await self._aws(
                "create MicroVM auth token",
                "create_microvm_auth_token",
                microvmIdentifier=sandbox.id,
                expirationInMinutes=_TOKEN_MINUTES,
                allowedPorts=[{"port": APP_PORT}],
            )
            endpoint = str(sandbox.data.get("endpoint") or "")
            if not endpoint:
                raise SandboxError(f"MicroVM {sandbox.id} has no endpoint")
            if "://" not in endpoint:
                endpoint = f"https://{endpoint}"
            cached = (
                endpoint.rstrip("/"),
                response["authToken"]["X-aws-proxy-auth"],
                time.monotonic() + _TOKEN_MINUTES * 60,
            )
            self._tokens[sandbox.id] = cached
        return cached[0], {"X-aws-proxy-auth": cached[1], "X-aws-proxy-port": str(APP_PORT)}

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        data = await self._aws(f"get MicroVM {instance_id}", "get_microvm", microvmIdentifier=instance_id)
        sandbox = LambdaMicrovmSandbox(self, data)
        if sandbox.state == "running":
            try:
                await sandbox.info()
            except SandboxError:
                pass
        return sandbox

    async def delete_sandbox(self, instance_id: str) -> None:
        self._tokens.pop(instance_id, None)
        await self._aws(f"terminate MicroVM {instance_id}", "terminate_microvm", microvmIdentifier=instance_id)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"maxResults": max(query.page_size, 1)}
            if token:
                kwargs["nextToken"] = token
            page = await self._aws("list MicroVMs", "list_microvms", **kwargs)
            for item in page.get("items", []):
                if item.get("state") in _ENDED_STATES:
                    continue
                sandbox = await self.get_sandbox(str(item["microvmId"]))
                if query.created_at_lte and (sandbox.created_at is None or sandbox.created_at > query.created_at_lte):
                    continue
                labels = sandbox.labels or {}
                if all(labels.get(key) == value for key, value in query.labels.items()):
                    yield sandbox
            token = page.get("nextToken")
            if not token:
                return

    async def close(self) -> None:
        await self.http.aclose()


__all__ = [
    "DEFAULT_BASE_DOCKERFILE",
    "LambdaMicrovmProviderConfig",
    "LambdaMicrovmSandbox",
    "LambdaMicrovmSandboxProvider",
]
