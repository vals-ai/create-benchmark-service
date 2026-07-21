from __future__ import annotations

import asyncio
import codecs
import json
import shlex
import time
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, TypeVar, cast

from benchmark_service.sandbox.kubernetes.control.api import (
    KubernetesApi,
    KubernetesApiError,
)
from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.control.egress import EgressPolicyDriver
from benchmark_service.sandbox.kubernetes.control.resources import (
    FINGERPRINT_ANNOTATION,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    AUTO_STOP_ANNOTATION,
    LAST_ACTIVITY_ANNOTATION,
    ORIGINAL_NAME_ANNOTATION,
    SANDBOX_ID_LABEL,
    build_ingress_policy,
    build_job,
    safe_label_value,
    sandbox_name,
    user_label_key,
)
from benchmark_service.sandbox.kubernetes.control.remote_exec import (
    RemoteExec,
    RemoteExecSession,
    decode_base64_chunks,
    encode_base64_chunks,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandEvent,
    CommandExitEvent,
    CommandOutputEvent,
    CommandRequest,
    ExecResponse,
    SandboxListPage,
    SandboxRecord,
)
from benchmark_service.sandbox.types import (
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    validate_command_env,
)

Result = TypeVar("Result")


def _dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _list(value: object) -> list[Any]:
    return cast(list[Any], value) if isinstance(value, list) else []


def _metadata(resource: dict[str, object]) -> dict[str, Any]:
    return _dict(resource.get("metadata"))


def _annotations(resource: dict[str, object]) -> dict[str, str]:
    return cast(dict[str, str], _metadata(resource).get("annotations", {}))


def _job_state(job: dict[str, object], pods: list[dict[str, object]]) -> str:
    status = _dict(job.get("status"))
    if status.get("failed") or any(
        _dict(condition).get("type") == "Failed" and _dict(condition).get("status") == "True"
        for condition in _list(status.get("conditions"))
    ):
        return "failed"
    if status.get("succeeded") or status.get("completionTime"):
        return "stopped"
    for pod in pods:
        pod_status = _dict(pod.get("status"))
        if pod_status.get("phase") == "Failed":
            return "failed"
        if pod_status.get("phase") == "Running" and any(
            _dict(condition).get("type") == "Ready" and _dict(condition).get("status") == "True"
            for condition in _list(pod_status.get("conditions"))
        ):
            return "running"
    return "pending"


def _pending_failure(job: dict[str, object], pods: list[dict[str, object]]) -> str | None:
    summary = json.dumps({"job": job.get("status"), "pods": [pod.get("status") for pod in pods]}).lower()
    if "imagepullbackoff" in summary or "errimagepull" in summary:
        return "Sandbox image pull failed"
    if "unschedulable" in summary:
        return "Sandbox could not be scheduled"
    if "quota" in summary or "failedcreate" in summary:
        return "Sandbox creation was rejected by cluster quota or policy"
    return None


class KubernetesSandboxBackend:
    """Reconcile EKS Jobs behind the private control-service protocol."""

    def __init__(
        self,
        settings: KubernetesControlSettings,
        api: KubernetesApi,
        remote_exec: RemoteExec | None = None,
        egress_driver: EgressPolicyDriver | None = None,
        *,
        wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.api = api
        self.remote_exec = remote_exec
        self.egress_driver = egress_driver
        self._wait = wait
        self._monotonic = monotonic
        self._now = now

    def _record(self, job: dict[str, object], state: str) -> SandboxRecord:
        metadata = _metadata(job)
        annotations = _annotations(job)
        name = str(metadata.get("name", ""))
        return SandboxRecord(
            id=name,
            name=annotations.get(ORIGINAL_NAME_ANNOTATION, name),
            state=state,
        )

    def _map_error(self, error: KubernetesApiError) -> SandboxError:
        if error.status in {401, 403}:
            return SandboxError("Kubernetes API access denied for the sandbox namespace")
        if error.status == 404:
            return SandboxNotFoundError("Sandbox resource was not found")
        if error.status == 429 or error.status >= 500 or error.status == 0:
            return SandboxConnectionError(str(error))
        return SandboxError(str(error))

    async def _call(self, operation: Callable[[], Awaitable[Result]]) -> Result:
        for attempt in range(3):
            try:
                return await operation()
            except KubernetesApiError as error:
                retryable = error.status == 429 or error.status >= 500 or error.status == 0
                if not retryable or attempt == 2:
                    raise self._map_error(error) from error
                await self._wait(0.25 * (2**attempt))
        raise SandboxConnectionError("Kubernetes API retry attempts exhausted")

    async def _pods(self, resource_name: str) -> list[dict[str, object]]:
        return await self._call(
            lambda: self.api.list_pods(
                self.settings.namespace,
                f"{SANDBOX_ID_LABEL}={resource_name}",
            )
        )

    async def _wait_ready(
        self,
        resource_name: str,
        timeout: float,
    ) -> SandboxRecord:
        deadline = self._monotonic() + timeout
        while True:
            job = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))
            if job is None:
                raise SandboxNotFoundError(f"Sandbox disappeared during creation: {resource_name}")
            pods = await self._pods(resource_name)
            failure = _pending_failure(job, pods)
            if failure:
                raise SandboxError(failure)
            state = _job_state(job, pods)
            if state == "running":
                return self._record(job, state)
            if state in {"failed", "stopped"}:
                raise SandboxError(f"Sandbox entered {state} state before becoming ready")
            if self._monotonic() >= deadline:
                raise SandboxConnectionError(f"Timed out waiting for sandbox readiness: {resource_name}")
            await self._wait(0.5)

    async def create_sandbox(self, request: SandboxCreateRequest) -> SandboxRecord:
        resource_name = sandbox_name(request.name)
        job_body = build_job(request, self.settings)
        expected_fingerprint = _annotations(job_body)[FINGERPRINT_ANNOTATION]
        existing = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))

        if existing is not None:
            if _annotations(existing).get(FINGERPRINT_ANNOTATION) != expected_fingerprint:
                raise SandboxConflictError(f"Sandbox {request.name} already exists with a conflicting specification")
            await self._call(
                lambda: self.api.create_network_policy(
                    self.settings.namespace,
                    build_ingress_policy(resource_name, self.settings.namespace),
                )
            )
            await self._touch(resource_name)
            return await self._wait_ready(resource_name, request.create_timeout)

        ingress_created = False
        job_created = False
        ready = False
        try:
            await self._call(
                lambda: self.api.create_network_policy(
                    self.settings.namespace,
                    build_ingress_policy(resource_name, self.settings.namespace),
                )
            )
            ingress_created = True
            await self._call(lambda: self.api.create_job(self.settings.namespace, job_body))
            job_created = True
            record = await self._wait_ready(resource_name, request.create_timeout)
            ready = True
            return record
        finally:
            if not ready and job_created:
                try:
                    await self._call(lambda: self.api.delete_job(self.settings.namespace, resource_name))
                except SandboxError:
                    pass
            if not ready and ingress_created:
                try:
                    await self._call(
                        lambda: self.api.delete_network_policy(
                            self.settings.namespace,
                            f"{resource_name}-ingress",
                        )
                    )
                except SandboxError:
                    pass

    async def get_sandbox(self, instance_id: str) -> SandboxRecord:
        resource_name = sandbox_name(instance_id)
        job = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))
        if job is None:
            raise SandboxNotFoundError(f"Sandbox not found: {instance_id}")
        await self._touch(resource_name)
        return self._record(job, _job_state(job, await self._pods(resource_name)))

    async def list_sandboxes(
        self,
        labels: dict[str, str],
        limit: int,
        continue_token: str | None,
    ) -> SandboxListPage:
        selectors = [f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"]
        selectors.extend(f"{user_label_key(name)}={safe_label_value(value)}" for name, value in sorted(labels.items()))
        result = await self._call(
            lambda: self.api.list_jobs(
                self.settings.namespace,
                ",".join(selectors),
                limit,
                continue_token,
            )
        )
        items: list[SandboxRecord] = []
        for job in cast(list[dict[str, object]], result.get("items", [])):
            resource_name = str(_metadata(job).get("name", ""))
            items.append(self._record(job, _job_state(job, await self._pods(resource_name))))
        metadata = _dict(result.get("metadata"))
        return SandboxListPage(items=items, continue_token=metadata.get("continue"))

    async def delete_sandbox(self, instance_id: str) -> None:
        resource_name = sandbox_name(instance_id)
        operations: list[Callable[[], Awaitable[None]]] = [
            lambda: self.api.delete_custom_object(
                self.settings.namespace,
                "ciliumnetworkpolicies",
                f"{resource_name}-egress",
            ),
            lambda: self.api.delete_job(self.settings.namespace, resource_name),
            lambda: self.api.delete_network_policy(self.settings.namespace, f"{resource_name}-ingress"),
        ]
        for operation in operations:
            try:
                await self._call(operation)
            except SandboxNotFoundError:
                pass

    def _remote_exec(self) -> RemoteExec:
        if self.remote_exec is None:
            raise SandboxError("Kubernetes remote exec is not configured")
        return self.remote_exec

    def _egress_driver(self) -> EgressPolicyDriver:
        if self.egress_driver is None:
            raise SandboxError("Kubernetes egress policy driver is not configured")
        return self.egress_driver

    async def _touch(self, instance_id: str) -> None:
        resource_name = sandbox_name(instance_id)
        await self._call(
            lambda: self.api.patch_job(
                self.settings.namespace,
                resource_name,
                {"metadata": {"annotations": {LAST_ACTIVITY_ANNOTATION: self._now().isoformat()}}},
            )
        )

    async def _ready_pod_name(self, instance_id: str) -> str:
        resource_name = sandbox_name(instance_id)
        pods = await self._pods(resource_name)
        for pod in pods:
            pod_status = _dict(pod.get("status"))
            ready = pod_status.get("phase") == "Running" and any(
                _dict(condition).get("type") == "Ready" and _dict(condition).get("status") == "True"
                for condition in _list(pod_status.get("conditions"))
            )
            if ready:
                name = _metadata(pod).get("name")
                if isinstance(name, str) and name:
                    return name
        raise SandboxConnectionError(f"Sandbox does not have a ready Pod: {instance_id}")

    def _shell_command(self, request: CommandRequest, command_id: str) -> str:
        env = validate_command_env(request.env_vars)
        command = request.command
        if request.timeout is not None:
            command = f"timeout {request.timeout:g} sh -lc {shlex.quote(command)}"
        if env:
            assignments = " ".join(f"{name}={shlex.quote(value)}" for name, value in sorted(env.items()))
            command = f"env {assignments} sh -lc {shlex.quote(command)}"
        if request.cwd:
            command = f"cd {shlex.quote(request.cwd)} && {command}"
        return (
            f"SANDBOX_COMMAND_ID={shlex.quote(command_id)}; export SANDBOX_COMMAND_ID; "
            "trap 'pkill -TERM -P $$ 2>/dev/null || true' TERM INT; "
            f"{command}"
        )

    async def _stream_session(
        self,
        session: RemoteExecSession,
    ) -> AsyncGenerator[CommandEvent, None]:
        stdout_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        stderr_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        while session.is_open():
            await session.update(0.1)
            stdout = await session.read_stdout()
            stderr = await session.read_stderr()
            if stdout and (text := stdout_decoder.decode(stdout)):
                yield CommandOutputEvent(type="stdout", data=text)
            if stderr and (text := stderr_decoder.decode(stderr)):
                yield CommandOutputEvent(type="stderr", data=text)
        if text := stdout_decoder.decode(b"", final=True):
            yield CommandOutputEvent(type="stdout", data=text)
        if text := stderr_decoder.decode(b"", final=True):
            yield CommandOutputEvent(type="stderr", data=text)
        yield CommandExitEvent(type="exit", exit_code=session.return_code if session.return_code is not None else 1)

    async def exec(self, instance_id: str, request: CommandRequest) -> ExecResponse:
        output: list[str] = []
        output_size = 0
        exit_code = 1
        stream = self.command(instance_id, request)
        try:
            async for event in stream:
                if isinstance(event, CommandOutputEvent):
                    output_size += len(event.data.encode())
                    if output_size > self.settings.exec_output_limit_bytes:
                        raise SandboxError(f"Command output exceeded {self.settings.exec_output_limit_bytes} bytes")
                    output.append(event.data)
                elif isinstance(event, CommandExitEvent):
                    exit_code = event.exit_code
        finally:
            await stream.aclose()
        return ExecResponse(exit_code=exit_code, output="".join(output))

    async def command(
        self,
        instance_id: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        await self._touch(instance_id)
        pod_name = await self._ready_pod_name(instance_id)
        command_id = f"sandbox-command-{uuid.uuid4().hex}"
        session = await self._remote_exec().open(
            pod_name,
            ["sh", "-lc", self._shell_command(request, command_id)],
        )
        try:
            async for event in self._stream_session(session):
                yield event
        except asyncio.CancelledError:
            await session.close()
            await self._remote_exec().terminate(pod_name, command_id)
            raise
        finally:
            await session.close()

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        await self._touch(instance_id)
        pod_name = await self._ready_pod_name(instance_id)
        parent = remote_path.rpartition("/")[0] or "."
        shell_command = f"mkdir -p {shlex.quote(parent)} && base64 -d > {shlex.quote(remote_path)}"
        session = await self._remote_exec().open(pod_name, ["sh", "-lc", shell_command], stdin=True)
        try:
            async for encoded in encode_base64_chunks(chunks):
                await session.write_stdin(encoded.encode("ascii"))
            await session.close_stdin()
            async for event in self._stream_session(session):
                if isinstance(event, CommandExitEvent) and event.exit_code != 0:
                    raise SandboxError(f"Could not upload file: {remote_path}")
        finally:
            await session.close()

    async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        await self._touch(instance_id)
        pod_name = await self._ready_pod_name(instance_id)
        session = await self._remote_exec().open(
            pod_name,
            ["sh", "-lc", f"base64 {shlex.quote(remote_path)}"],
        )

        async def encoded_chunks() -> AsyncGenerator[bytes, None]:
            while session.is_open():
                await session.update(0.1)
                if stdout := await session.read_stdout():
                    yield stdout
                if stderr := await session.read_stderr():
                    raise SandboxError(f"Could not download file {remote_path}: {stderr.decode(errors='replace')}")

        try:
            async for chunk in decode_base64_chunks(encoded_chunks()):
                yield chunk
            if session.return_code not in {None, 0}:
                raise SandboxError(f"Could not download file: {remote_path}")
        finally:
            await session.close()

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        try:
            await self._egress_driver().apply(instance_id, allowed_addresses)
        except KubernetesApiError as error:
            raise self._map_error(error) from error
        await self._touch(instance_id)

    async def clear_egress_rules(self, instance_id: str) -> None:
        try:
            await self._egress_driver().clear(instance_id)
        except KubernetesApiError as error:
            if error.status != 404:
                raise self._map_error(error) from error
        await self._touch(instance_id)

    async def delete_idle_sandboxes(self, now: datetime) -> int:
        continue_token: str | None = None
        deleted = 0
        while True:
            result = await self._call(
                lambda: self.api.list_jobs(
                    self.settings.namespace,
                    f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                    100,
                    continue_token,
                )
            )
            for job in cast(list[dict[str, object]], result.get("items", [])):
                annotations = _annotations(job)
                try:
                    interval_minutes = int(annotations.get(AUTO_STOP_ANNOTATION, "0"))
                    last_activity = datetime.fromisoformat(annotations[LAST_ACTIVITY_ANNOTATION])
                except (KeyError, TypeError, ValueError):
                    continue
                if interval_minutes <= 0 or last_activity.tzinfo is None:
                    continue
                if (now - last_activity).total_seconds() < interval_minutes * 60:
                    continue
                resource_name = _metadata(job).get("name")
                if isinstance(resource_name, str) and resource_name:
                    await self.delete_sandbox(resource_name)
                    deleted += 1
            metadata = _dict(result.get("metadata"))
            token = metadata.get("continue")
            continue_token = token if isinstance(token, str) and token else None
            if continue_token is None:
                return deleted

    async def close(self) -> None:
        if self.remote_exec is not None:
            await self.remote_exec.close()
        await self.api.close()
