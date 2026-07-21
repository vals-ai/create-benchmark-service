from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from typing import Any, TypeVar, cast

from benchmark_service.sandbox.kubernetes.control.api import (
    KubernetesApi,
    KubernetesApiError,
)
from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.control.resources import (
    FINGERPRINT_ANNOTATION,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ORIGINAL_NAME_ANNOTATION,
    SANDBOX_ID_LABEL,
    build_ingress_policy,
    build_job,
    safe_label_value,
    sandbox_name,
    user_label_key,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandEvent,
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
        *,
        wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.api = api
        self._wait = wait
        self._monotonic = monotonic

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

    async def exec(self, instance_id: str, request: CommandRequest) -> ExecResponse:
        raise NotImplementedError

    async def command(
        self,
        instance_id: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        raise NotImplementedError
        yield

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        raise NotImplementedError

    async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError
        yield

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        raise NotImplementedError

    async def clear_egress_rules(self, instance_id: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        await self.api.close()
