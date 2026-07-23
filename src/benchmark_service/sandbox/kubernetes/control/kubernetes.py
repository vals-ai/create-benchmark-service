from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import TypeVar, cast

from benchmark_service.sandbox.kubernetes.control.agent import PodDataPlane
from benchmark_service.sandbox.kubernetes.control.api import (
    KubernetesApi,
    KubernetesApiError,
)
from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.control.cache import ResourceCache
from benchmark_service.sandbox.kubernetes.control.data_plane import SandboxDataPlane
from benchmark_service.sandbox.kubernetes.control.egress import EgressPolicyDriver
from benchmark_service.sandbox.kubernetes.control.remote_exec import RemoteExec
from benchmark_service.sandbox.kubernetes.control.resource_data import (
    PodEndpoint,
    job_state,
    metadata,
    pending_failure,
    ready_pod_endpoint,
    ready_pod_name,
    resource_annotations,
    resource_dict,
)
from benchmark_service.sandbox.kubernetes.control.resources import (
    AUTO_STOP_ANNOTATION,
    FINGERPRINT_ANNOTATION,
    LAST_ACTIVITY_ANNOTATION,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ORIGINAL_NAME_ANNOTATION,
    SANDBOX_ID_LABEL,
    USER_LABELS_ANNOTATION,
    build_job,
    safe_label_value,
    sandbox_name,
    user_label_key,
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
)

Result = TypeVar("Result")


def _default_jitter(delay: float) -> float:
    return random.uniform(delay * 0.8, delay * 1.2)


def _user_labels(resource: dict[str, object]) -> dict[str, str]:
    encoded = resource_annotations(resource).get(USER_LABELS_ANNOTATION)
    if encoded is None:
        return {}
    try:
        labels = json.loads(encoded)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(labels, dict):
        return {}
    raw_labels = cast(dict[object, object], labels)
    return {name: value for name, value in raw_labels.items() if isinstance(name, str) and isinstance(value, str)}


class KubernetesSandboxBackend:
    """Reconcile EKS Jobs behind the private control-service protocol."""

    def __init__(
        self,
        settings: KubernetesControlSettings,
        api: KubernetesApi,
        remote_exec: RemoteExec | None = None,
        egress_driver: EgressPolicyDriver | None = None,
        *,
        resource_cache: ResourceCache | None = None,
        pod_agent: PodDataPlane | None = None,
        wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        jitter: Callable[[float], float] | None = None,
    ) -> None:
        self.settings = settings
        self.api = api
        self.egress_driver = egress_driver
        self.resource_cache = resource_cache
        self._wait = wait
        self._monotonic = monotonic
        self._now = now
        self._jitter: Callable[[float], float] = jitter or _default_jitter
        self._activity_lock = asyncio.Lock()
        self._activity_writes: dict[str, datetime] = {}
        self.data_plane = SandboxDataPlane(
            settings,
            remote_exec,
            pod_agent,
            self._ready_pod_name,
            self._ready_pod_endpoint,
        )

    def _record(self, job: dict[str, object], state: str) -> SandboxRecord:
        job_metadata = metadata(job)
        job_annotations = resource_annotations(job)
        name = str(job_metadata.get("name", ""))
        return SandboxRecord(
            id=name,
            name=job_annotations.get(ORIGINAL_NAME_ANNOTATION, name),
            state=state,
            labels=_user_labels(job),
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
                await self._wait(self._jitter(0.25 * (2**attempt)))
        raise SandboxConnectionError("Kubernetes API retry attempts exhausted")

    async def _pods(self, resource_name: str) -> list[dict[str, object]]:
        if self.resource_cache is not None:
            return await self.resource_cache.pods(resource_name)
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
        if self.resource_cache is not None:
            job, pods = await self.resource_cache.wait_ready(resource_name, timeout)
            return self._record(job, job_state(job, pods))
        deadline = self._monotonic() + timeout
        while True:
            job = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))
            if job is None:
                raise SandboxNotFoundError(f"Sandbox disappeared during creation: {resource_name}")
            pods = await self._pods(resource_name)
            failure = pending_failure(job, pods)
            if failure:
                raise SandboxError(failure)
            state = job_state(job, pods)
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
        expected_fingerprint = resource_annotations(job_body)[FINGERPRINT_ANNOTATION]
        if self.resource_cache is not None:
            existing = await self.resource_cache.job(resource_name)
        else:
            existing = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))

        if existing is not None:
            if resource_annotations(existing).get(FINGERPRINT_ANNOTATION) != expected_fingerprint:
                raise SandboxConflictError(f"Sandbox {request.name} already exists with a conflicting specification")
            await self._touch(resource_name)
            return await self._wait_ready(resource_name, request.create_timeout)

        job_created = False
        ready = False
        try:
            try:
                await self._call(lambda: self.api.create_job(self.settings.namespace, job_body))
            except SandboxError as error:
                cause = error.__cause__
                if not isinstance(cause, KubernetesApiError) or cause.status != 409:
                    raise
                raced_job = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))
                if raced_job is None:
                    raise SandboxConnectionError(
                        f"Sandbox create conflicted but no Job was found: {resource_name}"
                    ) from error
                if resource_annotations(raced_job).get(FINGERPRINT_ANNOTATION) != expected_fingerprint:
                    raise SandboxConflictError(
                        f"Sandbox {request.name} already exists with a conflicting specification"
                    ) from error
                await self._touch(resource_name)
                return await self._wait_ready(resource_name, request.create_timeout)
            job_created = True
            record = await self._wait_ready(resource_name, request.create_timeout)
            ready = True
            return record
        finally:
            if not ready and job_created:
                with suppress(SandboxError):
                    await self._call(lambda: self.api.delete_job(self.settings.namespace, resource_name))

    async def get_sandbox(self, instance_id: str) -> SandboxRecord:
        resource_name = sandbox_name(instance_id)
        if self.resource_cache is not None:
            job = await self.resource_cache.job(resource_name)
        else:
            job = await self._call(lambda: self.api.get_job(self.settings.namespace, resource_name))
        if job is None:
            raise SandboxNotFoundError(f"Sandbox not found: {instance_id}")
        await self._touch(resource_name)
        return self._record(job, job_state(job, await self._pods(resource_name)))

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
            resource_name = str(metadata(job).get("name", ""))
            items.append(self._record(job, job_state(job, await self._pods(resource_name))))
        page_metadata = resource_dict(result.get("metadata"))
        return SandboxListPage(items=items, continue_token=page_metadata.get("continue"))

    async def delete_sandbox(self, instance_id: str) -> None:
        resource_name = sandbox_name(instance_id)
        operations: list[Callable[[], Awaitable[None]]] = [
            lambda: self.api.delete_custom_object(
                self.settings.namespace,
                "ciliumnetworkpolicies",
                f"{resource_name}-egress",
            ),
            lambda: self.api.delete_job(self.settings.namespace, resource_name),
        ]
        for operation in operations:
            with suppress(SandboxNotFoundError):
                await self._call(operation)
        async with self._activity_lock:
            self._activity_writes.pop(resource_name, None)

    def _egress_driver(self) -> EgressPolicyDriver:
        if self.egress_driver is None:
            raise SandboxError("Kubernetes egress policy driver is not configured")
        return self.egress_driver

    async def _touch(self, instance_id: str) -> None:
        resource_name = sandbox_name(instance_id)
        now = self._now()
        async with self._activity_lock:
            last_write = self._activity_writes.get(resource_name)
            if (
                last_write is not None
                and (now - last_write).total_seconds() < self.settings.activity_write_interval_seconds
            ):
                return
            self._activity_writes[resource_name] = now
        try:
            await self._call(
                lambda: self.api.patch_job(
                    self.settings.namespace,
                    resource_name,
                    {"metadata": {"annotations": {LAST_ACTIVITY_ANNOTATION: now.isoformat()}}},
                )
            )
        except Exception:
            async with self._activity_lock:
                if self._activity_writes.get(resource_name) == now:
                    self._activity_writes.pop(resource_name, None)
            raise

    async def _ready_pod_name(self, instance_id: str) -> str:
        resource_name = sandbox_name(instance_id)
        if self.resource_cache is not None:
            return await self.resource_cache.ready_pod_name(resource_name)
        pods = await self._pods(resource_name)
        for pod in pods:
            if name := ready_pod_name(pod):
                return name
        raise SandboxConnectionError(f"Sandbox does not have a ready Pod: {instance_id}")

    async def _ready_pod_endpoint(self, instance_id: str) -> PodEndpoint:
        resource_name = sandbox_name(instance_id)
        if self.resource_cache is not None:
            return await self.resource_cache.ready_pod_endpoint(resource_name)
        pods = await self._pods(resource_name)
        for pod in pods:
            if endpoint := ready_pod_endpoint(pod):
                return endpoint
        raise SandboxConnectionError(f"Sandbox does not have a ready Pod IP: {instance_id}")

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

    async def _refresh_activity_after_interval(self, instance_id: str) -> None:
        """Persist activity after one debounce interval while a command remains open.

        Arguments
        - instance_id: Sandbox identifier to refresh.
        """
        await self._wait(self.settings.activity_write_interval_seconds)
        await self._touch(instance_id)

    async def command(
        self,
        instance_id: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        await self._touch(instance_id)
        stream = self.data_plane.command(instance_id, request)
        pending_event: asyncio.Task[CommandEvent] | None = None
        refresh_task: asyncio.Task[None] | None = None
        try:
            pending_event = asyncio.create_task(anext(stream))
            refresh_task = asyncio.create_task(self._refresh_activity_after_interval(instance_id))
            while True:
                completed, _ = await asyncio.wait(
                    (pending_event, refresh_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if refresh_task in completed:
                    refresh_task.result()
                    refresh_task = asyncio.create_task(self._refresh_activity_after_interval(instance_id))
                if pending_event not in completed:
                    continue
                try:
                    event = pending_event.result()
                except StopAsyncIteration:
                    return
                pending_event = asyncio.create_task(anext(stream))
                yield event
        finally:
            if pending_event is not None:
                pending_event.cancel()
            if refresh_task is not None:
                refresh_task.cancel()
            if pending_event is not None:
                await asyncio.gather(pending_event, return_exceptions=True)
            if refresh_task is not None:
                await asyncio.gather(refresh_task, return_exceptions=True)
            await stream.aclose()

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        await self._touch(instance_id)
        await self.data_plane.upload_file(instance_id, remote_path, chunks)

    async def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        await self._touch(instance_id)
        async for chunk in self.data_plane.stream_download(instance_id, remote_path):
            yield chunk

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
                lambda continue_token=continue_token: self.api.list_jobs(
                    self.settings.namespace,
                    f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                    100,
                    continue_token,
                )
            )
            for job in cast(list[dict[str, object]], result.get("items", [])):
                job_annotations = resource_annotations(job)
                try:
                    interval_minutes = int(job_annotations.get(AUTO_STOP_ANNOTATION, "0"))
                    last_activity = datetime.fromisoformat(job_annotations[LAST_ACTIVITY_ANNOTATION])
                except (KeyError, TypeError, ValueError):
                    continue
                if interval_minutes <= 0 or last_activity.tzinfo is None:
                    continue
                idle_threshold = interval_minutes * 60 + self.settings.activity_write_interval_seconds
                if (now - last_activity).total_seconds() < idle_threshold:
                    continue
                resource_name = metadata(job).get("name")
                if isinstance(resource_name, str) and resource_name:
                    await self.delete_sandbox(resource_name)
                    deleted += 1
            page_metadata = resource_dict(result.get("metadata"))
            token = page_metadata.get("continue")
            continue_token = token if isinstance(token, str) and token else None
            if continue_token is None:
                return deleted

    async def close(self) -> None:
        if self.resource_cache is not None:
            await self.resource_cache.close()
        await self.data_plane.close()
        await self.api.close()
