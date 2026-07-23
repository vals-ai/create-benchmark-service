from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol, cast

from benchmark_service.sandbox.kubernetes.control.api import (
    KubernetesApiError,
    KubernetesResourceWatchApi,
)
from benchmark_service.sandbox.kubernetes.control.resource_data import (
    PodEndpoint,
    job_state,
    metadata,
    pending_failure,
    ready_pod_endpoint,
    ready_pod_name,
    resource_dict,
)
from benchmark_service.sandbox.kubernetes.control.resources import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    SANDBOX_ID_LABEL,
)
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError, SandboxNotFoundError


class ResourceCache(Protocol):
    """Watched resource state consumed by the sandbox backend."""

    async def job(self, resource_name: str) -> dict[str, object] | None: ...

    async def pods(self, resource_name: str) -> list[dict[str, object]]: ...

    async def wait_ready(
        self,
        resource_name: str,
        timeout: float,
    ) -> tuple[dict[str, object], list[dict[str, object]]]: ...

    async def ready_pod_name(self, resource_name: str) -> str: ...

    async def ready_pod_endpoint(self, resource_name: str) -> PodEndpoint: ...

    async def close(self) -> None: ...


class SandboxResourceCache:
    """Share namespace-scoped Job and Pod watches across sandbox requests."""

    def __init__(
        self,
        namespace: str,
        api: KubernetesResourceWatchApi,
        *,
        wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.namespace = namespace
        self.api = api
        self._wait = wait
        self._jobs: dict[str, dict[str, object]] = {}
        self._pods: dict[str, dict[str, dict[str, object]]] = {}
        self._job_resource_version = ""
        self._pod_resource_version = ""
        self._revision = 0
        self._condition = asyncio.Condition()
        self._tasks: list[asyncio.Task[None]] = []
        self._error: SandboxConnectionError | None = None
        self._closed = False

    @property
    def _selector(self) -> str:
        return f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"

    async def _list_all_jobs(self) -> tuple[dict[str, dict[str, object]], str]:
        jobs: dict[str, dict[str, object]] = {}
        continue_token: str | None = None
        resource_version = ""
        while True:
            result = await self.api.list_jobs(self.namespace, self._selector, 500, continue_token)
            for job in cast(list[dict[str, object]], result.get("items", [])):
                name = metadata(job).get("name")
                if isinstance(name, str) and name:
                    jobs[name] = job
            page_metadata = resource_dict(result.get("metadata"))
            resource_version = str(
                page_metadata.get("resourceVersion") or page_metadata.get("resource_version") or resource_version
            )
            token = page_metadata.get("continue") or page_metadata.get("_continue")
            continue_token = token if isinstance(token, str) and token else None
            if continue_token is None:
                return jobs, resource_version

    async def _list_all_pods(self) -> tuple[dict[str, dict[str, dict[str, object]]], str]:
        pods: dict[str, dict[str, dict[str, object]]] = {}
        continue_token: str | None = None
        resource_version = ""
        while True:
            result = await self.api.list_pods_page(self.namespace, self._selector, 500, continue_token)
            for pod in cast(list[dict[str, object]], result.get("items", [])):
                pod_metadata = metadata(pod)
                name = pod_metadata.get("name")
                sandbox_id = resource_dict(pod_metadata.get("labels")).get(SANDBOX_ID_LABEL)
                if isinstance(name, str) and name and isinstance(sandbox_id, str) and sandbox_id:
                    pods.setdefault(sandbox_id, {})[name] = pod
            page_metadata = resource_dict(result.get("metadata"))
            resource_version = str(
                page_metadata.get("resourceVersion") or page_metadata.get("resource_version") or resource_version
            )
            token = page_metadata.get("continue") or page_metadata.get("_continue")
            continue_token = token if isinstance(token, str) and token else None
            if continue_token is None:
                return pods, resource_version

    async def _replace_jobs(self) -> None:
        jobs, resource_version = await self._list_all_jobs()
        async with self._condition:
            self._jobs = jobs
            self._job_resource_version = resource_version
            self._revision += 1
            self._condition.notify_all()

    async def _replace_pods(self) -> None:
        pods, resource_version = await self._list_all_pods()
        async with self._condition:
            self._pods = pods
            self._pod_resource_version = resource_version
            self._revision += 1
            self._condition.notify_all()

    async def start(self) -> None:
        """Load consistent snapshots and start one watch per resource type."""
        if self._tasks:
            return
        await self._replace_jobs()
        await self._replace_pods()
        self._tasks = [
            asyncio.create_task(self._watch_jobs()),
            asyncio.create_task(self._watch_pods()),
        ]

    async def _set_watch_error(self, error: KubernetesApiError) -> None:
        async with self._condition:
            self._error = SandboxConnectionError(str(error))
            self._condition.notify_all()

    def _retryable(self, error: KubernetesApiError) -> bool:
        return error.status in {0, 410, 429} or error.status >= 500

    async def _relist(
        self,
        replace: Callable[[], Awaitable[None]],
        retry_delay: float,
    ) -> float | None:
        while not self._closed:
            try:
                await replace()
                return 0.25
            except KubernetesApiError as error:
                if not self._retryable(error):
                    await self._set_watch_error(error)
                    return None
                await self._wait(retry_delay)
                retry_delay = min(retry_delay * 2, 5.0)
        return None

    async def _watch_jobs(self) -> None:
        retry_delay = 0.25
        while not self._closed:
            try:
                async for event_type, job in self.api.watch_jobs(
                    self.namespace,
                    self._selector,
                    self._job_resource_version,
                ):
                    job_metadata = metadata(job)
                    name = job_metadata.get("name")
                    if not isinstance(name, str) or not name:
                        continue
                    async with self._condition:
                        if event_type == "DELETED":
                            self._jobs.pop(name, None)
                        else:
                            self._jobs[name] = job
                        self._job_resource_version = str(
                            job_metadata.get("resourceVersion")
                            or job_metadata.get("resource_version")
                            or self._job_resource_version
                        )
                        self._revision += 1
                        self._condition.notify_all()
                    retry_delay = 0.25
            except KubernetesApiError as error:
                if not self._retryable(error):
                    await self._set_watch_error(error)
                    return
                if error.status == 410:
                    relist_delay = await self._relist(self._replace_jobs, retry_delay)
                    if relist_delay is None:
                        return
                    retry_delay = relist_delay
                else:
                    await self._wait(retry_delay)
                    retry_delay = min(retry_delay * 2, 5.0)
            else:
                await self._wait(0.25)
                retry_delay = 0.25

    async def _watch_pods(self) -> None:
        retry_delay = 0.25
        while not self._closed:
            try:
                async for event_type, pod in self.api.watch_pods(
                    self.namespace,
                    self._selector,
                    self._pod_resource_version,
                ):
                    pod_metadata = metadata(pod)
                    name = pod_metadata.get("name")
                    sandbox_id = resource_dict(pod_metadata.get("labels")).get(SANDBOX_ID_LABEL)
                    if not isinstance(name, str) or not name or not isinstance(sandbox_id, str) or not sandbox_id:
                        continue
                    async with self._condition:
                        sandbox_pods = self._pods.setdefault(sandbox_id, {})
                        if event_type == "DELETED":
                            sandbox_pods.pop(name, None)
                            if not sandbox_pods:
                                self._pods.pop(sandbox_id, None)
                        else:
                            sandbox_pods[name] = pod
                        self._pod_resource_version = str(
                            pod_metadata.get("resourceVersion")
                            or pod_metadata.get("resource_version")
                            or self._pod_resource_version
                        )
                        self._revision += 1
                        self._condition.notify_all()
                    retry_delay = 0.25
            except KubernetesApiError as error:
                if not self._retryable(error):
                    await self._set_watch_error(error)
                    return
                if error.status == 410:
                    relist_delay = await self._relist(self._replace_pods, retry_delay)
                    if relist_delay is None:
                        return
                    retry_delay = relist_delay
                else:
                    await self._wait(retry_delay)
                    retry_delay = min(retry_delay * 2, 5.0)
            else:
                await self._wait(0.25)
                retry_delay = 0.25

    async def job(self, resource_name: str) -> dict[str, object] | None:
        async with self._condition:
            return self._jobs.get(resource_name)

    async def pods(self, resource_name: str) -> list[dict[str, object]]:
        async with self._condition:
            return list(self._pods.get(resource_name, {}).values())

    async def wait_ready(
        self,
        resource_name: str,
        timeout: float,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        """Wait for watched resources to become ready without API polling."""
        seen = False
        try:
            async with asyncio.timeout(timeout):
                async with self._condition:
                    while True:
                        if self._error is not None:
                            raise self._error
                        job = self._jobs.get(resource_name)
                        pods = list(self._pods.get(resource_name, {}).values())
                        if job is not None:
                            seen = True
                            if failure := pending_failure(job, pods):
                                raise SandboxError(failure)
                            state = job_state(job, pods)
                            if state == "running":
                                return job, pods
                            if state in {"failed", "stopped"}:
                                raise SandboxError(f"Sandbox entered {state} state before becoming ready")
                        elif seen:
                            raise SandboxNotFoundError(f"Sandbox disappeared during creation: {resource_name}")
                        revision = self._revision
                        await self._condition.wait_for(
                            lambda revision=revision: self._revision != revision or self._error is not None
                        )
        except TimeoutError as error:
            raise SandboxConnectionError(f"Timed out waiting for sandbox readiness: {resource_name}") from error

    async def ready_pod_name(self, resource_name: str) -> str:
        async with self._condition:
            pods = list(self._pods.get(resource_name, {}).values())
        for pod in pods:
            if name := ready_pod_name(pod):
                return name
        raise SandboxConnectionError(f"Sandbox does not have a ready Pod: {resource_name}")

    async def ready_pod_endpoint(self, resource_name: str) -> PodEndpoint:
        """Return the watched name and private IP for one ready sandbox Pod."""
        async with self._condition:
            pods = list(self._pods.get(resource_name, {}).values())
        for pod in pods:
            if endpoint := ready_pod_endpoint(pod):
                return endpoint
        raise SandboxConnectionError(f"Sandbox does not have a ready Pod IP: {resource_name}")

    async def ready(self) -> bool:
        """Report whether both shared watches can serve current resource state."""
        async with self._condition:
            return (
                not self._closed
                and self._error is None
                and len(self._tasks) == 2
                and all(not task.done() for task in self._tasks)
            )

    async def close(self) -> None:
        """Stop watches and wake waiters before the API client closes."""
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        async with self._condition:
            self._error = SandboxConnectionError("Kubernetes resource cache closed")
            self._condition.notify_all()
