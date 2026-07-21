"""Tests for EKS sandbox lifecycle reconciliation and streaming helpers."""

from __future__ import annotations

from typing import Any, cast

import pytest

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApiError
from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.control.kubernetes import KubernetesSandboxBackend
from benchmark_service.sandbox.kubernetes.control.resources import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    build_job,
    user_label_key,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import (
    ImageSource,
    Resources,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
)

DIGEST = "a" * 64


class MockKubernetesApi:
    """Store dictionary resources and record narrow API operations."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, object]] = {}
        self.pods: dict[str, list[dict[str, object]]] = {}
        self.operations: list[tuple[str, object]] = []
        self.list_result: dict[str, object] | None = None
        self.get_error: KubernetesApiError | None = None
        self.ready_on_create = True
        self.closed = False

    async def create_job(self, namespace: str, body: dict[str, object]) -> dict[str, object]:
        name = str(_metadata(body)["name"])
        self.operations.append(("create_job", (namespace, name)))
        self.jobs[name] = body
        if self.ready_on_create:
            self.pods.setdefault(name, [_ready_pod(name)])
        return body

    async def get_job(self, namespace: str, name: str) -> dict[str, object] | None:
        self.operations.append(("get_job", (namespace, name)))
        if self.get_error:
            raise self.get_error
        return self.jobs.get(name)

    async def list_jobs(
        self,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
    ) -> dict[str, object]:
        self.operations.append(("list_jobs", (namespace, label_selector, limit, continue_token)))
        return self.list_result or {"items": list(self.jobs.values()), "metadata": {}}

    async def patch_job(self, namespace: str, name: str, body: dict[str, object]) -> None:
        self.operations.append(("patch_job", (namespace, name, body)))

    async def delete_job(self, namespace: str, name: str) -> None:
        self.operations.append(("delete_job", (namespace, name)))
        self.jobs.pop(name, None)

    async def list_pods(self, namespace: str, label_selector: str) -> list[dict[str, object]]:
        name = label_selector.partition("=")[2]
        self.operations.append(("list_pods", (namespace, label_selector)))
        return self.pods.get(name, [])

    async def create_network_policy(self, namespace: str, body: dict[str, object]) -> None:
        self.operations.append(("create_ingress", (namespace, _metadata(body)["name"])))

    async def delete_network_policy(self, namespace: str, name: str) -> None:
        self.operations.append(("delete_ingress", (namespace, name)))

    async def replace_custom_object(
        self,
        namespace: str,
        plural: str,
        name: str,
        body: dict[str, object],
    ) -> None:
        self.operations.append(("replace_custom", (namespace, plural, name, body)))

    async def delete_custom_object(self, namespace: str, plural: str, name: str) -> None:
        self.operations.append(("delete_custom", (namespace, plural, name)))

    async def close(self) -> None:
        self.closed = True


def _metadata(resource: dict[str, object]) -> dict[str, Any]:
    value = resource["metadata"]
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _settings() -> KubernetesControlSettings:
    return KubernetesControlSettings(
        api_token="test-token",
        docker_image=f"registry.internal/docker@sha256:{DIGEST}",
        allowed_image_prefixes=("registry.internal/",),
        require_image_digest=True,
    )


def _request(**changes: object) -> SandboxCreateRequest:
    values: dict[str, object] = {
        "source": ImageSource(image=f"registry.internal/python@sha256:{DIGEST}"),
        "resources": Resources(vcpu=2, memory=4, disk=20),
        "name": "task-1",
        "labels": {"run_id": "run-1"},
        "env_vars": {},
        "auto_stop_interval": 30,
        "create_timeout": 1,
    }
    values.update(changes)
    return SandboxCreateRequest.model_validate(values)


def _ready_pod(name: str) -> dict[str, object]:
    return {
        "metadata": {"name": f"{name}-pod"},
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
    }


class TestKubernetesSandboxBackendLifecycle:
    """Lifecycle reconciliation through the narrow Kubernetes API."""

    async def test_creates_reuses_lists_and_deletes_sandboxes(self) -> None:
        """Reconcile deterministic Jobs without leaking caller cluster control.

        Test cases:
        - Ingress is created before a Job and readiness returns a running record.
        - Matching retries reuse while conflicting specifications fail.
        - Listing preserves selectors and continuation; delete removes every owned resource.
        """
        api = MockKubernetesApi()
        backend = KubernetesSandboxBackend(_settings(), api)
        request = _request()
        created = await backend.create_sandbox(request)
        first_create_operations = [operation[0] for operation in api.operations]
        reused = await backend.create_sandbox(request)

        api.list_result = {"items": list(api.jobs.values()), "metadata": {"continue": "next"}}
        page = await backend.list_sandboxes({"run_id": "run-1"}, 5, "prior")
        await backend.delete_sandbox(created.id)

        assert created == reused and created.state == "running"
        assert first_create_operations.index("create_ingress") < first_create_operations.index("create_job")
        assert first_create_operations.count("create_job") == 1
        assert page.continue_token == "next" and [item.id for item in page.items] == [created.id]
        list_call = next(value for name, value in api.operations if name == "list_jobs")
        assert list_call == (
            "benchmark-sandboxes",
            f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{user_label_key('run_id')}=run-1",
            5,
            "prior",
        )
        assert ("delete_custom", ("benchmark-sandboxes", "ciliumnetworkpolicies", "task-1-egress")) in api.operations
        assert ("delete_job", ("benchmark-sandboxes", "task-1")) in api.operations
        assert ("delete_ingress", ("benchmark-sandboxes", "task-1-ingress")) in api.operations

        conflicting = _request(resources=Resources(vcpu=3, memory=4, disk=20))
        api.jobs["task-1"] = build_job(request, _settings())
        with pytest.raises(SandboxConflictError, match="conflicting specification"):
            await backend.create_sandbox(conflicting)

    async def test_reports_pending_failures_states_and_api_errors(self) -> None:
        """Translate readiness and API failures into stable sandbox errors.

        Test cases:
        - Image pull and scheduling failures have actionable messages.
        - Missing resources and API authorization/availability use shared errors.
        - Completed and failed Jobs produce stable states.
        """
        request = _request()
        cases = [
            ({"containerStatuses": [{"state": {"waiting": {"reason": "ImagePullBackOff"}}}]}, "image pull"),
            ({"conditions": [{"reason": "Unschedulable", "message": "no nodes"}]}, "scheduled"),
        ]
        for pod_status, message in cases:
            api = MockKubernetesApi()
            job = build_job(request, _settings())
            api.jobs["task-1"] = job
            api.pods["task-1"] = [{"status": pod_status}]
            backend = KubernetesSandboxBackend(_settings(), api)
            with pytest.raises(SandboxError, match=message):
                await backend.create_sandbox(request)

        api = MockKubernetesApi()
        backend = KubernetesSandboxBackend(_settings(), api)
        with pytest.raises(SandboxNotFoundError, match="not found"):
            await backend.get_sandbox("missing")

        for status, error_type in [(403, SandboxError), (503, SandboxConnectionError)]:
            api.get_error = KubernetesApiError(status, "failed")
            with pytest.raises(error_type):
                await backend.get_sandbox("task-1")
        api.get_error = None

        for status_value, expected in [({"succeeded": 1}, "stopped"), ({"failed": 1}, "failed")]:
            job = build_job(request, _settings())
            job["status"] = status_value
            api.jobs["task-1"] = job
            api.pods["task-1"] = []
            assert (await backend.get_sandbox("task-1")).state == expected

        timeout_api = MockKubernetesApi()
        timeout_api.ready_on_create = False
        timeout_backend = KubernetesSandboxBackend(_settings(), timeout_api)
        with pytest.raises(SandboxConnectionError, match="Timed out"):
            await timeout_backend.create_sandbox(_request(create_timeout=0))
        assert "task-1" not in timeout_api.jobs
        assert ("delete_ingress", ("benchmark-sandboxes", "task-1-ingress")) in timeout_api.operations

        await backend.close()
        assert api.closed is True
