"""Read state from serialized Kubernetes Job and Pod resources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, cast


@dataclass(frozen=True)
class PodEndpoint:
    """Ready sandbox Pod identity used by lifecycle and data-plane calls."""

    name: str
    ip: str


def resource_dict(value: object) -> dict[str, Any]:
    """Narrow one untyped Kubernetes object field to a dictionary."""
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def resource_list(value: object) -> list[Any]:
    """Narrow one untyped Kubernetes object field to a list."""
    return cast(list[Any], value) if isinstance(value, list) else []


def metadata(resource: dict[str, object]) -> dict[str, Any]:
    """Return Kubernetes metadata from a serialized resource."""
    return resource_dict(resource.get("metadata"))


def resource_annotations(resource: dict[str, object]) -> dict[str, str]:
    """Return string annotations from a serialized resource."""
    return cast(dict[str, str], metadata(resource).get("annotations", {}))


def pod_is_ready(pod: dict[str, object]) -> bool:
    """Report whether a serialized Pod is running and ready."""
    status = resource_dict(pod.get("status"))

    return status.get("phase") == "Running" and any(
        resource_dict(condition).get("type") == "Ready" and resource_dict(condition).get("status") == "True"
        for condition in resource_list(status.get("conditions"))
    )


def ready_pod_name(pod: dict[str, object]) -> str | None:
    """Return a ready Pod name when the resource contains one."""
    if not pod_is_ready(pod):
        return None
    name = metadata(pod).get("name")

    return name if isinstance(name, str) and name else None


def ready_pod_endpoint(pod: dict[str, object]) -> PodEndpoint | None:
    """Return a ready Pod name and private IP when both are available."""
    name = ready_pod_name(pod)
    pod_ip = resource_dict(pod.get("status")).get("podIP")
    if name is None or not isinstance(pod_ip, str) or not pod_ip:
        return None

    return PodEndpoint(name=name, ip=pod_ip)


def job_state(job: dict[str, object], pods: list[dict[str, object]]) -> str:
    """Return the shared sandbox state for one Job and its Pods."""
    status = resource_dict(job.get("status"))
    if status.get("failed") or any(
        resource_dict(condition).get("type") == "Failed" and resource_dict(condition).get("status") == "True"
        for condition in resource_list(status.get("conditions"))
    ):
        return "failed"
    if status.get("succeeded") or status.get("completionTime"):
        return "stopped"
    for pod in pods:
        pod_status = resource_dict(pod.get("status"))
        if pod_status.get("phase") == "Failed":
            return "failed"
        if pod_is_ready(pod):
            return "running"

    return "pending"


def pending_failure(job: dict[str, object], pods: list[dict[str, object]]) -> str | None:
    """Return a permanent creation failure when one is visible."""
    summary = json.dumps({"job": job.get("status"), "pods": [pod.get("status") for pod in pods]}).lower()
    permanent_image_errors = (
        "invalid reference format",
        "manifest unknown",
        "no matching manifest",
        "not found",
        "pull access denied",
        "unauthorized",
    )
    if any(message in summary for message in permanent_image_errors):
        return "Sandbox image pull failed"
    if "exceeded quota" in summary or ("resourcequota" in summary and "forbidden" in summary):
        return "Sandbox creation was rejected by cluster quota or policy"

    return None
