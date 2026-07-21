"""Tests for deterministic and secure EKS sandbox resources."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, cast

import pytest

from benchmark_service.sandbox.kubernetes.control.resources import (
    FINGERPRINT_LABEL,
    FINGERPRINT_ANNOTATION,
    SANDBOX_ID_LABEL,
    build_egress_policy,
    build_ingress_policy,
    build_job,
    request_fingerprint,
    sandbox_name,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import (
    ImageSource,
    Resources,
    SandboxCreateRequest,
    SandboxError,
    SnapshotSource,
)

DIGEST = "a" * 64


def _dict(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _list(value: object) -> list[Any]:
    assert isinstance(value, list)
    return cast(list[Any], value)


def _settings(**changes: object) -> KubernetesControlSettings:
    values: dict[str, object] = {
        "api_token": "test-token",
        "docker_image": f"registry.internal/docker@sha256:{DIGEST}",
        "allowed_image_prefixes": ("registry.internal/",),
        "require_image_digest": True,
    }
    values.update(changes)
    return KubernetesControlSettings.model_validate(values)


def _request(**changes: object) -> SandboxCreateRequest:
    values: dict[str, object] = {
        "source": ImageSource(image=f"registry.internal/python@sha256:{DIGEST}"),
        "resources": Resources(vcpu=2, memory=4, disk=20, gpu=2, gpu_type="H100"),
        "name": "task-1",
        "labels": {"run_id": "run-1", "task_id": "task-1"},
        "env_vars": {"WORKSPACE": "/workspace", "TASK_ID": "task-1"},
        "auto_stop_interval": 30,
        "create_timeout": 120,
    }
    values.update(changes)
    return SandboxCreateRequest.model_validate(values)


def test_builds_deterministic_isolated_job_and_network_policies() -> None:
    """Keep caller inputs inside the deployment-owned isolation boundary.

    Test cases:
    - Names and fingerprints are deterministic despite unsafe input or mapping order.
    - Jobs enforce resources, Kata, Pod security, workspace, DinD, image, and GPU rules.
    - Baseline ingress is denied while temporary egress allows DNS and exact destinations.
    - Unsupported sources and unsafe image or GPU requests fail before API calls.
    """
    unsafe_name = "Bad Name/" + "x" * 80
    assert len(sandbox_name(unsafe_name)) <= 63
    assert re.search(r"-[0-9a-f]{8}$", sandbox_name(unsafe_name))
    assert sandbox_name("task-1") == "task-1"

    request = _request(name=unsafe_name)
    reordered = request.model_copy(
        update={
            "labels": dict(reversed(list(request.labels.items()))),
            "env_vars": dict(reversed(list(request.env_vars.items()))),
        }
    )
    changed = request.model_copy(update={"resources": Resources(vcpu=3, memory=4, disk=20, gpu=2, gpu_type="H100")})
    assert request_fingerprint(request) == request_fingerprint(reordered)
    assert request_fingerprint(request) != request_fingerprint(changed)

    settings = _settings()
    job = build_job(request, settings)
    metadata = _dict(job["metadata"])
    spec = _dict(job["spec"])
    template = _dict(spec["template"])
    pod_spec = _dict(template["spec"])
    containers = _list(pod_spec["containers"])
    volumes = _list(pod_spec["volumes"])
    sandbox = _dict(containers[0])
    docker = _dict(containers[1])

    assert metadata["name"] == sandbox_name(unsafe_name)
    assert metadata["labels"][FINGERPRINT_LABEL] == request_fingerprint(request)[:63]
    assert metadata["annotations"][FINGERPRINT_ANNOTATION] == request_fingerprint(request)
    assert spec["backoffLimit"] == 0
    assert spec["activeDeadlineSeconds"] == settings.hard_lifetime_seconds
    assert spec["ttlSecondsAfterFinished"] == settings.finished_ttl_seconds
    assert pod_spec["runtimeClassName"] == "kata-qemu"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"] == {"seccompProfile": {"type": "RuntimeDefault"}}
    assert "hostNetwork" not in pod_spec and "hostPID" not in pod_spec and "hostIPC" not in pod_spec
    assert sandbox["resources"] == {
        "requests": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "20Gi", "nvidia.com/gpu": "2"},
        "limits": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "20Gi", "nvidia.com/gpu": "2"},
    }
    assert pod_spec["nodeSelector"] == {"sandbox.vals.ai/gpu-type": "H100"}
    assert {volume["name"]: volume for volume in volumes}["workspace"]["emptyDir"] == {"sizeLimit": "20Gi"}
    assert docker["securityContext"] == {"privileged": True}
    assert docker["readinessProbe"]["exec"]["command"][-1] == "docker info >/dev/null 2>&1"
    assert sandbox["readinessProbe"]["exec"]["command"] == ["sh", "-lc", "true"]
    assert all("hostPath" not in volume for volume in volumes)

    resource_name = sandbox_name(unsafe_name)
    ingress = build_ingress_policy(resource_name, settings.namespace)
    assert ingress["spec"] == {
        "podSelector": {"matchLabels": {SANDBOX_ID_LABEL: resource_name}},
        "policyTypes": ["Ingress"],
        "ingress": [],
    }
    egress = build_egress_policy(resource_name, settings.namespace, ["api.example.com", "10.0.0.3/24"])
    rules = _list(_dict(egress["spec"])["egress"])
    assert rules[1]["toCIDR"] == ["10.0.0.0/24"]
    assert rules[1]["toFQDNs"] == [{"matchName": "api.example.com"}]

    invalid_cases = [
        (_request(source=SnapshotSource(snapshot="snapshot-1")), _settings(), "source type"),
        (_request(resources=Resources(vcpu=2, memory=4, disk=20, gpu=1)), _settings(), "gpu_type"),
        (_request(source=ImageSource(image=f"outside/image@sha256:{DIGEST}")), _settings(), "allowlist"),
        (_request(source=ImageSource(image="registry.internal/python:latest")), _settings(), "digest"),
        (_request(resources=Resources(vcpu=65, memory=4, disk=20)), _settings(), "ceilings: vcpu"),
        (_request(resources=Resources(vcpu=0, memory=4, disk=20)), _settings(), "must be positive"),
        (_request(env_vars={"BAD-NAME": "value"}), _settings(), "Invalid sandbox environment"),
    ]
    for invalid_request, invalid_settings, message in invalid_cases:
        with pytest.raises(SandboxError, match=message):
            build_job(invalid_request, invalid_settings)

    with pytest.raises(ValueError, match="cannot be empty"):
        build_egress_policy("sandbox-1", settings.namespace, [])

    no_docker = build_job(_request(resources=Resources(vcpu=1, memory=1, disk=1)), _settings(docker_enabled=False))
    no_docker_copy = deepcopy(no_docker)
    no_docker_spec = _dict(_dict(_dict(no_docker_copy["spec"])["template"])["spec"])
    assert len(_list(no_docker_spec["containers"])) == 1
