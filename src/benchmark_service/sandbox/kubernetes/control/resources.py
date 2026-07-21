from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime

from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import ImageSource, SandboxCreateRequest, SandboxError

MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "benchmark-sandbox-control"
SANDBOX_ID_LABEL = "sandbox.vals.ai/id"
REQUEST_NAME_LABEL = "sandbox.vals.ai/request-name"
FINGERPRINT_LABEL = "sandbox.vals.ai/fingerprint"
FINGERPRINT_ANNOTATION = "sandbox.vals.ai/request-fingerprint"
ORIGINAL_NAME_ANNOTATION = "sandbox.vals.ai/original-name"
AUTO_STOP_ANNOTATION = "sandbox.vals.ai/auto-stop-minutes"
LAST_ACTIVITY_ANNOTATION = "sandbox.vals.ai/last-activity"
USER_LABEL_PREFIX = "sandbox.vals.ai/label-"

_INVALID_DNS = re.compile(r"[^a-z0-9]+")
_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sandbox_name(value: str) -> str:
    normalized = _INVALID_DNS.sub("-", value.lower()).strip("-") or "sandbox"
    changed = normalized != value or len(normalized) > 63
    if not changed:
        return normalized
    digest = hashlib.sha256(value.encode()).hexdigest()[:8]
    base = normalized[:54].rstrip("-") or "sandbox"
    return f"{base}-{digest}"


def safe_label_value(value: str) -> str:
    return sandbox_name(value)


def user_label_key(value: str) -> str:
    digest = hashlib.sha256(value.encode()).hexdigest()[:8]
    return f"{USER_LABEL_PREFIX}{sandbox_name(value)[:29].rstrip('-')}-{digest}"


def request_fingerprint(request: SandboxCreateRequest) -> str:
    payload = request.model_dump(mode="json")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_image(image: str, settings: KubernetesControlSettings) -> None:
    if settings.allowed_image_prefixes and not image.startswith(settings.allowed_image_prefixes):
        raise SandboxError(f"Image is outside the configured registry allowlist: {image}")
    if settings.require_image_digest and _DIGEST.search(image) is None:
        raise SandboxError(f"Image must use a sha256 digest: {image}")


def _labels(request: SandboxCreateRequest, resource_name: str) -> dict[str, str]:
    labels = {
        MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        SANDBOX_ID_LABEL: resource_name,
        REQUEST_NAME_LABEL: safe_label_value(request.name),
        FINGERPRINT_LABEL: request_fingerprint(request)[:63],
    }
    labels.update({user_label_key(name): safe_label_value(value) for name, value in request.labels.items()})
    return labels


def build_job(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    if not isinstance(request.source, ImageSource):
        raise SandboxError(f"Kubernetes sandbox provider does not support source type: {request.source.type}")

    image = request.source.image
    _validate_image(image, settings)
    if settings.docker_enabled:
        _validate_image(settings.docker_image, settings)
    if request.resources.gpu and not request.resources.gpu_type:
        raise SandboxError("Kubernetes sandbox provider requires gpu_type when gpu is requested")
    if request.resources.vcpu <= 0 or request.resources.memory <= 0 or request.resources.disk <= 0:
        raise SandboxError("Kubernetes sandbox vcpu, memory, and disk must be positive")
    if request.create_timeout <= 0 or request.auto_stop_interval < 0:
        raise SandboxError("Kubernetes sandbox create_timeout must be positive and auto_stop_interval nonnegative")
    invalid_env_names = sorted(name for name in request.env_vars if _ENV_NAME.fullmatch(name) is None)
    if invalid_env_names:
        raise SandboxError(f"Invalid sandbox environment variable names: {', '.join(invalid_env_names)}")
    ceilings = {
        "vcpu": (request.resources.vcpu, settings.max_vcpu),
        "memory": (request.resources.memory, settings.max_memory_gib),
        "disk": (request.resources.disk, settings.max_disk_gib),
        "gpu": (request.resources.gpu, settings.max_gpu),
        "create_timeout": (request.create_timeout, settings.max_create_timeout_seconds),
    }
    exceeded = [name for name, (requested, maximum) in ceilings.items() if requested > maximum]
    if exceeded:
        raise SandboxError(f"Sandbox request exceeds configured ceilings: {', '.join(exceeded)}")

    resource_name = sandbox_name(request.name)
    labels = _labels(request, resource_name)
    resources: dict[str, str] = {
        "cpu": str(request.resources.vcpu),
        "memory": f"{request.resources.memory}Gi",
        "ephemeral-storage": f"{request.resources.disk}Gi",
    }
    node_selector: dict[str, str] = {}
    if request.resources.gpu:
        resources[settings.gpu_resource_name] = str(request.resources.gpu)
        assert request.resources.gpu_type is not None
        node_selector[settings.gpu_type_label] = request.resources.gpu_type

    env = [{"name": name, "value": value} for name, value in sorted(request.env_vars.items())]
    volume_mounts: list[dict[str, object]] = [{"name": "workspace", "mountPath": "/workspace"}]
    volumes: list[dict[str, object]] = [{"name": "workspace", "emptyDir": {"sizeLimit": f"{request.resources.disk}Gi"}}]
    containers: list[dict[str, object]] = []
    if settings.docker_enabled:
        env.append({"name": "DOCKER_HOST", "value": "unix:///var/run/docker.sock"})
        volume_mounts.append({"name": "docker-socket", "mountPath": "/var/run"})
        volumes.append({"name": "docker-socket", "emptyDir": {}})
        containers.append(
            {
                "name": "docker",
                "image": settings.docker_image,
                "securityContext": {"privileged": True},
                "env": [{"name": "DOCKER_TLS_CERTDIR", "value": ""}],
                "volumeMounts": [{"name": "docker-socket", "mountPath": "/var/run"}],
                "readinessProbe": {
                    "exec": {"command": ["sh", "-lc", "docker info >/dev/null 2>&1"]},
                    "periodSeconds": 2,
                },
            }
        )

    containers.insert(
        0,
        {
            "name": settings.sandbox_container_name,
            "image": image,
            "command": ["sh", "-lc", "trap : TERM INT; while :; do sleep 3600; done"],
            "env": env,
            "resources": {"requests": resources, "limits": resources},
            "volumeMounts": volume_mounts,
            "readinessProbe": {
                "exec": {"command": ["sh", "-lc", "true"]},
                "periodSeconds": 2,
            },
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        },
    )

    annotations = {
        ORIGINAL_NAME_ANNOTATION: request.name,
        FINGERPRINT_ANNOTATION: request_fingerprint(request),
        AUTO_STOP_ANNOTATION: str(request.auto_stop_interval),
        LAST_ACTIVITY_ANNOTATION: (now or datetime.now(UTC)).isoformat(),
    }
    pod_spec: dict[str, object] = {
        "runtimeClassName": settings.runtime_class_name,
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "containers": containers,
        "volumes": volumes,
    }
    if node_selector:
        pod_spec["nodeSelector"] = node_selector

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": resource_name,
            "namespace": settings.namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": settings.hard_lifetime_seconds,
            "ttlSecondsAfterFinished": settings.finished_ttl_seconds,
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": pod_spec,
            },
        },
    }


def build_ingress_policy(resource_name: str, namespace: str) -> dict[str, object]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": f"{resource_name}-ingress", "namespace": namespace},
        "spec": {
            "podSelector": {"matchLabels": {SANDBOX_ID_LABEL: resource_name}},
            "policyTypes": ["Ingress"],
            "ingress": [],
        },
    }


def build_egress_policy(
    resource_name: str,
    namespace: str,
    allowed_addresses: list[str],
) -> dict[str, object]:
    cidrs, domains = resolve_allowed_addresses(allowed_addresses)
    destination_rule: dict[str, object] = {}
    if cidrs:
        destination_rule["toCIDR"] = cidrs
    if domains:
        destination_rule["toFQDNs"] = [{"matchName": domain} for domain in domains]

    return {
        "apiVersion": "cilium.io/v2",
        "kind": "CiliumNetworkPolicy",
        "metadata": {"name": f"{resource_name}-egress", "namespace": namespace},
        "spec": {
            "endpointSelector": {"matchLabels": {SANDBOX_ID_LABEL: resource_name}},
            "egress": [
                {
                    "toEndpoints": [
                        {
                            "matchLabels": {
                                "k8s:io.kubernetes.pod.namespace": "kube-system",
                                "k8s:k8s-app": "kube-dns",
                            }
                        }
                    ],
                    "toPorts": [
                        {
                            "ports": [
                                {"port": "53", "protocol": "UDP"},
                                {"port": "53", "protocol": "TCP"},
                            ]
                        }
                    ],
                },
                destination_rule,
            ],
        },
    }
