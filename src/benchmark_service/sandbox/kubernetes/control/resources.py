"""Build validated Kubernetes Job resources for sandbox runtime instances."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime

from benchmark_service.sandbox.kubernetes.control.agent import agent_token
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
USER_LABELS_ANNOTATION = "sandbox.vals.ai/user-labels"
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


def _validate_request(request: SandboxCreateRequest, settings: KubernetesControlSettings) -> ImageSource:
    if not isinstance(request.source, ImageSource):
        raise SandboxError(f"Kubernetes sandbox provider does not support source type: {request.source.type}")

    source = request.source
    _validate_image(source.image, settings)
    if settings.agent_image is not None:
        _validate_image(settings.agent_image, settings)
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

    return source


def _resource_requirements(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
) -> tuple[dict[str, str], dict[str, str]]:
    resources = {
        "cpu": str(request.resources.vcpu),
        "memory": f"{request.resources.memory}Gi",
        "ephemeral-storage": f"{request.resources.disk}Gi",
    }
    node_selector = dict(settings.sandbox_node_selector)
    if request.resources.gpu:
        resources[settings.gpu_resource_name] = str(request.resources.gpu)
        assert request.resources.gpu_type is not None
        node_selector[settings.gpu_type_label] = request.resources.gpu_type

    return resources, node_selector


def _build_agent_init_container(settings: KubernetesControlSettings) -> dict[str, object]:
    assert settings.agent_image is not None

    # The init container injects the control binary without changing benchmark images.
    return {
        "name": "install-sandbox-agent",
        "image": settings.agent_image,
        "command": [
            "cp",
            "/usr/local/bin/kubernetes-sandbox-agent",
            "/vals-agent/sandbox-agent",
        ],
        "volumeMounts": [{"name": "sandbox-agent", "mountPath": "/vals-agent"}],
        "securityContext": {
            "runAsUser": 0,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
    }


def _build_docker_sidecar(settings: KubernetesControlSettings) -> dict[str, object]:
    # Privileged access stays confined to the Docker sidecar.
    return {
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


def _build_sandbox_container(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    source: ImageSource,
    resource_name: str,
) -> dict[str, object]:
    env = [{"name": name, "value": value} for name, value in sorted(request.env_vars.items())]
    volume_mounts: list[dict[str, object]] = [{"name": "workspace", "mountPath": "/workspace"}]
    sandbox_command = ["sh", "-lc", "trap : TERM INT; while :; do sleep 3600; done"]
    sandbox_readiness: dict[str, object] = {
        "exec": {"command": ["sh", "-lc", "true"]},
        "periodSeconds": 2,
    }
    sandbox_ports: list[dict[str, object]] = []
    if settings.agent_image is not None:
        env.extend(
            [
                {
                    "name": "VALS_SANDBOX_AGENT_TOKEN",
                    "value": agent_token(settings.api_token, resource_name),
                },
                {"name": "VALS_SANDBOX_AGENT_PORT", "value": str(settings.agent_port)},
                {
                    "name": "VALS_SANDBOX_AGENT_HEARTBEAT_SECONDS",
                    "value": str(settings.agent_heartbeat_seconds),
                },
            ]
        )
        volume_mounts.append({"name": "sandbox-agent", "mountPath": "/vals-agent", "readOnly": True})
        sandbox_command = ["/vals-agent/sandbox-agent"]
        sandbox_readiness = {
            "tcpSocket": {"port": "agent"},
            "periodSeconds": 2,
        }
        sandbox_ports = [
            {"name": "agent", "containerPort": settings.agent_port, "protocol": "TCP"},
        ]
    if settings.docker_enabled:
        env.append({"name": "DOCKER_HOST", "value": "unix:///var/run/docker.sock"})
        volume_mounts.append({"name": "docker-socket", "mountPath": "/var/run"})

    resources, _node_selector = _resource_requirements(request, settings)

    return {
        "name": settings.sandbox_container_name,
        "image": source.image,
        "command": sandbox_command,
        "env": env,
        "resources": {"requests": resources, "limits": resources},
        "volumeMounts": volume_mounts,
        "ports": sandbox_ports,
        "readinessProbe": sandbox_readiness,
        "securityContext": {
            "allowPrivilegeEscalation": False,
        },
    }


def _build_pod_spec(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    source: ImageSource,
    resource_name: str,
) -> dict[str, object]:
    containers = [_build_sandbox_container(request, settings, source, resource_name)]
    volumes: list[dict[str, object]] = [{"name": "workspace", "emptyDir": {"sizeLimit": f"{request.resources.disk}Gi"}}]
    init_containers: list[dict[str, object]] = []
    if settings.agent_image is not None:
        volumes.append({"name": "sandbox-agent", "emptyDir": {}})
        init_containers.append(_build_agent_init_container(settings))
    if settings.docker_enabled:
        volumes.append({"name": "docker-socket", "emptyDir": {}})
        containers.append(_build_docker_sidecar(settings))

    _, node_selector = _resource_requirements(request, settings)
    pod_spec: dict[str, object] = {
        "runtimeClassName": settings.runtime_class_name,
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "tolerations": [
            {
                "key": "sandbox.vals.ai/dedicated",
                "operator": "Equal",
                "value": "sandboxes",
                "effect": "NoSchedule",
            }
        ],
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "containers": containers,
        "volumes": volumes,
    }
    if init_containers:
        pod_spec["initContainers"] = init_containers
    if node_selector:
        pod_spec["nodeSelector"] = node_selector

    return pod_spec


def _build_annotations(request: SandboxCreateRequest, now: datetime) -> dict[str, str]:
    return {
        ORIGINAL_NAME_ANNOTATION: request.name,
        FINGERPRINT_ANNOTATION: request_fingerprint(request),
        AUTO_STOP_ANNOTATION: str(request.auto_stop_interval),
        LAST_ACTIVITY_ANNOTATION: now.isoformat(),
        USER_LABELS_ANNOTATION: json.dumps(request.labels, sort_keys=True, separators=(",", ":")),
    }


def build_job(
    request: SandboxCreateRequest,
    settings: KubernetesControlSettings,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    source = _validate_request(request, settings)
    resource_name = sandbox_name(request.name)
    labels = _labels(request, resource_name)
    annotations = _build_annotations(request, now or datetime.now(UTC))
    pod_spec = _build_pod_spec(request, settings, source, resource_name)

    # Deployment annotations cannot replace control-owned lifecycle metadata.
    pod_annotations = {**settings.sandbox_pod_annotations, **annotations}

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
                "metadata": {"labels": labels, "annotations": pod_annotations},
                "spec": pod_spec,
            },
        },
    }
