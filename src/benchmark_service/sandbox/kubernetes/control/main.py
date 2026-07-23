"""Load control-service settings and start the Kubernetes control process."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping

import uvicorn

from benchmark_service.sandbox.kubernetes.control.agent import PodAgentClient
from benchmark_service.sandbox.kubernetes.control.api import KubernetesAsyncioApi
from benchmark_service.sandbox.kubernetes.control.app import create_kubernetes_control_app
from benchmark_service.sandbox.kubernetes.control.cache import SandboxResourceCache
from benchmark_service.sandbox.kubernetes.control.egress import select_egress_driver
from benchmark_service.sandbox.kubernetes.control.kubernetes import KubernetesSandboxBackend
from benchmark_service.sandbox.kubernetes.control.remote_exec import KubernetesRemoteExec
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _boolean(environ: Mapping[str, str], name: str, default: bool) -> bool:
    value = environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _integer(environ: Mapping[str, str], name: str, default: int) -> int:
    value = environ.get(name)
    return int(value) if value is not None else default


def _float(environ: Mapping[str, str], name: str, default: float) -> float:
    value = environ.get(name)
    return float(value) if value is not None else default


def _mapping(environ: Mapping[str, str], name: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in environ.get(name, "").split(","):
        if not item.strip():
            continue
        key, separator, value = item.partition("=")
        if not separator or not key.strip() or not value.strip():
            raise ValueError(f"{name} must contain comma-separated key=value pairs")
        result[key.strip()] = value.strip()
    return result


def load_settings(environ: Mapping[str, str] | None = None) -> KubernetesControlSettings:
    """Load control-service settings without accessing Kubernetes."""
    values = os.environ if environ is None else environ
    prefixes = tuple(
        prefix.strip()
        for prefix in values.get("KUBERNETES_SANDBOX_ALLOWED_IMAGE_PREFIXES", "").split(",")
        if prefix.strip()
    )
    return KubernetesControlSettings(
        namespace=values.get("KUBERNETES_SANDBOX_NAMESPACE", "benchmark-sandboxes"),
        api_token=_required(values, "KUBERNETES_SANDBOX_API_TOKEN"),
        runtime_class_name=values.get("KUBERNETES_SANDBOX_RUNTIME_CLASS", "kata-qemu"),
        sandbox_container_name=values.get("KUBERNETES_SANDBOX_CONTAINER_NAME", "sandbox"),
        agent_image=values.get("KUBERNETES_SANDBOX_AGENT_IMAGE"),
        agent_port=_integer(values, "KUBERNETES_SANDBOX_AGENT_PORT", 8787),
        agent_connect_timeout_seconds=_float(
            values,
            "KUBERNETES_SANDBOX_AGENT_CONNECT_TIMEOUT_SECONDS",
            5,
        ),
        agent_heartbeat_seconds=_float(values, "KUBERNETES_SANDBOX_AGENT_HEARTBEAT_SECONDS", 10),
        docker_image=_required(values, "KUBERNETES_SANDBOX_DOCKER_IMAGE"),
        docker_enabled=_boolean(values, "KUBERNETES_SANDBOX_DOCKER_ENABLED", True),
        hard_lifetime_seconds=_integer(values, "KUBERNETES_SANDBOX_HARD_LIFETIME_SECONDS", 86400),
        finished_ttl_seconds=_integer(values, "KUBERNETES_SANDBOX_FINISHED_TTL_SECONDS", 300),
        exec_output_limit_bytes=_integer(values, "KUBERNETES_SANDBOX_EXEC_OUTPUT_LIMIT_BYTES", 16 * 1024 * 1024),
        upload_limit_bytes=_integer(values, "KUBERNETES_SANDBOX_UPLOAD_LIMIT_BYTES", 8 * 1024 * 1024 * 1024),
        activity_write_interval_seconds=_float(
            values,
            "KUBERNETES_SANDBOX_ACTIVITY_WRITE_INTERVAL_SECONDS",
            30,
        ),
        command_heartbeat_seconds=_float(values, "KUBERNETES_SANDBOX_COMMAND_HEARTBEAT_SECONDS", 15),
        exec_connection_pool_size=_integer(values, "KUBERNETES_SANDBOX_EXEC_CONNECTION_POOL_SIZE", 512),
        max_create_timeout_seconds=_integer(values, "KUBERNETES_SANDBOX_MAX_CREATE_TIMEOUT_SECONDS", 900),
        max_vcpu=_integer(values, "KUBERNETES_SANDBOX_MAX_VCPU", 64),
        max_memory_gib=_integer(values, "KUBERNETES_SANDBOX_MAX_MEMORY_GIB", 256),
        max_disk_gib=_integer(values, "KUBERNETES_SANDBOX_MAX_DISK_GIB", 1024),
        max_gpu=_integer(values, "KUBERNETES_SANDBOX_MAX_GPU", 8),
        gpu_resource_name=values.get("KUBERNETES_SANDBOX_GPU_RESOURCE_NAME", "nvidia.com/gpu"),
        gpu_type_label=values.get("KUBERNETES_SANDBOX_GPU_TYPE_LABEL", "sandbox.vals.ai/gpu-type"),
        sandbox_node_selector=_mapping(values, "KUBERNETES_SANDBOX_NODE_SELECTOR"),
        sandbox_pod_annotations=_mapping(values, "KUBERNETES_SANDBOX_POD_ANNOTATIONS"),
        egress_driver=_required(values, "KUBERNETES_SANDBOX_EGRESS_DRIVER"),
        allowed_image_prefixes=prefixes,
        require_image_digest=_boolean(values, "KUBERNETES_SANDBOX_REQUIRE_IMAGE_DIGEST", False),
        allow_local_kubeconfig=_boolean(values, "KUBERNETES_SANDBOX_ALLOW_LOCAL_KUBECONFIG", False),
    )


async def _serve(environ: Mapping[str, str]) -> None:
    settings = load_settings(environ)
    egress_factory = select_egress_driver(settings)
    api = await KubernetesAsyncioApi.create(settings)
    resource_cache = SandboxResourceCache(settings.namespace, api)
    await resource_cache.start()
    remote_exec = None if settings.agent_image is not None else KubernetesRemoteExec(settings)
    pod_agent = PodAgentClient(settings) if settings.agent_image is not None else None
    egress = egress_factory(api)
    backend = KubernetesSandboxBackend(
        settings,
        api,
        remote_exec,
        egress,
        resource_cache=resource_cache,
        pod_agent=pod_agent,
    )
    app = create_kubernetes_control_app(settings, backend, readiness=resource_cache.ready)
    host = environ.get("KUBERNETES_SANDBOX_HOST", "0.0.0.0")
    port = _integer(environ, "KUBERNETES_SANDBOX_PORT", 8080)
    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port))
    await server.serve()


def main() -> None:
    """Run the private control service without installing cluster resources."""
    asyncio.run(_serve(os.environ))
