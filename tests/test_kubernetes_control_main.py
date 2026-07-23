"""Tests for Kubernetes control-service environment parsing."""

from collections.abc import Awaitable, Callable, Mapping
from typing import cast

import pytest

from benchmark_service.sandbox.kubernetes.control import janitor
from benchmark_service.sandbox.kubernetes.control import main as control_main
from benchmark_service.sandbox.kubernetes.control.main import load_settings


def test_loads_required_and_typed_control_service_settings() -> None:
    """Keep deployment settings explicit and secret-safe.

    Test cases:
    - Token, Docker image, and egress driver are required.
    - Boolean, numeric, mapping, image-prefix, and local-kubeconfig overrides are parsed.
    - Local kubeconfig stays disabled and the token is absent from repr by default.
    """
    for missing in [
        {},
        {"KUBERNETES_SANDBOX_API_TOKEN": "secret"},
        {"KUBERNETES_SANDBOX_DOCKER_IMAGE": "registry.internal/docker"},
        {
            "KUBERNETES_SANDBOX_API_TOKEN": "secret",
            "KUBERNETES_SANDBOX_DOCKER_IMAGE": "registry.internal/docker",
        },
    ]:
        with pytest.raises(ValueError, match="required"):
            load_settings(missing)

    settings = load_settings(
        {
            "KUBERNETES_SANDBOX_API_TOKEN": "secret-value",
            "KUBERNETES_SANDBOX_DOCKER_IMAGE": "registry.internal/docker@sha256:abc",
            "KUBERNETES_SANDBOX_AGENT_IMAGE": "registry.internal/control@sha256:def",
            "KUBERNETES_SANDBOX_AGENT_PORT": "8788",
            "KUBERNETES_SANDBOX_AGENT_CONNECT_TIMEOUT_SECONDS": "3.5",
            "KUBERNETES_SANDBOX_AGENT_HEARTBEAT_SECONDS": "7.5",
            "KUBERNETES_SANDBOX_DOCKER_ENABLED": "false",
            "KUBERNETES_SANDBOX_HARD_LIFETIME_SECONDS": "600",
            "KUBERNETES_SANDBOX_UPLOAD_LIMIT_BYTES": "4096",
            "KUBERNETES_SANDBOX_MAX_VCPU": "8",
            "KUBERNETES_SANDBOX_COMMAND_HEARTBEAT_SECONDS": "12.5",
            "KUBERNETES_SANDBOX_EXEC_CONNECTION_POOL_SIZE": "768",
            "KUBERNETES_SANDBOX_NODE_SELECTOR": "karpenter.sh/nodepool=sandbox,vals.ai/capacity=benchmark",
            "KUBERNETES_SANDBOX_POD_ANNOTATIONS": "karpenter.sh/do-not-disrupt=true,vals.ai/class=sandbox",
            "KUBERNETES_SANDBOX_EGRESS_DRIVER": "cilium",
            "KUBERNETES_SANDBOX_ALLOWED_IMAGE_PREFIXES": "registry.internal/, mirror.internal/",
            "KUBERNETES_SANDBOX_REQUIRE_IMAGE_DIGEST": "true",
        }
    )

    assert settings.docker_enabled is False
    assert settings.agent_image == "registry.internal/control@sha256:def"
    assert settings.agent_port == 8788
    assert settings.agent_connect_timeout_seconds == 3.5
    assert settings.agent_heartbeat_seconds == 7.5
    assert settings.hard_lifetime_seconds == 600
    assert settings.upload_limit_bytes == 4096
    assert settings.max_vcpu == 8
    assert settings.command_heartbeat_seconds == 12.5
    assert settings.exec_connection_pool_size == 768
    assert settings.sandbox_node_selector == {
        "karpenter.sh/nodepool": "sandbox",
        "vals.ai/capacity": "benchmark",
    }
    assert settings.sandbox_pod_annotations == {
        "karpenter.sh/do-not-disrupt": "true",
        "vals.ai/class": "sandbox",
    }
    assert settings.egress_driver == "cilium"
    assert settings.allowed_image_prefixes == ("registry.internal/", "mirror.internal/")
    assert settings.require_image_digest is True
    assert settings.allow_local_kubeconfig is False
    assert "secret-value" not in repr(settings)


async def test_rejects_unsupported_egress_before_kubernetes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject unsupported egress configuration before allocating Kubernetes resources.

    Test cases:
    - The control service does not create a Kubernetes client.
    - The one-shot janitor does not create a Kubernetes client.
    """
    client_created = False

    async def create_api(_settings: object) -> None:
        nonlocal client_created
        client_created = True

    monkeypatch.setattr(control_main.KubernetesAsyncioApi, "create", create_api)
    run_control_service = cast(
        Callable[[Mapping[str, str]], Awaitable[object]],
        getattr(control_main, "_serve"),
    )
    environ = {
        "KUBERNETES_SANDBOX_API_TOKEN": "test-token",
        "KUBERNETES_SANDBOX_DOCKER_IMAGE": "registry.internal/docker@sha256:abc",
        "KUBERNETES_SANDBOX_EGRESS_DRIVER": "unsupported",
    }

    for entrypoint in (run_control_service, janitor.run_janitor_once):
        with pytest.raises(ValueError, match="Unsupported Kubernetes egress driver: unsupported"):
            await entrypoint(environ)
        assert client_created is False
