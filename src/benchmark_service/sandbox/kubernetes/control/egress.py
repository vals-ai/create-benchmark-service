"""Select the deployment-configured egress policy implementation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApi
from benchmark_service.sandbox.kubernetes.control.cilium_egress import CiliumEgressPolicyDriver
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings


class EgressPolicyDriver(Protocol):
    async def apply(self, instance_id: str, allowed_addresses: list[str]) -> None: ...

    async def clear(self, instance_id: str) -> None: ...


type EgressPolicyDriverFactory = Callable[[KubernetesApi], EgressPolicyDriver]


def select_egress_driver(settings: KubernetesControlSettings) -> EgressPolicyDriverFactory:
    """Select the deployment-configured egress driver before cluster access."""
    if settings.egress_driver == "cilium":
        return lambda api: CiliumEgressPolicyDriver(settings, api)
    raise ValueError(f"Unsupported Kubernetes egress driver: {settings.egress_driver}")


def create_egress_driver(
    settings: KubernetesControlSettings,
    api: KubernetesApi,
) -> EgressPolicyDriver:
    """Create the deployment-selected egress policy driver."""
    return select_egress_driver(settings)(api)
