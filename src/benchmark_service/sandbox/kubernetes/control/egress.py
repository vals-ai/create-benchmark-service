from __future__ import annotations

from typing import Protocol

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApi
from benchmark_service.sandbox.kubernetes.control.resources import build_egress_policy, sandbox_name
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import SandboxError


class EgressPolicyDriver(Protocol):
    async def apply(self, instance_id: str, allowed_addresses: list[str]) -> None: ...

    async def clear(self, instance_id: str) -> None: ...


class CiliumEgressPolicyDriver:
    """Replace per-sandbox Cilium policy without changing baseline ingress."""

    def __init__(self, settings: KubernetesControlSettings, api: KubernetesApi) -> None:
        self.settings = settings
        self.api = api

    async def apply(self, instance_id: str, allowed_addresses: list[str]) -> None:
        resource_name = sandbox_name(instance_id)
        try:
            body = build_egress_policy(resource_name, self.settings.namespace, allowed_addresses)
        except ValueError as error:
            raise SandboxError(str(error)) from error
        await self.api.replace_custom_object(
            self.settings.namespace,
            "ciliumnetworkpolicies",
            f"{resource_name}-egress",
            body,
        )

    async def clear(self, instance_id: str) -> None:
        resource_name = sandbox_name(instance_id)
        await self.api.delete_custom_object(
            self.settings.namespace,
            "ciliumnetworkpolicies",
            f"{resource_name}-egress",
        )
