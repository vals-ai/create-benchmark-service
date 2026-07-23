from __future__ import annotations

from typing import Protocol

from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.kubernetes.control.api import KubernetesApi
from benchmark_service.sandbox.kubernetes.control.resources import SANDBOX_ID_LABEL, sandbox_name
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.types import SandboxError


class EgressPolicyDriver(Protocol):
    async def apply(self, instance_id: str, allowed_addresses: list[str]) -> None: ...

    async def clear(self, instance_id: str) -> None: ...


def build_egress_policy(
    resource_name: str,
    namespace: str,
    allowed_addresses: list[str],
) -> dict[str, object]:
    """Build one Cilium egress policy for a sandbox."""
    cidrs, domains = resolve_allowed_addresses(allowed_addresses)
    destination_rules: list[dict[str, object]] = []
    if cidrs:
        destination_rules.append({"toCIDR": cidrs})
    if domains:
        destination_rules.append({"toFQDNs": [{"matchName": domain} for domain in domains]})

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
                                {"port": "53", "protocol": "ANY"},
                            ],
                            "rules": {"dns": [{"matchPattern": "*"}]},
                        }
                    ],
                },
                *destination_rules,
            ],
        },
    }


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
