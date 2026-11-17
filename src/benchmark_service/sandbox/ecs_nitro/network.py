from __future__ import annotations

import asyncio
import ipaddress
import time
from collections.abc import Mapping
from typing import Any

from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError, Ec2Api
from benchmark_service.sandbox.ecs_nitro.config import SANDBOX_ID_TAG, EcsNitroProviderConfig
from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import SandboxError, UnsupportedSandboxConfigError


class SecurityGroups:
    """One security group per sandbox: agent ingress from the control group only, egress inside the ceiling."""

    def __init__(
        self, config: EcsNitroProviderConfig, ec2: Ec2Api, *, poll_seconds: float, delete_timeout: float
    ) -> None:
        self.config = config
        self.ec2 = ec2
        self._poll = poll_seconds
        self._delete_timeout = delete_timeout

    async def _call(self, action: str, params: Mapping[str, Any], visible_by: float | None) -> dict[str, Any]:
        # EC2 is eventually consistent: a group it just created can still read as InvalidGroup.NotFound.
        while True:
            try:
                return await self.ec2.call(action, params)
            except AwsApiError as error:
                if error.code != "InvalidGroup.NotFound" or visible_by is None:
                    raise
                if time.monotonic() >= visible_by:
                    raise SandboxError(f"new security group was still not visible to {action}: {error}") from error
            await asyncio.sleep(self._poll)

    def allowed_egress(self, allowed_addresses: list[str]) -> list[str]:
        cidrs, domains = resolve_allowed_addresses(allowed_addresses)
        if domains:
            raise UnsupportedSandboxConfigError(
                "ecs_nitro enforces egress with security group CIDRs and cannot allow hostnames: "
                f"{', '.join(domains)}. Route host policies through the egress proxy and allow its CIDRs."
            )
        ceiling = [ipaddress.IPv4Network(cidr) for cidr in self.config.egress_ceiling_cidrs]
        outside = [
            cidr for cidr in cidrs if not any(ipaddress.IPv4Network(cidr).subnet_of(allowed) for allowed in ceiling)
        ]
        if outside:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro egress CIDRs are outside the customer egress ceiling: {', '.join(outside)}"
            )
        return sorted({str(ipaddress.IPv4Network(cidr)) for cidr in cidrs})

    async def create(self, sandbox_id: str, initial_egress: list[str]) -> str:
        created = await self.ec2.call(
            "create_security_group",
            {
                "GroupName": f"cbs-ecs-nitro-{sandbox_id}",
                "Description": "CBS ECS Nitro sandbox",
                "VpcId": self.config.vpc_id,
                "TagSpecifications": [
                    {
                        "ResourceType": "security-group",
                        "Tags": [
                            *({"Key": key, "Value": value} for key, value in self.config.resource_tags.items()),
                            {"Key": SANDBOX_ID_TAG, "Value": sandbox_id},
                        ],
                    }
                ],
            },
        )
        group_id = str(created["GroupId"])
        visible_by = time.monotonic() + self._delete_timeout
        try:
            await self.set_egress(group_id, initial_egress, visible_by=visible_by)
            await self._call(
                "authorize_security_group_ingress",
                {
                    "GroupId": group_id,
                    "IpPermissions": [
                        {
                            "IpProtocol": "tcp",
                            "FromPort": self.config.agent_port,
                            "ToPort": self.config.agent_port,
                            "UserIdGroupPairs": [{"GroupId": self.config.control_security_group_id}],
                        }
                    ],
                },
                visible_by,
            )
        except BaseException:
            await asyncio.shield(self.delete(group_id))
            raise
        return group_id

    async def set_egress(self, group_id: str, cidrs: list[str], *, visible_by: float | None = None) -> None:
        if not group_id:
            raise SandboxError("sandbox has no security group to hold its egress rules")
        described = await self._call("describe_security_groups", {"GroupIds": [group_id]}, visible_by)
        current = described["SecurityGroups"][0].get("IpPermissionsEgress", [])
        if current:
            await self._call(
                "revoke_security_group_egress", {"GroupId": group_id, "IpPermissions": current}, visible_by
            )
        if cidrs:
            await self._call(
                "authorize_security_group_egress",
                {
                    "GroupId": group_id,
                    "IpPermissions": [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": cidr} for cidr in cidrs]}],
                },
                visible_by,
            )

    async def delete(self, group_id: str) -> None:
        deadline = time.monotonic() + self._delete_timeout
        while True:
            try:
                await self.ec2.call("delete_security_group", {"GroupId": group_id})
                return
            except AwsApiError as error:
                if error.code == "InvalidGroup.NotFound":
                    return
                if error.code != "DependencyViolation" or time.monotonic() >= deadline:
                    raise SandboxError(f"security group {group_id} was not deleted: {error}") from error
            await asyncio.sleep(self._poll)
