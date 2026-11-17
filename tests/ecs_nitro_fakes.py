from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ecs_nitro_agents import KEY

from benchmark_service.sandbox.ecs_nitro.config import (
    EcsNitroProviderConfig,
)


def make_config(port: int = 7070, **overrides: Any) -> EcsNitroProviderConfig:
    values: dict[str, Any] = {
        "region": "us-east-1",
        "cluster": "proof",
        "capacity_provider": "nitro-mi",
        "vpc_id": "vpc-1",
        "subnet_ids": ["subnet-a"],
        "control_security_group_id": "sg-control",
        "execution_role_arn": "arn:aws:iam::123:role/exec",
        "agent_image": "123.dkr.ecr.us-east-1.amazonaws.com/agent@sha256:abc",
        "agent_token_key": KEY,
        "agent_port": port,
        "egress_ceiling_cidrs": ["10.0.0.0/16"],
    }
    values.update(overrides)
    return EcsNitroProviderConfig(**values)


class FakeEc2:
    def __init__(self) -> None:
        self.groups: dict[str, list[dict[str, Any]]] = {}
        self.ingress: dict[str, list[dict[str, Any]]] = {}
        self.deleted: list[str] = []
        self.created: list[dict[str, Any]] = []

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        if action == "describe_subnets":
            return {"Subnets": [{"SubnetId": "subnet-a", "VpcId": "vpc-1", "MapPublicIpOnLaunch": False}]}
        if action == "create_security_group":
            self.created.append(dict(params))
            group = f"sg-{len(self.groups)}"
            self.groups[group] = [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]
            return {"GroupId": group}
        if action == "describe_security_groups":
            group = params["GroupIds"][0]
            return {"SecurityGroups": [{"IpPermissionsEgress": list(self.groups[group])}]}
        if action == "revoke_security_group_egress":
            self.groups[params["GroupId"]] = []
            return {}
        if action == "authorize_security_group_egress":
            self.groups[params["GroupId"]].extend(params["IpPermissions"])
            return {}
        if action == "authorize_security_group_ingress":
            self.ingress[params["GroupId"]] = list(params["IpPermissions"])
            return {}
        if action == "delete_security_group":
            self.deleted.append(params["GroupId"])
            self.groups.pop(params["GroupId"], None)
            return {}
        raise AssertionError(f"unexpected EC2 call {action}")


def egress_of(ec2: FakeEc2, group: str) -> list[str]:
    return [r["CidrIp"] for permission in ec2.groups[group] for r in permission.get("IpRanges", [])]
