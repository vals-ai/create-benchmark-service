from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any

from ecs_nitro_agents import KEY

from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError
from benchmark_service.sandbox.ecs_nitro.config import (
    SANDBOX_ID_TAG,
    SECURITY_GROUP_TAG,
    STARTED_BY,
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
        self.invisible_calls = 0

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        if action == "describe_subnets":
            return {"Subnets": [{"SubnetId": "subnet-a", "VpcId": "vpc-1", "MapPublicIpOnLaunch": False}]}
        if action not in ("create_security_group", "delete_security_group", "describe_subnets"):
            if self.invisible_calls:
                self.invisible_calls -= 1
                raise AwsApiError("ec2", action, "InvalidGroup.NotFound", "group not visible yet")
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


def run_task_params() -> dict[str, Any]:
    return {
        "startedBy": STARTED_BY,
        "capacityProviderStrategy": [{"capacityProvider": "nitro-mi", "weight": 1}],
        "tags": [{"key": SANDBOX_ID_TAG, "value": "no-agent"}, {"key": SECURITY_GROUP_TAG, "value": "sg-x"}],
    }


class FakeEcs:
    def __init__(
        self, *, nitro: bool = True, run_status: str = "RUNNING", place: bool = True, cluster_name: str | None = "proof"
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.tasks: dict[str, dict[str, Any]] = {}
        self.definitions: dict[str, dict[str, Any]] = {}
        self.nitro = nitro
        self.run_status = run_status
        self.place = place
        self.tokens: dict[str, str] = {}
        self.describe_lag = 0
        self.cluster_name = cluster_name

    async def call(self, action: str, params: Mapping[str, Any]) -> dict[str, Any]:
        self.calls.append((action, dict(params)))
        if action == "DescribeClusters":
            cluster = {
                "clusterArn": "arn:aws:ecs:us-east-1:123456789012:cluster/proof",
                "status": "ACTIVE",
                "capacityProviders": ["nitro-mi"],
            }
            if self.cluster_name is not None:
                cluster["clusterName"] = self.cluster_name
            return {"clusters": [cluster]}
        if action == "DescribeCapacityProviders":
            if not re.fullmatch(r"[a-zA-Z0-9\-_]+", params.get("cluster", "")):
                raise AwsApiError("ecs", action, "ClientException", "cluster name must match ^[a-zA-Z0-9\\-_]+$")
            template = {"nitroEnclavesEnabled": self.nitro}
            return {
                "capacityProviders": [
                    {"status": "ACTIVE", "managedInstancesProvider": {"instanceLaunchTemplate": template}}
                ]
            }
        if action == "DescribeTaskDefinition":
            family = params["taskDefinition"]
            if family not in self.definitions:
                raise AwsApiError("ecs", action, "ClientException", "Unable to describe task definition.")
            return {"taskDefinition": self.definitions[family]}
        if action == "RegisterTaskDefinition":
            definition = {**params, "taskDefinitionArn": f"arn:td/{params['family']}:1", "status": "ACTIVE"}
            self.definitions[params["family"]] = definition
            return {"taskDefinition": definition}
        if action == "RunTask" and params.get("clientToken") in self.tokens:
            return {"tasks": [self.tasks[self.tokens[params["clientToken"]]]]}
        if action == "RunTask":
            if not self.place:
                return {"tasks": [], "failures": [{"reason": "RESOURCE:CPU"}]}
            task_id = f"task{len(self.tasks)}"
            task = {
                "taskArn": f"arn:aws:ecs:us-east-1:123:task/proof/{task_id}",
                "lastStatus": self.run_status,
                "startedBy": params["startedBy"],
                "capacityProviderName": params["capacityProviderStrategy"][0]["capacityProvider"],
                "launchType": "MANAGED_INSTANCES",
                "createdAt": time.time(),
                "cpu": params.get("overrides", {}).get("cpu"),
                "memory": params.get("overrides", {}).get("memory"),
                "tags": params["tags"],
                "attachments": [
                    {
                        "type": "ElasticNetworkInterface",
                        "details": [{"name": "privateIPv4Address", "value": "127.0.0.1"}],
                    }
                ],
            }
            self.tasks[task_id] = task
            if params.get("clientToken"):
                self.tokens[params["clientToken"]] = task_id
            return {"tasks": [task]}
        if action == "DescribeTasks" and self.describe_lag:
            self.describe_lag -= 1
            return {"tasks": []}
        if action == "DescribeTasks":
            found = [
                self.tasks[ref.rsplit("/", 1)[-1]] for ref in params["tasks"] if ref.rsplit("/", 1)[-1] in self.tasks
            ]
            return {"tasks": found}
        if action == "StopTask":
            self.tasks[params["task"].rsplit("/", 1)[-1]]["lastStatus"] = "STOPPED"
            return {}
        if action == "ListTasks":
            stopped = params.get("desiredStatus") == "STOPPED"
            return {
                "taskArns": [t["taskArn"] for t in self.tasks.values() if (t["lastStatus"] == "STOPPED") == stopped]
            }
        raise AssertionError(f"unexpected ECS call {action}")

    async def close(self) -> None:
        pass
