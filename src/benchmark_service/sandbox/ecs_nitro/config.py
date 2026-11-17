from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field, field_validator

from benchmark_service.sandbox.types import UnsupportedSandboxConfigError

if TYPE_CHECKING:
    from benchmark_service.sandbox.types import SandboxProvider

logger = logging.getLogger("benchmark_service.sandbox.ecs_nitro")

PROVIDER_TYPE = "ecs_nitro"
ISOLATION_MODE = "ISOLATED_SANDBOX"
LAUNCH_TYPE = "MANAGED_INSTANCES"
# Isolated tasks get a fixed 20 GiB of ephemeral storage that a task definition cannot raise.
ISOLATED_TASK_DISK_GIB = 20
# A Nitro Sandbox needs at least 256 MiB, and the 1 GiB floor keeps the agent's interpreter comfortable.
MIN_MEMORY_GIB = 1
STARTED_BY = "cbs-ecs-nitro"
# Every isolated task charges its fixed disk, so the disk dimension caps the number of sandboxes.
CAPACITY_CLASS = "nitro-sandbox"
SANDBOX_ID_TAG = "cbs:sandbox-id"
SANDBOX_NAME_TAG = "cbs:sandbox-name"
SECURITY_GROUP_TAG = "cbs:security-group"
RESERVED_TAG_PREFIXES = ("aws:", "cbs:")
ENDED_STATUSES = frozenset({"DEPROVISIONING", "STOPPING", "STOPPED", "DELETED"})


class EcsNitroProviderConfig(BaseModel):
    type: Literal["ecs_nitro"] = "ecs_nitro"
    region: str = Field(min_length=1)
    cluster: str = Field(min_length=1)
    capacity_provider: str = Field(
        min_length=1, description="ECS Managed Instances capacity provider with Nitro Enclaves"
    )
    vpc_id: str = Field(min_length=1)
    subnet_ids: list[str] = Field(min_length=1, description="Private subnets for task ENIs")
    control_security_group_id: str = Field(
        min_length=1, description="Security group of the benchmark service host, the only agent client"
    )
    execution_role_arn: str = Field(
        min_length=1, description="Role ECS uses to pull images, write logs and read secrets"
    )
    agent_image: str = Field(min_length=1, description="Image built from docker/ecs-nitro-agent")
    agent_token_key: str = Field(
        min_length=32, repr=False, description="HMAC key that derives each sandbox's agent token"
    )
    agent_port: int = Field(default=7070, ge=1024, le=65535)
    egress_ceiling_cidrs: list[str] = Field(
        default_factory=list, description="IPv4 CIDRs sandbox egress may ever reach; empty denies all egress"
    )
    max_vcpu: float = Field(default=16, gt=0)
    max_memory_gib: int = Field(default=64, gt=0)
    max_sandboxes: int = Field(default=100, gt=0)
    log_group: str | None = Field(default=None, description="CloudWatch Logs group for agent and sandbox output")
    task_family_prefix: str = Field(
        default="cbs-ecs-nitro",
        pattern=r"^[A-Za-z0-9_-]{1,200}$",
        description="Task definition family prefix the execution IAM policy may scope RunTask to",
    )

    resource_tags: dict[str, str] = Field(
        default_factory=dict,
        description="Tags on every task, task definition and security group, such as valkyrie:deployment for IAM",
    )

    @field_validator("resource_tags")
    @classmethod
    def _validate_resource_tags(cls, value: dict[str, str]) -> dict[str, str]:
        reserved = sorted(key for key in value if key.startswith(RESERVED_TAG_PREFIXES))
        if reserved:
            raise ValueError(f"resource_tags cannot use reserved tag prefixes: {', '.join(reserved)}")
        return value

    @field_validator("egress_ceiling_cidrs")
    @classmethod
    def _validate_ceiling(cls, value: list[str]) -> list[str]:
        return [str(ipaddress.IPv4Network(cidr, strict=False)) for cidr in value]

    def create_provider(self) -> SandboxProvider:
        from benchmark_service.sandbox.ecs_nitro.provider import EcsNitroSandboxProvider

        return EcsNitroSandboxProvider(self)


def agent_token(key: str, sandbox_id: str) -> str:
    return hmac.new(key.encode(), sandbox_id.encode(), hashlib.sha256).hexdigest()


def task_tags(task: Mapping[str, Any]) -> dict[str, str]:
    items: list[dict[str, str]] = task.get("tags") or []
    return {item["key"]: item["value"] for item in items}


def iso(value: Any) -> str | None:
    return None if value is None else datetime.fromtimestamp(float(value), UTC).isoformat()


def now() -> str:
    return datetime.now(UTC).isoformat()


def log_timing(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, "provider": PROVIDER_TYPE, **fields}, default=str))


def validate_labels(labels: Mapping[str, str]) -> None:
    reserved = sorted(key for key in labels if key.startswith(RESERVED_TAG_PREFIXES))
    if reserved:
        raise UnsupportedSandboxConfigError(f"ecs_nitro labels cannot use reserved tag prefixes: {', '.join(reserved)}")
