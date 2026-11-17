from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncGenerator
from typing import Any

from benchmark_service.sandbox.ecs_nitro.aws import Ec2Api, EcsApi
from benchmark_service.sandbox.ecs_nitro.config import (
    CAPACITY_CLASS,
    ISOLATED_TASK_DISK_GIB,
    MIN_MEMORY_GIB,
    STARTED_BY,
    EcsNitroProviderConfig,
)
from benchmark_service.sandbox.types import (
    ImageSource,
    ResourceCapacity,
    Resources,
    SandboxCapacity,
    SandboxCapacityDomain,
    SandboxError,
    SandboxReservationDemand,
    SandboxSource,
    UnsupportedSandboxConfigError,
)


async def list_tasks(
    ecs: EcsApi, cluster: str, statuses: tuple[str, ...], page_size: int = 100
) -> AsyncGenerator[dict[str, Any], None]:
    for status in statuses:
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "cluster": cluster,
                "startedBy": STARTED_BY,
                "desiredStatus": status,
                "maxResults": min(max(page_size, 1), 100),
            }
            if token:
                params["nextToken"] = token
            page = await ecs.call("ListTasks", params)
            arns = page.get("taskArns", [])
            if arns:
                described = await ecs.call("DescribeTasks", {"cluster": cluster, "tasks": arns, "include": ["TAGS"]})
                for task in described.get("tasks", []):
                    yield task
            token = page.get("nextToken")
            if not token:
                break


def validate_source(source: SandboxSource) -> ImageSource:
    if not isinstance(source, ImageSource):
        raise UnsupportedSandboxConfigError(
            f"ecs_nitro runs prebuilt images only; {source.type} sources are not supported"
        )
    return source


class Admission:
    """Checks requests against the provider limits and the cluster's Nitro capacity before any task launches."""

    def __init__(self, config: EcsNitroProviderConfig, ecs: EcsApi, ec2: Ec2Api) -> None:
        self.config = config
        self.ecs = ecs
        self.ec2 = ec2
        self._verified = False
        self._lock = asyncio.Lock()

    @property
    def target_id(self) -> str:
        return f"{self.config.region}/{self.config.cluster}/{self.config.capacity_provider}"

    def validate_resources(self, *, requires_nested_docker: bool, resources: Resources) -> None:
        if requires_nested_docker:
            raise UnsupportedSandboxConfigError("ecs_nitro tasks cannot run nested Docker")
        if resources.gpu or resources.acceptable_gpu_types:
            raise UnsupportedSandboxConfigError(
                "ecs_nitro does not support GPUs: Nitro Sandboxes have no GPU passthrough"
            )
        if resources.vcpu > self.config.max_vcpu:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro allows at most {self.config.max_vcpu} vCPU, requested {resources.vcpu}"
            )
        if not MIN_MEMORY_GIB <= resources.memory <= self.config.max_memory_gib:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro memory must be {MIN_MEMORY_GIB}-{self.config.max_memory_gib} GiB, requested {resources.memory}"
            )
        if resources.disk > ISOLATED_TASK_DISK_GIB:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro isolated tasks have a fixed {ISOLATED_TASK_DISK_GIB} GiB disk, requested {resources.disk}"
            )

    async def verify_infrastructure(self) -> None:
        if self._verified:
            return
        async with self._lock:
            if self._verified:
                return
            config = self.config
            clusters = await self.ecs.call("DescribeClusters", {"clusters": [config.cluster]})
            cluster = next(iter(clusters.get("clusters", [])), None)
            if cluster is None or cluster.get("status") != "ACTIVE":
                raise SandboxError(f"ECS cluster {config.cluster} is not ACTIVE")
            if config.capacity_provider not in cluster.get("capacityProviders", []):
                raise SandboxError(f"capacity provider {config.capacity_provider} is not attached to {config.cluster}")
            described = await self.ecs.call(
                "DescribeCapacityProviders",
                {"capacityProviders": [config.capacity_provider]},
            )
            provider = next(iter(described.get("capacityProviders", [])), None)
            if provider is None or provider.get("status") != "ACTIVE":
                raise SandboxError(f"capacity provider {config.capacity_provider} is not ACTIVE")
            managed = provider.get("managedInstancesProvider")
            if not managed:
                raise SandboxError(
                    f"capacity provider {config.capacity_provider} is not an ECS Managed Instances provider"
                )
            if managed.get("instanceLaunchTemplate", {}).get("nitroEnclavesEnabled") is not True:
                raise SandboxError(f"capacity provider {config.capacity_provider} does not enable Nitro Enclaves")
            subnets = await self.ec2.call("describe_subnets", {"SubnetIds": config.subnet_ids})
            for subnet in subnets.get("Subnets", []):
                if subnet.get("VpcId") != config.vpc_id:
                    raise SandboxError(f"subnet {subnet.get('SubnetId')} is not in {config.vpc_id}")
                if subnet.get("MapPublicIpOnLaunch"):
                    raise SandboxError(f"subnet {subnet.get('SubnetId')} assigns public IPs and is not private")
            self._verified = True

    def demand(self, source: SandboxSource, resources: Resources) -> SandboxReservationDemand:
        validate_source(source)
        self.validate_resources(requires_nested_docker=False, resources=resources)
        return SandboxReservationDemand(
            target_id=self.target_id,
            sandbox_class=CAPACITY_CLASS,
            vcpu=math.ceil(resources.vcpu * 1024) / 1024,
            memory=resources.memory,
            disk=ISOLATED_TASK_DISK_GIB,
            gpu=0,
        )

    async def capacity_domains(self) -> list[SandboxCapacityDomain]:
        await self.verify_infrastructure()
        vcpu = memory = 0.0
        count = 0
        async for task in list_tasks(self.ecs, self.config.cluster, ("RUNNING",)):
            count += 1
            vcpu += float(task.get("cpu") or 0) / 1024
            memory += float(task.get("memory") or 0) / 1024
        limit = self.config.max_sandboxes
        capacity = SandboxCapacity(
            cpu=ResourceCapacity(total=limit * self.config.max_vcpu, used=vcpu),
            memory=ResourceCapacity(total=limit * self.config.max_memory_gib, used=memory),
            disk=ResourceCapacity(total=limit * ISOLATED_TASK_DISK_GIB, used=count * ISOLATED_TASK_DISK_GIB),
        )
        return [SandboxCapacityDomain(target_id=self.target_id, sandbox_class=CAPACITY_CLASS, capacity=capacity)]

    async def admits(self, source: SandboxSource, resources: Resources) -> bool:
        [domain] = await self.capacity_domains()
        demand = self.demand(source, resources)
        capacity = domain.capacity
        return (
            demand.vcpu <= capacity.cpu.available
            and demand.memory <= capacity.memory.available
            and demand.disk <= capacity.disk.available
        )
