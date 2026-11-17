from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import AsyncGenerator, Mapping
from typing import Any

from benchmark_service.sandbox.ecs_nitro.admission import Admission, list_tasks, validate_source
from benchmark_service.sandbox.ecs_nitro.agent import IDLE_TIMEOUT_ENV, PORT_ENV, TOKEN_ENV
from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError, Boto3Ec2Client, Ec2Api, EcsApi, EcsJsonClient
from benchmark_service.sandbox.ecs_nitro.config import (
    ENDED_STATUSES,
    LAUNCH_TYPE,
    PROVIDER_TYPE,
    SANDBOX_ID_TAG,
    SANDBOX_NAME_TAG,
    SECURITY_GROUP_TAG,
    STARTED_BY,
    EcsNitroProviderConfig,
    agent_token,
    iso,
    log_timing,
    logger,
    now,
    task_tags,
    validate_labels,
)
from benchmark_service.sandbox.ecs_nitro.network import SecurityGroups
from benchmark_service.sandbox.ecs_nitro.sandbox import EcsNitroSandbox
from benchmark_service.sandbox.ecs_nitro.task_definition import SANDBOX_CONTAINER, TaskDefinitions
from benchmark_service.sandbox.types import (
    Resources,
    Sandbox,
    SandboxCapacityDomain,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxReservationDemand,
    SandboxSource,
    UnsupportedSandboxConfigError,
)

_SECRET_ARN_PREFIXES = ("arn:aws:secretsmanager:", "arn:aws:ssm:")
_TASK_POLL_SECONDS = 3.0
_AGENT_POLL_SECONDS = 1.0
_STOP_TIMEOUT_SECONDS = 300.0


class EcsNitroSandboxProvider(SandboxProvider):
    def __init__(
        self,
        config: EcsNitroProviderConfig,
        *,
        ecs: EcsApi | None = None,
        ec2: Ec2Api | None = None,
        poll_seconds: float = _TASK_POLL_SECONDS,
        cleanup_timeout: float = _STOP_TIMEOUT_SECONDS,
    ) -> None:
        self.config = config
        self.ecs = ecs or EcsJsonClient(config.region)
        ec2 = ec2 or Boto3Ec2Client(config.region)
        self._poll = poll_seconds
        self._cleanup_timeout = cleanup_timeout
        self.admission = Admission(config, self.ecs, ec2)
        self.network = SecurityGroups(config, ec2, poll_seconds=poll_seconds, delete_timeout=cleanup_timeout)
        self.task_definitions = TaskDefinitions(config, self.ecs)

    @property
    def admission_pool_id(self) -> str | None:
        return f"{PROVIDER_TYPE}:{self.config.region}:{self.config.cluster}:{self.config.capacity_provider}"

    def validate_sandbox_config(self, *, requires_nested_docker: bool, resources: Resources) -> None:
        self.admission.validate_resources(requires_nested_docker=requires_nested_docker, resources=resources)

    async def reservable_demand(self, source: SandboxSource, resources: Resources) -> SandboxReservationDemand:
        return self.admission.demand(source, resources)

    async def get_capacity_domains(self) -> list[SandboxCapacityDomain] | None:
        return await self.admission.capacity_domains()

    async def check_admission(self, source: SandboxSource, resources: Resources) -> bool:
        return await self.admission.admits(source, resources)

    def _sandbox(self, task: Mapping[str, Any]) -> EcsNitroSandbox:
        return EcsNitroSandbox(task, config=self.config, network=self.network, describe=self.describe_task)

    async def describe_task(self, task_id: str) -> dict[str, Any] | None:
        described = await self.ecs.call(
            "DescribeTasks", {"cluster": self.config.cluster, "tasks": [task_id], "include": ["TAGS"]}
        )
        task = next(iter(described.get("tasks", [])), None)
        if task is None or task.get("startedBy") != STARTED_BY:
            return None
        return task

    async def _wait_running(self, task_arn: str, deadline: float) -> dict[str, Any]:
        while True:
            task = await self.describe_task(task_arn)
            if task is None:
                raise SandboxError(f"ECS task {task_arn} disappeared during startup")
            status = task.get("lastStatus")
            if status == "RUNNING":
                return task
            if status in ENDED_STATUSES:
                raise SandboxError(f"ECS task {task_arn} stopped during startup: {task.get('stoppedReason', status)}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"ECS task {task_arn} was still {status} at the create timeout")
            await asyncio.sleep(self._poll)

    def _check_placement(self, task: Mapping[str, Any]) -> None:
        if task.get("capacityProviderName") != self.config.capacity_provider:
            raise SandboxError(
                f"ECS task {task['taskArn']} landed on {task.get('capacityProviderName')}, "
                f"not {self.config.capacity_provider}"
            )
        if task.get("launchType") not in (None, LAUNCH_TYPE):
            raise SandboxError(f"ECS task {task['taskArn']} uses launch type {task.get('launchType')}")

    async def _wait_agent(self, sandbox: EcsNitroSandbox, deadline: float) -> None:
        client = sandbox.agent()
        while not await client.healthy():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"sandbox agent on ECS task {sandbox.id} did not answer by the create timeout")
            await asyncio.sleep(_AGENT_POLL_SECONDS)

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        source = validate_source(request.source)
        self.admission.validate_resources(
            requires_nested_docker=request.requires_nested_docker, resources=request.resources
        )
        if request.volumes:
            raise UnsupportedSandboxConfigError("ecs_nitro does not support persistent volumes")
        bad_secrets = sorted(
            name for name, ref in request.sandbox_secrets.items() if not ref.startswith(_SECRET_ARN_PREFIXES)
        )
        if bad_secrets:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro sandbox secrets must be Secrets Manager or SSM parameter ARNs: {', '.join(bad_secrets)}"
            )
        validate_labels(request.labels)
        await self.admission.verify_infrastructure()

        deadline = time.monotonic() + request.provider_timeout
        create_started_at = now()
        sandbox_id = uuid.uuid4().hex
        task_definition = await self.task_definitions.ensure(source.image, request.sandbox_secrets)
        initial_egress = [] if request.network_block_all else list(self.config.egress_ceiling_cidrs)
        group_id = await self.network.create(sandbox_id, initial_egress)
        task_arn: str | None = None
        try:
            environment = {
                **request.env_vars,
                TOKEN_ENV: agent_token(self.config.agent_token_key, sandbox_id),
                PORT_ENV: str(self.config.agent_port),
                IDLE_TIMEOUT_ENV: str(request.auto_stop_interval * 60),
            }
            tags = {
                **request.labels,
                **self.config.resource_tags,
                SANDBOX_ID_TAG: sandbox_id,
                SANDBOX_NAME_TAG: request.name,
                SECURITY_GROUP_TAG: group_id,
            }
            run_task_submitted_at = now()
            started = await self.ecs.call(
                "RunTask",
                {
                    "cluster": self.config.cluster,
                    "taskDefinition": task_definition,
                    "count": 1,
                    "capacityProviderStrategy": [{"capacityProvider": self.config.capacity_provider, "weight": 1}],
                    "networkConfiguration": {
                        "awsvpcConfiguration": {
                            "subnets": self.config.subnet_ids,
                            "securityGroups": [group_id],
                            "assignPublicIp": "DISABLED",
                        }
                    },
                    "overrides": {
                        "cpu": str(math.ceil(request.resources.vcpu * 1024)),
                        "memory": str(request.resources.memory * 1024),
                        "containerOverrides": [
                            {
                                "name": SANDBOX_CONTAINER,
                                "environment": [{"name": k, "value": v} for k, v in environment.items()],
                            }
                        ],
                    },
                    "enableExecuteCommand": False,
                    "startedBy": STARTED_BY,
                    "clientToken": sandbox_id,
                    "tags": [{"key": key, "value": value} for key, value in tags.items()],
                },
            )
            tasks = started.get("tasks", [])
            if not tasks:
                reasons = "; ".join(
                    f"{f.get('arn', '')} {f.get('reason', '')}".strip() for f in started.get("failures", [])
                )
                raise SandboxError(f"ECS RunTask placed no task: {reasons or 'no reason given'}")
            task_arn = str(tasks[0]["taskArn"])
            task = await self._wait_running(task_arn, deadline)
            self._check_placement(task)
            sandbox = self._sandbox(task)
            await self._wait_agent(sandbox, deadline)
            log_timing(
                "ecs_nitro.sandbox_ready",
                sandbox_id=sandbox.id,
                sandbox_name=request.name,
                task_arn=task_arn,
                create_started_at=create_started_at,
                run_task_submitted_at=run_task_submitted_at,
                task_created_at=iso(task.get("createdAt")),
                pull_started_at=iso(task.get("pullStartedAt")),
                pull_stopped_at=iso(task.get("pullStoppedAt")),
                task_started_at=iso(task.get("startedAt")),
                agent_ready_at=now(),
                container_instance_arn=task.get("containerInstanceArn"),
                availability_zone=task.get("availabilityZone"),
            )
            return sandbox
        except BaseException:
            await asyncio.shield(self._cleanup(sandbox_id, task_arn, group_id))
            raise

    async def _find_task_arn(self, sandbox_id: str) -> str | None:
        async for task in list_tasks(self.ecs, self.config.cluster, ("RUNNING",)):
            if task_tags(task).get(SANDBOX_ID_TAG) == sandbox_id:
                return str(task["taskArn"])
        return None

    async def _cleanup(self, sandbox_id: str, task_arn: str | None, group_id: str) -> None:
        try:
            if task_arn is None:
                task_arn = await self._find_task_arn(sandbox_id)
            if task_arn is not None:
                await self._stop_and_wait(task_arn)
            await self.network.delete(group_id)
        except Exception:
            logger.exception("ecs_nitro cleanup failed for task %s and security group %s", task_arn, group_id)

    async def _stop_and_wait(self, task_arn: str) -> None:
        try:
            await self.ecs.call(
                "StopTask", {"cluster": self.config.cluster, "task": task_arn, "reason": "CBS sandbox deleted"}
            )
        except AwsApiError as error:
            if error.code != "InvalidParameterException":
                raise
        deadline = time.monotonic() + self._cleanup_timeout
        while True:
            task = await self.describe_task(task_arn)
            if task is None or task.get("lastStatus") in ("STOPPED", "DELETED"):
                return
            if time.monotonic() >= deadline:
                raise SandboxError(f"ECS task {task_arn} did not stop within {self._cleanup_timeout:.0f}s")
            await asyncio.sleep(self._poll)

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        task = await self.describe_task(instance_id)
        if task is None:
            raise SandboxNotFoundError(f"ECS sandbox task {instance_id} not found")
        return self._sandbox(task)

    async def delete_sandbox(self, instance_id: str) -> None:
        task = await self.describe_task(instance_id)
        if task is None:
            return
        await self._stop_and_wait(str(task["taskArn"]))
        group_id = task_tags(task).get(SECURITY_GROUP_TAG)
        if group_id:
            await self.network.delete(group_id)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        async for task in list_tasks(self.ecs, self.config.cluster, ("RUNNING", "STOPPED"), query.page_size):
            sandbox = self._sandbox(task)
            labels = sandbox.labels or {}
            if any(labels.get(key) != value for key, value in query.labels.items()):
                continue
            if query.created_at_lte is not None and (
                sandbox.created_at is None or sandbox.created_at > query.created_at_lte
            ):
                continue
            yield sandbox

    async def close(self) -> None:
        await self.ecs.close()
