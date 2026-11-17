from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import time
import uuid
from collections.abc import AsyncGenerator, Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field, SecretStr, field_validator

from benchmark_service.sandbox.ecs_nitro.agent import IDLE_TIMEOUT_ENV, PORT_ENV, TIMEOUT_EXIT_CODE, TOKEN_ENV
from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError, Boto3Ec2Client, Ec2Api, EcsApi, EcsJsonClient
from benchmark_service.sandbox.egress import resolve_allowed_addresses
from benchmark_service.sandbox.types import (
    ExecResult,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxSource,
    UnsupportedSandboxConfigError,
    validate_command_env,
)

logger = logging.getLogger(__name__)

PROVIDER_TYPE = "ecs_nitro"
ISOLATION_MODE = "ISOLATED_SANDBOX"
LAUNCH_TYPE = "MANAGED_INSTANCES"
# Isolated tasks get a fixed 20 GiB of ephemeral storage that a task definition cannot raise.
ISOLATED_TASK_DISK_GIB = 20
# A Nitro Sandbox needs at least 256 MiB, and the 1 GiB floor keeps the agent's interpreter comfortable.
MIN_MEMORY_GIB = 1
SANDBOX_CONTAINER = "sandbox"
AGENT_INIT_CONTAINER = "cbs-agent-init"
AGENT_VOLUME = "cbs-agent"
AGENT_MOUNT = "/cbs-agent"
# The interpreter is linked against musl, so the bundled loader runs it on glibc and musl images alike.
AGENT_ENTRYPOINT = (
    f"{AGENT_MOUNT}/lib/ld-musl-x86_64.so.1",
    f"{AGENT_MOUNT}/python/bin/python3.12",
    "-I",
    f"{AGENT_MOUNT}/agent.py",
)
STARTED_BY = "cbs-ecs-nitro"
SANDBOX_ID_TAG = "cbs:sandbox-id"
SANDBOX_NAME_TAG = "cbs:sandbox-name"
SECURITY_GROUP_TAG = "cbs:security-group"
_RESERVED_TAG_PREFIXES = ("aws:", "cbs:")
_SECRET_ARN_PREFIXES = ("arn:aws:secretsmanager:", "arn:aws:ssm:")
_TASK_POLL_SECONDS = 3.0
_AGENT_POLL_SECONDS = 1.0
_STOP_TIMEOUT_SECONDS = 300.0
_SG_DELETE_TIMEOUT_SECONDS = 300.0
_COMMAND_GRACE_SECONDS = 30.0
_ENDED_STATUSES = frozenset({"DEPROVISIONING", "STOPPING", "STOPPED", "DELETED"})


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
    agent_token_key: SecretStr = Field(description="HMAC key that derives each sandbox's agent token")
    agent_port: int = Field(default=7070, ge=1024, le=65535)
    egress_ceiling_cidrs: list[str] = Field(
        default_factory=list, description="IPv4 CIDRs sandbox egress may ever reach; empty denies all egress"
    )
    max_vcpu: float = Field(default=16, gt=0)
    max_memory_gib: int = Field(default=64, gt=0)
    max_sandboxes: int = Field(default=100, gt=0)
    log_group: str | None = Field(default=None, description="CloudWatch Logs group for agent and sandbox output")
    ecs_endpoint_url: str | None = None

    @field_validator("agent_token_key")
    @classmethod
    def _validate_key(cls, value: SecretStr) -> SecretStr:
        if len(value.get_secret_value()) < 32:
            raise ValueError("agent_token_key must be at least 32 characters")
        return value

    @field_validator("egress_ceiling_cidrs")
    @classmethod
    def _validate_ceiling(cls, value: list[str]) -> list[str]:
        return [str(ipaddress.IPv4Network(cidr, strict=False)) for cidr in value]

    def create_provider(self) -> SandboxProvider:
        return EcsNitroSandboxProvider(self)


def agent_token(key: str, sandbox_id: str) -> str:
    return hmac.new(key.encode(), sandbox_id.encode(), hashlib.sha256).hexdigest()


def _tags(items: list[dict[str, str]] | None) -> dict[str, str]:
    return {item["key"]: item["value"] for item in items or []}


def _iso(value: Any) -> str | None:
    return None if value is None else datetime.fromtimestamp(float(value), UTC).isoformat()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _log_timing(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, "provider": PROVIDER_TYPE, **fields}, default=str))


def _validate_labels(labels: Mapping[str, str]) -> None:
    reserved = sorted(key for key in labels if key.startswith(_RESERVED_TAG_PREFIXES))
    if reserved:
        raise UnsupportedSandboxConfigError(f"ecs_nitro labels cannot use reserved tag prefixes: {', '.join(reserved)}")


class _AgentClient:
    def __init__(self, address: str, port: int, token: str) -> None:
        self._base = f"http://{address}:{port}"
        self._headers = {"Authorization": f"Bearer {token}"}

    def _client(self, timeout: float | None) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            headers=self._headers,
            timeout=httpx.Timeout(connect=10.0, read=timeout, write=60.0, pool=10.0),
        )

    @staticmethod
    def _error(response: httpx.Response, action: str) -> SandboxError:
        try:
            detail = str(response.json().get("error"))
        except ValueError:
            detail = response.text[:500]
        if response.status_code == 404:
            return SandboxNotFoundError(f"{action}: {detail}")
        return SandboxError(f"{action} failed with HTTP {response.status_code}: {detail}")

    async def healthy(self) -> bool:
        try:
            async with self._client(5.0) as client:
                return (await client.get("/health")).status_code == 200
        except httpx.HTTPError:
            return False

    async def run(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str]
    ) -> AsyncGenerator[bytes | int, None]:
        body = {"command": command, "cwd": cwd, "env": dict(env), "timeout": timeout}
        read_timeout = None if timeout is None else timeout + _COMMAND_GRACE_SECONDS
        try:
            async with self._client(read_timeout) as client, client.stream("POST", "/exec", json=body) as response:
                if response.status_code != 200:
                    await response.aread()
                    raise self._error(response, "exec")
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    frame = json.loads(line)
                    if "output" in frame:
                        yield base64.b64decode(frame["output"])
                    elif "exit_code" in frame:
                        yield int(frame["exit_code"])
                        return
                    else:
                        raise SandboxError(f"exec failed: {frame.get('error')}")
        except httpx.ReadTimeout:
            yield TIMEOUT_EXIT_CODE
            return
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"sandbox agent at {self._base} unreachable: {error}") from error
        raise SandboxConnectionError("sandbox agent closed the exec stream before reporting an exit code")

    async def upload(self, remote_path: str, content: bytes) -> None:
        try:
            async with self._client(300.0) as client:
                response = await client.put(f"/files?path={quote(remote_path)}", content=content)
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"upload to {remote_path} failed: {error}") from error
        if response.status_code != 200:
            raise self._error(response, f"upload {remote_path}")

    async def download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        try:
            async with (
                self._client(300.0) as client,
                client.stream("GET", f"/files?path={quote(remote_path)}") as response,
            ):
                if response.status_code != 200:
                    await response.aread()
                    raise self._error(response, f"download {remote_path}")
                async for chunk in response.aiter_bytes():
                    yield chunk
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"download of {remote_path} failed: {error}") from error


class EcsNitroSandbox(Sandbox):
    def __init__(self, provider: EcsNitroSandboxProvider, task: Mapping[str, Any]) -> None:
        self._provider = provider
        self._task = task
        tags = _tags(task.get("tags"))
        self._sandbox_id = tags.get(SANDBOX_ID_TAG, "")
        self._security_group = tags.get(SECURITY_GROUP_TAG, "")
        self.labels = {key: value for key, value in tags.items() if not key.startswith(_RESERVED_TAG_PREFIXES)}
        created = task.get("createdAt")
        self.created_at = datetime.fromtimestamp(float(created), UTC) if created is not None else None
        self._name = tags.get(SANDBOX_NAME_TAG, self.id)
        self._agent: _AgentClient | None = None
        self._first_exec_logged = False

    @property
    def id(self) -> str:
        return str(self._task["taskArn"]).rsplit("/", 1)[-1]

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> str:
        return str(self._task.get("lastStatus", "UNKNOWN")).lower()

    @property
    def has_ended(self) -> bool:
        return str(self._task.get("lastStatus")) in _ENDED_STATUSES

    @property
    def security_group_id(self) -> str:
        return self._security_group

    @property
    def private_ip(self) -> str | None:
        for attachment in self._task.get("attachments", []):
            if attachment.get("type") != "ElasticNetworkInterface":
                continue
            for detail in attachment.get("details", []):
                if detail.get("name") == "privateIPv4Address":
                    return str(detail["value"])
        return None

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        metadata = {
            "task_arn": str(self._task["taskArn"]),
            "capacity_provider": str(self._task.get("capacityProviderName", "")),
            "security_group": self._security_group,
        }
        if self.private_ip:
            metadata["private_ip"] = self.private_ip
        return metadata

    def agent(self) -> _AgentClient:
        if self._agent is None:
            address = self.private_ip
            if not address or self.has_ended:
                raise SandboxConnectionError(f"ECS task {self.id} is {self.state} with no reachable agent")
            if not self._sandbox_id:
                raise SandboxError(f"ECS task {self.id} has no {SANDBOX_ID_TAG} tag")
            self._agent = self._provider.agent_client(address, self._sandbox_id)
        return self._agent

    async def _run(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str]
    ) -> AsyncGenerator[bytes | int, None]:
        validate_command_env(env)
        first = not self._first_exec_logged
        self._first_exec_logged = True
        started = _now()
        exit_code: int | None = None
        try:
            async for item in self.agent().run(command, cwd, timeout, env):
                if isinstance(item, int):
                    exit_code = item
                yield item
        finally:
            if first:
                _log_timing(
                    "ecs_nitro.first_exec",
                    sandbox_id=self.id,
                    exec_started_at=started,
                    exec_finished_at=_now(),
                    exit_code=exit_code,
                )

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        output = bytearray()
        exit_code = TIMEOUT_EXIT_CODE
        async for item in self._run(command, cwd, timeout, {}):
            if isinstance(item, int):
                exit_code = item
            else:
                output.extend(item)
        return ExecResult(exit_code=exit_code, output=output.decode(errors="replace"))

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        async for item in self._run(command, cwd, timeout, env_vars or {}):
            if isinstance(item, int):
                if item != 0:
                    raise SandboxCommandError(item)
                return
            yield item.decode(errors="replace")

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await self.agent().upload(remote_path, content)

    async def download_file(self, remote_path: str) -> bytes:
        return b"".join([chunk async for chunk in self.agent().download(remote_path)])

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        async for chunk in self.agent().download(remote_path):
            yield chunk

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._provider.set_egress(self._security_group, self._provider.allowed_egress(allowed_addresses))

    async def block_all_egress(self) -> None:
        await self._provider.set_egress(self._security_group, [])

    async def clear_egress_rules(self) -> None:
        await self._provider.set_egress(self._security_group, list(self._provider.config.egress_ceiling_cidrs))


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
        self._ecs = ecs
        self._ec2 = ec2
        self._poll = poll_seconds
        self._cleanup_timeout = cleanup_timeout
        self._verified = False
        self._verify_lock = asyncio.Lock()
        self._task_definitions: dict[str, str] = {}
        self._family_locks: dict[str, asyncio.Lock] = {}

    @property
    def ecs(self) -> EcsApi:
        if self._ecs is None:
            self._ecs = EcsJsonClient(self.config.region, self.config.ecs_endpoint_url)
        return self._ecs

    @property
    def ec2(self) -> Ec2Api:
        if self._ec2 is None:
            self._ec2 = Boto3Ec2Client(self.config.region)
        return self._ec2

    @property
    def admission_pool_id(self) -> str | None:
        return f"{PROVIDER_TYPE}:{self.config.region}:{self.config.cluster}:{self.config.capacity_provider}"

    def agent_client(self, address: str, sandbox_id: str) -> _AgentClient:
        token = agent_token(self.config.agent_token_key.get_secret_value(), sandbox_id)
        return _AgentClient(address, self.config.agent_port, token)

    def validate_sandbox_config(self, *, requires_nested_docker: bool, resources: Resources) -> None:
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

    def _validate_source(self, source: SandboxSource) -> ImageSource:
        if not isinstance(source, ImageSource):
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro runs prebuilt images only; {source.type} sources are not supported"
            )
        return source

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

    async def verify_infrastructure(self) -> None:
        if self._verified:
            return
        async with self._verify_lock:
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
                {"capacityProviders": [config.capacity_provider], "cluster": config.cluster},
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

    async def _running_count(self) -> int:
        count = 0
        token: str | None = None
        while True:
            params: dict[str, Any] = {"cluster": self.config.cluster, "startedBy": STARTED_BY, "maxResults": 100}
            if token:
                params["nextToken"] = token
            page = await self.ecs.call("ListTasks", params)
            count += len(page.get("taskArns", []))
            token = page.get("nextToken")
            if not token:
                return count

    async def check_admission(self, source: SandboxSource, resources: Resources) -> bool:
        self._validate_source(source)
        self.validate_sandbox_config(requires_nested_docker=False, resources=resources)
        await self.verify_infrastructure()
        return await self._running_count() < self.config.max_sandboxes

    def _task_definition(self, image: str, secrets: Mapping[str, str]) -> dict[str, Any]:
        log_configuration: dict[str, Any] | None = None
        if self.config.log_group:
            log_configuration = {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": self.config.log_group,
                    "awslogs-region": self.config.region,
                    "awslogs-stream-prefix": "cbs",
                },
            }
        init: dict[str, Any] = {
            "name": AGENT_INIT_CONTAINER,
            "image": self.config.agent_image,
            "essential": False,
            "mountPoints": [{"sourceVolume": AGENT_VOLUME, "containerPath": AGENT_MOUNT}],
        }
        sandbox: dict[str, Any] = {
            "name": SANDBOX_CONTAINER,
            "image": image,
            "essential": True,
            "entryPoint": list(AGENT_ENTRYPOINT),
            "command": [],
            "dependsOn": [{"containerName": AGENT_INIT_CONTAINER, "condition": "SUCCESS"}],
            "mountPoints": [{"sourceVolume": AGENT_VOLUME, "containerPath": AGENT_MOUNT, "readOnly": True}],
            "portMappings": [{"containerPort": self.config.agent_port, "protocol": "tcp"}],
            "readonlyRootFilesystem": False,
            "privileged": False,
            "secrets": [{"name": name, "valueFrom": arn} for name, arn in sorted(secrets.items())],
        }
        if log_configuration:
            init["logConfiguration"] = log_configuration
            sandbox["logConfiguration"] = log_configuration
        spec: dict[str, Any] = {
            "requiresCompatibilities": [LAUNCH_TYPE],
            "networkMode": "awsvpc",
            "executionRoleArn": self.config.execution_role_arn,
            "cpu": "1024",
            "memory": "2048",
            "runtimePlatform": {"operatingSystemFamily": "LINUX", "cpuArchitecture": "X86_64"},
            "trustedExecutionConfiguration": {"isolationMode": ISOLATION_MODE},
            "volumes": [{"name": AGENT_VOLUME}],
            "containerDefinitions": [init, sandbox],
        }
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:20]
        return {"family": f"cbs-ecs-nitro-{digest}", **spec}

    async def _ensure_task_definition(self, image: str, secrets: Mapping[str, str]) -> str:
        spec = self._task_definition(image, secrets)
        family = spec["family"]
        if family in self._task_definitions:
            return self._task_definitions[family]
        async with self._family_locks.setdefault(family, asyncio.Lock()):
            if family in self._task_definitions:
                return self._task_definitions[family]
            try:
                described = (await self.ecs.call("DescribeTaskDefinition", {"taskDefinition": family}))[
                    "taskDefinition"
                ]
            except AwsApiError as error:
                if error.code != "ClientException":
                    raise
                described = (await self.ecs.call("RegisterTaskDefinition", spec))["taskDefinition"]
            trusted: dict[str, Any] = described.get("trustedExecutionConfiguration") or {}
            isolation = trusted.get("isolationMode")
            if isolation != ISOLATION_MODE or described.get("status") != "ACTIVE":
                raise SandboxError(
                    f"task definition {family} reports isolationMode={isolation} status={described.get('status')}, "
                    f"refusing to run without {ISOLATION_MODE}"
                )
            self._task_definitions[family] = str(described["taskDefinitionArn"])
            return self._task_definitions[family]

    async def _create_security_group(self, sandbox_id: str, initial_egress: list[str]) -> str:
        created = await self.ec2.call(
            "create_security_group",
            {
                "GroupName": f"cbs-ecs-nitro-{sandbox_id}",
                "Description": "CBS ECS Nitro sandbox",
                "VpcId": self.config.vpc_id,
                "TagSpecifications": [
                    {"ResourceType": "security-group", "Tags": [{"Key": SANDBOX_ID_TAG, "Value": sandbox_id}]}
                ],
            },
        )
        group_id = str(created["GroupId"])
        try:
            await self.set_egress(group_id, initial_egress)
            await self.ec2.call(
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
            )
        except BaseException:
            await asyncio.shield(self._delete_security_group(group_id))
            raise
        return group_id

    async def set_egress(self, group_id: str, cidrs: list[str]) -> None:
        if not group_id:
            raise SandboxError("sandbox has no security group to hold its egress rules")
        described = await self.ec2.call("describe_security_groups", {"GroupIds": [group_id]})
        current = described["SecurityGroups"][0].get("IpPermissionsEgress", [])
        if current:
            await self.ec2.call("revoke_security_group_egress", {"GroupId": group_id, "IpPermissions": current})
        if cidrs:
            await self.ec2.call(
                "authorize_security_group_egress",
                {
                    "GroupId": group_id,
                    "IpPermissions": [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": cidr} for cidr in cidrs]}],
                },
            )

    async def _delete_security_group(self, group_id: str) -> None:
        deadline = time.monotonic() + self._cleanup_timeout
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

    async def _describe(self, task_id: str) -> dict[str, Any] | None:
        described = await self.ecs.call(
            "DescribeTasks", {"cluster": self.config.cluster, "tasks": [task_id], "include": ["TAGS"]}
        )
        task = next(iter(described.get("tasks", [])), None)
        if task is None or task.get("startedBy") != STARTED_BY:
            return None
        return task

    async def _wait_running(self, task_arn: str, deadline: float) -> dict[str, Any]:
        while True:
            task = await self._describe(task_arn)
            if task is None:
                raise SandboxError(f"ECS task {task_arn} disappeared during startup")
            status = task.get("lastStatus")
            if status == "RUNNING":
                return task
            if status in _ENDED_STATUSES:
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
        source = self._validate_source(request.source)
        self.validate_sandbox_config(requires_nested_docker=request.requires_nested_docker, resources=request.resources)
        if request.volumes:
            raise UnsupportedSandboxConfigError("ecs_nitro does not support persistent volumes")
        bad_secrets = sorted(
            name for name, ref in request.sandbox_secrets.items() if not ref.startswith(_SECRET_ARN_PREFIXES)
        )
        if bad_secrets:
            raise UnsupportedSandboxConfigError(
                f"ecs_nitro sandbox secrets must be Secrets Manager or SSM parameter ARNs: {', '.join(bad_secrets)}"
            )
        _validate_labels(request.labels)
        await self.verify_infrastructure()

        deadline = time.monotonic() + request.provider_timeout
        create_started_at = _now()
        sandbox_id = uuid.uuid4().hex
        task_definition = await self._ensure_task_definition(source.image, request.sandbox_secrets)
        initial_egress = [] if request.network_block_all else list(self.config.egress_ceiling_cidrs)
        group_id = await self._create_security_group(sandbox_id, initial_egress)
        task_arn: str | None = None
        try:
            environment = {
                **request.env_vars,
                TOKEN_ENV: agent_token(self.config.agent_token_key.get_secret_value(), sandbox_id),
                PORT_ENV: str(self.config.agent_port),
                IDLE_TIMEOUT_ENV: str(request.auto_stop_interval * 60),
            }
            tags = {
                **request.labels,
                SANDBOX_ID_TAG: sandbox_id,
                SANDBOX_NAME_TAG: request.name,
                SECURITY_GROUP_TAG: group_id,
            }
            run_task_submitted_at = _now()
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
            sandbox = EcsNitroSandbox(self, task)
            await self._wait_agent(sandbox, deadline)
            _log_timing(
                "ecs_nitro.sandbox_ready",
                sandbox_id=sandbox.id,
                sandbox_name=request.name,
                task_arn=task_arn,
                create_started_at=create_started_at,
                run_task_submitted_at=run_task_submitted_at,
                task_created_at=_iso(task.get("createdAt")),
                pull_started_at=_iso(task.get("pullStartedAt")),
                pull_stopped_at=_iso(task.get("pullStoppedAt")),
                task_started_at=_iso(task.get("startedAt")),
                agent_ready_at=_now(),
                container_instance_arn=task.get("containerInstanceArn"),
                availability_zone=task.get("availabilityZone"),
            )
            return sandbox
        except BaseException:
            await asyncio.shield(self._cleanup(task_arn, group_id))
            raise

    async def _cleanup(self, task_arn: str | None, group_id: str) -> None:
        try:
            if task_arn is not None:
                await self._stop_and_wait(task_arn)
            await self._delete_security_group(group_id)
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
            task = await self._describe(task_arn)
            if task is None or task.get("lastStatus") in ("STOPPED", "DELETED"):
                return
            if time.monotonic() >= deadline:
                raise SandboxError(f"ECS task {task_arn} did not stop within {self._cleanup_timeout:.0f}s")
            await asyncio.sleep(self._poll)

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        task = await self._describe(instance_id)
        if task is None:
            raise SandboxNotFoundError(f"ECS sandbox task {instance_id} not found")
        return EcsNitroSandbox(self, task)

    async def delete_sandbox(self, instance_id: str) -> None:
        task = await self._describe(instance_id)
        if task is None:
            return
        await self._stop_and_wait(str(task["taskArn"]))
        group_id = _tags(task.get("tags")).get(SECURITY_GROUP_TAG)
        if group_id:
            await self._delete_security_group(group_id)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "cluster": self.config.cluster,
                "startedBy": STARTED_BY,
                "maxResults": min(max(query.page_size, 1), 100),
            }
            if token:
                params["nextToken"] = token
            page = await self.ecs.call("ListTasks", params)
            arns = page.get("taskArns", [])
            if arns:
                described = await self.ecs.call(
                    "DescribeTasks", {"cluster": self.config.cluster, "tasks": arns, "include": ["TAGS"]}
                )
                for task in described.get("tasks", []):
                    sandbox = EcsNitroSandbox(self, task)
                    labels = sandbox.labels or {}
                    if any(labels.get(key) != value for key, value in query.labels.items()):
                        continue
                    if query.created_at_lte is not None and (
                        sandbox.created_at is None or sandbox.created_at > query.created_at_lte
                    ):
                        continue
                    yield sandbox
            token = page.get("nextToken")
            if not token:
                return

    async def close(self) -> None:
        if self._ecs is not None:
            await self._ecs.close()
