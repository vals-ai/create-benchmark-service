from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from typing import Any

from benchmark_service.sandbox.ecs_nitro.aws import AwsApiError, EcsApi
from benchmark_service.sandbox.ecs_nitro.config import ISOLATION_MODE, LAUNCH_TYPE, EcsNitroProviderConfig
from benchmark_service.sandbox.types import SandboxError

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


class TaskDefinitions:
    """Registers one isolated task definition family per image and secret set, and refuses any without isolation."""

    def __init__(self, config: EcsNitroProviderConfig, ecs: EcsApi) -> None:
        self.config = config
        self.ecs = ecs
        self._arns: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def spec(self, image: str, secrets: Mapping[str, str]) -> dict[str, Any]:
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
        if self.config.resource_tags:
            spec["tags"] = [{"key": key, "value": value} for key, value in sorted(self.config.resource_tags.items())]
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:20]
        return {"family": f"{self.config.task_family_prefix}-{digest}", **spec}

    async def ensure(self, image: str, secrets: Mapping[str, str]) -> str:
        spec = self.spec(image, secrets)
        family = spec["family"]
        if family in self._arns:
            return self._arns[family]
        async with self._locks.setdefault(family, asyncio.Lock()):
            if family in self._arns:
                return self._arns[family]
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
            self._arns[family] = str(described["taskDefinitionArn"])
            return self._arns[family]
