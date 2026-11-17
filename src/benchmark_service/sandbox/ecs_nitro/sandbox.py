from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from benchmark_service.sandbox.ecs_nitro.agent import TIMEOUT_EXIT_CODE
from benchmark_service.sandbox.ecs_nitro.client import AgentClient
from benchmark_service.sandbox.ecs_nitro.config import (
    ENDED_STATUSES,
    RESERVED_TAG_PREFIXES,
    SANDBOX_ID_TAG,
    SANDBOX_NAME_TAG,
    SECURITY_GROUP_TAG,
    EcsNitroProviderConfig,
    agent_token,
    log_timing,
    now,
    task_tags,
)
from benchmark_service.sandbox.ecs_nitro.network import SecurityGroups
from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxError,
    SandboxNotFoundError,
    validate_command_env,
)


class EcsNitroSandbox(Sandbox):
    def __init__(
        self,
        task: Mapping[str, Any],
        *,
        config: EcsNitroProviderConfig,
        network: SecurityGroups,
        describe: Callable[[str], Awaitable[Mapping[str, Any] | None]],
    ) -> None:
        self._task = task
        self._config = config
        self._network = network
        self._describe = describe
        tags = task_tags(task)
        self._sandbox_id = tags.get(SANDBOX_ID_TAG, "")
        self._security_group = tags.get(SECURITY_GROUP_TAG, "")
        self.labels = {
            key: value
            for key, value in tags.items()
            if not key.startswith(RESERVED_TAG_PREFIXES) and key not in config.resource_tags
        }
        created = task.get("createdAt")
        self.created_at = datetime.fromtimestamp(float(created), UTC) if created is not None else None
        self._name = tags.get(SANDBOX_NAME_TAG, self.id)
        self._agent: AgentClient | None = None
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
        return str(self._task.get("lastStatus")) in ENDED_STATUSES

    @property
    def task(self) -> Mapping[str, Any]:
        return self._task

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id

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

    def agent(self) -> AgentClient:
        if self._agent is None:
            address = self.private_ip
            if self.has_ended:
                raise SandboxNotFoundError(f"ECS task {self.id} is {self.state}")
            if not address:
                raise SandboxConnectionError(f"ECS task {self.id} is {self.state} with no reachable agent")
            if not self._sandbox_id:
                raise SandboxError(f"ECS task {self.id} has no {SANDBOX_ID_TAG} tag")
            token = agent_token(self._config.agent_token_key, self._sandbox_id)
            self._agent = AgentClient(address, self._config.agent_port, token)
        return self._agent

    async def _lost(self, error: SandboxConnectionError) -> SandboxError:
        task = await self._describe(str(self._task["taskArn"]))
        if task is None or str(task.get("lastStatus")) in ENDED_STATUSES:
            self._task = task or {**self._task, "lastStatus": "DELETED"}
            return SandboxNotFoundError(f"ECS task {self.id} ended: {error}")
        return error

    async def _run(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str]
    ) -> AsyncGenerator[bytes | int, None]:
        validate_command_env(env)
        first = not self._first_exec_logged
        self._first_exec_logged = True
        started = now()
        exit_code: int | None = None
        try:
            async for item in self.agent().run(command, cwd, timeout, env):
                if isinstance(item, int):
                    exit_code = item
                yield item
        except SandboxConnectionError as error:
            raise await self._lost(error) from error
        finally:
            if first:
                log_timing(
                    "ecs_nitro.first_exec",
                    sandbox_id=self.id,
                    exec_started_at=started,
                    exec_finished_at=now(),
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
        try:
            await self.agent().upload(remote_path, content)
        except SandboxConnectionError as error:
            raise await self._lost(error) from error

    async def download_file(self, remote_path: str) -> bytes:
        return b"".join([chunk async for chunk in self.stream_download(remote_path)])

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        try:
            async for chunk in self.agent().download(remote_path):
                yield chunk
        except SandboxConnectionError as error:
            raise await self._lost(error) from error

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._network.set_egress(self._security_group, self._network.allowed_egress(allowed_addresses))

    async def block_all_egress(self) -> None:
        await self._network.set_egress(self._security_group, [])

    async def clear_egress_rules(self) -> None:
        await self._network.set_egress(self._security_group, list(self._config.egress_ceiling_cidrs))
