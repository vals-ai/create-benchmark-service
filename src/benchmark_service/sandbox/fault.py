"""Fault injection for sandbox providers.

`FaultySandboxProvider` wraps a real provider and fails scripted calls the way the real providers
do under infra trouble (connection drops, lost sandboxes, nested-dockerd startup failures), so a
caller's retry and resume paths can be tested against a sandbox that otherwise behaves normally.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.local.docker import DockerProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ExecResult,
    Resources,
    Sandbox,
    SandboxCapabilities,
    SandboxCapacity,
    SandboxCapacityDomain,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    SandboxReservationDemand,
    SandboxSource,
    SnapshotSource,
)

FaultPoint = Literal[
    "create_sandbox",
    "get_sandbox",
    "delete_sandbox",
    "exec",
    "command",
    "upload_file",
    "download_file",
    "modify_egress_rules",
    "block_all_egress",
    "clear_egress_rules",
    "snapshot",
]
FaultEffect = Literal["connection_error", "not_found", "error", "command_failure"]


class Fault(BaseModel):
    """One scripted failure. Faults fire in plan order, once per `times`, then the call passes through."""

    model_config = ConfigDict(frozen=True)

    at: FaultPoint
    effect: FaultEffect = "connection_error"
    message: str = ""
    exit_code: int = Field(default=1, description="Exit code reported by a command_failure")
    when: Literal["before", "after"] = Field(
        default="before",
        description=(
            "'before' fails without touching the sandbox; 'after' lets the real call finish (its side "
            "effects happen) and then reports the failure instead of the result, like a connection lost "
            "while waiting for a reply"
        ),
    )
    command_contains: str | None = Field(
        default=None,
        description="For exec/command: only fire for commands containing this text",
    )
    times: int = Field(default=1, ge=1)

    def matches(self, point: FaultPoint, command: str | None) -> bool:
        if self.at != point:
            return False
        return self.command_contains is None or (command is not None and self.command_contains in command)

    def error(self) -> SandboxError:
        match self.effect:
            case "connection_error":
                return SandboxConnectionError(self.message or f"Injected connection error at {self.at}")
            case "not_found":
                return SandboxNotFoundError(self.message or f"Injected sandbox not found at {self.at}")
            case "error":
                return SandboxError(self.message or f"Injected error at {self.at}")
            case "command_failure":
                return SandboxCommandError(self.exit_code)


class InjectedFault(BaseModel):
    """A fault that fired, for assertions on what the caller actually experienced."""

    model_config = ConfigDict(frozen=True)

    fault: Fault
    sandbox_id: str | None = None
    command: str | None = None


class FaultPlan:
    """Ordered faults shared by a provider and the sandboxes it hands out."""

    def __init__(self, faults: Sequence[Fault]) -> None:
        self._remaining: list[tuple[Fault, int]] = [(fault, fault.times) for fault in faults]
        self.injected: list[InjectedFault] = []

    @property
    def remaining(self) -> list[Fault]:
        return [fault for fault, _ in self._remaining]

    def take(self, point: FaultPoint, *, sandbox_id: str | None = None, command: str | None = None) -> Fault | None:
        """Consume and return the first pending fault for `point`, or None to let the call through."""
        for index, (fault, times) in enumerate(self._remaining):
            if not fault.matches(point, command):
                continue
            if times == 1:
                del self._remaining[index]
            else:
                self._remaining[index] = (fault, times - 1)
            self.injected.append(InjectedFault(fault=fault, sandbox_id=sandbox_id, command=command))
            return fault
        return None


class FaultySandbox(Sandbox):
    def __init__(self, inner: Sandbox, plan: FaultPlan) -> None:
        self._inner = inner
        self._plan = plan

    @property
    def inner(self) -> Sandbox:
        return self._inner

    @property
    def id(self) -> str:
        return self._inner.id

    @property
    def name(self) -> str:
        return self._inner.name

    @property
    def state(self) -> str:
        return self._inner.state

    @property
    def labels(self) -> Mapping[str, str] | None:
        return self._inner.labels

    @labels.setter
    def labels(self, value: Mapping[str, str] | None) -> None:
        self._inner.labels = value

    @property
    def created_at(self) -> datetime | None:
        return self._inner.created_at

    @created_at.setter
    def created_at(self, value: datetime | None) -> None:
        self._inner.created_at = value

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        return self._inner.provider_metadata

    def _take(self, point: FaultPoint, command: str | None = None) -> Fault | None:
        return self._plan.take(point, sandbox_id=self._inner.id, command=command)

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        fault = self._take("exec", command)
        if fault is None:
            return await self._inner.exec(command, cwd=cwd, timeout=timeout)
        if fault.when == "after":
            await self._inner.exec(command, cwd=cwd, timeout=timeout)
        if fault.effect == "command_failure":
            return ExecResult(exit_code=fault.exit_code, output=fault.message)
        raise fault.error()

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        fault = self._take("command", command)
        if fault is None or fault.when == "after":
            async for chunk in self._inner.command(command, cwd=cwd, timeout=timeout, env_vars=env_vars):
                yield chunk
            if fault is None:
                return
        if fault.effect == "command_failure" and fault.message:
            yield fault.message
        raise fault.error()

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        fault = self._take("upload_file")
        if fault is None or fault.when == "after":
            await self._inner.upload_file(remote_path, content)
        if fault is not None:
            raise fault.error()

    async def download_file(self, remote_path: str) -> bytes:
        fault = self._take("download_file")
        if fault is None:
            return await self._inner.download_file(remote_path)
        if fault.when == "after":
            await self._inner.download_file(remote_path)
        raise fault.error()

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        fault = self._take("download_file")
        if fault is None or fault.when == "after":
            async for chunk in self._inner.stream_download(remote_path):
                yield chunk
            if fault is None:
                return
        raise fault.error()

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        fault = self._take("modify_egress_rules")
        if fault is None or fault.when == "after":
            await self._inner.modify_egress_rules(allowed_addresses)
        if fault is not None:
            raise fault.error()

    async def block_all_egress(self) -> None:
        fault = self._take("block_all_egress")
        if fault is None or fault.when == "after":
            await self._inner.block_all_egress()
        if fault is not None:
            raise fault.error()

    async def clear_egress_rules(self) -> None:
        fault = self._take("clear_egress_rules")
        if fault is None or fault.when == "after":
            await self._inner.clear_egress_rules()
        if fault is not None:
            raise fault.error()

    async def snapshot(self, name: str) -> SnapshotSource:
        fault = self._take("snapshot")
        if fault is None:
            return await self._inner.snapshot(name)
        if fault.when == "after":
            await self._inner.snapshot(name)
        raise fault.error()


class FaultySandboxProvider(SandboxProvider):
    """Delegates to `inner` and fails calls according to `faults`; see `Fault`."""

    def __init__(self, inner: SandboxProvider, faults: Sequence[Fault]) -> None:
        self._inner = inner
        self._plan = FaultPlan(faults)

    @property
    def inner(self) -> SandboxProvider:
        return self._inner

    @property
    def injected(self) -> list[InjectedFault]:
        """Faults that have fired so far, in order."""
        return self._plan.injected

    @property
    def pending(self) -> list[Fault]:
        """Faults that have not fired yet."""
        return self._plan.remaining

    @property
    def admission_pool_id(self) -> str | None:
        return self._inner.admission_pool_id

    @property
    def capabilities(self) -> SandboxCapabilities:
        return self._inner.capabilities

    async def check_admission(self, source: SandboxSource, resources: Resources) -> bool:
        return await self._inner.check_admission(source, resources)

    async def reservable_demand(self, source: SandboxSource, resources: Resources) -> SandboxReservationDemand:
        return await self._inner.reservable_demand(source, resources)

    async def get_capacity(self) -> SandboxCapacity | None:
        return await self._inner.get_capacity()

    async def get_capacity_domains(self) -> list[SandboxCapacityDomain] | None:
        return await self._inner.get_capacity_domains()

    def _wrap(self, sandbox: Sandbox) -> Sandbox:
        return FaultySandbox(sandbox, self._plan)

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        fault = self._plan.take("create_sandbox")
        if fault is None:
            return self._wrap(await self._inner.create_sandbox(request))
        if fault.when == "after":
            await self._inner.create_sandbox(request)
        raise fault.error()

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        fault = self._plan.take("get_sandbox", sandbox_id=instance_id)
        if fault is None:
            return self._wrap(await self._inner.get_sandbox(instance_id))
        if fault.when == "after":
            await self._inner.get_sandbox(instance_id)
        raise fault.error()

    async def delete_sandbox(self, instance_id: str) -> None:
        fault = self._plan.take("delete_sandbox", sandbox_id=instance_id)
        if fault is None or fault.when == "after":
            await self._inner.delete_sandbox(instance_id)
        if fault is not None:
            raise fault.error()

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        async for sandbox in self._inner.list_sandboxes(query):
            yield self._wrap(sandbox)

    async def delete_snapshot(self, snapshot: SnapshotSource) -> None:
        await self._inner.delete_snapshot(snapshot)

    async def close(self) -> None:
        await self._inner.close()


InnerProviderConfig = Annotated[
    DaytonaProviderConfig | ModalProviderConfig | DockerProviderConfig, Field(discriminator="type")
]


class FaultyProviderConfig(BaseModel):
    """Selects a real provider and the faults to inject on top of it; for tests and canaries only."""

    type: Literal["faulty"] = "faulty"
    inner: InnerProviderConfig
    faults: list[Fault] = Field(default_factory=list)

    def create_provider(self) -> SandboxProvider:
        return FaultySandboxProvider(self.inner.create_provider(), self.faults)
