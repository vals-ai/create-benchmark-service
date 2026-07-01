from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Mapping
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field


class ImageSource(BaseModel):
    type: Literal["image"] = "image"
    image: str


class SnapshotSource(BaseModel):
    type: Literal["snapshot"] = "snapshot"
    snapshot: str


BaseSandboxSource = Annotated[ImageSource | SnapshotSource, Field(discriminator="type")]


class ComposeSource(BaseModel):
    type: Literal["compose"] = "compose"
    outer: BaseSandboxSource
    service: str = "main"
    compose_command: str = "docker compose"


SandboxSource = Annotated[ImageSource | SnapshotSource | ComposeSource, Field(discriminator="type")]

_ENV_VAR_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_COMMAND_ENV_NAMES = frozenset({"LANG", "TERM"})


def validate_command_env(env_vars: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(env_vars or {})
    invalid_names = [name for name in env if _ENV_VAR_NAME.fullmatch(name) is None]
    if invalid_names:
        raise ValueError(f"Invalid environment variable names: {', '.join(invalid_names)}")
    reserved_names = sorted(env.keys() & _RESERVED_COMMAND_ENV_NAMES)
    if reserved_names:
        raise ValueError(f"Reserved command environment variable names: {', '.join(reserved_names)}")
    return env


class Resources(BaseModel):
    vcpu: int = Field(description="Logical sandbox CPU count")
    memory: int = Field(description="Sandbox memory")
    disk: int = Field(description="Sandbox ephemeral disk")


class SandboxCreateRequest(BaseModel):
    source: SandboxSource
    resources: Resources
    name: str
    labels: dict[str, str]
    env_vars: dict[str, str]
    auto_stop_interval: int
    create_timeout: int
    network_block_all: bool = False
    reuse: bool = True


class SandboxQuery(BaseModel):
    labels: dict[str, str]
    page_size: int = 10


class MissingSandboxConfigError(ValueError):
    pass


class SandboxError(Exception):
    pass


class SandboxNotFoundError(SandboxError):
    pass


class SandboxConnectionError(SandboxError):
    pass


class SandboxCommandError(SandboxError):
    def __init__(self, exit_code: int) -> None:
        self.exit_code = exit_code
        super().__init__(f"Command failed with exit code {exit_code}")


class ExecResult(BaseModel):
    exit_code: int
    output: str = ""

    @property
    def stdout(self) -> str:
        return self.output


class Sandbox(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def state(self) -> str: ...

    @abstractmethod
    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]: ...

    @abstractmethod
    async def upload_file(self, remote_path: str, content: bytes) -> None: ...

    @abstractmethod
    async def download_file(self, remote_path: str) -> bytes: ...

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        raise SandboxError("Sandbox provider does not support modifying egress rules")

    async def clear_egress_rules(self) -> None:
        raise SandboxError("Sandbox provider does not support modifying egress rules")


class SandboxProvider(ABC):
    @abstractmethod
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox: ...

    @abstractmethod
    async def get_sandbox(self, instance_id: str) -> Sandbox: ...

    @abstractmethod
    async def delete_sandbox(self, instance_id: str) -> None: ...

    @abstractmethod
    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]: ...

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()
