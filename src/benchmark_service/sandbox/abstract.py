from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Literal, Self, TypeAlias

from pydantic import BaseModel, Field


class SandboxProviderType(StrEnum):
    DAYTONA = "daytona"
    MODAL = "modal"


class SandboxResources(BaseModel):
    cpu: int = Field(gt=0)
    memory: int = Field(gt=0)
    disk: int = Field(gt=0)


class ExecResult(BaseModel):
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""


class SandboxFile(BaseModel):
    content: bytes
    remote_path: str


class SandboxQuery(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict)
    limit: int = 10


class SandboxCreateRequestBase(BaseModel):
    resources: SandboxResources | None = None
    env_vars: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] = Field(default_factory=dict)
    name: str | None = None
    auto_delete_interval: int | None = None
    creation_timeout: int = 360
    network_blocked: bool = False


class ImageSandboxCreateRequest(SandboxCreateRequestBase):
    source_type: Literal["image"] = "image"
    image: str


class SnapshotSandboxCreateRequest(SandboxCreateRequestBase):
    source_type: Literal["snapshot"] = "snapshot"
    snapshot: str


SandboxCreateRequest: TypeAlias = ImageSandboxCreateRequest | SnapshotSandboxCreateRequest


class SandboxError(Exception):
    pass


class SandboxNotFoundError(SandboxError):
    pass


class Sandbox(ABC):
    def __init__(self, provider: SandboxProvider, id: str, name: str) -> None:
        self.provider = provider
        self.id = id
        self.name = name

    @abstractmethod
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        pass

    @abstractmethod
    async def upload_file(self, file: SandboxFile) -> None:
        pass

    @abstractmethod
    async def upload_local_file(self, local_path: Path, remote_path: str) -> None:
        pass

    @abstractmethod
    async def upload_files(self, files: list[SandboxFile]) -> None:
        pass

    @abstractmethod
    async def download_file(self, remote_path: str) -> bytes:
        pass

    @abstractmethod
    async def wait_until_ready(self) -> None:
        pass

    @abstractmethod
    async def wait_until_stopped(self) -> None:
        pass


class SandboxProvider(ABC):
    provider_type: ClassVar[SandboxProviderType]

    @classmethod
    @abstractmethod
    async def from_headers(cls, headers: Mapping[str, str]) -> Self:
        pass

    @abstractmethod
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        pass

    @abstractmethod
    async def get_sandbox(self, id: str) -> Sandbox:
        pass

    @abstractmethod
    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        pass

    @abstractmethod
    def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[Sandbox]:
        pass

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()
