from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field


class ImageSource(BaseModel):
    type: Literal["image"] = "image"
    image: str


class SnapshotSource(BaseModel):
    type: Literal["snapshot"] = "snapshot"
    snapshot: str


SandboxSource = Annotated[ImageSource | SnapshotSource, Field(discriminator="type")]


class Resources(BaseModel):
    cpu: int = Field(description="Logical sandbox CPU count")
    memory_gb: int = Field(description="Sandbox memory in GB")
    disk_gb: int = Field(description="Sandbox ephemeral disk in GB")


class SandboxCreateRequest(BaseModel):
    source: SandboxSource
    resources: Resources
    name: str
    labels: dict[str, str]
    env_vars: dict[str, str]


class SandboxQuery(BaseModel):
    labels: dict[str, str]
    page_size: int = 10


class DaytonaBackendConfig(BaseModel):
    type: Literal["daytona"] = "daytona"
    api_key: str
    api_url: str
    target: str


SandboxBackendConfig = DaytonaBackendConfig


class MissingSandboxConfigError(ValueError):
    pass


class SandboxError(Exception):
    pass


class SandboxNotFoundError(SandboxError):
    pass


class ExecResult(BaseModel):
    exit_code: int
    stdout: str = ""
    stderr: str = ""


class Sandbox(ABC):
    id: str
    name: str
    state: str | None = None

    @abstractmethod
    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ExecResult: ...

    @abstractmethod
    async def upload_file(self, remote_path: str, content: bytes) -> None: ...

    @abstractmethod
    async def download_file(self, remote_path: str) -> bytes: ...


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
