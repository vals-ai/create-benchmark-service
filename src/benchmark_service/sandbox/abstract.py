from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Mapping
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field


_DEFAULT_CREATION_TIMEOUT = 360

class SandboxResources(BaseModel):
    cpu: int = Field(gt=0, le=32)
    memory: int = Field(gt=0, le=64) # in GB
    disk: int = Field(gt=0, le=256) # in GB


class SandboxQuery(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict)
    limit: int = 10


class ExecResult(BaseModel):
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    duration: float | None = None


class SandboxFile(BaseModel):
    content: bytes
    remote_path: str


class SandboxProviderType(StrEnum):
    DAYTONA = "daytona"


class SandboxSourceType(StrEnum):
    IMAGE = "image"
    SNAPSHOT = "snapshot"


class SandboxCreateRequest(BaseModel):
    source_id: str
    source_type: SandboxSourceType = SandboxSourceType.IMAGE
    resources: SandboxResources | None = None
    network_blocked: bool = False
    env_vars: dict[str, str] = Field(default_factory=dict)
    labels: dict[str, str] = Field(default_factory=dict)
    name: str | None = None
    auto_delete_interval: int | None = None
    creation_timeout: int = _DEFAULT_CREATION_TIMEOUT


class SandboxError(Exception):
    pass


class InvalidSandboxConfigurationError(SandboxError):
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
        """Execute a command in the sandbox.
        When on_stdout/on_stderr are provided, output is streamed via callbacks.
        When omitted, uses a simpler non-streaming execution path.
        Returns exit code, stdout/stderr, and optionally duration.
        """
        pass

    @abstractmethod
    async def upload_file(self, file: SandboxFile) -> None:
        pass

    @abstractmethod
    async def upload_files(self, files: list[SandboxFile]) -> None:
        pass

    @abstractmethod
    async def download_file(self, remote_path: str) -> bytes:
        pass

    @abstractmethod
    async def create_folder(self, remote_path: str) -> None:
        pass

    @abstractmethod
    async def wait_until_ready(self) -> None:
        """Block until the sandbox is ready to accept commands."""
        pass


class SandboxProvider(ABC):
    @classmethod
    @abstractmethod
    async def from_headers(cls, headers: Mapping[str, str], **kwargs: Any) -> Self:
        """Authenticate with a sandbox provider using provided headers."""
        pass

    @abstractmethod
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        """Create sandbox from base image or snapshot."""
        pass

    @abstractmethod
    async def get_sandbox(self, id: str) -> Sandbox:
        pass

    @abstractmethod
    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        pass

    @abstractmethod
    async def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[Sandbox]:
        pass

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
