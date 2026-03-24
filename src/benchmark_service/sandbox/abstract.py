from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Self


@dataclass
class SandboxResources:
    cpu: float
    memory_gb: int
    disk_gb: int


@dataclass
class SandboxQuery:
    labels: dict[str, str] = field(default_factory=dict)
    limit: int = 100
    page: int = 1

@dataclass
class ExecResult:
    exit_code: int
    stdout: str = ""
    stderr: str = ""

@dataclass
class SandboxFile:
    content: bytes
    remote_path: str


class SandboxSourceType(StrEnum):
    IMAGE = "image"
    SNAPSHOT = "snapshot"

@dataclass
class SandboxCreateRequest:
    source_id: str  # image name or snapshot id depending on source_type
    source_type: SandboxSourceType = SandboxSourceType.IMAGE
    resources: SandboxResources | None = None
    network_blocked: bool = False
    env_vars: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    name: str | None = None
    idle_timeout: int | None = None
    auto_delete_interval: int | None = None
    timeout: float | None = None


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
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> ExecResult:
        """Execute a command in the sandbox.
        accepts on_stdout and on_stderr callbacks to stream the output of the command.
        e.g. internally this could use daytona websockets to stream the output of the command.
        of for non-streaming, could call the callbacks on each new line of the output/stderr.
        
        Returns the exit code of the command and stdout/stderr.
        """
        pass

    @abstractmethod
    async def upload_file(self, content: bytes, remote_path: str) -> None:
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
    async def upload_directory(self, local_path: str, remote_path: str) -> None:
        pass

    @abstractmethod
    async def download_directory(self, remote_path: str) -> bytes:
        pass


class SandboxProvider(ABC):
    @classmethod
    @abstractmethod
    def from_headers(cls, headers: Mapping[str, str], **kwargs: Any) -> Self:
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
    def list_sandboxes(self, query: SandboxQuery | None = None) -> AsyncIterator[Sandbox]:
        """Yields sandboxes matching the query, handling pagination internally."""
        pass

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
