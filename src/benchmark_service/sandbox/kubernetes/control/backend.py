from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from typing import Protocol

from benchmark_service.sandbox.kubernetes.protocol import (
    CommandEvent,
    CommandRequest,
    ExecResponse,
    SandboxListPage,
    SandboxRecord,
)
from benchmark_service.sandbox.types import SandboxCreateRequest, SandboxError


class SandboxConflictError(SandboxError):
    """A sandbox name already exists with a different request specification."""


class SandboxControlBackend(Protocol):
    async def create_sandbox(self, request: SandboxCreateRequest) -> SandboxRecord: ...

    async def get_sandbox(self, instance_id: str) -> SandboxRecord: ...

    async def list_sandboxes(
        self,
        labels: dict[str, str],
        limit: int,
        continue_token: str | None,
    ) -> SandboxListPage: ...

    async def delete_sandbox(self, instance_id: str) -> None: ...

    async def exec(self, instance_id: str, request: CommandRequest) -> ExecResponse: ...

    def command(self, instance_id: str, request: CommandRequest) -> AsyncGenerator[CommandEvent, None]: ...

    async def upload_file(
        self,
        instance_id: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None: ...

    def stream_download(self, instance_id: str, remote_path: str) -> AsyncGenerator[bytes, None]: ...

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None: ...

    async def clear_egress_rules(self, instance_id: str) -> None: ...

    async def close(self) -> None: ...
