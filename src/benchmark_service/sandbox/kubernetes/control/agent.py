"""Connect the control service to the authenticated sandbox Pod agent."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
from collections.abc import AsyncGenerator, AsyncIterable
from typing import Protocol

import httpx

from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandErrorEvent,
    CommandEvent,
    CommandExitEvent,
    CommandRequest,
    command_event_adapter,
)
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError


def agent_token(api_token: str, resource_name: str) -> str:
    """Derive a sandbox-specific data-plane token from the control secret."""
    return hmac.new(api_token.encode(), resource_name.encode(), hashlib.sha256).hexdigest()


class PodDataPlane(Protocol):
    """Command and file operations served from inside one sandbox Pod."""

    def command(
        self,
        pod_ip: str,
        resource_name: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]: ...

    async def upload_file(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None: ...

    def stream_download(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]: ...

    async def close(self) -> None: ...


class PodAgentClient:
    """Stream commands and files directly to the agent inside a sandbox container."""

    def __init__(
        self,
        settings: KubernetesControlSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=settings.exec_connection_pool_size,
                max_keepalive_connections=min(settings.exec_connection_pool_size, 256),
            ),
            timeout=httpx.Timeout(
                settings.agent_connect_timeout_seconds,
                connect=settings.agent_connect_timeout_seconds,
                read=max(settings.agent_heartbeat_seconds * 3, settings.agent_connect_timeout_seconds),
                write=None,
                pool=settings.agent_connect_timeout_seconds,
            ),
            transport=transport,
            trust_env=False,
        )

    def _url(self, pod_ip: str, path: str) -> str:
        try:
            address = ipaddress.ip_address(pod_ip)
        except ValueError as error:
            raise SandboxError(f"Invalid sandbox Pod IP: {pod_ip}") from error
        host = f"[{address}]" if address.version == 6 else str(address)
        return f"http://{host}:{self.settings.agent_port}{path}"

    def _headers(self, resource_name: str) -> dict[str, str]:
        token = agent_token(self.settings.api_token, resource_name)
        return {"Authorization": f"Bearer {token}"}

    async def _raise_for_response(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        await response.aread()
        message = response.text.strip() or "sandbox agent request failed"
        detail = f"Sandbox agent returned HTTP {response.status_code}: {message}"
        if response.status_code >= 500:
            raise SandboxConnectionError(detail)
        raise SandboxError(detail)

    async def command(
        self,
        pod_ip: str,
        resource_name: str,
        request: CommandRequest,
    ) -> AsyncGenerator[CommandEvent, None]:
        """Yield ordered command events without routing bytes through the API server."""
        terminal_received = False
        try:
            async with self._client.stream(
                "POST",
                self._url(pod_ip, "/v1/command"),
                headers=self._headers(resource_name),
                json=request.model_dump(mode="json"),
            ) as response:
                await self._raise_for_response(response)
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    try:
                        event = command_event_adapter.validate_json(line)
                    except ValueError as error:
                        raise SandboxConnectionError("Sandbox agent returned an invalid command event") from error
                    terminal_received = isinstance(event, (CommandExitEvent, CommandErrorEvent))
                    yield event
                    if terminal_received:
                        return
        except httpx.TransportError as error:
            raise SandboxConnectionError(f"Sandbox agent connection failed: {error}") from error
        if not terminal_received:
            raise SandboxConnectionError("Sandbox agent command stream ended without an exit event")

    async def upload_file(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
        chunks: AsyncIterable[bytes],
    ) -> None:
        """Stream a binary upload directly into the sandbox filesystem."""
        try:
            response = await self._client.put(
                self._url(pod_ip, "/v1/files"),
                headers=self._headers(resource_name),
                params={"path": remote_path},
                content=chunks,
            )
        except httpx.TransportError as error:
            raise SandboxConnectionError(f"Sandbox agent connection failed: {error}") from error
        await self._raise_for_response(response)

    async def stream_download(
        self,
        pod_ip: str,
        resource_name: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        """Stream a binary download directly from the sandbox filesystem."""
        try:
            async with self._client.stream(
                "GET",
                self._url(pod_ip, "/v1/files"),
                headers=self._headers(resource_name),
                params={"path": remote_path},
            ) as response:
                await self._raise_for_response(response)
                async for chunk in response.aiter_bytes():
                    if chunk:
                        yield chunk
        except httpx.TransportError as error:
            raise SandboxConnectionError(f"Sandbox agent connection failed: {error}") from error

    async def close(self) -> None:
        """Close the connections shared by Pod-agent operations."""
        await self._client.aclose()
