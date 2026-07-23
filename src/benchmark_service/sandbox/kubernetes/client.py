"""Implement the provider-side client for the private Kubernetes control API."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from benchmark_service.sandbox.kubernetes.protocol import (
    CommandExitEvent,
    CommandOutputEvent,
    CommandRequest,
    ControlErrorDetail,
    ControlErrorResponse,
    EgressRequest,
    ExecResponse,
    SandboxListPage,
    SandboxRecord,
    command_event_adapter,
)
from benchmark_service.sandbox.kubernetes.runtime import KubernetesRuntimeDriver
from benchmark_service.sandbox.kubernetes.sandbox import KubernetesSandbox
from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxQuery,
)

_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_LIST_PAGE_LIMIT = 100


class KubernetesControlClientDriver(KubernetesRuntimeDriver):
    """Call the private Kubernetes sandbox control API."""

    def __init__(
        self,
        api_url: str,
        api_token: str,
        *,
        connect_timeout: float = 10,
        request_timeout: float = 60,
        stream_read_timeout: float = 45,
        max_connections: int = 256,
        max_keepalive_connections: int = 64,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if connect_timeout <= 0 or request_timeout <= 0 or stream_read_timeout <= 0:
            raise ValueError("Kubernetes control API timeouts must be positive")
        if max_connections <= 0 or max_keepalive_connections < 0:
            raise ValueError("Kubernetes control API connection limits must not be negative")
        if max_keepalive_connections > max_connections:
            raise ValueError("Kubernetes keepalive connections cannot exceed total connections")

        self._api_url = api_url.rstrip("/")
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._stream_read_timeout = stream_read_timeout
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=httpx.Timeout(request_timeout, connect=connect_timeout),
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            transport=transport,
        )

    def _url(self, path: str) -> str:
        return f"{self._api_url}{path}"

    def _sandbox(self, record: SandboxRecord) -> KubernetesSandbox:
        return KubernetesSandbox(
            instance_id=record.id,
            name=record.name,
            state=record.state,
            driver=self,
            labels=record.labels,
        )

    def _error_from_detail(
        self,
        detail: ControlErrorDetail,
        *,
        status_code: int | None = None,
    ) -> SandboxError:
        message = detail.message
        if detail.request_id:
            message = f"{message} (request_id={detail.request_id})"
        if detail.code == "not_found" or status_code == 404:
            return SandboxNotFoundError(message)
        return SandboxError(message)

    def _raise_for_response(
        self,
        response: httpx.Response,
        *,
        not_found_ok: bool = False,
    ) -> None:
        if response.is_success or (not_found_ok and response.status_code == 404):
            return
        try:
            detail = ControlErrorResponse.model_validate(response.json()).error
        except (ValueError, TypeError):
            detail = ControlErrorDetail(
                code="not_found" if response.status_code == 404 else "control_error",
                message=response.text or f"Control API returned HTTP {response.status_code}",
            )
        if response.status_code in _RETRYABLE_STATUSES:
            raise SandboxConnectionError(detail.message)
        raise self._error_from_detail(detail, status_code=response.status_code)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        retryable: bool,
        not_found_ok: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        attempts = 3 if retryable else 1
        last_error: httpx.TransportError | None = None
        for attempt in range(attempts):
            try:
                response = await self._client.request(method, self._url(path), **kwargs)
            except httpx.TransportError as error:
                last_error = error
                if attempt == attempts - 1:
                    raise SandboxConnectionError(str(error)) from error
            else:
                if response.status_code not in _RETRYABLE_STATUSES or attempt == attempts - 1:
                    self._raise_for_response(response, not_found_ok=not_found_ok)
                    return response
            await asyncio.sleep(0.25 * (2**attempt))

        raise SandboxConnectionError(str(last_error or "Control API request failed"))

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        response = await self._request(
            "POST",
            "/v1/sandboxes",
            retryable=True,
            json=request.model_dump(mode="json"),
        )
        return self._sandbox(SandboxRecord.model_validate(response.json()))

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        response = await self._request(
            "GET",
            f"/v1/sandboxes/{quote(instance_id, safe='')}",
            retryable=True,
        )
        return self._sandbox(SandboxRecord.model_validate(response.json()))

    async def delete_sandbox(self, instance_id: str) -> None:
        await self._request(
            "DELETE",
            f"/v1/sandboxes/{quote(instance_id, safe='')}",
            retryable=True,
            not_found_ok=True,
        )

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        continue_token: str | None = None
        yielded = 0
        while yielded < query.page_size:
            params = [("label", f"{name}={value}") for name, value in sorted(query.labels.items())]
            params.append(("limit", str(min(query.page_size - yielded, _LIST_PAGE_LIMIT))))
            if continue_token:
                params.append(("continue_token", continue_token))
            response = await self._request(
                "GET",
                "/v1/sandboxes",
                retryable=True,
                params=params,
            )
            page = SandboxListPage.model_validate(response.json())
            for record in page.items:
                yield self._sandbox(record)
                yielded += 1
                if yielded == query.page_size:
                    return
            if not page.continue_token:
                return
            continue_token = page.continue_token

    async def exec(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        response = await self._request(
            "POST",
            f"/v1/sandboxes/{quote(instance_id, safe='')}/exec",
            retryable=False,
            json=CommandRequest(command=command, cwd=cwd, timeout=timeout).model_dump(mode="json"),
        )
        result = ExecResponse.model_validate(response.json())
        return ExecResult(exit_code=result.exit_code, output=result.output)

    async def command(
        self,
        instance_id: str,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        """Stream command output and fail when the control service omits a terminal event."""
        request = CommandRequest(
            command=command,
            cwd=cwd,
            timeout=timeout,
            env_vars=dict(env_vars) if env_vars is not None else None,
        )
        path = f"/v1/sandboxes/{quote(instance_id, safe='')}/command"
        terminal_error: SandboxError | None = None
        terminal_received = False
        try:
            async with self._client.stream(
                "POST",
                self._url(path),
                json=request.model_dump(mode="json"),
                timeout=httpx.Timeout(
                    self._request_timeout,
                    connect=self._connect_timeout,
                    read=self._stream_read_timeout,
                ),
            ) as response:
                self._raise_for_response(response)
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    try:
                        event = command_event_adapter.validate_json(line)
                    except ValueError as error:
                        raise SandboxConnectionError("Control API returned an invalid command event") from error
                    if isinstance(event, CommandOutputEvent):
                        yield event.data
                    elif isinstance(event, CommandExitEvent):
                        terminal_received = True
                        if event.exit_code != 0:
                            terminal_error = SandboxCommandError(event.exit_code)
                        break
                    else:
                        terminal_received = True
                        terminal_error = self._error_from_detail(
                            ControlErrorDetail(
                                code=event.code,
                                message=event.message,
                                request_id=event.request_id,
                            )
                        )
                        break
        except httpx.TransportError as error:
            raise SandboxConnectionError(str(error)) from error
        if not terminal_received:
            raise SandboxConnectionError("Control API command stream ended without an exit event")
        if terminal_error is not None:
            raise terminal_error

    async def upload_file(self, instance_id: str, remote_path: str, content: bytes) -> None:
        await self._request(
            "PUT",
            f"/v1/sandboxes/{quote(instance_id, safe='')}/files",
            retryable=False,
            params={"path": remote_path},
            content=content,
        )

    async def download_file(self, instance_id: str, remote_path: str) -> bytes:
        return b"".join([chunk async for chunk in self.stream_download(instance_id, remote_path)])

    async def stream_download(
        self,
        instance_id: str,
        remote_path: str,
    ) -> AsyncGenerator[bytes, None]:
        """Stream remote-file bytes without buffering them in the provider process."""
        try:
            async with self._client.stream(
                "GET",
                self._url(f"/v1/sandboxes/{quote(instance_id, safe='')}/files"),
                params={"path": remote_path},
            ) as response:
                self._raise_for_response(response)
                async for chunk in response.aiter_bytes():
                    if chunk:
                        yield chunk
        except httpx.TransportError as error:
            raise SandboxConnectionError(str(error)) from error

    async def modify_egress_rules(self, instance_id: str, allowed_addresses: list[str]) -> None:
        await self._request(
            "PUT",
            f"/v1/sandboxes/{quote(instance_id, safe='')}/egress",
            retryable=False,
            json=EgressRequest(allowed_addresses=allowed_addresses).model_dump(mode="json"),
        )

    async def clear_egress_rules(self, instance_id: str) -> None:
        await self._request(
            "DELETE",
            f"/v1/sandboxes/{quote(instance_id, safe='')}/egress",
            retryable=False,
            not_found_ok=True,
        )

    async def close(self) -> None:
        """Close the HTTP connections held by this driver."""
        await self._client.aclose()
