from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Mapping
from typing import Any
from urllib.parse import quote

import httpx
from httpx_ws import HTTPXWSException, WebSocketUpgradeError, aconnect_ws

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

_RETRYABLE_STATUSES = frozenset({500, 502, 503, 504})


class KubernetesControlClientDriver(KubernetesRuntimeDriver):
    """Call the private Kubernetes sandbox control API."""

    def __init__(
        self,
        api_url: str,
        api_token: str,
        *,
        connect_timeout: float = 10,
        request_timeout: float = 60,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if connect_timeout <= 0 or request_timeout <= 0:
            raise ValueError("Kubernetes control API timeouts must be positive")

        self._api_url = api_url.rstrip("/")
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=httpx.Timeout(request_timeout, connect=connect_timeout),
            follow_redirects=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            transport=transport,
        )

    def _url(self, path: str) -> str:
        return f"{self._api_url}{path}"

    def _ws_url(self, path: str) -> str:
        return self._url(path).replace("https://", "wss://", 1).replace("http://", "ws://", 1)

    def _sandbox(self, record: SandboxRecord) -> KubernetesSandbox:
        return KubernetesSandbox(
            instance_id=record.id,
            name=record.name,
            state=record.state,
            driver=self,
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
            params.append(("limit", str(query.page_size - yielded)))
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
        request = CommandRequest(
            command=command,
            cwd=cwd,
            timeout=timeout,
            env_vars=dict(env_vars) if env_vars is not None else None,
        )
        path = f"/v1/sandboxes/{quote(instance_id, safe='')}/command"
        terminal_error: SandboxError | None = None
        try:
            async with aconnect_ws(self._ws_url(path), client=self._client) as websocket:
                await websocket.send_json(request.model_dump(mode="json"))
                while True:
                    event = command_event_adapter.validate_python(await websocket.receive_json())
                    if isinstance(event, CommandOutputEvent):
                        yield event.data
                    elif isinstance(event, CommandExitEvent):
                        if event.exit_code != 0:
                            terminal_error = SandboxCommandError(event.exit_code)
                        break
                    else:
                        terminal_error = self._error_from_detail(
                            ControlErrorDetail(
                                code=event.code,
                                message=event.message,
                                request_id=event.request_id,
                            )
                        )
                        break
        except WebSocketUpgradeError as error:
            self._raise_for_response(error.response)
            raise SandboxConnectionError("WebSocket upgrade failed") from error
        except (httpx.TransportError, HTTPXWSException) as error:
            raise SandboxConnectionError(str(error)) from error
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
        await self._client.aclose()
