from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import quote

import httpx

from benchmark_service.sandbox.kubernetes.protocol import (
    ControlErrorDetail,
    ControlErrorResponse,
    SandboxListPage,
    SandboxRecord,
)
from benchmark_service.sandbox.kubernetes.runtime import KubernetesRuntimeDriver
from benchmark_service.sandbox.kubernetes.sandbox import KubernetesSandbox
from benchmark_service.sandbox.types import (
    Sandbox,
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

    async def close(self) -> None:
        await self._client.aclose()
