"""Tests for the catalog API allowlist client."""

from __future__ import annotations

import httpx
import pytest
from benchmark_service.allowlist import (
    CatalogAllowlistClient,
    TenantConfig,
)


def _response(payload: object, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=payload)


class _ReusableTransport(httpx.AsyncBaseTransport):
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.closed:
            raise RuntimeError("transport is closed")
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def _transport(
    responses: list[httpx.Response | Exception],
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    return httpx.MockTransport(handler), requests


@pytest.mark.asyncio
async def test_fetches_and_validates_tenant_policy() -> None:
    transport, requests = _transport(
        [_response({"name": "example-service", "datasets": ["default"], "trial_mode": True})]
    )
    client = CatalogAllowlistClient(
        "https://catalog.example.test/",
        "example-service",
        transport=transport,
    )

    config = await client.get_tenant_config("key-acme", "acme")

    assert config == TenantConfig(datasets=["default"], trial_mode=True)
    assert len(requests) == 1
    assert str(requests[0].url) == "https://catalog.example.test/benchmark-services/example-service"
    assert requests[0].headers["x-descope-api-key"] == "key-acme"


@pytest.mark.asyncio
async def test_reuses_transport_until_client_is_closed() -> None:
    transport = _ReusableTransport(
        [
            _response({}, status_code=404),
            _response({"name": "example-service", "datasets": ["default"], "trial_mode": False}),
        ]
    )
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") is None
    assert await client.get_tenant_config("key-acme", "acme") is not None
    assert transport.closed is False

    await client.aclose()
    assert transport.closed is False
    await transport.aclose()
    assert transport.closed is True


@pytest.mark.asyncio
async def test_client_does_not_close_caller_owned_transport() -> None:
    transport = _ReusableTransport([_response({}, status_code=404)])
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    await client.aclose()

    assert transport.closed is False
    await transport.aclose()
    assert transport.closed is True


@pytest.mark.asyncio
async def test_success_is_cached_per_tenant() -> None:
    transport, requests = _transport(
        [_response({"name": "example-service", "datasets": ["default"], "trial_mode": False})]
    )
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") is not None
    assert await client.get_tenant_config("key-acme", "acme") is not None

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_miss_is_not_cached() -> None:
    transport, requests = _transport(
        [
            _response({}, status_code=404),
            _response({"name": "example-service", "datasets": ["default"], "trial_mode": False}),
        ]
    )
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") is None
    assert await client.get_tenant_config("key-acme", "acme") is not None

    assert len(requests) == 2


@pytest.mark.asyncio
async def test_transport_and_decode_failures_are_not_cached() -> None:
    transport, requests = _transport(
        [
            httpx.ConnectError("catalog unavailable"),
            httpx.Response(200, content=b"not-json"),
            _response({"name": "example-service", "datasets": ["default"], "trial_mode": False}),
        ]
    )
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") is None
    assert await client.get_tenant_config("key-acme", "acme") is None
    assert await client.get_tenant_config("key-acme", "acme") is not None
    assert len(requests) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"name": "example-service", "datasets": ["default"]},
        {"name": "example-service", "datasets": [1], "trial_mode": False},
        {"name": "example-service", "datasets": ["default"], "trial_mode": 1},
        {"name": "other-service", "datasets": ["default"], "trial_mode": False},
    ],
)
async def test_invalid_policy_is_rejected(payload: object) -> None:
    transport, requests = _transport([_response(payload)])
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") is None
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_unknown_policy_fields_are_ignored() -> None:
    transport, requests = _transport(
        [_response({"name": "example-service", "datasets": ["default"], "trial_mode": False, "future": True})]
    )
    client = CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport)

    assert await client.get_tenant_config("key-acme", "acme") == TenantConfig(datasets=["default"])
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_positive_policy_expires_after_300_seconds() -> None:
    clock = [0.0]
    transport, requests = _transport(
        [
            _response({"name": "example-service", "datasets": ["default"], "trial_mode": False}),
            _response({"name": "example-service", "datasets": ["validation"], "trial_mode": False}),
        ]
    )
    client = CatalogAllowlistClient(
        "https://catalog.example.test",
        "example-service",
        transport=transport,
        cache_timer=lambda: clock[0],
    )

    assert await client.get_tenant_config("key-acme", "acme") == TenantConfig(datasets=["default"])
    clock[0] = 301.0
    assert await client.get_tenant_config("key-acme", "acme") == TenantConfig(datasets=["validation"])
    assert len(requests) == 2


def test_client_rejects_empty_configuration() -> None:
    with pytest.raises(ValueError, match="base URL"):
        CatalogAllowlistClient("", "example-service")
    with pytest.raises(ValueError, match="service name"):
        CatalogAllowlistClient("https://catalog.example.test", "")
