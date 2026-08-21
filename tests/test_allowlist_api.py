"""Tests for catalog allowlist integration with authentication."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from benchmark_service import auth as auth_module
from benchmark_service.allowlist import CatalogAllowlistClient
from benchmark_service.auth import (
    clear_allowlist_cache,
    clear_auth_cache,
    clear_request_tenant_config,
    get_tenant_config,
    load_allowlist,
    resolve_descope_tenant,
)
from tests.conftest import StubBenchmark


@pytest.fixture(autouse=True)
def reset_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.delenv("SERVICE_NAME", raising=False)
    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)


def _configure_api(
    monkeypatch: pytest.MonkeyPatch,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.example.test/")
    monkeypatch.setenv("SERVICE_NAME", "example-service")
    if transport is not None:
        monkeypatch.setattr(
            auth_module,
            "_catalog_client",
            CatalogAllowlistClient("https://catalog.example.test", "example-service", transport=transport),
        )


def _transport(response: httpx.Response) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return response

    return httpx.MockTransport(handler), requests


@pytest.mark.asyncio
async def test_catalog_policy_reaches_auth_and_dataset_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, requests = _transport(
        httpx.Response(200, json={"name": "example-service", "datasets": ["default"], "trial_mode": True})
    )
    _configure_api(monkeypatch, transport)

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    service = StubBenchmark()
    config = get_tenant_config("acme")
    assert config is not None and config.trial_mode is True
    assert await service.check_dataset_access("acme", "default") is True
    assert await service.check_dataset_access("acme", "alt") is False
    assert requests[0].headers["x-descope-api-key"] == "key-acme"


@pytest.mark.asyncio
async def test_authenticated_request_keeps_policy_snapshot_after_cache_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    transport, _ = _transport(
        httpx.Response(200, json={"name": "example-service", "datasets": ["default"], "trial_mode": True})
    )
    _configure_api(monkeypatch)
    monkeypatch.setattr(
        auth_module,
        "_catalog_client",
        CatalogAllowlistClient(
            "https://catalog.example.test",
            "example-service",
            transport=transport,
            cache_timer=lambda: clock[0],
        ),
    )

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    clock[0] = 301.0
    config = get_tenant_config("acme")
    assert config is not None and config.trial_mode is True

    clear_request_tenant_config()
    assert get_tenant_config("acme") is None


@pytest.mark.asyncio
async def test_catalog_client_is_built_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    transport, requests = _transport(
        httpx.Response(200, json={"name": "example-service", "datasets": ["default"], "trial_mode": False})
    )

    def build_client(base_url: str, service_name: str) -> CatalogAllowlistClient:
        return CatalogAllowlistClient(base_url, service_name, transport=transport)

    monkeypatch.setattr(auth_module, "CatalogAllowlistClient", build_client)
    _configure_api(monkeypatch)

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_catalog_failure_does_not_fall_back_to_legacy_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport, _ = _transport(httpx.Response(503))
    _configure_api(monkeypatch, transport)
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert load_allowlist().tenants == {}


@pytest.mark.asyncio
async def test_catalog_mode_without_service_name_fails_closed_without_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.example.test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert load_allowlist().tenants == {}
