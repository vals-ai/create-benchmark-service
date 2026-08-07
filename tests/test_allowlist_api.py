"""Tests for the catalog API-backed tenant policy loader."""

from __future__ import annotations

import json
from collections.abc import Mapping
from unittest.mock import AsyncMock, patch

import pytest
from cachetools import TTLCache

from benchmark_service import auth as auth_module
from benchmark_service.auth import (
    AUTH_CACHE_MAX_SIZE,
    TenantConfig,
    clear_allowlist_cache,
    clear_auth_cache,
    get_tenant_config,
    load_allowlist,
    resolve_descope_tenant,
)
from tests.conftest import StubBenchmark


class _Response:
    def __init__(self, status_code: int, payload: object = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _AsyncClient:
    def __init__(self, responses: list[_Response | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Mapping[str, str]]] = []

    async def __aenter__(self) -> _AsyncClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get(self, url: str, *, headers: Mapping[str, str]) -> _Response:
        self.calls.append((url, headers))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _AsyncClientFactory:
    def __init__(self, client: _AsyncClient) -> None:
        self.client = client

    def __call__(self, **_kwargs: object) -> _AsyncClient:
        return self.client


@pytest.fixture(autouse=True)
def reset_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.delenv("SERVICE_NAME", raising=False)
    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)


def _configure_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.example.test/")
    monkeypatch.setenv("SERVICE_NAME", "example-service")


def _valid_response(*, trial_mode: bool = False) -> _Response:
    return _Response(
        200,
        {"name": "example-service", "datasets": ["default"], "trial_mode": trial_mode},
    )


@pytest.mark.asyncio
async def test_api_success_is_cached_per_service_and_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([_valid_response(trial_mode=True)])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    assert client.calls == [
        (
            "https://catalog.example.test/benchmark-services/example-service",
            {"x-descope-api-key": "key-acme"},
        )
    ]
    config = get_tenant_config("acme")
    assert config == TenantConfig(datasets=["default"], trial_mode=True)


@pytest.mark.asyncio
async def test_api_miss_is_not_cached_and_refreshes_unknown_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([_Response(404), _valid_response()])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_api_positive_cache_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([_valid_response(), _valid_response()])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))
    clock = [0.0]
    monkeypatch.setattr(
        auth_module,
        "_api_allowlist_cache",
        TTLCache[tuple[str, str], TenantConfig](
            maxsize=AUTH_CACHE_MAX_SIZE,
            ttl=300,
            timer=lambda: clock[0],
        ),
    )

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"
        clock[0] = 301.0
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_api_revocation_is_seen_after_positive_cache_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([_valid_response(), _Response(404)])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))
    clock = [0.0]
    monkeypatch.setattr(
        auth_module,
        "_api_allowlist_cache",
        TTLCache[tuple[str, str], TenantConfig](
            maxsize=AUTH_CACHE_MAX_SIZE,
            ttl=300,
            timer=lambda: clock[0],
        ),
    )

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"
        clock[0] = 301.0
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_api_failure_does_not_fall_back_to_legacy_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_api(monkeypatch)
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    client = _AsyncClient([_Response(503)])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert load_allowlist().tenants == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        _Response(503),
        RuntimeError("catalog unavailable"),
        _Response(200, {"name": "example-service", "datasets": ["default"]}),
        _Response(200, {"name": "example-service", "datasets": [1], "trial_mode": False}),
        _Response(
            200,
            {
                "name": "example-service",
                "datasets": ["default"],
                "trial_mode": False,
                "unexpected": True,
            },
        ),
        _Response(200, {"name": "example-service", "datasets": ["default"], "trial_mode": 1}),
    ],
)
async def test_api_failure_or_malformed_schema_fails_closed_without_negative_cache(
    monkeypatch: pytest.MonkeyPatch,
    response: _Response | Exception,
) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([response, _valid_response()])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) == "acme"

    assert len(client.calls) == 2


def test_api_mode_does_not_fetch_without_a_caller(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    assert load_allowlist().tenants == {}
    assert client.calls == []


@pytest.mark.asyncio
async def test_api_mode_without_service_name_fails_closed_without_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.example.test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    client = _AsyncClient([])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        new=AsyncMock(return_value={"tenants": {"acme": {}}}),
    ):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert load_allowlist().tenants == {}
    assert client.calls == []


@pytest.mark.asyncio
async def test_api_policy_preserves_trial_and_dataset_authorization(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_api(monkeypatch)
    client = _AsyncClient([_valid_response(trial_mode=True)])
    monkeypatch.setattr(auth_module.httpx, "AsyncClient", _AsyncClientFactory(client))

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


def test_legacy_allowlist_is_used_when_api_url_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["validation"], "trial_mode": True}}}),
    )

    config = load_allowlist()

    assert config.tenants["acme"].datasets == ["validation"]
    assert config.tenants["acme"].trial_mode is True
    assert get_tenant_config("acme") == config.tenants["acme"]
