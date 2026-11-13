"""Tests for catalog allowlist integration with authentication."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketDenialResponse
from starlette.websockets import WebSocketDisconnect

from benchmark_service.auth import AuthenticationUnavailableError
from benchmark_service.vals import auth as auth_module
from benchmark_service.draining import is_drain_rejection
from benchmark_service.vals.allowlist import (
    ALLOWLIST_UNAVAILABLE_GRACE_SECONDS,
    DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS,
    CatalogAllowlistClient,
)
from benchmark_service.vals.app import ValsBenchmarkServiceApp
from benchmark_service.vals.auth import (
    clear_allowlist_cache,
    clear_auth_cache,
    clear_request_tenant_config,
    get_tenant_config,
    load_allowlist,
    resolve_descope_tenant,
)
from tests.conftest import ValsStubBenchmark as StubBenchmark


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
        with pytest.raises(AuthenticationUnavailableError):
            await resolve_descope_tenant({"x-descope-api-key": "key-acme"})

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "jwt_response",
    [
        {"tenants": {"acme": {}}, "valkyrie_read_only": True},
        {"tenants": {"acme": {}}, "sessionToken": {"customClaims": {"valkyrie_read_only": True}}},
        {"tenants": {"acme": {}}, "sessionToken": {"nsec": {"valkyrie_read_only": True}}},
        {"tenants": {"acme": {}}, "customClaims": {"valkyrie_read_only": True}, "sessionToken": {"customClaims": {}}},
    ],
)
async def test_read_only_tracker_key_is_refused(
    monkeypatch: pytest.MonkeyPatch, jwt_response: dict[str, object]
) -> None:
    transport, requests = _transport(httpx.Response(200, json={"name": "example-service", "datasets": ["default"]}))
    _configure_api(monkeypatch, transport)

    with patch.object(auth_module, "_exchange_descope_access_key", new=AsyncMock(return_value=jwt_response)):
        assert await resolve_descope_tenant({"x-descope-api-key": "key-acme"}) is None

    assert requests == []


class _StallingCatalog:
    """Answer the first policy read, then stall like a proxy CONNECT that never completes."""

    def __init__(self, *refresh: httpx.Response) -> None:
        self.refresh = list(refresh)
        self.requests = 0

    async def __call__(self, _request: httpx.Request) -> httpx.Response:
        self.requests += 1
        if self.requests == 1:
            return httpx.Response(200, json={"name": "example-service", "datasets": ["default"], "trial_mode": False})
        if self.refresh:
            return self.refresh.pop(0)
        await asyncio.sleep(10)
        raise AssertionError("the catalog deadline did not cancel the read")


def _catalog_app(monkeypatch: pytest.MonkeyPatch, catalog: _StallingCatalog, clock: list[float]) -> TestClient:
    _configure_api(monkeypatch)
    monkeypatch.setattr(
        auth_module,
        "_catalog_client",
        CatalogAllowlistClient(
            "https://catalog.example.test",
            "example-service",
            transport=httpx.MockTransport(catalog),
            timeout=0.05,
            cache_timer=lambda: clock[0],
        ),
    )
    monkeypatch.setattr(auth_module, "_exchange_descope_access_key", AsyncMock(return_value={"tenants": {"acme": {}}}))
    return TestClient(ValsBenchmarkServiceApp(StubBenchmark))


_KEY = {"x-descope-api-key": "key-acme"}


def test_stalled_catalog_refresh_keeps_an_authorized_tenant_on_the_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """A known tenant is not refused when its policy refresh times out after the cache TTL.

    This is the Prod failure: grading opened ``/ws/evaluate-instance`` after the policy
    expired, the catalog read stalled at the outbound proxy until the 5 s deadline, and
    the stream closed 1008 "Unauthorized".
    """
    clock = [0.0]
    catalog = _StallingCatalog()
    with _catalog_app(monkeypatch, catalog, clock) as client:
        assert client.get("/verify-task-ids", headers=_KEY).status_code == 200
        clock[0] = DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS + 1.0

        with client.websocket_connect("/ws/evaluate-instance", headers=_KEY) as ws:
            ws.send_json({})
            assert ws.receive_json()["type"] == "error"

    assert catalog.requests > 1


def test_stalled_catalog_without_a_policy_denies_the_unstarted_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a policy in grace, an unavailable catalog is a retryable 503, not a refusal."""
    clock = [0.0]
    catalog = _StallingCatalog()
    with _catalog_app(monkeypatch, catalog, clock) as client:
        assert client.get("/verify-task-ids", headers=_KEY).status_code == 200
        clock[0] = DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS + ALLOWLIST_UNAVAILABLE_GRACE_SECONDS + 1.0

        response = client.get("/verify-task-ids", headers=_KEY)
        assert response.status_code == 503
        assert is_drain_rejection(response.status_code, response.headers)

        with pytest.raises(WebSocketDenialResponse) as denial:
            with client.websocket_connect("/ws/evaluate-instance", headers=_KEY):
                pass
        assert is_drain_rejection(denial.value.status_code, denial.value.headers)


def test_catalog_refusal_after_expiry_still_closes_1008(monkeypatch: pytest.MonkeyPatch) -> None:
    """A catalog that answers 403 for a known tenant refuses it at once, without grace."""
    clock = [0.0]
    refusal = httpx.Response(403, json={"detail": "Tenant is not allowlisted"})
    catalog = _StallingCatalog(refusal, refusal)
    with _catalog_app(monkeypatch, catalog, clock) as client:
        assert client.get("/verify-task-ids", headers=_KEY).status_code == 200
        clock[0] = DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS + 1.0

        with client.websocket_connect("/ws/evaluate-instance", headers=_KEY) as ws:
            with pytest.raises(WebSocketDisconnect) as closed:
                ws.receive_json()
        assert (closed.value.code, closed.value.reason) == (1008, "Unauthorized")

        assert client.get("/verify-task-ids", headers=_KEY).status_code == 401
