"""Tests for resolve_descope_tenant and resolve_caller_tenant."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from benchmark_service import Sandbox
from benchmark_service.vals import auth as auth_module
from benchmark_service.vals.auth import (
    UNAUTHENTICATED_TENANT_SENTINEL,
    clear_allowlist_cache,
    clear_auth_cache,
    resolve_caller_tenant,
    resolve_descope_tenant,
)
from benchmark_service.vals.allowlist import CatalogUnavailable, TenantConfig
from benchmark_service.vals.base import ValsBenchmarkService
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
)


@pytest.fixture(autouse=True)
def reset_caches() -> None:
    clear_allowlist_cache()
    clear_auth_cache()


def _allowlist_env(payload: dict[str, Any]) -> str:
    return json.dumps(payload)


@pytest.fixture
def descope_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_DISABLED", raising=False)
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        _allowlist_env(
            {
                "tenants": {
                    "acme-corp": {"datasets": ["validation"]},
                    "vals-internal": {"datasets": ["validation", "test", "default"]},
                }
            }
        ),
    )


def _mock_jwt_response(tenants: list[str]) -> dict[str, Any]:
    return {"tenants": {t: {} for t in tenants}}


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_returns_tenant_when_in_allowlist() -> None:
    headers = {"x-descope-api-key": "key-acme"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp"]),
    ):
        tenant = await resolve_descope_tenant(headers)
    assert tenant == "acme-corp"


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_returns_none_when_tenant_not_in_allowlist() -> None:
    headers = {"x-descope-api-key": "key-rogue"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["unknown-org"]),
    ):
        tenant = await resolve_descope_tenant(headers)
    assert tenant is None


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_rejects_multi_tenant_jwt() -> None:
    headers = {"x-descope-api-key": "key-multi"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp", "vals-internal"]),
    ):
        tenant = await resolve_descope_tenant(headers)
    assert tenant is None


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_rejects_reserved_sentinel_tenant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        _allowlist_env({"tenants": {UNAUTHENTICATED_TENANT_SENTINEL: {"datasets": ["secret"]}}}),
    )
    headers = {"x-descope-api-key": "key-reserved"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response([UNAUTHENTICATED_TENANT_SENTINEL]),
    ):
        tenant = await resolve_descope_tenant(headers)
    assert tenant is None


async def test_resolve_caller_tenant_rejects_static_bearer_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AUTH_DISABLED", raising=False)
    monkeypatch.setenv("BENCHMARK_API_KEY", "secret123")
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    tenant = await resolve_caller_tenant({"authorization": "Bearer secret123"})
    assert tenant is None


@pytest.mark.parametrize("auth_required", [None, "false"])
async def test_resolve_caller_tenant_returns_sentinel_when_auth_not_required(
    monkeypatch: pytest.MonkeyPatch,
    auth_required: str | None,
) -> None:
    if auth_required is None:
        monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    else:
        monkeypatch.setenv("AUTH_REQUIRED", auth_required)
    tenant = await resolve_caller_tenant({})
    assert tenant == UNAUTHENTICATED_TENANT_SENTINEL


async def test_auth_disabled_does_not_override_auth_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("AUTH_DISABLED", "true")
    monkeypatch.delenv("DESCOPE_PROJECT_ID", raising=False)
    assert await resolve_caller_tenant({}) is None


@pytest.mark.parametrize("policy_source", ["allowlist", "catalog"])
def test_local_tenant_never_loads_hosted_policy(monkeypatch: pytest.MonkeyPatch, policy_source: str) -> None:
    """Local requests bypass policy lookup even when hosted settings are present.

    Test cases:
    - Malformed allowlist JSON is not loaded for the reserved local tenant.
    - A configured catalog is not contacted for the reserved local tenant.
    """
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "invalid JSON")
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    if policy_source == "catalog":
        monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.invalid")
        monkeypatch.setenv("SERVICE_NAME", "local-benchmark")

    def unexpected_catalog_lookup() -> None:
        raise AssertionError("local request consulted the catalog")

    monkeypatch.setattr(auth_module, "_get_catalog_client", unexpected_catalog_lookup)
    assert auth_module.get_tenant_config(UNAUTHENTICATED_TENANT_SENTINEL) is None


class _BareBenchmark(ValsBenchmarkService):
    """Service that uses the framework's default tenant resolution."""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        return {"default": {}}

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse: ...  # type: ignore[return]

    def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any: ...

    def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult: ...  # type: ignore[return]


async def test_check_dataset_access_unauthenticated_sentinel_always_allowed() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access(UNAUTHENTICATED_TENANT_SENTINEL, "anything") is True
    assert await service.check_dataset_access(UNAUTHENTICATED_TENANT_SENTINEL, None) is True


@pytest.mark.parametrize("failures", [0, 1, 2, 3])
async def test_catalog_preauth_retries_only_unavailable(monkeypatch: pytest.MonkeyPatch, failures: int) -> None:
    client = AsyncMock()
    policy = TenantConfig(datasets=["default"])
    client.get_tenant_config.side_effect = [CatalogUnavailable("outage")] * failures + [policy]
    monkeypatch.setattr(auth_module, "_get_catalog_client", lambda: client)
    if failures == 3:
        with pytest.raises(CatalogUnavailable):
            await auth_module._fetch_api_tenant_config("key", "tenant")  # pyright: ignore[reportPrivateUsage]
    else:
        assert await auth_module._fetch_api_tenant_config("key", "tenant") == policy  # pyright: ignore[reportPrivateUsage]
    assert client.get_tenant_config.await_count == min(failures + 1, 3)


async def test_catalog_preauth_denial_and_cancellation_are_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()
    monkeypatch.setattr(auth_module, "_get_catalog_client", lambda: client)
    client.get_tenant_config.return_value = None
    assert await auth_module._fetch_api_tenant_config("key", "tenant") is None  # pyright: ignore[reportPrivateUsage]
    assert client.get_tenant_config.await_count == 1
    client.get_tenant_config.reset_mock()
    client.get_tenant_config.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await auth_module._fetch_api_tenant_config("key", "tenant")  # pyright: ignore[reportPrivateUsage]
    assert client.get_tenant_config.await_count == 1


async def test_catalog_preauth_total_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()

    async def stalled(*args: object) -> None:
        await asyncio.sleep(1)

    client.get_tenant_config.side_effect = stalled
    monkeypatch.setattr(auth_module, "_get_catalog_client", lambda: client)
    timeout = asyncio.timeout
    budgets: list[float] = []

    def short_timeout(seconds: float) -> asyncio.Timeout:
        budgets.append(seconds)
        return timeout(0.01)

    monkeypatch.setattr(auth_module.asyncio, "timeout", short_timeout)
    with pytest.raises(CatalogUnavailable):
        await auth_module._fetch_api_tenant_config("key", "tenant")  # pyright: ignore[reportPrivateUsage]
    assert budgets == [15]
    assert client.get_tenant_config.await_count == 1
