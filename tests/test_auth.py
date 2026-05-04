"""Tests for resolve_descope_tenant and resolve_caller_tenant."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import patch

import pytest
from daytona import AsyncSandbox

from benchmark_service import auth as auth_module
from benchmark_service.auth import (
    clear_allowlist_cache,
    clear_auth_cache,
    resolve_caller_tenant,
    resolve_descope_tenant,
)
from benchmark_service.base import BenchmarkService
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
async def test_resolve_descope_tenant_returns_none_when_no_header() -> None:
    tenant = await resolve_descope_tenant({})
    assert tenant is None


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_caches_resolved_tenant() -> None:
    headers = {"x-descope-api-key": "key-acme"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp"]),
    ) as mock_exchange:
        first = await resolve_descope_tenant(headers)
        second = await resolve_descope_tenant(headers)
    assert first == "acme-corp"
    assert second == "acme-corp"
    assert mock_exchange.call_count == 1


async def test_resolve_caller_tenant_legacy_no_api_key_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.delenv("BENCHMARK_API_KEY", raising=False)
    tenant = await resolve_caller_tenant({})
    assert tenant == "_legacy"


async def test_resolve_caller_tenant_legacy_correct_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "secret123")
    tenant = await resolve_caller_tenant({"authorization": "Bearer secret123"})
    assert tenant == "_legacy"


async def test_resolve_caller_tenant_legacy_wrong_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "secret123")
    tenant = await resolve_caller_tenant({"authorization": "Bearer wrong"})
    assert tenant is None


@pytest.mark.usefixtures("descope_env")
async def test_resolve_caller_tenant_descope_path() -> None:
    headers = {"x-descope-api-key": "key-acme"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp"]),
    ):
        tenant = await resolve_caller_tenant(headers)
    assert tenant == "acme-corp"


class _BareBenchmark(BenchmarkService):
    """Service that does NOT override check_auth; uses the new resolve_tenant default."""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        return {"default": {}}

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse: ...  # type: ignore[return]

    def setup_task(
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any: ...

    def evaluate_instance(
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult: ...  # type: ignore[return]


class _LegacyOverrideBenchmark(_BareBenchmark):
    """Service that overrides check_auth (legacy bool API)."""

    def __init__(self, allow: bool) -> None:
        self._allow = allow

    async def check_auth(self, headers: dict[str, str]) -> bool:
        return self._allow


@pytest.mark.usefixtures("descope_env")
async def test_resolve_tenant_no_override_uses_descope_path() -> None:
    service = _BareBenchmark()
    headers = {"x-descope-api-key": "key-acme"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp"]),
    ):
        tenant = await service.resolve_tenant(headers)
    assert tenant == "acme-corp"


async def test_resolve_tenant_legacy_override_returns_sentinel_on_true() -> None:
    service = _LegacyOverrideBenchmark(allow=True)
    tenant = await service.resolve_tenant({})
    assert tenant == "_legacy"


async def test_resolve_tenant_legacy_override_returns_none_on_false() -> None:
    service = _LegacyOverrideBenchmark(allow=False)
    tenant = await service.resolve_tenant({})
    assert tenant is None


@pytest.mark.usefixtures("descope_env")
async def test_check_dataset_access_allowed() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("acme-corp", "validation") is True


@pytest.mark.usefixtures("descope_env")
async def test_check_dataset_access_disallowed() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("acme-corp", "test") is False


@pytest.mark.usefixtures("descope_env")
async def test_check_dataset_access_unknown_tenant() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("rogue-org", "validation") is False


@pytest.mark.usefixtures("descope_env")
async def test_check_dataset_access_default_dataset_when_none() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("vals-internal", None) is True
    assert await service.check_dataset_access("acme-corp", None) is False


async def test_check_dataset_access_legacy_sentinel_always_allowed() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("_legacy", "anything") is True
    assert await service.check_dataset_access("_legacy", None) is True
