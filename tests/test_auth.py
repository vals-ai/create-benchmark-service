"""Tests for resolve_descope_tenant and resolve_caller_tenant."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import patch

import pytest

from benchmark_service import Sandbox
from benchmark_service import auth as auth_module
from benchmark_service.auth import (
    UNAUTHENTICATED_TENANT_SENTINEL,
    clear_allowlist_cache,
    clear_auth_cache,
    require_supported_auth_config,
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
    monkeypatch.delenv("AUTH_DISABLED", raising=False)
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
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    tenant = await resolve_caller_tenant({"authorization": "Bearer secret123"})
    assert tenant is None


async def test_resolve_caller_tenant_returns_sentinel_when_auth_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_DISABLED", "true")
    tenant = await resolve_caller_tenant({})
    assert tenant == UNAUTHENTICATED_TENANT_SENTINEL


def test_require_supported_auth_config_rejects_auth_required_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")

    with pytest.raises(RuntimeError, match="no longer supported"):
        require_supported_auth_config()


def test_require_supported_auth_config_allows_descope_deploys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    require_supported_auth_config()

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    require_supported_auth_config()


class _BareBenchmark(BenchmarkService):
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
