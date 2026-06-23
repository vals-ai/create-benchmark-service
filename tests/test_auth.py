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
    AuthFailure,
    AuthResult,
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


# Table-driven: every AuthFailure reason including REJECTED -> expected AuthResult
@pytest.mark.parametrize(
    ("headers", "exchange_tenants", "exchange_raises", "expected_failure"),
    [
        ({}, None, False, AuthFailure.NO_KEY),
        ({"x-descope-api-key": "k"}, None, True, AuthFailure.INVALID_KEY),
        ({"x-descope-api-key": "k"}, ["t1", "t2"], False, AuthFailure.MULTI_TENANT),
        ({"x-descope-api-key": "k"}, ["_legacy"], False, AuthFailure.LEGACY_TENANT),
        ({"x-descope-api-key": "k"}, ["unlisted-org"], False, AuthFailure.NOT_ALLOWLISTED),
    ],
)
@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_failure_table(
    headers: dict[str, str],
    exchange_tenants: list[str] | None,
    exchange_raises: bool,
    expected_failure: AuthFailure,
) -> None:
    if exchange_tenants is not None or exchange_raises:
        side_effect = RuntimeError("bad") if exchange_raises else None
        return_value = _mock_jwt_response(exchange_tenants or []) if not exchange_raises else None
        with patch.object(
            auth_module,
            "_exchange_descope_access_key",
            side_effect=side_effect,
            return_value=return_value,
        ):
            result = await resolve_descope_tenant(headers)
    else:
        result = await resolve_descope_tenant(headers)
    assert result == AuthResult(failure=expected_failure)
    assert not result.ok


@pytest.mark.usefixtures("descope_env")
async def test_resolve_descope_tenant_success() -> None:
    headers = {"x-descope-api-key": "key-acme"}
    with patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value=_mock_jwt_response(["acme-corp"]),
    ):
        result = await resolve_descope_tenant(headers)
    assert result == AuthResult(tenant="acme-corp")
    assert result.ok


async def test_resolve_caller_tenant_legacy_no_api_key_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.delenv("BENCHMARK_API_KEY", raising=False)
    result = await resolve_caller_tenant({})
    assert result == AuthResult(tenant="_legacy")
    assert result.ok


async def test_resolve_caller_tenant_legacy_correct_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "secret123")
    result = await resolve_caller_tenant({"authorization": "Bearer secret123"})
    assert result == AuthResult(tenant="_legacy")
    assert result.ok


async def test_resolve_caller_tenant_legacy_wrong_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "secret123")
    result = await resolve_caller_tenant({"authorization": "Bearer wrong"})
    assert result == AuthResult(failure=AuthFailure.REJECTED)
    assert not result.ok


class _BareBenchmark(BenchmarkService):
    """Service that does NOT override check_auth; uses the new resolve_tenant default."""

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


class _LegacyOverrideBenchmark(_BareBenchmark):
    """Service that overrides check_auth (legacy bool API)."""

    def __init__(self, allow: bool) -> None:
        self._allow = allow

    async def check_auth(self, headers: dict[str, str]) -> bool:
        return self._allow


async def test_resolve_tenant_legacy_override_returns_sentinel_on_true() -> None:
    service = _LegacyOverrideBenchmark(allow=True)
    result = await service.resolve_tenant({})
    assert result == AuthResult(tenant="_legacy")
    assert result.ok


async def test_resolve_tenant_legacy_override_returns_none_on_false() -> None:
    service = _LegacyOverrideBenchmark(allow=False)
    result = await service.resolve_tenant({})
    assert result == AuthResult(failure=AuthFailure.REJECTED)
    assert not result.ok


async def test_check_dataset_access_legacy_sentinel_always_allowed() -> None:
    service = _BareBenchmark()
    assert await service.check_dataset_access("_legacy", "anything") is True
    assert await service.check_dataset_access("_legacy", None) is True


def test_auth_types_exported_from_package() -> None:
    import benchmark_service

    assert hasattr(benchmark_service, "AuthResult")
    assert hasattr(benchmark_service, "AuthFailure")
    assert benchmark_service.AuthResult is AuthResult
    assert benchmark_service.AuthFailure is AuthFailure
