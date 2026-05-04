"""Tests for eager allowlist validation during app startup."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from benchmark_service.app import BenchmarkServiceApp
from tests.conftest import StubBenchmark


def test_lifespan_raises_on_malformed_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "{not valid json")

    with pytest.raises(ValueError):
        with TestClient(BenchmarkServiceApp(StubBenchmark)):
            pass


def test_lifespan_boots_with_absent_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)

    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        response = client.get("/health")
    assert response.status_code == 200


def test_lifespan_skips_eager_load_when_auth_not_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "{not valid json")

    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        response = client.get("/health")
    assert response.status_code == 200
