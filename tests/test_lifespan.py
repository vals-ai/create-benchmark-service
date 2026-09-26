"""Tests for eager allowlist validation during app startup."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from templates.vals_ai.app import ValsBenchmarkServiceApp
from tests.conftest import ValsStubBenchmark as StubBenchmark


def test_lifespan_raises_on_malformed_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "{not valid json")

    with pytest.raises(ValueError):
        with TestClient(ValsBenchmarkServiceApp(StubBenchmark)):
            pass


@pytest.mark.parametrize("auth_required", [None, "false"])
def test_local_lifespan_ignores_hosted_allowlist_and_quota_configuration(
    monkeypatch: pytest.MonkeyPatch, auth_required: str | None,
) -> None:
    """Local Vals startup and evaluation do not require hosted tenant configuration.

    Test cases:
    - Missing and false AUTH_REQUIRED bypass malformed allowlist validation.
    - Evaluation succeeds without quota storage or a Descope project.
    """
    if auth_required is None:
        monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    else:
        monkeypatch.setenv("AUTH_REQUIRED", auth_required)
    monkeypatch.delenv("DESCOPE_PROJECT_ID", raising=False)
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.delenv("EVALUATION_QUOTA_TABLE_NAME", raising=False)
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "{not valid json")

    with TestClient(ValsBenchmarkServiceApp(StubBenchmark)) as client:
        response = client.post(
            "/v1/evaluate",
            json={
                "run_id": "local-run",
                "task_id": "task-1",
                "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"},
            },
        )
    assert response.status_code == 200
    assert response.json()["result"] == {"resolved": True}
