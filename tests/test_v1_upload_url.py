"""Tests for POST /v1/submissions/upload-url."""

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service import lab_artifacts
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from tests.conftest import StubBenchmark


@pytest.fixture
def descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    monkeypatch.setattr(lab_artifacts, "presigned_put_url", lambda key, **_: f"https://signed.example/{key}")
    app = BenchmarkServiceApp(StubBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_upload_url_returns_namespaced_key_and_signed_url(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "filename": "submission.xlsx"},
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["key"] == "lab-submissions/run-1/task-9/submission.xlsx"
    assert body["url"] == "https://signed.example/lab-submissions/run-1/task-9/submission.xlsx"
    assert body["expires_in"] == lab_artifacts.DEFAULT_UPLOAD_EXPIRY_S


def test_upload_url_requires_auth(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "filename": "submission.xlsx"},
    )
    assert resp.status_code in (401, 403)
