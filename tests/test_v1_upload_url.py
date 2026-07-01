"""Tests for POST /v1/submissions/upload-url."""

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service import submission_artifacts
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from tests.conftest import StubBenchmark


def _install_signed_url_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_presigned_put_url(
        key: str,
        *,
        expires_in: int = submission_artifacts.DEFAULT_UPLOAD_EXPIRY_S,
    ) -> str:
        return f"https://signed.example/{key}?expires={expires_in}"

    monkeypatch.setattr(submission_artifacts, "presigned_put_url", fake_presigned_put_url)


def _descope_client(
    monkeypatch: pytest.MonkeyPatch,
    *,
    tenant: str,
    tenant_config: dict[str, object],
) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {tenant: tenant_config}}),
    )
    _install_signed_url_stub(monkeypatch)
    app = BenchmarkServiceApp(StubBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {tenant: {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


@pytest.fixture
def descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    yield from _descope_client(
        monkeypatch,
        tenant="acme",
        tenant_config={"datasets": ["default"]},
    )


@pytest.fixture
def trial_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    yield from _descope_client(
        monkeypatch,
        tenant="trial",
        tenant_config={"datasets": ["default"], "trial_mode": True},
    )


def test_upload_url_returns_namespaced_key_and_signed_url(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "dataset": "default", "filename": "submission.xlsx"},
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["key"] == "submission-artifacts/acme/default/run-1/task-9/submission.xlsx"
    assert body["url"] == "https://signed.example/submission-artifacts/acme/default/run-1/task-9/submission.xlsx?expires=3600"
    assert body["expires_in"] == submission_artifacts.DEFAULT_UPLOAD_EXPIRY_S


def test_upload_url_rejects_unauthorized_dataset(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "dataset": "alt", "filename": "submission.xlsx"},
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403


def test_trial_tenant_can_request_upload_url(trial_client: TestClient) -> None:
    resp = trial_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "dataset": "default", "filename": "submission.xlsx"},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 200
    assert resp.json()["key"] == "submission-artifacts/trial/default/run-1/task-9/submission.xlsx"


def test_upload_url_requires_auth(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/submissions/upload-url",
        json={"run_id": "run-1", "task_id": "task-9", "dataset": "default", "filename": "submission.xlsx"},
    )
    assert resp.status_code in (401, 403)
