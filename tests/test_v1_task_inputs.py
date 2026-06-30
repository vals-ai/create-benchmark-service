"""Tests for the task-inputs serving endpoints used by manifest generation."""

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.schemas import TaskInput
from tests.conftest import StubBenchmark


@pytest.fixture
def inputs_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    src = tmp_path / "template.xlsx"
    src.write_bytes(b"XLSX-BYTES")

    class _Svc(StubBenchmark):
        async def list_task_inputs(self, task_id: str, dataset: str | None = None) -> list[TaskInput]:
            return [TaskInput(filename="template.xlsx", dest="/workspace/submission.xlsx")]

        def task_input_path(self, task_id: str, filename: str, dataset: str | None = None) -> Path:
            if filename != "template.xlsx":
                raise KeyError(filename)
            return src

    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}))
    app = BenchmarkServiceApp(_Svc)

    async def _stub_exchange(_p: str, _k: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_list_inputs_returns_declared_metadata(inputs_client: TestClient) -> None:
    resp = inputs_client.get("/v1/datasets/default/tasks/task-1/inputs", headers={"x-descope-api-key": "k"})
    assert resp.status_code == 200
    assert resp.json()["inputs"] == [{"filename": "template.xlsx", "dest": "/workspace/submission.xlsx"}]


def test_download_input_returns_bytes(inputs_client: TestClient) -> None:
    resp = inputs_client.get(
        "/v1/datasets/default/tasks/task-1/inputs/template.xlsx", headers={"x-descope-api-key": "k"}
    )
    assert resp.status_code == 200
    assert resp.content == b"XLSX-BYTES"


def test_download_rejects_undeclared_filename(inputs_client: TestClient) -> None:
    resp = inputs_client.get(
        "/v1/datasets/default/tasks/task-1/inputs/secret.txt", headers={"x-descope-api-key": "k"}
    )
    assert resp.status_code == 404
