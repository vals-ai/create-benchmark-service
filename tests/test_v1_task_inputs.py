"""Tests for the lab-facing per-task input endpoints."""

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
    template = tmp_path / "template.xlsx"
    template.write_bytes(b"workbook")
    undeclared = tmp_path / "answer-key.xlsx"
    undeclared.write_bytes(b"secret")

    class InputBenchmark(StubBenchmark):
        async def list_task_inputs(self, task_id: str, dataset: str | None = None) -> list[TaskInput]:
            return [
                TaskInput(
                    filename="template.xlsx",
                    dest="/workspace/submission.xlsx",
                )
            ]

        def task_input_path(self, task_id: str, filename: str, dataset: str | None = None) -> Path:
            if filename == "template.xlsx":
                return template
            return undeclared

    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    app = BenchmarkServiceApp(InputBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_v1_task_inputs_lists_and_downloads_declared_file(inputs_client: TestClient) -> None:
    headers = {"x-descope-api-key": "key-acme"}

    listed = inputs_client.get(
        "/v1/datasets/default/tasks/task-1/inputs",
        headers=headers,
    )
    downloaded = inputs_client.get(
        "/v1/datasets/default/tasks/task-1/inputs/template.xlsx",
        headers=headers,
    )

    assert listed.status_code == 200
    assert listed.json() == {
        "inputs": [
            {
                "filename": "template.xlsx",
                "dest": "/workspace/submission.xlsx",
            }
        ]
    }
    assert downloaded.status_code == 200
    assert downloaded.content == b"workbook"


def test_v1_task_input_download_rejects_undeclared_file(inputs_client: TestClient) -> None:
    response = inputs_client.get(
        "/v1/datasets/default/tasks/task-1/inputs/answer-key.xlsx",
        headers={"x-descope-api-key": "key-acme"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Task input not found: answer-key.xlsx"


def test_v1_task_inputs_rejects_unknown_task(inputs_client: TestClient) -> None:
    response = inputs_client.get(
        "/v1/datasets/default/tasks/unknown/inputs",
        headers={"x-descope-api-key": "key-acme"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Task not found: unknown"


def test_v1_task_inputs_checks_dataset_access_before_task_existence(inputs_client: TestClient) -> None:
    response = inputs_client.get(
        "/v1/datasets/alt/tasks/unknown/inputs",
        headers={"x-descope-api-key": "key-acme"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Dataset=alt access not allowed"
