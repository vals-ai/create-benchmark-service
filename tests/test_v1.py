"""Tests for the lab-facing /v1/ API surface."""

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1Task,
)
from tests.conftest import StubBenchmark


class TaskListingBenchmark(StubBenchmark):
    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        return [
            V1Task(id=task_id, question=task["problem"])
            for task_id, task in self.get_dataset(dataset).items()
        ]


@pytest.fixture
def descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    """A client with a Descope tenant 'acme' allowed to see the 'default' dataset."""
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    app = BenchmarkServiceApp(StubBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


@pytest.fixture
def task_listing_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    app = BenchmarkServiceApp(TaskListingBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_v1_task_allows_benchmark_specific_extras() -> None:
    """V1Task accepts benchmark-specific per-task fields (e.g. SWE-bench's
    repo/base_commit, an artifact benchmark's docker image hints). Per-benchmark
    fields should be documented in each benchmark's README and validated on the
    runner side with a typed Task subclass."""
    t = V1Task.model_validate({
        "id": "01-011",
        "question": "What is fair use?",
        "timeout": 600,
        "system_prompt": "be helpful",
    })
    assert t.id == "01-011"
    assert t.timeout == 600
    raw = t.model_dump(mode="json")
    assert raw["system_prompt"] == "be helpful"


def test_v1_dataset_tasks_response_round_trip() -> None:
    resp = V1DatasetTasksResponse(
        dataset="validation",
        tasks=[
            V1Task(id="t1", question="q1"),
            V1Task(id="t2", question="q2", timeout=120),
        ],
    )
    raw = resp.model_dump(mode="json")
    rehydrated = V1DatasetTasksResponse.model_validate(raw)
    assert rehydrated == resp
    assert raw["tasks"][1]["timeout"] == 120


def test_v1_list_dataset_tasks_returns_full_task_list(task_listing_client: TestClient) -> None:
    resp = task_listing_client.get(
        "/v1/datasets/default/tasks",
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset"] == "default"
    assert {t["id"] for t in body["tasks"]} == {"task-1", "task-2", "task-3"}
    assert {t["question"] for t in body["tasks"]} == {"What is 1+1?", "What is 2+2?", "What is 3+3?"}


def test_v1_list_dataset_tasks_requires_explicit_projection(descope_client: TestClient) -> None:
    resp = descope_client.get(
        "/v1/datasets/default/tasks",
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 501
    assert "list_tasks" in resp.json()["detail"]


def test_v1_list_dataset_tasks_rejects_unauthorized_dataset_with_403(descope_client: TestClient) -> None:
    resp = descope_client.get(
        "/v1/datasets/alt/tasks",
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Dataset=alt access not allowed"


def test_v1_list_dataset_tasks_unauthorized_nonexistent_dataset_gets_403_not_404(descope_client: TestClient) -> None:
    """Existence-leak regression: an unauthorized tenant must not learn whether
    a dataset exists. The access check fires before list_tasks, so a tenant
    without 'phantom-dataset' in its allowlist gets 403, never 404."""
    resp = descope_client.get(
        "/v1/datasets/phantom-dataset/tasks",
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403


def test_v1_list_dataset_tasks_rejects_legacy_bearer_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "legacy-key")
    app = BenchmarkServiceApp(StubBenchmark)
    with TestClient(app) as client:
        resp = client.get(
            "/v1/datasets/default/tasks",
            headers={"Authorization": "Bearer legacy-key"},
        )
    assert resp.status_code == 403


def test_v1_list_dataset_tasks_404_for_unknown_dataset_in_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the tenant *is* allowed to access dataset X but X isn't in load_datasets,
    return 404 rather than leaking the difference between 'not allowed' and 'doesn't exist'."""
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default", "ghost-dataset"]}}}),
    )
    app = BenchmarkServiceApp(StubBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            resp = client.get(
                "/v1/datasets/ghost-dataset/tasks",
                headers={"x-descope-api-key": "key-acme"},
            )
    assert resp.status_code == 404
