"""Trial-tenant task-list sanitization."""

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.trial import sanitize_v1_dataset_tasks_response
from benchmark_service.v1_schemas import V1DatasetTasksResponse, V1Task
from tests.conftest import StubBenchmark


class _TrialTaskBenchmark(StubBenchmark):
    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        return [
            V1Task.model_validate({
                "id": "task-1",
                "question": "What is 1+1?",
                "timeout": 60,
                "rubric_hint": "answer is 2",
                "judge_model": "claude-sonnet-4-6",
            })
        ]


@pytest.fixture
def trial_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"trial": {"datasets": ["default"], "trial_mode": True}}}),
    )
    app = BenchmarkServiceApp(_TrialTaskBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"trial": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app, raise_server_exceptions=False) as client:
            yield client


def test_sanitize_v1_dataset_tasks_strips_benchmark_extras() -> None:
    response = V1DatasetTasksResponse(
        dataset="default",
        tasks=[
            V1Task.model_validate({
                "id": "task-1",
                "question": "What is 1+1?",
                "timeout": 60,
                "rubric_hint": "answer is 2",
            })
        ],
    )

    sanitized = sanitize_v1_dataset_tasks_response(response)

    assert sanitized.model_dump(mode="json") == {
        "dataset": "default",
        "tasks": [{"id": "task-1", "question": "What is 1+1?", "timeout": 60.0}],
    }


def test_v1_dataset_tasks_strips_extras_for_trial_tenant(trial_client: TestClient) -> None:
    resp = trial_client.get(
        "/v1/datasets/default/tasks",
        headers={"x-descope-api-key": "trial-key"},
    )

    assert resp.status_code == 200
    task = resp.json()["tasks"][0]
    assert task == {"id": "task-1", "question": "What is 1+1?", "timeout": 60.0}


def test_trial_tenant_blocked_on_internal_endpoint(trial_client: TestClient) -> None:
    resp = trial_client.post(
        "/evaluate-response/",
        json={"task_id": "task-1", "response": "2"},
        headers={"x-descope-api-key": "trial-key"},
    )

    assert resp.status_code == 403
    assert "/v1/" in resp.json()["detail"]


def test_trial_tenant_blocked_on_removed_v1_eval_endpoint(trial_client: TestClient) -> None:
    resp = trial_client.post(
        "/v1/evaluate",
        json={},
        headers={"x-descope-api-key": "trial-key"},
    )

    assert resp.status_code == 403
    assert "approved /v1 endpoints" in resp.json()["detail"]


def test_trial_tenant_blocked_on_unapproved_v1_endpoint(trial_client: TestClient) -> None:
    resp = trial_client.get(
        "/v1/submissions/sub-1",
        headers={"x-descope-api-key": "trial-key"},
    )

    assert resp.status_code == 403
    assert "approved /v1 endpoints" in resp.json()["detail"]
