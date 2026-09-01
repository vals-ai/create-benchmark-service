"""Tests for the lab-facing /v1/ eval API surface."""

import json
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ValidationError

from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.schemas import FinalScoreResult
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalRequest,
    V1EvalResponse,
    V1EvalStatus,
    V1PayloadType,
    V1ScoreRequest,
    V1ScoreResponse,
    V1Task,
)
from tests.conftest import StubBenchmark


class DatasetVersionBenchmark(StubBenchmark):
    def get_dataset_version(self, dataset: str | None = None) -> str | None:
        return f"{dataset or 'default'}-dataset-2026-06-10"


class TaskListingBenchmark(DatasetVersionBenchmark):
    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        return [
            V1Task(id=task_id, question=task["problem"])
            for task_id, task in self.get_dataset(dataset).items()
        ]


class CapturingScoreBenchmark(StubBenchmark):
    captured_evaluation_results: dict[str, Any] | None = None
    captured_dataset: str | None = None

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        type(self).captured_evaluation_results = evaluation_results
        type(self).captured_dataset = dataset
        return await super().calculate_final_score(evaluation_results, dataset=dataset)


@pytest.fixture
def descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    """A client with a Descope tenant 'acme' allowed to see the 'default' dataset."""
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    app = BenchmarkServiceApp(StubBenchmark)
    app._service_version = "stub-service-1.0"  # pyright: ignore[reportPrivateUsage]

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


@pytest.fixture
def task_listing_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
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


@pytest.fixture
def scoring_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    CapturingScoreBenchmark.captured_evaluation_results = None
    CapturingScoreBenchmark.captured_dataset = None
    app = BenchmarkServiceApp(CapturingScoreBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_eval_request_accepts_text_payload() -> None:
    req = V1EvalRequest.model_validate({
        "run_id": "external-run-123",
        "task_id": "01-011",
        "dataset": "validation",
        "payload": {"type": "text", "schema": "fabv2.text.v1", "data": "42"},
        "versions": {"runner": "0.0.10-a1b2c3d"},
    })
    assert req.payload.type == V1PayloadType.TEXT
    assert req.payload.data == "42"
    assert req.payload.schema_id == "fabv2.text.v1"
    raw = req.model_dump(mode="json", by_alias=True)
    assert raw["payload"]["schema"] == "fabv2.text.v1"
    assert "schema_id" not in raw["payload"]


def test_eval_request_rejects_unknown_payload_type() -> None:
    with pytest.raises(ValidationError):
        V1EvalRequest.model_validate({
            "run_id": "r",
            "task_id": "t",
            "payload": {"type": "movie", "schema": "x.v1", "data": ""},
        })


def test_eval_response_round_trips_evaluated_status() -> None:
    resp = V1EvalResponse(
        run_id="external-run-123",
        task_id="01-011",
        status=V1EvalStatus.EVALUATED,
        evaluator_version="0.0.2",
        result={"pass_percentage": 0.83},
        errors=[],
    )
    raw = resp.model_dump(mode="json")
    rehydrated = V1EvalResponse.model_validate(raw)
    assert rehydrated == resp
    assert raw["status"] == V1EvalStatus.EVALUATED


def test_score_request_carries_run_id_and_nullable_results() -> None:
    req = V1ScoreRequest.model_validate({
        "run_id": "external-run-123",
        "dataset": "validation",
        "evaluation_results": {
            "01-011": {"status": "evaluated", "result": {"pass_percentage": 0.83}},
            "01-012": None,
        },
    })
    assert req.run_id == "external-run-123"
    assert req.evaluation_results["01-012"] is None


def test_score_response_requires_final_score_and_tasks() -> None:
    resp = V1ScoreResponse(
        run_id="external-run-123",
        tasks_evaluated=["01-011", "01-012"],
        final_score=0.62,
        metadata={},
    )
    raw = resp.model_dump(mode="json")
    assert raw["final_score"] == 0.62
    assert raw["tasks_evaluated"] == ["01-011", "01-012"]


def test_v1_evaluate_returns_evaluated_envelope_for_text_payload(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "external-run-123",
            "task_id": "task-1",
            "dataset": "default",
            "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "external-run-123"
    assert body["task_id"] == "task-1"
    assert body["status"] == "evaluated"
    assert body["evaluator_version"] == "stub-service-1.0"
    # StubBenchmark.evaluate_response returns {"resolved": <bool>}.
    # The framework passes the benchmark-specific result through under `result`.
    assert body["result"] == {"resolved": True}


def test_v1_evaluate_serializes_pydantic_result(
    descope_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """evaluate_response may return a Pydantic BaseModel; v1 must model_dump it
    rather than silently nulling the result field."""
    class StubEvalResult(BaseModel):
        pass_percentage: float
        eval_version: str

    async def stub_eval(self: object, request: object, dataset: object = None) -> StubEvalResult:
        return StubEvalResult(pass_percentage=0.83, eval_version="v1.0")

    monkeypatch.setattr(StubBenchmark, "evaluate_response", stub_eval)

    resp = descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "r1",
            "task_id": "task-1",
            "dataset": "default",
            "payload": {"type": "text", "schema": "stub.text.v1", "data": "x"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == {"pass_percentage": 0.83, "eval_version": "v1.0"}
    assert body["evaluator_version"] == "stub-service-1.0"


@pytest.mark.parametrize("result", [[1, 2], "complete"])
def test_v1_evaluate_preserves_json_compatible_list_and_scalar_results(
    descope_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    result: Any,
) -> None:
    async def stub_eval(self: object, request: object, dataset: object = None) -> Any:
        return result

    monkeypatch.setattr(StubBenchmark, "evaluate_response", stub_eval)

    resp = descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "r1",
            "task_id": "task-1",
            "dataset": "default",
            "payload": {"type": "text", "schema": "stub.text.v1", "data": "x"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    assert resp.json()["result"] == result


def test_v1_evaluate_rejects_artifact_payload_with_400(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "r",
            "task_id": "task-1",
            "dataset": "default",
            "payload": {"type": "artifact", "schema": "stub.artifact.v1", "data": "AAAA"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 400


def test_v1_evaluate_rejects_unauthorized_dataset_with_403(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "r",
            "task_id": "task-1",
            "dataset": "alt",
            "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403


def test_v1_score_hands_final_scoring_the_grader_payload(scoring_client: TestClient) -> None:
    resp = scoring_client.post(
        "/v1/score",
        json={
            "run_id": "external-run-123",
            "dataset": "default",
            "evaluation_results": {
                "task-1": {"status": "evaluated", "result": {"resolved": True}, "errors": []},
                "task-2": {"status": "did_not_complete", "errors": ["timed out"]},
                "task-3": None,
            },
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "external-run-123"
    assert abs(float(body["final_score"]) - (100 / 3)) < 1e-9
    assert body["metadata"] == {"total": 3, "resolved": 1}
    assert body["tasks_evaluated"] == ["task-1", "task-2", "task-3"]
    assert CapturingScoreBenchmark.captured_dataset == "default"
    # The grader payload verbatim, exactly as /final-score/ delivers it. A task that reached
    # no verdict is None, which every benchmark already reads as incomplete.
    assert CapturingScoreBenchmark.captured_evaluation_results == {
        "task-1": {"resolved": True},
        "task-2": None,
        "task-3": None,
    }


def test_both_scoring_surfaces_deliver_one_shape(scoring_client: TestClient) -> None:
    """calculate_final_score is a single hook, so it must not receive two shapes.

    It did: /v1/score wrapped the payload as {task_id, status, result} while /final-score/
    forwarded it verbatim. Three benchmarks wrote private unwrappers and one did not, scoring
    every packaged run zero while its evaluations passed.
    """
    payload = {"resolved": True, "detail": "compiles"}

    v1 = scoring_client.post(
        "/v1/score",
        json={
            "run_id": "r",
            "dataset": "default",
            "evaluation_results": {"task-1": {"status": "evaluated", "result": payload}},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert v1.status_code == 200
    from_v1 = CapturingScoreBenchmark.captured_evaluation_results

    legacy = scoring_client.post(
        "/final-score/",
        json={"dataset": "default", "evaluation_results": {"task-1": payload}},
        headers={"x-descope-api-key": "key-acme"},
    )
    assert legacy.status_code == 200
    from_legacy = CapturingScoreBenchmark.captured_evaluation_results

    assert from_v1 == from_legacy == {"task-1": payload}
    assert v1.json()["final_score"] == legacy.json()["final_score"] == 100.0


def test_v1_score_rejects_unauthorized_dataset_with_403(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/score",
        json={
            "run_id": "r",
            "dataset": "alt",
            "evaluation_results": {"task-1": {"status": "evaluated", "result": {"resolved": True}}},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403


def test_v1_evaluate_rejects_unauthenticated_mode_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app = BenchmarkServiceApp(StubBenchmark)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/evaluate",
            json={
                "run_id": "r",
                "task_id": "task-1",
                "dataset": "default",
                "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"},
            },
        )
    assert resp.status_code == 403
    assert "descope" in resp.json()["detail"].lower()


def test_v1_score_rejects_unauthenticated_mode_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app = BenchmarkServiceApp(StubBenchmark)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/score",
            json={"run_id": "r", "dataset": "default", "evaluation_results": {}},
        )
    assert resp.status_code == 403
    assert "descope" in resp.json()["detail"].lower()


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
        dataset_version="validation-2026-06-10",
        tasks=[
            V1Task(id="t1", question="q1"),
            V1Task(id="t2", question="q2", timeout=120),
        ],
    )
    raw = resp.model_dump(mode="json")
    rehydrated = V1DatasetTasksResponse.model_validate(raw)
    assert rehydrated == resp
    assert raw["dataset_version"] == "validation-2026-06-10"
    assert raw["tasks"][1]["timeout"] == 120


def test_v1_list_dataset_tasks_returns_full_task_list(task_listing_client: TestClient) -> None:
    resp = task_listing_client.get(
        "/v1/datasets/default/tasks",
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset"] == "default"
    assert body["dataset_version"] == "default-dataset-2026-06-10"
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


def test_v1_list_dataset_tasks_rejects_unauthenticated_mode_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app = BenchmarkServiceApp(StubBenchmark)
    with TestClient(app) as client:
        resp = client.get("/v1/datasets/default/tasks")
    assert resp.status_code == 403


def test_v1_list_dataset_tasks_404_for_unknown_dataset_in_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the tenant *is* allowed to access dataset X but X isn't in load_datasets,
    return 404 rather than leaking the difference between 'not allowed' and 'doesn't exist'."""
    clear_allowlist_cache()
    clear_auth_cache()
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
