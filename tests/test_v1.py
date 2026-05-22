"""Tests for the lab-facing /v1/ eval API surface."""

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.v1_schemas import (
    V1EvalRequest,
    V1EvalResponse,
    V1EvalStatus,
    V1PayloadType,
    V1ScoreRequest,
    V1ScoreResponse,
)
from tests.conftest import StubBenchmark


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
    assert req.payload.schema_ == "fabv2.text.v1"
    raw = req.model_dump(mode="json", by_alias=True)
    assert raw["payload"]["schema"] == "fabv2.text.v1"
    assert "schema_" not in raw["payload"]


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
    # StubBenchmark.evaluate_response returns {"resolved": <bool>}.
    # The framework passes the benchmark-specific result through under `result`.
    assert body["result"] == {"resolved": True}


def test_v1_evaluate_serializes_pydantic_result(
    descope_client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """evaluate_response may return a Pydantic BaseModel; v1 must model_dump it
    rather than silently nulling the result field."""
    from pydantic import BaseModel

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
    assert body["evaluator_version"] == "v1.0"


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


def test_v1_score_aggregates_and_wraps_with_run_id(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/score",
        json={
            "run_id": "external-run-123",
            "dataset": "default",
            "evaluation_results": {
                "task-1": {"resolved": True},
                "task-2": None,
            },
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "external-run-123"
    assert "final_score" in body
    assert "tasks_evaluated" in body


def test_v1_score_rejects_unauthorized_dataset_with_403(descope_client: TestClient) -> None:
    resp = descope_client.post(
        "/v1/score",
        json={"run_id": "r", "dataset": "alt", "evaluation_results": {"task-1": {"resolved": True}}},
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 403


def test_v1_evaluate_rejects_legacy_bearer_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "legacy-key")
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
            headers={"Authorization": "Bearer legacy-key"},
        )
    assert resp.status_code == 403
    assert "legacy" in resp.json()["detail"].lower()


def test_v1_score_rejects_legacy_bearer_with_403(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("BENCHMARK_API_KEY", "legacy-key")
    app = BenchmarkServiceApp(StubBenchmark)
    with TestClient(app) as client:
        resp = client.post(
            "/v1/score",
            json={"run_id": "r", "dataset": "default", "evaluation_results": {}},
            headers={"Authorization": "Bearer legacy-key"},
        )
    assert resp.status_code == 403
    assert "legacy" in resp.json()["detail"].lower()
