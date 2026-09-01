"""Trial-tenant response sanitizers."""

import json
from collections.abc import Generator
from typing import Any, cast
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import auth as auth_module
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.schemas import EvaluateResponseRequest, FinalScoreResult
from benchmark_service.trial import (
    sanitize_v1_eval_response,
    sanitize_v1_score_response,
)
from benchmark_service.v1_schemas import (
    V1EvalResponse,
    V1EvalStatus,
    V1ScoreResponse,
    V1Task,
)
from tests.conftest import StubBenchmark


def _project(result: Any) -> dict[str, Any]:
    """Stand-in for a benchmark's project_trial_result in unit tests."""
    return {"pass_percentage": result.get("pass_percentage")}


def test_sanitize_v1_eval_keeps_run_id_task_id_status_pass_percentage() -> None:
    resp = V1EvalResponse(
        run_id="run-1",
        task_id="VL-35",
        status=V1EvalStatus.EVALUATED,
        evaluator_version="0.0.2",
        result={
            "pass_percentage": 100.0,
            "eval_version": "v1",
            "resolved": True,
            "score": 1.0,
            "judge_model": "claude-sonnet-4-6",
            "judge_metadata": {"cost": {"total": 0.0035}},
            "check_results": [{"criteria": "...", "feedback": "..."}],
        },
        errors=[],
    )
    out = sanitize_v1_eval_response(resp, _project)
    assert out.run_id == "run-1"
    assert out.task_id == "VL-35"
    assert out.status == V1EvalStatus.EVALUATED
    assert out.result == {"pass_percentage": 100.0}
    assert out.evaluator_version is None  # judge identity stripped
    assert out.errors == []


def test_sanitize_v1_eval_preserves_error_status_but_drops_error_text() -> None:
    """Error status is informative; the underlying exception message is not."""
    resp = V1EvalResponse(
        run_id="run-1",
        task_id="t",
        status=V1EvalStatus.ERROR,
        evaluator_version="0.0.2",
        result=None,
        errors=["IndexError: rubric loader crashed at line 42"],
    )
    out = sanitize_v1_eval_response(resp, _project)
    assert out.status == V1EvalStatus.ERROR
    assert out.errors == ["error"]


def test_sanitize_v1_eval_handles_null_result() -> None:
    resp = V1EvalResponse(
        run_id="run-1", task_id="t", status=V1EvalStatus.DID_NOT_COMPLETE,
        result=None, errors=[],
    )
    out = sanitize_v1_eval_response(resp, _project)
    assert out.result is None
    assert out.status == V1EvalStatus.DID_NOT_COMPLETE


def test_sanitize_v1_score_keeps_run_id_tasks_and_final_score() -> None:
    resp = V1ScoreResponse(
        run_id="run-1",
        tasks_evaluated=["VL-35"],
        final_score=6.67,
        metadata={
            "rollup": {"per_area_of_law": {"Family": {"mean_weighted_pass_percentage": 33.3}}},
            "status_counts": {"evaluated": 1, "missing": 14},
            "eval_service_version": "0.3.1",
        },
    )
    out = sanitize_v1_score_response(resp)
    assert out.run_id == "run-1"
    assert out.tasks_evaluated == ["VL-35"]
    assert out.final_score == 6.67
    assert out.metadata == {}


class _TrialResultBenchmark(StubBenchmark):
    """Stub whose evaluate_response surfaces a real pass_percentage alongside
    leaky sibling fields, so the sanitizer test proves extraction, not just key-presence."""

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

    def get_dataset_version(self, dataset: str | None = None) -> str | None:
        return f"{dataset or 'default'}-dataset-2026-06-10"

    async def evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> dict[str, object]:
        return {
            "pass_percentage": 88.0,
            "resolved": True,
            "score": 1.0,
            "judge_model": "claude-sonnet-4-6",
            "judge_metadata": {"cost": {"total": 0.0031}},
            "check_results": [{"criteria": "secret rubric", "feedback": "leaky"}],
        }

    def project_trial_result(self, result: Any) -> dict[str, Any]:
        # Prospect-visible score + the field the scorer aggregates; drop the rest.
        return {
            "pass_percentage": result.get("pass_percentage"),
            "resolved": result.get("resolved"),
        }


class _AllPassTrialBenchmark(StubBenchmark):
    async def evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> dict[str, object]:
        return {
            "pass_percentage": 25.0,
            "all_pass": True,
            "judge_model": "claude-sonnet-4-6",
            "check_results": [{"criteria": "secret rubric", "feedback": "leaky"}],
        }

    def project_trial_result(self, result: Any) -> dict[str, Any]:
        result_dict = cast(dict[str, object], result)
        return {
            "pass_percentage": result_dict.get("pass_percentage"),
            "all_pass": result_dict.get("all_pass"),
        }

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        passed = 0
        total = len(evaluation_results)
        for item in evaluation_results.values():
            if not isinstance(item, dict):
                continue
            result = cast(dict[str, object], item)
            all_pass = result.get("all_pass")
            if all_pass is None:
                all_pass = result.get("resolved")
            passed += 1 if all_pass else 0
        score = (passed / total * 100.0) if total else 0.0
        return FinalScoreResult(score=score, metadata={"all_pass_tasks": passed})


@pytest.fixture
def trial_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"trial": {"datasets": ["default"], "trial_mode": True}}}),
    )
    app = BenchmarkServiceApp(_TrialResultBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"trial": {}}}

    # raise_server_exceptions=False so the handled 500 in the leak test is returned, not re-raised.
    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


@pytest.fixture
def all_pass_trial_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"trial": {"datasets": ["default"], "trial_mode": True}}}),
    )
    app = BenchmarkServiceApp(_AllPassTrialBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"trial": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c


def test_v1_evaluate_sanitizes_response_for_trial_tenant(trial_client: TestClient) -> None:
    resp = trial_client.post(
        "/v1/evaluate",
        json={"run_id": "r1", "task_id": "task-1", "dataset": "default",
              "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"}},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == {"pass_percentage": 88.0, "resolved": True}  # benchmark's projection; leaky fields dropped
    assert body["evaluator_version"] is None


def test_v1_score_sanitizes_response_for_trial_tenant(trial_client: TestClient) -> None:
    resp = trial_client.post(
        "/v1/score",
        json={"run_id": "r1", "dataset": "default",
              "evaluation_results": {"task-1": {"status": "evaluated", "result": {"resolved": True}}}},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["metadata"] == {}
    assert body["final_score"] == 100.0


def test_trial_eval_projection_preserves_fields_used_by_final_score(
    all_pass_trial_client: TestClient,
) -> None:
    eval_resp = all_pass_trial_client.post(
        "/v1/evaluate",
        json={"run_id": "r1", "task_id": "task-1", "dataset": "default",
              "payload": {"type": "text", "schema": "stub.text.v1", "data": "answer"}},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert eval_resp.status_code == 200
    eval_result = eval_resp.json()["result"]
    assert eval_result == {"pass_percentage": 25.0, "all_pass": True}

    score_resp = all_pass_trial_client.post(
        "/v1/score",
        json={"run_id": "r1", "dataset": "default",
              "evaluation_results": {"task-1": {"status": "evaluated", "result": eval_result}}},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert score_resp.status_code == 200
    body = score_resp.json()
    assert body["final_score"] == 100.0
    assert body["metadata"] == {}


def test_internal_error_does_not_leak_traceback(trial_client: TestClient) -> None:
    with patch.object(_TrialResultBenchmark, "calculate_final_score",
                      side_effect=Exception("boom at /srv/legal/rubric.py:42")):
        resp = trial_client.post(
            "/v1/score",
            json={"run_id": "r1", "dataset": "default",
                  "evaluation_results": {"task-1": {"status": "evaluated", "result": {"resolved": True}}}},
            headers={"x-descope-api-key": "trial-key"},
        )
    assert resp.status_code == 500
    assert resp.json()["detail"] == "Internal server error"
    assert "Traceback" not in resp.text and "rubric.py" not in resp.text


def test_trial_tenant_blocked_on_internal_endpoint(trial_client: TestClient) -> None:
    # A trial tenant has a valid Descope key + dataset access, so without a path gate it could
    # curl the internal (unsanitized) endpoints and read the raw eval result that /v1/ withholds.
    resp = trial_client.post(
        "/evaluate-response/",
        json={"task_id": "task-1", "response": "2"},
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 403
    assert "/v1/" in resp.json()["detail"]


def test_trial_tenant_blocked_on_unapproved_v1_endpoint(trial_client: TestClient) -> None:
    resp = trial_client.get(
        "/v1/submissions/sub-1",
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 403
    assert "approved /v1 endpoints" in resp.json()["detail"]


def test_v1_dataset_tasks_strips_extras_for_trial_tenant(trial_client: TestClient) -> None:
    resp = trial_client.get(
        "/v1/datasets/default/tasks",
        headers={"x-descope-api-key": "trial-key"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_version"] == "default-dataset-2026-06-10"
    task = body["tasks"][0]
    assert task == {"id": "task-1", "question": "What is 1+1?", "timeout": 60.0}
