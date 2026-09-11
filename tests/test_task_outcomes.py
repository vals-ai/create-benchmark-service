import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from benchmark_service.schemas import FinalScoreRequest


def test_legacy_service_receives_unchanged_results(client: TestClient) -> None:
    response = client.post(
        "/final-score/",
        json={
            "evaluation_results": {"task-1": {"resolved": True}, "task-2": None},
            "task_outcomes": {
                "task-1": {"kind": "evaluated"},
                "task-2": {
                    "kind": "error",
                    "producer": "tracker",
                    "operation": "evaluate",
                    "error_type": "EvaluationError",
                    "cause_code": "resumable_evaluation_infrastructure",
                },
            },
        },
    )
    assert response.status_code == 200
    assert response.json()["final_score"] == 50.0


@pytest.mark.parametrize("outcomes", [{}, {"task-1": {"kind": "unknown"}}])
def test_outcomes_reject_mismatched_tasks_and_unknown_kinds(outcomes: object) -> None:
    with pytest.raises(ValidationError):
        FinalScoreRequest.model_validate({"evaluation_results": {"task-1": None}, "task_outcomes": outcomes})


def test_evaluated_outcome_requires_a_result() -> None:
    with pytest.raises(ValidationError, match="Only evaluated"):
        FinalScoreRequest.model_validate(
            {"evaluation_results": {"task-1": None}, "task_outcomes": {"task-1": {"kind": "evaluated"}}}
        )
