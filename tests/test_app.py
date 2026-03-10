"""Tests for FastAPI app endpoints."""

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_verify_task_ids_all(client: TestClient) -> None:
    response = client.get("/verify-task-ids")
    assert response.status_code == 200
    assert response.json() == {"task_ids": ["task-1", "task-2", "task-3"]}


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-1", "task-3"], ["task-1", "task-3"]),
        (["task-1", "task-1"], ["task-1"]),  # deduplication
    ],
)
def test_verify_task_ids_filtered(client: TestClient, task_ids: list[str], expected: list[str]) -> None:
    response = client.get("/verify-task-ids", params=[("task_ids", t) for t in task_ids])
    assert response.status_code == 200
    assert response.json() == {"task_ids": expected}


@pytest.mark.parametrize(
    ("slice_str", "expected"),
    [
        ("0:1", ["task-1"]),
        ("0:2", ["task-1", "task-2"]),
        ("1:3", ["task-2", "task-3"]),
    ],
)
def test_verify_task_ids_slice(client: TestClient, slice_str: str, expected: list[str]) -> None:
    response = client.get("/verify-task-ids", params={"slice": slice_str})
    assert response.status_code == 200
    assert response.json() == {"task_ids": expected}


def test_retrieve_task(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "task-1"})
    assert response.status_code == 200
    data = response.json()
    assert data["problem_path"] == "/tmp/problem_statement.txt"
    assert data["docker_image"] == "python:3.12-slim"
    assert data["request_setup"] is False


def test_retrieve_task_invalid(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "nonexistent"})
    assert response.status_code == 400


def test_retrieve_task_skip_validation(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "nonexistent", "skip_validation": True})
    assert response.status_code == 200


@pytest.mark.parametrize(
    ("task_id", "response_text", "expected_resolved"),
    [
        ("task-1", "2", True),
        ("task-1", "wrong", False),
        ("task-2", "4", True),
        ("task-2", "wrong", False),
    ],
)
def test_evaluate_response(
    client: TestClient,
    task_id: str,
    response_text: str,
    expected_resolved: bool,
) -> None:
    response = client.post("/evaluate-response/", json={"task_id": task_id, "response": response_text})
    assert response.status_code == 200
    assert response.json()["resolved"] is expected_resolved


def test_evaluate_response_invalid_task(client: TestClient) -> None:
    with pytest.raises(HTTPException) as exc_info:
        client.post("/evaluate-response/", json={"task_id": "nonexistent", "response": "2"})
    assert exc_info.value.status_code == 500


@pytest.mark.parametrize(
    ("evaluation_results", "expected_score"),
    [
        ({"task-1": {"resolved": True}, "task-2": {"resolved": True}}, 100.0),
        ({"task-1": {"resolved": True}, "task-2": {"resolved": False}}, 50.0),
        ({"task-1": {"resolved": False}, "task-2": {"resolved": False}}, 0.0),
    ],
)
def test_final_score(
    client: TestClient,
    evaluation_results: dict[str, dict[str, bool]],
    expected_score: float,
) -> None:
    response = client.post("/final-score/", json={"evaluation_results": evaluation_results})
    assert response.status_code == 200
    data = response.json()
    assert data["final_score"] == expected_score
    assert set(data["tasks_evaluated"]) == set(evaluation_results.keys())


def test_final_score_invalid_task(client: TestClient) -> None:
    response = client.post("/final-score/", json={"evaluation_results": {"nonexistent": {"resolved": True}}})
    assert response.status_code == 400


def test_verify_task_ids_with_dataset(client: TestClient) -> None:
    response = client.get("/verify-task-ids", params={"dataset": "alt"})
    assert response.status_code == 200
    assert response.json() == {"task_ids": ["alt-task-1", "alt-task-2"]}


def test_verify_task_ids_invalid_dataset(client: TestClient) -> None:
    response = client.get("/verify-task-ids", params={"dataset": "nonexistent"})
    assert response.status_code == 400


def test_retrieve_task_with_dataset(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "alt-task-1", "dataset": "alt"})
    assert response.status_code == 200
    assert response.json()["problem_path"] == "/tmp/problem_statement.txt"


def test_evaluate_response_with_dataset(client: TestClient) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "alt-task-1", "response": "10", "dataset": "alt"})
    assert response.status_code == 200
    assert response.json()["resolved"] is True


def test_final_score_with_dataset(client: TestClient) -> None:
    response = client.post("/final-score/", json={
        "evaluation_results": {"alt-task-1": {"resolved": True}},
        "dataset": "alt",
    })
    assert response.status_code == 200
    assert response.json()["final_score"] == 100.0


def test_websocket_setup_task_missing_headers(client: TestClient) -> None:
    with client.websocket_connect("/ws/setup-task") as ws:
        ws.close()


def test_websocket_evaluate_instance_missing_headers(client: TestClient) -> None:
    with client.websocket_connect("/ws/evaluate-instance") as ws:
        ws.close()
