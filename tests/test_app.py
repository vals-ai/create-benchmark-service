"""Tests for FastAPI app endpoints."""

from collections.abc import Generator

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


class TestAuthMiddleware:
    """Test that the built-in check_auth hook rejects unauthorized requests."""

    AUTH_TOKEN = "my-secret-token"

    @pytest.fixture
    def auth_client(self) -> Generator[TestClient, None, None]:
        from tests.conftest import StubBenchmark

        from benchmark_service.app import BenchmarkServiceApp

        class AuthBenchmark(StubBenchmark):
            async def check_auth(self, headers: dict[str, str]) -> bool:
                return headers.get("authorization") == TestAuthMiddleware.AUTH_TOKEN

        with TestClient(BenchmarkServiceApp(AuthBenchmark)) as c:
            yield c

    def test_no_auth_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids")
        assert response.status_code == 401

    def test_wrong_auth_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"Authorization": "wrong"})
        assert response.status_code == 401

    def test_correct_auth_returns_200(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"Authorization": self.AUTH_TOKEN})
        assert response.status_code == 200

    def test_health_skips_auth(self, auth_client: TestClient) -> None:
        response = auth_client.get("/health")
        assert response.status_code == 200


class TestEnvVarAuth:
    """Test the default check_auth behavior with BENCHMARK_API_KEY env var."""

    API_KEY = "test-api-key-123"

    @pytest.fixture
    def auth_client(self, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
        monkeypatch.setenv("BENCHMARK_API_KEY", self.API_KEY)
        from benchmark_service.app import BenchmarkServiceApp
        from tests.conftest import StubBenchmark

        with TestClient(BenchmarkServiceApp(StubBenchmark)) as c:
            yield c

    @pytest.fixture
    def no_auth_client(self, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
        monkeypatch.delenv("BENCHMARK_API_KEY", raising=False)
        from benchmark_service.app import BenchmarkServiceApp
        from tests.conftest import StubBenchmark

        with TestClient(BenchmarkServiceApp(StubBenchmark)) as c:
            yield c

    def test_no_key_env_allows_all(self, no_auth_client: TestClient) -> None:
        response = no_auth_client.get("/verify-task-ids")
        assert response.status_code == 200

    def test_alb_rejects_missing_header(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"X-Forwarded-Proto": "https"})
        assert response.status_code == 401

    def test_alb_rejects_wrong_header(self, auth_client: TestClient) -> None:
        response = auth_client.get(
            "/verify-task-ids",
            headers={"X-Forwarded-Proto": "https", "Authorization": "Bearer wrong-key"},
        )
        assert response.status_code == 401

    def test_alb_accepts_correct_header(self, auth_client: TestClient) -> None:
        response = auth_client.get(
            "/verify-task-ids",
            headers={"X-Forwarded-Proto": "https", "Authorization": f"Bearer {self.API_KEY}"},
        )
        assert response.status_code == 200

    def test_internal_request_skips_auth(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids")
        assert response.status_code == 200

    def test_health_skips_auth(self, auth_client: TestClient) -> None:
        response = auth_client.get("/health")
        assert response.status_code == 200
