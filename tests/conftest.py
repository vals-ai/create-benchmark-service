"""Shared test fixtures and stub implementations."""

from collections.abc import AsyncGenerator, Generator
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from benchmark_service import ImageSource, Resources, Sandbox
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.base import BenchmarkService
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
)


def _score_item_resolved(score_item: object) -> bool:
    if not isinstance(score_item, dict):
        return False

    item = cast(dict[str, object], score_item)
    if "status" in item and "result" in item:
        result = item.get("result")
        if not isinstance(result, dict):
            return False
        nested_result = cast(dict[str, object], result)
        return item.get("status") == "evaluated" and nested_result.get("resolved") is True

    return item.get("resolved") is True


class StubBenchmark(BenchmarkService):
    """Minimal concrete implementation for testing."""

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        return {
            "default": {
                "task-1": {"problem": "What is 1+1?", "answer": "2"},
                "task-2": {"problem": "What is 2+2?", "answer": "4"},
                "task-3": {"problem": "What is 3+3?", "answer": "6"},
            },
            "alt": {
                "alt-task-1": {"problem": "What is 5+5?", "answer": "10"},
                "alt-task-2": {"problem": "What is 6+6?", "answer": "12"},
            },
        }

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        if not skip_validation:
            await self.validate_task_ids([task_id], dataset=dataset)
        return RetrieveTaskResponse(
            source=ImageSource(image="python:3.12-slim"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/workspace",
            agent_timeout=60.0,
            resources=Resources(vcpu=2, memory=4, disk=10),
        )

    def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        if request.eval_resume_state is not None:
            return {"task_id": request.task_id, "state": request.eval_resume_state}

        assert request.response is not None
        ds = self.get_dataset(dataset)
        task = ds[request.task_id]
        return {"resolved": request.response == task["answer"]}

    def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        total = len(evaluation_results)
        resolved = sum(1 for r in evaluation_results.values() if _score_item_resolved(r))
        score = (resolved / total * 100) if total > 0 else 0.0
        return FinalScoreResult(score=score, metadata={"total": total, "resolved": resolved})


@pytest.fixture
async def benchmark_client() -> AsyncGenerator[tuple[BenchmarkServiceClient, AsyncMock], None]:
    """A BenchmarkServiceClient with a mocked HTTP client."""
    client = BenchmarkServiceClient(url="http://localhost:8000", headers={"Authorization": "Bearer token"}, timeout=10)
    real_http = client._http_client  # pyright: ignore[reportPrivateUsage]
    mock_http = AsyncMock()
    client._http_client = mock_http  # pyright: ignore[reportPrivateUsage]
    yield client, mock_http
    await real_http.aclose()


@pytest.fixture
async def service() -> StubBenchmark:
    return await StubBenchmark.create()


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    with TestClient(BenchmarkServiceApp(StubBenchmark)) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_auth_caches() -> None:
    from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache

    clear_allowlist_cache()
    clear_auth_cache()
