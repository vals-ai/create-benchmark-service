"""Shared test fixtures and stub implementations."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from daytona import AsyncSandbox
from fastapi.testclient import TestClient

from benchmark_service.app import create_app
from benchmark_service.base import BenchmarkService
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    Resources,
    StreamChunk,
)


class StubBenchmark(BenchmarkService):
    """Minimal concrete implementation for testing."""

    def load_dataset(self) -> dict[str, Any]:
        return {
            "task-1": {"problem": "What is 1+1?", "answer": "2"},
            "task-2": {"problem": "What is 2+2?", "answer": "4"},
            "task-3": {"problem": "What is 3+3?", "answer": "6"},
        }

    def retrieve_task(self, task_id: str, skip_validation: bool = False) -> RetrieveTaskResponse:
        if not skip_validation:
            self.validate_task_ids([task_id])
        task = self.tasks[task_id]
        return RetrieveTaskResponse(
            docker_image="python:3.12-slim",
            problem_statement=task["problem"],
            request_setup=False,
            cwd="/workspace",
            resources=Resources(vcpu=2, memory=4, disk=10),
        )

    async def setup_task(self, task_id: str, sandbox: AsyncSandbox) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    def evaluate_response(self, request: EvaluateResponseRequest) -> Any:
        task = self.tasks[request.task_id]
        return {"resolved": request.response == task["answer"]}

    async def evaluate_instance(self, task_id: str, sandbox: AsyncSandbox) -> AsyncGenerator[StreamChunk, None]: ...  # type: ignore[return]

    def calculate_final_score(self, evaluation_results: dict[str, Any]) -> FinalScoreResult:
        total = len(evaluation_results)
        resolved = sum(1 for r in evaluation_results.values() if r.get("resolved"))
        score = (resolved / total * 100) if total > 0 else 0.0
        return FinalScoreResult(score=score, metadata={"total": total, "resolved": resolved})


@pytest.fixture
def service() -> StubBenchmark:
    return StubBenchmark()


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app(StubBenchmark()))
