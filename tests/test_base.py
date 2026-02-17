"""Tests for BenchmarkService base class non-abstract methods."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from daytona import AsyncSandbox

from benchmark_service.base import BenchmarkService
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
    TaskFilter,
)


class StubBenchmark(BenchmarkService):
    """Minimal concrete implementation for testing."""

    def load_dataset(self) -> dict[str, Any]:
        return {
            "task-1": {"problem": "a"},
            "task-2": {"problem": "b"},
            "task-3": {"problem": "c"},
        }

    def retrieve_task(self, task_id: str, skip_validation: bool = False) -> RetrieveTaskResponse: ...
    def setup_task(self, task_id: str, sandbox: AsyncSandbox) -> AsyncGenerator[StreamChunk, None]: ...
    def evaluate_response(self, request: EvaluateResponseRequest) -> Any: ...
    def evaluate_instance(self, task_id: str, sandbox: AsyncSandbox) -> AsyncGenerator[StreamChunk, None]: ...
    def calculate_final_score(self, evaluation_results: dict[str, Any]) -> FinalScoreResult: ...


@pytest.fixture
def service() -> StubBenchmark:
    return StubBenchmark()


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-1", "task-3"], ["task-1", "task-3"]),
        (["task-1", "task-2", "task-3"], ["task-1", "task-2", "task-3"]),
    ],
)
def test_validate_task_ids_valid(service: StubBenchmark, task_ids: list[str], expected: list[str]) -> None:
    assert service.validate_task_ids(task_ids) == expected


@pytest.mark.parametrize("task_ids", [["nonexistent"], ["task-1", "nonexistent"]])
def test_validate_task_ids_invalid(service: StubBenchmark, task_ids: list[str]) -> None:
    with pytest.raises(ValueError, match="Task ID not found"):
        service.validate_task_ids(task_ids)


def test_filter_tasks_no_filter(service: StubBenchmark) -> None:
    result = service.filter_tasks(TaskFilter())
    assert result == ["task-1", "task-2", "task-3"]


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-2", "task-3"], ["task-2", "task-3"]),
    ],
)
def test_filter_tasks_by_ids(service: StubBenchmark, task_ids: list[str], expected: list[str]) -> None:
    result = service.filter_tasks(TaskFilter(task_ids=task_ids))
    assert result == expected


@pytest.mark.parametrize(
    ("slice_str", "expected"),
    [
        ("0:1", ["task-1"]),
        ("0:2", ["task-1", "task-2"]),
        ("1:3", ["task-2", "task-3"]),
    ],
)
def test_filter_tasks_by_slice(service: StubBenchmark, slice_str: str, expected: list[str]) -> None:
    result = service.filter_tasks(TaskFilter(slice_str=slice_str))
    assert result == expected
