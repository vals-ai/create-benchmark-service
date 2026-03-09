"""Tests for BenchmarkService base class non-abstract methods."""

import pytest

from benchmark_service.schemas import TaskFilter
from tests.conftest import StubBenchmark


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-1", "task-3"], ["task-1", "task-3"]),
        (["task-1", "task-2", "task-3"], ["task-1", "task-2", "task-3"]),
    ],
)
async def test_validate_task_ids_valid(service: StubBenchmark, task_ids: list[str], expected: list[str]) -> None:
    assert await service.validate_task_ids(task_ids) == expected


@pytest.mark.parametrize("task_ids", [["nonexistent"], ["task-1", "nonexistent"]])
async def test_validate_task_ids_invalid(service: StubBenchmark, task_ids: list[str]) -> None:
    with pytest.raises(ValueError, match="Task ID not found"):
        await service.validate_task_ids(task_ids)


async def test_filter_tasks_no_filter(service: StubBenchmark) -> None:
    result = await service.filter_tasks(TaskFilter())
    assert result == ["task-1", "task-2", "task-3"]


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-2", "task-3"], ["task-2", "task-3"]),
    ],
)
async def test_filter_tasks_by_ids(service: StubBenchmark, task_ids: list[str], expected: list[str]) -> None:
    result = await service.filter_tasks(TaskFilter(task_ids=task_ids))
    assert result == expected


@pytest.mark.parametrize(
    ("slice_str", "expected"),
    [
        ("0:1", ["task-1"]),
        ("0:2", ["task-1", "task-2"]),
        ("1:3", ["task-2", "task-3"]),
    ],
)
async def test_filter_tasks_by_slice(service: StubBenchmark, slice_str: str, expected: list[str]) -> None:
    result = await service.filter_tasks(TaskFilter(slice_str=slice_str))
    assert result == expected
