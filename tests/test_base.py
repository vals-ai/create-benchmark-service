"""Tests for BenchmarkService base class non-abstract methods."""

import pytest

from benchmark_service.schemas import TaskFilter
from benchmark_service.v1_schemas import V1Task
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


async def test_get_dataset_default(service: StubBenchmark) -> None:
    dataset = service.get_dataset(None)
    assert "task-1" in dataset


async def test_get_dataset_explicit_default(service: StubBenchmark) -> None:
    dataset = service.get_dataset("default")
    assert "task-1" in dataset


async def test_get_dataset_named(service: StubBenchmark) -> None:
    dataset = service.get_dataset("alt")
    assert "alt-task-1" in dataset


async def test_get_dataset_invalid(service: StubBenchmark) -> None:
    with pytest.raises(ValueError, match="Dataset 'nonexistent' not found"):
        service.get_dataset("nonexistent")


async def test_filter_tasks_with_dataset(service: StubBenchmark) -> None:
    result = await service.filter_tasks(TaskFilter(), dataset="alt")
    assert result == ["alt-task-1", "alt-task-2"]


async def test_validate_task_ids_with_dataset(service: StubBenchmark) -> None:
    result = await service.validate_task_ids(["alt-task-1"], dataset="alt")
    assert result == ["alt-task-1"]


async def test_validate_task_ids_wrong_dataset(service: StubBenchmark) -> None:
    with pytest.raises(ValueError, match="Task ID not found"):
        await service.validate_task_ids(["task-1"], dataset="alt")


@pytest.mark.asyncio
async def test_list_tasks_default_extracts_id_question_timeout_from_load_datasets() -> None:
    """Default list_tasks walks get_dataset() and extracts id/question/timeout."""
    service = await StubBenchmark.create()
    tasks = await service.list_tasks(dataset="default")
    ids = [t.id for t in tasks]
    assert set(ids) == {"task-1", "task-2", "task-3"}
    by_id = {t.id: t for t in tasks}
    assert by_id["task-1"].question == "What is 1+1?"
    assert all(t.timeout is None for t in tasks)
    assert all(isinstance(t, V1Task) for t in tasks)


@pytest.mark.asyncio
async def test_list_tasks_default_strips_evaluator_only_fields() -> None:
    """The default impl projects only id/question/timeout. Evaluator-only fields
    in the task dict (`answer`, rubric, grader hints, etc.) must NOT leak."""
    service = await StubBenchmark.create()
    tasks = await service.list_tasks(dataset="default")
    # StubBenchmark tasks have `answer`; verify it's stripped from the V1Task projection.
    for t in tasks:
        dumped = t.model_dump()
        assert "answer" not in dumped


@pytest.mark.asyncio
async def test_list_tasks_default_uses_default_dataset_when_none() -> None:
    service = await StubBenchmark.create()
    tasks = await service.list_tasks(dataset=None)
    assert {t.id for t in tasks} == {"task-1", "task-2", "task-3"}


@pytest.mark.asyncio
async def test_list_tasks_default_raises_value_error_for_unknown_dataset() -> None:
    service = await StubBenchmark.create()
    with pytest.raises(ValueError):
        await service.list_tasks(dataset="does-not-exist")
