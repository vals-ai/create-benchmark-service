from benchmark_service.schemas import TaskInput
from tests.conftest import StubBenchmark


async def test_list_task_inputs_defaults_empty() -> None:
    service = await StubBenchmark.create()
    assert await service.list_task_inputs("task-1") == []


async def test_list_task_inputs_override_returns_declared_files() -> None:
    class _WithInputs(StubBenchmark):
        async def list_task_inputs(self, task_id: str, dataset: str | None = None) -> list[TaskInput]:
            return [TaskInput(filename="template.xlsx", dest="/workspace/submission.xlsx")]

    service = await _WithInputs.create()
    inputs = await service.list_task_inputs("task-1")
    assert inputs == [TaskInput(filename="template.xlsx", dest="/workspace/submission.xlsx")]
