"""Response sanitizers for tenants flagged `trial_mode: true` in the allowlist."""

from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1Task,
)


def sanitize_v1_dataset_tasks_response(resp: V1DatasetTasksResponse) -> V1DatasetTasksResponse:
    return V1DatasetTasksResponse(
        dataset=resp.dataset,
        tasks=[
            V1Task(id=task.id, question=task.question, timeout=task.timeout)
            for task in resp.tasks
        ],
    )
