"""Task-list sanitizer for tenants flagged `trial_mode: true` in the allowlist.

Trial tenants may discover only the task fields needed to run generation:
`id`, `question`, and `timeout`. Benchmark-specific extras are stripped because
task objects can otherwise carry evaluator-only hints such as rubrics, answers,
or grader config.
"""

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
