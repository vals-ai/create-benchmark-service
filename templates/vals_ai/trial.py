"""Response sanitizers for tenants flagged `trial_mode: true` in the allowlist.

They trim the v1 response models to score-only fields so a prospect can see how
their model scored without getting back the rubric, judge identity, or per-task
breakdowns. The per-task result projection is benchmark-owned: the handler
passes `BenchmarkService.project_trial_result`, which returns the trial-safe
view. That projection must retain any field the benchmark's
`calculate_final_score` needs, since a trial caller only ever resubmits it to
/v1/score.
"""

from collections.abc import Callable
from typing import Any

from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalResponse,
    V1ScoreResponse,
    V1Task,
)


def sanitize_v1_eval_response(
    resp: V1EvalResponse, project: Callable[[Any], Any]
) -> V1EvalResponse:
    sanitized_result = project(resp.result) if resp.result is not None else None

    sanitized_errors: list[str] = []
    if resp.errors:
        # Keep the error-count signal but drop content (may leak rubric / judge details).
        sanitized_errors = ["error"] * len(resp.errors)

    return V1EvalResponse(
        run_id=resp.run_id,
        task_id=resp.task_id,
        status=resp.status,
        evaluator_version=None,
        result=sanitized_result,
        errors=sanitized_errors,
    )


def sanitize_v1_score_response(resp: V1ScoreResponse) -> V1ScoreResponse:
    return V1ScoreResponse(
        run_id=resp.run_id,
        tasks_evaluated=resp.tasks_evaluated,
        final_score=resp.final_score,
        metadata={},
    )


def sanitize_v1_dataset_tasks_response(resp: V1DatasetTasksResponse) -> V1DatasetTasksResponse:
    return V1DatasetTasksResponse(
        dataset=resp.dataset,
        dataset_version=resp.dataset_version,
        tasks=[
            V1Task(id=task.id, question=task.question, timeout=task.timeout)
            for task in resp.tasks
        ],
    )
