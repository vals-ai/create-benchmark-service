"""Response sanitizers for tenants flagged `trial_mode: true` in the allowlist.

They trim the v1 response models to score-only fields so a prospect can see how
their model scored without getting back the rubric, judge identity, or per-task
breakdowns. `pass_percentage` is read from the top level of the benchmark's raw
result dict; a benchmark whose evaluator doesn't surface it returns `null` to
trial callers (fix the evaluator before enabling trial mode for it).

KNOWN COUPLING (v0): hard-coding `pass_percentage` bakes a benchmark-specific
convention into the framework. Acceptable while one benchmark is in trial mode;
the second adopter is the trigger to replace it with a benchmark-owned
projection hook or a required top-level score field in the v1 contract.
"""

from collections.abc import Mapping
from typing import Any, cast

from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalResponse,
    V1ScoreResponse,
    V1Task,
)


def sanitize_v1_eval_response(resp: V1EvalResponse) -> V1EvalResponse:
    sanitized_result: dict[str, Any] | None = None
    if isinstance(resp.result, Mapping):
        result = cast(Mapping[str, Any], resp.result)
        sanitized_result = {"pass_percentage": result.get("pass_percentage")}

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
        tasks=[
            V1Task(id=task.id, question=task.question, timeout=task.timeout)
            for task in resp.tasks
        ],
    )
