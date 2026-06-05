"""Lab-facing /v1/ dataset task-list schemas."""

from pydantic import BaseModel, ConfigDict


class V1Task(BaseModel):
    """A task as exposed on the lab-facing /v1/ surface.

    The framework guarantees `id`, `question`, and `timeout` across all
    benchmarks. `extra="allow"` lets benchmarks expose benchmark-specific
    per-task fields (e.g. SWE-bench's `repo`/`base_commit`, an artifact
    benchmark's docker hints) without each addition requiring a framework
    change. Per-benchmark fields should be documented in each benchmark's
    README, and benchmark runners typically validate the full shape with
    their own typed `Task` subclass on receipt.

    The default `BenchmarkService.list_tasks` raises `NotImplementedError`,
    so any field that appears on this surface is there because the
    benchmark's override explicitly put it there — there is no implicit
    pass-through from internal task storage.
    """

    model_config = ConfigDict(extra="allow")
    id: str
    question: str
    timeout: float | None = None


class V1DatasetTasksResponse(BaseModel):
    dataset: str
    tasks: list[V1Task]


__all__ = [
    "V1DatasetTasksResponse",
    "V1Task",
]
