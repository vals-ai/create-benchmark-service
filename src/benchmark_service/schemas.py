"""Request and response models for the benchmark service API."""

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class TaskFilter(BaseModel):
    """Filter for selecting tasks from your benchmark dataset."""

    task_ids: list[str] | None = Field(default=None, description="List of specific task IDs to filter")
    slice_str: str | None = Field(default=None, description="Slice notation for selecting tasks (e.g., '3:10:1')")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")

    def parse_slice(self) -> slice:
        """Parse slice string into Python slice object."""
        if not self.slice_str:
            raise ValueError("Slice is not provided")

        parts = self.slice_str.split(":")

        if not 1 <= len(parts) <= 3:
            raise ValueError("Invalid slice format")

        def int_conversion(p: str) -> int | None:
            return int(p) if p else None

        while len(parts) < 3:
            parts.append("")

        start, stop, step = (int_conversion(p) for p in parts)
        return slice(start, stop, step)


class VerifyTaskIdsResponse(BaseModel):
    """Response containing verified task IDs that exist in your benchmark."""

    task_ids: list[str] = Field(description="List of verified task IDs that exist in the benchmark")


class Resources(BaseModel):
    """Computational resources required to run a task."""

    vcpu: int = Field(description="Number of vCPUs required")
    memory: int = Field(description="Memory in GB")
    disk: int = Field(description="Disk space in GB")


class RetrieveTaskResponse(BaseModel):
    """
    Response containing task metadata and setup requirements.

    Customize fields based on what your benchmark tasks need.
    """

    docker_image: str = Field(description="Docker image name or path")
    problem_path: str = Field(
        description="Path inside the sandbox where the problem statement file will be written during setup"
    )
    cwd: str = Field(description="Working directory inside the container")
    agent_timeout: float | None = Field(
        default=None, description="Agent execution max time in seconds (None for no timeout)"
    )
    resources: Resources = Field(description="Computational resources needed")


class SetupTaskRequest(BaseModel):
    """Request to setup a task in a sandbox environment."""

    task_id: str = Field(description="Unique identifier for the task")
    instance_id: str = Field(description="Unique identifier for the sandbox instance")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")


class SetupTaskResponse(BaseModel):
    """Response after task setup completion."""

    status: str = Field(description="Status of setup operation ('ok' or error message)")


class EvaluateResponseRequest(BaseModel):
    """
    Request to evaluate without a live sandbox.

    Use this for benchmarks where evaluation can run from a text response or
    benchmark-owned durable resume state.
    """

    task_id: str = Field(description="Unique identifier for the task")
    response: str | None = Field(default=None, description="The agent's response to evaluate")
    eval_resume_state: dict[str, Any] | None = Field(
        default=None, description="Opaque benchmark-owned evaluation resume state"
    )
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")

    @model_validator(mode="after")
    def require_one_eval_input(self) -> "EvaluateResponseRequest":
        if (self.response is None) == (self.eval_resume_state is None):
            raise ValueError("Exactly one of response or eval_resume_state is required")
        return self


class EvaluateInstanceRequest(BaseModel):
    """Request to evaluate a task instance in a sandbox."""

    task_id: str = Field(description="Unique identifier for the task")
    instance_id: str = Field(description="Sandbox instance where the solution was implemented")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")


class FinalScoreRequest(BaseModel):
    """
    Request containing all evaluation results to calculate final score.

    The evaluation_results values can be any benchmark-specific result object.
    Define your own result structure based on your benchmark's needs.

    Examples:
    - {"resolved": True, "score": 1.0, "tests_passed": 10}
    - {"correct": False, "error": "Wrong answer"}
    - Any Pydantic model or dict with your evaluation data
    """

    evaluation_results: dict[str, Any] = Field(description="Mapping of task_id to benchmark-specific evaluation result")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")


class FinalScoreResult(BaseModel):
    """Result from calculate_final_score method."""

    score: float = Field(description="Aggregate score (e.g., percentage of resolved tasks)")
    metadata: dict[str, Any] = Field(description="Benchmark-specific metadata")


class FinalScoreResponse(BaseModel):
    """Final aggregated score across all evaluated tasks."""

    tasks_evaluated: list[str] = Field(description="All task IDs that were evaluated")
    final_score: float = Field(description="Aggregate score (e.g., percentage of resolved tasks)")
    metadata: Any = Field(description="Benchmark-specific metadata")


class HealthCheckResponse(BaseModel):
    """Simple health check response."""

    status: str = Field(description="Status of the service ('ok' if running)")


class StreamMessageChunk(BaseModel):
    """Streaming chunk for log messages and progress updates."""

    type: Literal["message"] = Field(description="Chunk type identifier")
    data: str = Field(description="Log message or progress update")


class StreamResultChunk(BaseModel):
    """Streaming chunk for final results."""

    type: Literal["result"] = Field(description="Chunk type identifier")
    data: dict[str, Any] = Field(description="Final result data (benchmark-specific structure)")


class StreamErrorChunk(BaseModel):
    """Streaming chunk for error messages."""

    type: Literal["error"] = Field(description="Chunk type identifier")
    data: str = Field(description="Error message")


class StreamEvalResumeStateChunk(BaseModel):
    """Streaming chunk for benchmark-owned evaluation resume state."""

    type: Literal["eval_resume_state"] = Field(description="Chunk type identifier")
    data: dict[str, Any] = Field(description="Opaque benchmark-owned evaluation resume state")


# Union type for all streaming chunks
StreamChunk = StreamMessageChunk | StreamResultChunk | StreamErrorChunk | StreamEvalResumeStateChunk
