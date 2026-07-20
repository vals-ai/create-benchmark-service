"""Request and response models for the benchmark service API."""

from typing import Any, Literal, cast

from pydantic import BaseModel, Field, computed_field, model_validator

from benchmark_service.sandbox import SandboxProviderConfig
from benchmark_service.sandbox.types import ImageSource, Resources, SandboxSource, SnapshotSource

_COMPOSE_LEGACY_DOCKER_IMAGE = "compose+source-required"


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


class StandardSetupLifecycle(BaseModel):
    """Setup requires no benchmark-owned cleanup when Tracker stops early."""

    type: Literal["standard"] = "standard"


class AbortableSetupLifecycle(BaseModel):
    """Tracker must call abort-task when setup starts but evaluation does not."""

    type: Literal["abortable"] = "abortable"


SetupLifecycle = StandardSetupLifecycle | AbortableSetupLifecycle


def legacy_docker_image(source: SandboxSource) -> str:
    if isinstance(source, ImageSource):
        return source.image
    if isinstance(source, SnapshotSource):
        return f"snapshot:{source.snapshot}"
    return _COMPOSE_LEGACY_DOCKER_IMAGE


class RetrieveTaskResponse(BaseModel):
    """
    Response containing task metadata and setup requirements.

    Customize fields based on what your benchmark tasks need.
    """

    source: SandboxSource = Field(description="Sandbox source image or snapshot")
    problem_path: str = Field(
        description="Path inside the sandbox where the problem statement file will be written during setup"
    )
    cwd: str = Field(description="Working directory inside the container")
    agent_timeout: float | None = Field(
        default=None, description="Agent execution max time in seconds (None for no timeout)"
    )
    resources: Resources = Field(description="Computational resources needed")
    setup_lifecycle: SetupLifecycle = Field(
        default_factory=StandardSetupLifecycle,
        discriminator="type",
        description="Cleanup required when task execution stops after setup starts",
    )

    @computed_field(description="Legacy sandbox image field for older Valkyrie clients")
    @property
    def docker_image(self) -> str:
        return legacy_docker_image(self.source)

    @model_validator(mode="before")
    @classmethod
    def parse_legacy_fields(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        legacy = cast(dict[str, object], data)
        if "source" in legacy or "docker_image" not in legacy:
            return legacy

        docker_image = legacy["docker_image"]
        if isinstance(docker_image, str) and docker_image.startswith("snapshot:"):
            return {**legacy, "source": {"type": "snapshot", "snapshot": docker_image.removeprefix("snapshot:")}}
        return {**legacy, "source": {"type": "image", "image": docker_image}}


class SetupTaskRequest(BaseModel):
    """Request to setup a task in a sandbox environment."""

    task_id: str = Field(description="Unique identifier for the task")
    instance_id: str = Field(description="Unique identifier for the sandbox instance")
    sandbox_provider: SandboxProviderConfig | None = Field(default=None, description="Sandbox provider config")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")


class SetupTaskResponse(BaseModel):
    """Response after task setup completion."""

    status: str = Field(description="Status of setup operation ('ok' or error message)")
    egress_allowlist: list[str] = Field(
        default_factory=list,
        description="Task-specific addresses the agent must be able to reach after setup",
    )


class AbortTaskRequest(BaseModel):
    """Request to clean up benchmark-owned resources after setup starts."""

    task_id: str = Field(description="Unique identifier for the task")
    run_id: str = Field(description="Unique identifier for the benchmark run")
    instance_id: str = Field(description="Unique identifier for the agent sandbox instance")
    dataset: str | None = Field(default=None, description="Dataset name to use (defaults to 'default')")


class AbortTaskResponse(BaseModel):
    """Response after idempotent benchmark-owned cleanup completes."""

    status: Literal["ok"] = "ok"


class EvaluateResponseRequest(BaseModel):
    """
    Request to evaluate without a live sandbox.

    Use this for benchmarks where evaluation can run from a text response or
    benchmark-owned durable resume state.
    """

    task_id: str = Field(description="Unique identifier for the task")
    response: str | None = Field(default=None, description="The agent's response to evaluate")
    eval_resume_state: dict[str, Any] | None = Field(
        default=None, description="Benchmark-specific evaluation progress state"
    )
    sandbox_provider: SandboxProviderConfig | None = Field(
        default=None,
        description="Request-scoped sandbox provider config for evaluation work that creates a sandbox",
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
    sandbox_provider: SandboxProviderConfig | None = Field(default=None, description="Sandbox provider config")
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


class VersionResponse(BaseModel):
    """Response for GET /version reporting framework + deployed-service version."""

    framework_version: str
    service_name: str | None = None
    service_version: str | None = None
    dataset_version: str | None = None


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

    type: Literal["eval_resume_state"]
    data: dict[str, Any] = Field(description="Benchmark-specific evaluation progress state")


# Union type for all streaming chunks
StreamChunk = StreamMessageChunk | StreamResultChunk | StreamErrorChunk | StreamEvalResumeStateChunk
