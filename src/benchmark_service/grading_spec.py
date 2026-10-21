"""How a grading sandbox is resolved from a task, shared by ``/v1`` grading and eval-resume."""

import math
from dataclasses import dataclass

from benchmark_service.sandbox import (
    ComposeSource,
    Resources,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
)
from benchmark_service.sandbox.types import BaseSandboxSource
from benchmark_service.schemas import RetrieveTaskResponse

DEFAULT_GRADE_TIMEOUT_S = 1800.0
TEARDOWN_TIMEOUT_S = 120.0


def auto_stop_minutes(budget_s: float) -> int:
    """Backstop derived from the grading deadline so it never stops a sandbox
    mid-grade; the explicit delete in grade_instance_stream is the real
    teardown, and stopped sandboxes auto-delete (auto_delete_interval=0)."""
    return math.ceil(budget_s / 60) + 10


@dataclass(frozen=True)
class ResolvedSpec:
    """The task's eval-sandbox spec with generation-value fallbacks applied."""

    source: BaseSandboxSource | TargetedSnapshotSource
    resources: Resources
    volumes: list[VolumeMount]
    network_block_all: bool
    grade_timeout: float


def resolve_grading_spec(task: RetrieveTaskResponse) -> ResolvedSpec:
    spec = task.eval_sandbox
    source = spec.source if spec is not None and spec.source is not None else task.source
    if isinstance(source, ComposeSource):
        raise ValueError(
            "sandbox grading does not support ComposeSource; set eval_sandbox.source to an image or snapshot"
        )
    if spec is not None and spec.resources is not None and isinstance(
        source, (SnapshotSource, TargetedSnapshotSource)
    ):
        raise ValueError(
            "eval_sandbox.resources cannot override a snapshot-backed sandbox; use an image source with explicit resources"
        )
    resources = spec.resources if spec is not None and spec.resources is not None else task.resources
    if isinstance(source, (SnapshotSource, TargetedSnapshotSource)):
        # The snapshot owns its resources; generation GPU settings cannot be
        # forwarded through the required create-request resources field.
        resources = resources.model_copy(update={"gpu": 0, "gpu_type": None})
    if spec is None:
        return ResolvedSpec(
            source=source,
            resources=resources,
            volumes=task.volumes,
            network_block_all=True,
            grade_timeout=DEFAULT_GRADE_TIMEOUT_S,
        )
    return ResolvedSpec(
        source=source,
        resources=resources,
        volumes=task.volumes,
        network_block_all=spec.network_block_all,
        grade_timeout=spec.timeout_s if spec.timeout_s is not None else DEFAULT_GRADE_TIMEOUT_S,
    )
