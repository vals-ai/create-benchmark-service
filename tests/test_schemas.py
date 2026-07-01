"""Unit tests for benchmark_service.schemas models."""

from benchmark_service import ImageSource, Resources, SnapshotSource
from benchmark_service.schemas import RetrieveTaskResponse


def test_retrieve_task_response_generation_image_defaults_none() -> None:
    resp = RetrieveTaskResponse(
        source=SnapshotSource(snapshot="eval-snap"),
        problem_path="/problem.txt",
        cwd="/workspace",
        agent_timeout=60.0,
        resources=Resources(vcpu=1, memory=2, disk=5),
    )
    assert resp.generation_image is None


def test_retrieve_task_response_accepts_generation_image() -> None:
    resp = RetrieveTaskResponse(
        source=SnapshotSource(snapshot="eval-snap"),
        generation_image=ImageSource(image="valsai/code-migration-task-1@sha256:abc123"),
        problem_path="/problem.txt",
        cwd="/workspace",
        agent_timeout=60.0,
        resources=Resources(vcpu=1, memory=2, disk=5),
    )
    assert resp.generation_image == ImageSource(image="valsai/code-migration-task-1@sha256:abc123")
