# pyright: reportPrivateUsage=false

"""Tests for sandbox-graded /v1/evaluate (eval_mode == SANDBOX)."""

import asyncio
import json
import re
from collections.abc import AsyncGenerator, Generator, Mapping
from threading import Event
from typing import Any, cast
from unittest.mock import patch

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from benchmark_service import (
    ComposeSource,
    ImageSource,
    ModalProviderConfig,
    Resources,
    Sandbox,
    SnapshotSource,
)
from benchmark_service import auth as auth_module
from benchmark_service import grading
from benchmark_service.app import (
    BenchmarkServiceApp,
    _DuplicateGradingRequest,
    _GradingAdmission,
    _GradingCapacityExceeded,
    _grading_provider_config,
)
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.grading import (
    SUBMISSION_ARTIFACT_SANDBOX_PATH,
    collapse_stream,
    evaluate_submission,
)
from benchmark_service.sandbox import MissingSandboxConfigError, SandboxCreateRequest, SandboxProvider
from benchmark_service.sandbox.types import ExecResult
from benchmark_service.schemas import (
    ArtifactGradingSubmission,
    EvalMode,
    EvalSandboxSpec,
    GradingSubmission,
    StreamChunk,
    StreamErrorChunk,
    StreamResultChunk,
    TextGradingSubmission,
)
from benchmark_service.submission_artifacts import (
    SubmissionArtifactChanged,
    SubmissionArtifactNotFound,
    SubmissionArtifactReference,
)
from benchmark_service.v1_schemas import V1EvalRequest, V1EvalStatus, V1Payload, V1PayloadType
from tests.conftest import StubBenchmark


class FakeSandbox(Sandbox):
    """In-memory sandbox: upload/download to a dict, no-op exec/command."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    @property
    def id(self) -> str:
        return "fake-sandbox"

    @property
    def name(self) -> str:
        return "fake-sandbox"

    @property
    def state(self) -> str:
        return "started"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        return ExecResult(exit_code=0, output="")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        return
        yield  # marks this as an (empty) async generator

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.files[remote_path] = content

    async def download_file(self, remote_path: str) -> bytes:
        return self.files[remote_path]


class FakeProvider(SandboxProvider):
    """Records create/delete; hands out a single FakeSandbox."""

    def __init__(self, sandbox: FakeSandbox) -> None:
        self.sandbox = sandbox
        self.created: list[SandboxCreateRequest] = []
        self.deleted: list[str] = []

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        self.created.append(request)
        return self.sandbox

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        return self.sandbox

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleted.append(instance_id)

    async def list_sandboxes(self, query: Any) -> AsyncGenerator[Sandbox, None]:
        return
        yield


class SignalingProvider(FakeProvider):
    def __init__(self, sandbox: FakeSandbox) -> None:
        super().__init__(sandbox)
        self.deleted_event = asyncio.Event()

    async def delete_sandbox(self, instance_id: str) -> None:
        await super().delete_sandbox(instance_id)
        self.deleted_event.set()


class FakeProviderConfig:
    """Duck-typed stand-in for SandboxProviderConfig: only create_provider is used."""

    def __init__(self, provider: FakeProvider) -> None:
        self._provider = provider

    def create_provider(self) -> SandboxProvider:
        return self._provider


class SandboxStub(StubBenchmark):
    """A SANDBOX-mode benchmark: inject the answer, then grade by reading it back."""

    eval_mode = EvalMode.SANDBOX
    accepted_submission_schemas = {
        V1PayloadType.TEXT: frozenset({"stub.text.v1"}),
    }

    async def prepare_grading_sandbox(
        self,
        sandbox: Sandbox,
        submission: GradingSubmission,
        dataset: str | None = None,
    ) -> None:
        assert isinstance(submission, TextGradingSubmission)
        await sandbox.upload_file("/workspace/answer.txt", submission.text.encode())

    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        content = (await sandbox.download_file("/workspace/answer.txt")).decode()
        answer = self.get_dataset(dataset)[task_id]["answer"]
        resolved = content == answer
        yield StreamResultChunk(
            type="result",
            data={"resolved": resolved, "weighted_pass_percentage": 100.0 if resolved else 0.0},
        )


async def test_prepare_grading_sandbox_receives_typed_text_submission() -> None:
    service = await SandboxStub.create()
    sandbox = FakeSandbox()
    submission = TextGradingSubmission(task_id="task-1", schema_id="stub.text.v1", text="2")
    await service.prepare_grading_sandbox(sandbox, submission, dataset="default")
    assert sandbox.files["/workspace/answer.txt"] == b"2"


async def test_prepare_grading_sandbox_default_raises_not_implemented() -> None:
    service = await StubBenchmark.create()
    submission = TextGradingSubmission(task_id="task-1", schema_id="stub.text.v1", text="2")
    with pytest.raises(NotImplementedError, match="prepare_grading_sandbox"):
        await service.prepare_grading_sandbox(FakeSandbox(), submission, dataset="default")


class ErrorChunkStub(SandboxStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        yield StreamErrorChunk(type="error", data="grader blew up")


class RaisingStub(SandboxStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        raise RuntimeError("boom")
        yield  # pragma: no cover


class SlowStub(SandboxStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        await asyncio.sleep(0.1)
        yield StreamResultChunk(type="result", data={"resolved": True})


class NoResultStub(SandboxStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        return
        yield  # pragma: no cover


def _submission() -> TextGradingSubmission:
    return TextGradingSubmission(task_id="task-1", schema_id="stub.text.v1", text="2")


async def _grade(service: StubBenchmark, provider: FakeProvider, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "service": service,
        "run_id": "run-1",
        "tenant": "acme",
        "submission": _submission(),
        "provider": provider,
        "evaluator_version": "stub-1.0",
        "dataset": "default",
    }
    kwargs.update(overrides)
    return await evaluate_submission(**kwargs)


async def test_grade_instance_returns_evaluated_with_passthrough_result() -> None:
    service = await SandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.EVALUATED
    assert resp.run_id == "run-1"
    assert resp.task_id == "task-1"
    assert resp.evaluator_version == "stub-1.0"
    assert resp.result == {"resolved": True, "weighted_pass_percentage": 100.0}


async def test_grade_instance_creates_isolated_sandbox_from_task_source_and_deletes_it() -> None:
    service = await SandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    await _grade(service, provider)
    assert len(provider.created) == 1
    create = provider.created[0]
    assert create.source == ImageSource(image="python:3.12-slim")
    assert create.resources == Resources(vcpu=2, memory=4, disk=10)
    assert create.network_block_all is True
    assert create.env_vars == {}
    # run_id is caller-supplied: the sandbox name must be a sanitized slug plus
    # a unique suffix, and the labels must carry the tracker vocabulary plus
    # lab attribution so leaked sandboxes are findable.
    assert re.fullmatch(r"grade-run-1-task-1-[0-9a-f]{10}", create.name)
    assert create.labels["Benchmark"] == "SandboxStub"
    assert create.labels["Task"] == "task-1"
    assert create.labels["tenant"] == "acme"
    assert create.labels["run-id"] == "run-1"
    assert provider.deleted == ["fake-sandbox"]


class SpecStub(SandboxStub):
    """Declares grading-sandbox overrides distinct from the generation values."""

    async def retrieve_task(self, task_id: str, skip_validation: bool = False, dataset: str | None = None) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "eval_sandbox": EvalSandboxSpec(
                    source=ImageSource(image="grader:1.0"),
                    resources=Resources(vcpu=8, memory=16, disk=50),
                    network_block_all=False,
                )
            }
        )


def test_eval_sandbox_spec_rejects_secret_bearing_environment_values() -> None:
    with pytest.raises(ValidationError, match="env_vars"):
        EvalSandboxSpec.model_validate({"env_vars": {"GRADER_API_KEY": "secret"}})


async def test_grade_instance_applies_benchmark_declared_eval_sandbox_spec() -> None:
    service = await SpecStub.create()
    provider = FakeProvider(FakeSandbox())
    await _grade(service, provider)
    create = provider.created[0]
    assert create.source == ImageSource(image="grader:1.0")
    assert create.resources == Resources(vcpu=8, memory=16, disk=50)
    assert create.network_block_all is False
    assert create.env_vars == {}


class ComposeGenerationSourceStub(SandboxStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "source": ComposeSource(
                    outer=ImageSource(image="docker:28-dind"),
                    service="main",
                )
            }
        )


class ComposeGenerationWithEvalOverrideStub(ComposeGenerationSourceStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "eval_sandbox": EvalSandboxSpec(
                    source=ImageSource(image="grader:1.0"),
                )
            }
        )


async def test_grade_instance_uses_eval_override_for_compose_generation_source() -> None:
    service = await ComposeGenerationWithEvalOverrideStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.EVALUATED
    assert provider.created[0].source == ImageSource(image="grader:1.0")


async def test_grade_instance_rejects_compose_generation_source_without_eval_override() -> None:
    service = await ComposeGenerationSourceStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.ERROR
    assert resp.errors == [
        "sandbox grading does not support ComposeSource; set eval_sandbox.source to an image or snapshot"
    ]
    assert provider.created == []


class SnapshotResourceSpecStub(SandboxStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "eval_sandbox": EvalSandboxSpec(
                    source=SnapshotSource(snapshot="grader-snapshot"),
                    resources=Resources(vcpu=8, memory=16, disk=50),
                )
            }
        )


async def test_grade_instance_rejects_snapshot_resource_override_before_create() -> None:
    service = await SnapshotResourceSpecStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.ERROR
    assert "cannot override a snapshot-backed sandbox" in resp.errors[0]
    assert provider.created == []


class SnapshotEvalForGpuTaskStub(SandboxStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "resources": Resources(vcpu=8, memory=16, disk=50, gpu=1, gpu_type="H100"),
                "eval_sandbox": EvalSandboxSpec(source=SnapshotSource(snapshot="grader-snapshot")),
            }
        )


async def test_grade_instance_does_not_apply_generation_gpu_to_snapshot_override() -> None:
    service = await SnapshotEvalForGpuTaskStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.EVALUATED
    assert provider.created[0].source == SnapshotSource(snapshot="grader-snapshot")
    assert provider.created[0].resources.gpu == 0
    assert provider.created[0].resources.gpu_type is None


class SnapshotGenerationGpuTaskStub(SandboxStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(
            update={
                "source": SnapshotSource(snapshot="generation-snapshot"),
                "resources": Resources(vcpu=8, memory=16, disk=50, gpu=1, gpu_type="H100"),
            }
        )


async def test_grade_instance_does_not_apply_generation_gpu_to_default_snapshot() -> None:
    service = await SnapshotGenerationGpuTaskStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.EVALUATED
    assert provider.created[0].source == SnapshotSource(snapshot="generation-snapshot")
    assert provider.created[0].resources.gpu == 0
    assert provider.created[0].resources.gpu_type is None


class ArtifactSandboxStub(SandboxStub):
    """Grades from the artifact the framework materialized in the sandbox."""

    accepted_submission_schemas = {
        V1PayloadType.ARTIFACT: frozenset({"stub.artifact.v1"}),
    }

    async def prepare_grading_sandbox(
        self,
        sandbox: Sandbox,
        submission: GradingSubmission,
        dataset: str | None = None,
    ) -> None:
        assert isinstance(submission, ArtifactGradingSubmission)
        assert submission.schema_id == "stub.artifact.v1"
        assert submission.artifact_reference.key.startswith("submission-artifacts/acme/")
        tarball = await sandbox.download_file(submission.sandbox_path)
        await sandbox.upload_file("/workspace/answer.txt", tarball)


class InProcessArtifactStub(StubBenchmark):
    """Grades admitted bytes without creating a grading sandbox."""

    eval_mode = EvalMode.IN_PROCESS_ARTIFACT
    accepted_submission_schemas = {
        V1PayloadType.ARTIFACT: frozenset({"stub.workbook.v1"}),
    }
    artifact_call: tuple[str, str, str, bytes, str | None] | None = None

    async def evaluate_artifact(
        self,
        run_id: str,
        task_id: str,
        schema_id: str,
        artifact: bytes,
        dataset: str | None = None,
    ) -> AsyncGenerator[StreamChunk, None]:
        self.artifact_call = (run_id, task_id, schema_id, artifact, dataset)
        yield StreamResultChunk(
            type="result",
            data={"resolved": artifact == b"workbook-bytes"},
        )


async def test_grade_instance_maps_missing_artifact_to_error(monkeypatch: pytest.MonkeyPatch) -> None:
    artifact_reference = SubmissionArtifactReference(
        key="submission-artifacts/acme/default/run-1/task-1/a.bin",
        size_bytes=1,
        etag='"etag-1"',
    )

    async def missing_download(reference: SubmissionArtifactReference, *, tenant: str) -> bytes:
        raise SubmissionArtifactNotFound(f"no artifact was uploaded for key {reference.key}")

    monkeypatch.setattr(grading.submission_artifacts, "download", missing_download)
    service = await ArtifactSandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(
        service,
        provider,
        submission=ArtifactGradingSubmission(
            task_id="task-1",
            schema_id="stub.artifact.v1",
            artifact_reference=artifact_reference,
            sandbox_path=SUBMISSION_ARTIFACT_SANDBOX_PATH,
        ),
    )
    assert resp.status == V1EvalStatus.ERROR
    assert any("no artifact was uploaded" in e for e in resp.errors)
    assert provider.deleted == ["fake-sandbox"]


async def test_grade_instance_maps_error_chunk_to_error_status() -> None:
    service = await ErrorChunkStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert resp.errors == ["grader blew up"]
    assert provider.deleted == ["fake-sandbox"]


async def test_grade_instance_maps_exception_to_error_and_still_deletes_sandbox() -> None:
    service = await RaisingStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert any("boom" in e for e in resp.errors)
    assert provider.deleted == ["fake-sandbox"]


class SlowSpecStub(SlowStub):
    """A slow grade under a task-declared sub-second eval timeout."""

    async def retrieve_task(self, task_id: str, skip_validation: bool = False, dataset: str | None = None) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(update={"eval_sandbox": EvalSandboxSpec(timeout_s=0.05)})


async def test_grade_instance_starts_a_fresh_task_timeout_after_preparation() -> None:
    service = await SlowSpecStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert any("0.05" in e and "grade timeout" in e for e in resp.errors)
    assert provider.deleted == ["fake-sandbox"]
    assert provider.created[0].auto_stop_interval == 21


class BlockingCleanupStub(SandboxStub):
    cleanup_started: asyncio.Event
    cleanup_finished: asyncio.Event
    release_cleanup: asyncio.Event

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(update={"eval_sandbox": EvalSandboxSpec(timeout_s=0.01)})

    async def block_cleanup(self) -> None:
        self.cleanup_started.set()
        try:
            await self.release_cleanup.wait()
        finally:
            self.cleanup_finished.set()


class StuckEvaluationStub(BlockingCleanupStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        try:
            await asyncio.Event().wait()
            yield StreamResultChunk(type="result", data={"resolved": True})
        finally:
            await self.block_cleanup()


class StuckFinalizerStub(BlockingCleanupStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        try:
            yield StreamResultChunk(type="result", data={"resolved": True})
        finally:
            await self.block_cleanup()


def _initialize_cleanup_events(service: BlockingCleanupStub) -> None:
    service.cleanup_started = asyncio.Event()
    service.cleanup_finished = asyncio.Event()
    service.release_cleanup = asyncio.Event()


async def _release_cleanup(service: BlockingCleanupStub) -> None:
    service.release_cleanup.set()
    finished = asyncio.create_task(service.cleanup_finished.wait())
    done, _ = await asyncio.wait({finished}, timeout=1)
    if finished not in done:
        finished.cancel()


async def _event_set(event: asyncio.Event, timeout: float = 0.2) -> bool:
    if event.is_set():
        return True
    waiter = asyncio.create_task(event.wait())
    done, _ = await asyncio.wait({waiter}, timeout=timeout)
    if waiter not in done:
        waiter.cancel()
        return False
    return True


async def _thread_event_set(event: Event, timeout: float = 0.2) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not event.is_set():
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(0.001)
    return True


async def test_grade_timeout_does_not_wait_for_slow_evaluator_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(grading, "_STREAM_CLOSE_TIMEOUT_S", 0.01)
    service = await StuckEvaluationStub.create()
    _initialize_cleanup_events(service)
    provider = FakeProvider(FakeSandbox())
    grade_task = asyncio.create_task(_grade(service, provider))

    try:
        done, _ = await asyncio.wait({grade_task}, timeout=0.2)
        assert grade_task in done
        response = grade_task.result()
        assert response.status == V1EvalStatus.ERROR
        assert response.errors == ["grading exceeded the 0.01s grade timeout"]
        assert provider.deleted == ["fake-sandbox"]
        assert service.cleanup_started.is_set()
        assert await _event_set(service.cleanup_finished)
    finally:
        await _release_cleanup(service)


async def test_terminal_result_survives_slow_finalizer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(grading, "_STREAM_CLOSE_TIMEOUT_S", 0.01)
    service = await StuckFinalizerStub.create()
    _initialize_cleanup_events(service)
    provider = FakeProvider(FakeSandbox())
    grade_task = asyncio.create_task(_grade(service, provider))

    try:
        done, _ = await asyncio.wait({grade_task}, timeout=0.2)
        assert grade_task in done
        response = grade_task.result()
        assert response.status == V1EvalStatus.EVALUATED
        assert response.result == {"resolved": True}
        assert provider.deleted == ["fake-sandbox"]
        assert service.cleanup_started.is_set()
        assert await _event_set(service.cleanup_finished)
    finally:
        await _release_cleanup(service)


async def test_request_cancellation_keeps_admission_until_sandbox_deletion() -> None:
    service = await StuckFinalizerStub.create()
    _initialize_cleanup_events(service)
    provider = SignalingProvider(FakeSandbox())
    admission = _GradingAdmission(
        max_concurrency=1,
        max_queued=0,
        max_admitted_per_tenant=1,
        queue_timeout_s=0.01,
    )
    key = ("acme", "run-1", "task-1")

    async def admitted_grade() -> Any:
        async with admission.acquire(key):
            return await _grade(service, provider)

    grade_task = asyncio.create_task(admitted_grade())
    cleanup_started = asyncio.create_task(service.cleanup_started.wait())

    try:
        done, _ = await asyncio.wait({cleanup_started}, timeout=0.2)
        assert cleanup_started in done
        grade_task.cancel()
        await asyncio.sleep(0)

        assert not grade_task.done()
        with pytest.raises(_GradingCapacityExceeded):
            async with admission.acquire(("other", "run-2", "task-2")):
                pass

        service.release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await grade_task
        assert provider.deleted == ["fake-sandbox"]
        async with admission.acquire(key):
            pass
    finally:
        service.release_cleanup.set()
        if not grade_task.done():
            grade_task.cancel()
        await asyncio.gather(grade_task, return_exceptions=True)


class SlowRetrieveStub(SandboxStub):
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> Any:
        await asyncio.sleep(0.1)
        return await super().retrieve_task(task_id, skip_validation, dataset=dataset)


async def test_grade_instance_bounds_task_retrieval_as_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(grading, "_PREPARATION_TIMEOUT_S", 0.05)
    service = await SlowRetrieveStub.create()
    provider = FakeProvider(FakeSandbox())

    resp = await _grade(service, provider)

    assert resp.status == V1EvalStatus.ERROR
    assert resp.errors == ["grading preparation exceeded 0.05s"]
    assert provider.created == []


async def test_grade_instance_uses_a_unique_sandbox_name_per_request() -> None:
    """Create-conflict recovery adopts by name, so a reused name could hand one
    request another tenant's live sandbox; names must be unique per request."""
    service = await SandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    await _grade(service, provider)
    await _grade(service, provider)
    assert len({create.name for create in provider.created}) == 2


class SlowDeleteProvider(FakeProvider):
    async def delete_sandbox(self, instance_id: str) -> None:
        await asyncio.sleep(0.3)
        await super().delete_sandbox(instance_id)


class FastSpecStub(SandboxStub):
    """An instant grade under a task-declared sub-second eval timeout."""

    async def retrieve_task(self, task_id: str, skip_validation: bool = False, dataset: str | None = None) -> Any:
        task = await super().retrieve_task(task_id, skip_validation, dataset=dataset)
        return task.model_copy(update={"eval_sandbox": EvalSandboxSpec(timeout_s=0.2)})


async def test_grade_completing_near_deadline_keeps_result_and_delete() -> None:
    """Teardown runs outside the grading deadline: a grade that finishes just
    before the deadline must not lose its computed result — or its delete — to
    a teardown that outlives the deadline."""
    service = await FastSpecStub.create()
    provider = SlowDeleteProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.EVALUATED
    assert resp.result == {"resolved": True, "weighted_pass_percentage": 100.0}
    assert provider.deleted == ["fake-sandbox"]


async def test_grade_instance_maps_missing_result_chunk_to_error() -> None:
    service = await NoResultStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert resp.result is None
    assert resp.errors == ["evaluate_instance completed without a result chunk"]
    assert provider.deleted == ["fake-sandbox"]


async def test_collapse_stream_preserves_terminal_result_when_cleanup_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def stream() -> AsyncGenerator[StreamChunk, None]:
        try:
            yield StreamResultChunk(type="result", data={"score": 1})
        finally:
            raise RuntimeError("cleanup failed")

    resp = await collapse_stream(
        stream(),
        run_id="run-1",
        task_id="task-1",
        evaluator_version="stub-1.0",
    )

    assert resp.status == V1EvalStatus.EVALUATED
    assert resp.result == {"score": 1}
    assert "cleanup failed" in caplog.text


async def test_collapse_stream_bounds_slow_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(grading, "_COLLAPSE_CLOSE_TIMEOUT_S", 0.01)
    cleanup_started = asyncio.Event()
    cleanup_finished = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def stream() -> AsyncGenerator[StreamChunk, None]:
        try:
            yield StreamResultChunk(type="result", data={"score": 1})
        finally:
            cleanup_started.set()
            try:
                await release_cleanup.wait()
            finally:
                cleanup_finished.set()

    collapse_task = asyncio.create_task(
        collapse_stream(
            stream(),
            run_id="run-1",
            task_id="task-1",
            evaluator_version="stub-1.0",
        )
    )
    try:
        done, _ = await asyncio.wait({collapse_task}, timeout=0.2)
        assert collapse_task in done
        response = collapse_task.result()
        assert response.status == V1EvalStatus.EVALUATED
        assert response.result == {"score": 1}
        assert cleanup_started.is_set()
        assert await _event_set(cleanup_finished)
    finally:
        release_cleanup.set()
        finished = asyncio.create_task(cleanup_finished.wait())
        await asyncio.wait({finished}, timeout=1)


def test_sandbox_mode_without_hook_fails_at_class_definition() -> None:
    """A SANDBOX service without prepare_grading_sandbox is broken on every
    grading path; the subclass hook rejects it before it can be deployed."""
    with pytest.raises(TypeError, match="prepare_grading_sandbox"):

        class _MissingHook(StubBenchmark):  # pyright: ignore[reportUnusedClass]
            eval_mode = EvalMode.SANDBOX


def test_sandbox_mode_without_accepted_schemas_fails_at_class_definition() -> None:
    with pytest.raises(TypeError, match="accepted_submission_schemas"):

        class _MissingSchemas(StubBenchmark):  # pyright: ignore[reportUnusedClass]
            eval_mode = EvalMode.SANDBOX

            async def prepare_grading_sandbox(
                self,
                sandbox: Sandbox,
                submission: GradingSubmission,
                dataset: str | None = None,
            ) -> None:
                pass


def test_in_process_artifact_mode_requires_its_bytes_hook() -> None:
    with pytest.raises(TypeError, match="evaluate_artifact"):

        class _MissingArtifactHook(StubBenchmark):  # pyright: ignore[reportUnusedClass]
            eval_mode = EvalMode.IN_PROCESS_ARTIFACT
            accepted_submission_schemas = {
                V1PayloadType.ARTIFACT: frozenset({"stub.workbook.v1"}),
            }


def test_in_process_artifact_mode_rejects_non_artifact_schemas() -> None:
    with pytest.raises(TypeError, match="only non-empty artifact"):

        class _TextSchema(StubBenchmark):  # pyright: ignore[reportUnusedClass]
            eval_mode = EvalMode.IN_PROCESS_ARTIFACT
            accepted_submission_schemas = {
                V1PayloadType.TEXT: frozenset({"stub.text.v1"}),
            }

            async def evaluate_artifact(
                self,
                run_id: str,
                task_id: str,
                schema_id: str,
                artifact: bytes,
                dataset: str | None = None,
            ) -> AsyncGenerator[StreamChunk, None]:
                yield StreamResultChunk(type="result", data={})


class FailingDeleteProvider(FakeProvider):
    async def delete_sandbox(self, instance_id: str) -> None:
        raise RuntimeError("delete failed")


async def test_grade_instance_preserves_successful_result_when_delete_fails() -> None:
    service = await SandboxStub.create()
    provider = FailingDeleteProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.EVALUATED
    assert resp.result == {"resolved": True, "weighted_pass_percentage": 100.0}


async def test_grade_instance_reports_grading_error_when_delete_also_fails() -> None:
    service = await RaisingStub.create()
    provider = FailingDeleteProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert any("boom" in e for e in resp.errors)


class _FakeDaytonaConfig:
    """Stands in for DaytonaProviderConfig in app boot: hands out FakeProvider."""

    provider: FakeProvider

    @classmethod
    def from_env(cls) -> "FakeProviderConfig":
        return FakeProviderConfig(cls.provider)


def test_grading_provider_config_reads_modal_credentials_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "modal")
    monkeypatch.setenv("MODAL_TOKEN_ID", "modal-id")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "modal-secret")

    config = _grading_provider_config()

    assert config == ModalProviderConfig(
        MODAL_TOKEN_ID="modal-id",
        MODAL_TOKEN_SECRET="modal-secret",
    )


def test_grading_provider_config_does_not_render_present_modal_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "modal")
    monkeypatch.delenv("MODAL_TOKEN_ID", raising=False)
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "must-not-appear")

    with pytest.raises(MissingSandboxConfigError) as exc_info:
        _grading_provider_config()

    assert "MODAL_TOKEN_ID" in str(exc_info.value)
    assert "must-not-appear" not in str(exc_info.value)


def _sandbox_app(monkeypatch: pytest.MonkeyPatch, service_cls: type[StubBenchmark] = SandboxStub) -> BenchmarkServiceApp:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    _FakeDaytonaConfig.provider = FakeProvider(FakeSandbox())
    monkeypatch.setattr("benchmark_service.app.DaytonaProviderConfig", _FakeDaytonaConfig)
    app = BenchmarkServiceApp(service_cls)
    app._service_version = "stub-service-1.0"  # pyright: ignore[reportPrivateUsage]
    return app


@pytest.fixture
def sandbox_descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    app = _sandbox_app(monkeypatch)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def test_v1_evaluate_dispatches_sandbox_mode_to_the_grading_engine(sandbox_descope_client: TestClient) -> None:
    resp = sandbox_descope_client.post(
        "/v1/evaluate",
        json={
            "run_id": "external-run-1",
            "task_id": "task-1",
            "dataset": "default",
            "payload": {"type": "text", "schema": "stub.text.v1", "data": "2"},
        },
        headers={"x-descope-api-key": "key-acme"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "evaluated"
    # weighted_pass_percentage is produced only by the SANDBOX path's evaluate_instance,
    # so its presence proves dispatch routed to the sandbox engine (not evaluate_response).
    assert body["result"] == {"resolved": True, "weighted_pass_percentage": 100.0}
    assert body["evaluator_version"] == "stub-service-1.0"


@pytest.mark.parametrize(
    ("payload", "detail"),
    [
        ({"type": "text", "schema": "other.text.v1", "data": "2"}, "Use one of: stub.text.v1"),
        (
            {"type": "artifact", "schema": "stub.artifact.v1", "data": "untrusted-key"},
            "does not accept artifact submissions",
        ),
    ],
)
def test_v1_evaluate_rejects_undeclared_submission_before_storage_or_sandbox(
    sandbox_descope_client: TestClient,
    payload: dict[str, str],
    detail: str,
) -> None:
    resp = _post_eval(sandbox_descope_client, payload)

    assert resp.status_code == 400
    assert detail in resp.json()["detail"]
    assert _FakeDaytonaConfig.provider.created == []


def test_sandbox_mode_missing_server_sandbox_env_fails_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SANDBOX deployment without grading-sandbox creds must fail at startup,
    not 503 (or burn a sandbox) on the first lab call."""
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.delenv("DAYTONA_API_URL", raising=False)
    monkeypatch.delenv("DAYTONA_TARGET", raising=False)
    app = BenchmarkServiceApp(SandboxStub)

    with pytest.raises(MissingSandboxConfigError):
        with TestClient(app):
            pass


def test_sandbox_mode_missing_artifact_storage_fails_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SANDBOX deployment needs artifact storage for rehydration; fully-unset
    storage is legal for TEXT services but a boot error here."""
    app = _sandbox_app(monkeypatch, ArtifactSandboxStub)
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)

    with pytest.raises(RuntimeError, match="SUBMISSION_ARTIFACT_BUCKET"):
        with TestClient(app):
            pass


def test_in_process_artifact_mode_missing_storage_fails_at_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _sandbox_app(monkeypatch, InProcessArtifactStub)
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)

    with pytest.raises(RuntimeError, match="SUBMISSION_ARTIFACT_BUCKET"):
        with TestClient(app):
            pass


@pytest.fixture
def artifact_descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    app = _sandbox_app(monkeypatch, ArtifactSandboxStub)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


@pytest.fixture
def in_process_artifact_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    app = _sandbox_app(monkeypatch, InProcessArtifactStub)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            yield client


def _post_eval(client: TestClient, payload: dict[str, Any]) -> Any:
    return client.post(
        "/v1/evaluate",
        json={"run_id": "external-run-1", "task_id": "task-1", "dataset": "default", "payload": payload},
        headers={"x-descope-api-key": "key-acme"},
    )


def test_v1_evaluate_artifact_payload_grades_from_materialized_artifact(
    artifact_descope_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = "submission-artifacts/acme/default/external-run-1/task-1/answer.bin"
    artifact_reference = SubmissionArtifactReference(key=key, size_bytes=1, etag='"etag-1"')
    stat_calls = 0

    async def fake_stat(k: str, *, tenant: str) -> SubmissionArtifactReference:
        nonlocal stat_calls
        stat_calls += 1
        assert (k, tenant) == (key, "acme")
        return artifact_reference

    async def fake_download(reference: SubmissionArtifactReference, *, tenant: str) -> bytes:
        assert reference is artifact_reference
        assert tenant == "acme"
        return b"2"

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", fake_stat)
    monkeypatch.setattr("benchmark_service.submission_artifacts.download", fake_download)

    resp = _post_eval(artifact_descope_client, {"type": "artifact", "schema": "stub.artifact.v1", "data": key})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "evaluated"
    assert body["result"] == {"resolved": True, "weighted_pass_percentage": 100.0}
    assert stat_calls == 1


def test_v1_evaluate_passes_only_admitted_bytes_to_in_process_benchmark(
    in_process_artifact_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = "submission-artifacts/acme/default/external-run-1/task-1/submission.xlsx"
    reference = SubmissionArtifactReference(key=key, size_bytes=14, etag='"etag-1"')

    async def fake_stat(k: str, *, tenant: str) -> SubmissionArtifactReference:
        assert (k, tenant) == (key, "acme")
        return reference

    async def fake_download(admitted: SubmissionArtifactReference, *, tenant: str) -> bytes:
        assert admitted is reference
        assert tenant == "acme"
        return b"workbook-bytes"

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", fake_stat)
    monkeypatch.setattr("benchmark_service.submission_artifacts.download", fake_download)

    response = _post_eval(
        in_process_artifact_client,
        {"type": "artifact", "schema": "stub.workbook.v1", "data": key},
    )

    assert response.status_code == 200
    assert response.json()["result"] == {"resolved": True}
    app = cast(BenchmarkServiceApp, in_process_artifact_client.app)
    service = cast(InProcessArtifactStub, app.service)
    assert service.artifact_call == (
        "external-run-1",
        "task-1",
        "stub.workbook.v1",
        b"workbook-bytes",
        "default",
    )
    assert _FakeDaytonaConfig.provider.created == []


@pytest.mark.parametrize(
    "key",
    [
        "submission-artifacts/other/default/external-run-1/task-1/submission.xlsx",
        "submission-artifacts/acme/other/external-run-1/task-1/submission.xlsx",
        "submission-artifacts/acme/default/other/task-1/submission.xlsx",
        "submission-artifacts/acme/default/external-run-1/other/submission.xlsx",
    ],
)
def test_v1_evaluate_rejects_artifact_outside_authenticated_request_before_storage(
    in_process_artifact_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
) -> None:
    async def unexpected_stat(_key: str, *, tenant: str) -> SubmissionArtifactReference:
        raise AssertionError(f"storage reached for tenant {tenant}")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", unexpected_stat)

    response = _post_eval(
        in_process_artifact_client,
        {"type": "artifact", "schema": "stub.workbook.v1", "data": key},
    )

    assert response.status_code == 404
    app = cast(BenchmarkServiceApp, in_process_artifact_client.app)
    assert cast(InProcessArtifactStub, app.service).artifact_call is None


def test_v1_evaluate_rejects_in_process_schema_before_storage(
    in_process_artifact_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_stat(_key: str, *, tenant: str) -> SubmissionArtifactReference:
        raise AssertionError(f"storage reached for tenant {tenant}")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", unexpected_stat)
    response = _post_eval(
        in_process_artifact_client,
        {
            "type": "artifact",
            "schema": "other.workbook.v1",
            "data": "submission-artifacts/acme/default/external-run-1/task-1/submission.xlsx",
        },
    )

    assert response.status_code == 400
    assert "Use one of: stub.workbook.v1" in response.json()["detail"]


def test_v1_evaluate_rejects_unknown_task_before_artifact_storage(
    in_process_artifact_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_stat(_key: str, *, tenant: str) -> SubmissionArtifactReference:
        raise AssertionError(f"storage reached for tenant {tenant}")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", unexpected_stat)
    response = in_process_artifact_client.post(
        "/v1/evaluate",
        json={
            "run_id": "external-run-1",
            "task_id": "unknown-task",
            "dataset": "default",
            "payload": {
                "type": "artifact",
                "schema": "stub.workbook.v1",
                "data": "submission-artifacts/acme/default/external-run-1/unknown-task/submission.xlsx",
            },
        },
        headers={"x-descope-api-key": "key-acme"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Task not found: unknown-task"


def test_v1_evaluate_rejects_changed_artifact_before_in_process_hook(
    in_process_artifact_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = "submission-artifacts/acme/default/external-run-1/task-1/submission.xlsx"
    reference = SubmissionArtifactReference(key=key, size_bytes=14, etag='"etag-1"')

    async def fake_stat(_key: str, *, tenant: str) -> SubmissionArtifactReference:
        assert tenant == "acme"
        return reference

    async def changed_download(
        _reference: SubmissionArtifactReference,
        *,
        tenant: str,
    ) -> bytes:
        assert tenant == "acme"
        raise SubmissionArtifactChanged("artifact changed after it was accepted")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", fake_stat)
    monkeypatch.setattr("benchmark_service.submission_artifacts.download", changed_download)

    response = _post_eval(
        in_process_artifact_client,
        {"type": "artifact", "schema": "stub.workbook.v1", "data": key},
    )

    assert response.status_code == 409
    app = cast(BenchmarkServiceApp, in_process_artifact_client.app)
    assert cast(InProcessArtifactStub, app.service).artifact_call is None


async def test_v1_evaluate_bounds_artifact_preflight_with_grading_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def key_for(run_id: str) -> str:
        return f"submission-artifacts/acme/default/{run_id}/task-1/answer.bin"

    artifact_reference = SubmissionArtifactReference(key=key_for("run-1"), size_bytes=1, etag='"etag-1"')
    stat_entered = asyncio.Event()
    release_stat = asyncio.Event()
    stat_calls = 0

    async def blocking_stat(k: str, *, tenant: str) -> SubmissionArtifactReference:
        nonlocal stat_calls
        assert (k, tenant) == (key_for("run-1"), "acme")
        stat_calls += 1
        if stat_calls == 1:
            stat_entered.set()
            await release_stat.wait()
        return artifact_reference

    async def fake_download(reference: SubmissionArtifactReference, *, tenant: str) -> bytes:
        assert reference is artifact_reference
        assert tenant == "acme"
        return b"2"

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", blocking_stat)
    monkeypatch.setattr("benchmark_service.submission_artifacts.download", fake_download)
    app = _sandbox_app(monkeypatch, ArtifactSandboxStub)
    app.service = await ArtifactSandboxStub.create()
    app._grading_provider = FakeProvider(FakeSandbox())
    app._grading_admission = _GradingAdmission(
        max_concurrency=1,
        max_queued=0,
        max_admitted_per_tenant=1,
        queue_timeout_s=0.01,
    )

    async def evaluate(run_id: str) -> Any:
        request = Request({"type": "http"})
        request.state.tenant = "acme"
        body = V1EvalRequest(
            run_id=run_id,
            task_id="task-1",
            dataset="default",
            payload=V1Payload(
                type=V1PayloadType.ARTIFACT,
                schema="stub.artifact.v1",
                data=key_for(run_id),
            ),
        )
        return await app._v1_evaluate(request, body)

    first_evaluation = asyncio.create_task(evaluate("run-1"))
    try:
        assert await _event_set(stat_entered)
        with pytest.raises(HTTPException) as exc_info:
            await evaluate("run-2")
        assert exc_info.value.status_code == 429
        assert stat_calls == 1
    finally:
        release_stat.set()
        first_response = await first_evaluation
    assert first_response.status == V1EvalStatus.EVALUATED
    assert first_response.result == {"resolved": True, "weighted_pass_percentage": 100.0}


@pytest.mark.parametrize("operation", ["stat", "download"])
async def test_canceled_artifact_storage_worker_keeps_grading_admission(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    key = "submission-artifacts/acme/default/external-run-1/task-1/answer.bin"
    reference = SubmissionArtifactReference(key=key, size_bytes=1, etag='"etag-1"')
    worker_started = Event()
    release_worker = Event()

    def wait_for_release() -> None:
        worker_started.set()
        if not release_worker.wait(timeout=1):
            raise RuntimeError("test did not release artifact storage worker")

    def blocking_stat(k: str, tenant: str) -> SubmissionArtifactReference:
        assert (k, tenant) == (key, "acme")
        wait_for_release()
        return reference

    def blocking_download(
        artifact_reference: SubmissionArtifactReference,
        tenant: str,
    ) -> bytes:
        assert artifact_reference is reference
        assert tenant == "acme"
        wait_for_release()
        raise RuntimeError("late artifact storage failure")

    if operation == "stat":
        monkeypatch.setattr(grading.submission_artifacts, "_stat_sync", blocking_stat)
    else:
        monkeypatch.setattr(grading.submission_artifacts, "_download_sync", blocking_download)

    async def run_storage_operation() -> None:
        if operation == "stat":
            await grading.submission_artifacts.stat(key, tenant="acme")
        else:
            await grading.submission_artifacts.download(reference, tenant="acme")

    admission = _GradingAdmission(
        max_concurrency=1,
        max_queued=0,
        max_admitted_per_tenant=1,
        queue_timeout_s=0.01,
    )

    async def admitted_storage_operation() -> None:
        async with admission.acquire(("acme", "run-1", "task-1")):
            await run_storage_operation()

    storage_task = asyncio.create_task(admitted_storage_operation())
    try:
        assert await _thread_event_set(worker_started)
        storage_task.cancel()
        await asyncio.sleep(0)
        storage_task.cancel()
        await asyncio.sleep(0)

        assert not storage_task.done()
        with pytest.raises(_GradingCapacityExceeded):
            async with admission.acquire(("other", "run-2", "task-2")):
                pass

        release_worker.set()
        with pytest.raises(asyncio.CancelledError):
            await storage_task
        async with admission.acquire(("acme", "run-1", "task-1")):
            pass
    finally:
        release_worker.set()
        if not storage_task.done():
            storage_task.cancel()
        await asyncio.gather(storage_task, return_exceptions=True)


def test_v1_evaluate_artifact_missing_returns_404_before_any_sandbox(
    artifact_descope_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def missing_stat(k: str, *, tenant: str) -> SubmissionArtifactReference:
        raise SubmissionArtifactNotFound(f"no artifact was uploaded for key {k}")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", missing_stat)

    resp = _post_eval(
        artifact_descope_client,
        {
            "type": "artifact",
            "schema": "stub.artifact.v1",
            "data": "submission-artifacts/acme/default/external-run-1/task-1/a.bin",
        },
    )

    assert resp.status_code == 404
    assert "no artifact was uploaded" in resp.json()["detail"]
    assert _FakeDaytonaConfig.provider.created == []


def test_v1_evaluate_artifact_payload_on_text_benchmark_returns_400(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _sandbox_app(monkeypatch, StubBenchmark)

    async def _stub_exchange(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"acme": {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", _stub_exchange):
        with TestClient(app) as client:
            resp = _post_eval(client, {"type": "artifact", "schema": "stub.artifact.v1", "data": "some-key"})

    assert resp.status_code == 400
    assert "does not accept artifact" in resp.json()["detail"]


async def test_grading_admission_rejects_duplicate_and_excess_work_while_held() -> None:
    admission = _GradingAdmission(
        max_concurrency=1,
        max_queued=0,
        max_admitted_per_tenant=1,
        queue_timeout_s=0.01,
    )
    key = ("acme", "run-1", "task-1")

    async with admission.acquire(key):
        with pytest.raises(_DuplicateGradingRequest):
            async with admission.acquire(key):
                pass
        with pytest.raises(_GradingCapacityExceeded):
            async with admission.acquire(("other", "run-2", "task-2")):
                pass


async def test_grading_admission_bounds_each_tenant_and_queue_wait() -> None:
    admission = _GradingAdmission(
        max_concurrency=1,
        max_queued=1,
        max_admitted_per_tenant=1,
        queue_timeout_s=0.01,
    )
    queued_key = ("other", "run-2", "task-2")

    async with admission.acquire(("acme", "run-1", "task-1")):
        with pytest.raises(_GradingCapacityExceeded):
            async with admission.acquire(("acme", "run-2", "task-2")):
                pass
        with pytest.raises(_GradingCapacityExceeded):
            async with admission.acquire(queued_key):
                pass

    async with admission.acquire(queued_key):
        pass


def test_ws_evaluate_response_stays_sandboxless_for_sandbox_benchmarks(monkeypatch: pytest.MonkeyPatch) -> None:
    """/ws/evaluate-response's real traffic is Valkyrie eval-resume; sandbox
    dispatch lives on /v1/evaluate only, so the ws path must keep the
    service-owned in-process evaluation and never provision sandboxes."""
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    monkeypatch.delenv("BENCHMARK_API_KEY", raising=False)
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    _FakeDaytonaConfig.provider = FakeProvider(FakeSandbox())
    monkeypatch.setattr("benchmark_service.app.DaytonaProviderConfig", _FakeDaytonaConfig)

    with TestClient(BenchmarkServiceApp(SandboxStub)) as client:
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2", "dataset": "default"})
            msg = ws.receive_json()

    assert msg == {"type": "result", "data": {"resolved": True}}
    assert _FakeDaytonaConfig.provider.created == []


def test_grading_max_concurrency_of_zero_fails_at_boot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Semaphore(0) has no permits and none are ever released; every sandbox
    evaluation would hang forever with no error or log."""
    monkeypatch.setenv("GRADING_MAX_CONCURRENCY", "0")
    with pytest.raises(ValueError, match="GRADING_MAX_CONCURRENCY"):
        BenchmarkServiceApp(StubBenchmark)
