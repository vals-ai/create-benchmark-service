"""Tests for sandbox-graded /v1/evaluate (eval_mode == SANDBOX)."""

import asyncio
import json
import re
from collections.abc import AsyncGenerator, Generator
from typing import Any, cast
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import ImageSource, Resources, Sandbox
from benchmark_service import auth as auth_module
from benchmark_service import grading
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.grading import SUBMISSION_ARTIFACT_SANDBOX_PATH, grade_instance
from benchmark_service.sandbox import MissingSandboxConfigError, SandboxCreateRequest, SandboxProvider
from benchmark_service.sandbox.types import ExecResult
from benchmark_service.schemas import (
    EvalMode,
    EvalSandboxSpec,
    EvaluateResponseRequest,
    StreamChunk,
    StreamErrorChunk,
    StreamResultChunk,
)
from benchmark_service.submission_artifacts import SubmissionArtifactNotFound
from benchmark_service.v1_schemas import V1EvalStatus
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
        self, command: str, *, cwd: str | None = None, timeout: float | None = None
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


class FakeProviderConfig:
    """Duck-typed stand-in for SandboxProviderConfig: only create_provider is used."""

    def __init__(self, provider: FakeProvider) -> None:
        self._provider = provider

    def create_provider(self) -> SandboxProvider:
        return self._provider


class SandboxStub(StubBenchmark):
    """A SANDBOX-mode benchmark: inject the answer, then grade by reading it back."""

    def eval_mode(self) -> EvalMode:
        return EvalMode.SANDBOX

    async def prepare_grading_sandbox(
        self, sandbox: Sandbox, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> None:
        await sandbox.upload_file("/workspace/answer.txt", (request.response or "").encode())

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


async def test_prepare_grading_sandbox_injects_artifact_into_sandbox() -> None:
    service = await SandboxStub.create()
    sandbox = FakeSandbox()
    req = EvaluateResponseRequest(task_id="task-1", response="2", dataset="default")
    await service.prepare_grading_sandbox(sandbox, req, dataset="default")
    assert sandbox.files["/workspace/answer.txt"] == b"2"


async def test_prepare_grading_sandbox_default_raises_not_implemented() -> None:
    service = await StubBenchmark.create()
    req = EvaluateResponseRequest(task_id="task-1", response="2", dataset="default")
    with pytest.raises(NotImplementedError, match="prepare_grading_sandbox"):
        await service.prepare_grading_sandbox(FakeSandbox(), req, dataset="default")


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
        await asyncio.sleep(10)
        yield StreamResultChunk(type="result", data={"resolved": True})


class NoResultStub(SandboxStub):
    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        return
        yield  # pragma: no cover


class MissingHookSandboxStub(StubBenchmark):
    def eval_mode(self) -> EvalMode:
        return EvalMode.SANDBOX


def _req() -> EvaluateResponseRequest:
    return EvaluateResponseRequest(task_id="task-1", response="2", dataset="default")


async def _grade(service: StubBenchmark, provider: FakeProvider, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "service": service,
        "run_id": "run-1",
        "tenant": "acme",
        "request": _req(),
        "provider": provider,
        "evaluator_version": "stub-1.0",
        "dataset": "default",
    }
    kwargs.update(overrides)
    return await grade_instance(**kwargs)


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
    assert create.reuse is False
    # run_id is caller-supplied: the sandbox name must be a sanitized slug with
    # a deterministic digest, and the labels must carry the tracker vocabulary
    # plus lab attribution so leaked sandboxes are findable.
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
                    env_vars={"GRADER_MODE": "strict"},
                )
            }
        )


async def test_grade_instance_applies_benchmark_declared_eval_sandbox_spec() -> None:
    service = await SpecStub.create()
    provider = FakeProvider(FakeSandbox())
    await _grade(service, provider)
    create = provider.created[0]
    assert create.source == ImageSource(image="grader:1.0")
    assert create.resources == Resources(vcpu=8, memory=16, disk=50)
    assert create.network_block_all is False
    assert create.env_vars == {"GRADER_MODE": "strict"}


class ArtifactSandboxStub(SandboxStub):
    """Grades from the artifact the framework materialized in the sandbox."""

    async def prepare_grading_sandbox(
        self, sandbox: Sandbox, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> None:
        tarball = await sandbox.download_file(SUBMISSION_ARTIFACT_SANDBOX_PATH)
        await sandbox.upload_file("/workspace/answer.txt", tarball)


async def test_grade_instance_materializes_artifact_before_the_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_download(key: str, *, tenant: str) -> bytes:
        assert key == "submission-artifacts/acme/default/run-1/task-1/answer.bin"
        assert tenant == "acme"
        return b"2"

    monkeypatch.setattr(grading.submission_artifacts, "download", fake_download)
    service = await ArtifactSandboxStub.create()
    sandbox = FakeSandbox()
    provider = FakeProvider(sandbox)
    resp = await _grade(
        service,
        provider,
        artifact_key="submission-artifacts/acme/default/run-1/task-1/answer.bin",
    )
    assert resp.status == V1EvalStatus.EVALUATED
    assert sandbox.files[SUBMISSION_ARTIFACT_SANDBOX_PATH] == b"2"


async def test_grade_instance_maps_missing_artifact_to_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def missing_download(key: str, *, tenant: str) -> bytes:
        raise SubmissionArtifactNotFound(f"no artifact was uploaded for key {key}")

    monkeypatch.setattr(grading.submission_artifacts, "download", missing_download)
    service = await ArtifactSandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider, artifact_key="submission-artifacts/acme/default/run-1/task-1/a.bin")
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


async def test_grade_instance_times_out_and_still_deletes_sandbox() -> None:
    service = await SlowStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider, timeout_s=0.05)
    assert resp.status == V1EvalStatus.ERROR
    assert any("timeout" in e.lower() for e in resp.errors)
    assert provider.deleted == ["fake-sandbox"]


async def test_grade_instance_maps_missing_result_chunk_to_error() -> None:
    service = await NoResultStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert resp.result is None
    assert resp.errors == ["evaluate_instance completed without a result chunk"]
    assert provider.deleted == ["fake-sandbox"]


async def test_grade_instance_fails_fast_when_sandbox_hook_missing() -> None:
    service = await MissingHookSandboxStub.create()
    provider = FakeProvider(FakeSandbox())
    resp = await _grade(service, provider)
    assert resp.status == V1EvalStatus.ERROR
    assert any("prepare_grading_sandbox" in error for error in resp.errors)
    assert provider.created == []
    assert provider.deleted == []


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


def test_v1_evaluate_dispatches_sandbox_mode_to_grade_instance(sandbox_descope_client: TestClient) -> None:
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
    # so its presence proves dispatch routed to grade_instance (not evaluate_response).
    assert body["result"] == {"resolved": True, "weighted_pass_percentage": 100.0}
    assert body["evaluator_version"] == "stub-service-1.0"


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
    app = _sandbox_app(monkeypatch)
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

    async def fake_stat(k: str, *, tenant: str) -> int:
        assert (k, tenant) == (key, "acme")
        return 1

    async def fake_download(k: str, *, tenant: str) -> bytes:
        assert (k, tenant) == (key, "acme")
        return b"2"

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", fake_stat)
    monkeypatch.setattr("benchmark_service.submission_artifacts.download", fake_download)

    resp = _post_eval(artifact_descope_client, {"type": "artifact", "schema": "stub.artifact.v1", "data": key})

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "evaluated"
    assert body["result"] == {"resolved": True, "weighted_pass_percentage": 100.0}


def test_v1_evaluate_artifact_missing_returns_404_before_any_sandbox(
    artifact_descope_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def missing_stat(k: str, *, tenant: str) -> int:
        raise SubmissionArtifactNotFound(f"no artifact was uploaded for key {k}")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", missing_stat)

    resp = _post_eval(
        artifact_descope_client,
        {"type": "artifact", "schema": "stub.artifact.v1", "data": "submission-artifacts/acme/default/r/t/a.bin"},
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
    assert "sandbox-grading" in resp.json()["detail"]


def test_v1_evaluate_duplicate_in_flight_returns_409(sandbox_descope_client: TestClient) -> None:
    app = cast(BenchmarkServiceApp, sandbox_descope_client.app)
    app._grading_in_flight.add(("acme", "external-run-1", "task-1"))  # pyright: ignore[reportPrivateUsage]
    try:
        resp = _post_eval(sandbox_descope_client, {"type": "text", "schema": "stub.text.v1", "data": "2"})
    finally:
        app._grading_in_flight.discard(("acme", "external-run-1", "task-1"))  # pyright: ignore[reportPrivateUsage]

    assert resp.status_code == 409


def test_ws_evaluate_response_dispatches_sandbox_grading(monkeypatch: pytest.MonkeyPatch) -> None:
    """A SANDBOX benchmark grades in a fresh sandbox on the ws path too; only
    resume requests keep the service-owned stream_evaluate_response."""
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

    assert msg == {"type": "result", "data": {"resolved": True, "weighted_pass_percentage": 100.0}}
    assert len(_FakeDaytonaConfig.provider.created) == 1
    assert _FakeDaytonaConfig.provider.deleted == ["fake-sandbox"]
