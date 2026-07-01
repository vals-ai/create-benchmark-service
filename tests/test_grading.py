"""Tests for sandbox-graded /v1/evaluate (eval_mode == SANDBOX)."""

import asyncio
import json
from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from benchmark_service import ImageSource, Resources, Sandbox
from benchmark_service import auth as auth_module
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.grading import grade_instance
from benchmark_service.sandbox import SandboxCreateRequest, SandboxProvider
from benchmark_service.sandbox.types import ExecResult
from benchmark_service.schemas import (
    EvalMode,
    EvaluateResponseRequest,
    StreamChunk,
    StreamErrorChunk,
    StreamResultChunk,
)
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


def _req() -> EvaluateResponseRequest:
    return EvaluateResponseRequest(task_id="task-1", response="2", dataset="default")


async def _grade(service: StubBenchmark, provider: FakeProvider, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "service": service,
        "run_id": "run-1",
        "request": _req(),
        "sandbox_config": FakeProviderConfig(provider),
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


@pytest.fixture
def sandbox_descope_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme": {"datasets": ["default"]}}}),
    )
    app = BenchmarkServiceApp(SandboxStub)
    app._service_version = "stub-service-1.0"  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(
        "benchmark_service.app._grading_sandbox_config",
        lambda: FakeProviderConfig(FakeProvider(FakeSandbox())),
    )

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
