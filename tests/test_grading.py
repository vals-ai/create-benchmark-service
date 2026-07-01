"""Tests for sandbox-graded /v1/evaluate (eval_mode == SANDBOX)."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest

from benchmark_service import Sandbox
from benchmark_service.sandbox import SandboxCreateRequest, SandboxProvider
from benchmark_service.sandbox.types import ExecResult
from benchmark_service.schemas import (
    EvalMode,
    EvaluateResponseRequest,
    StreamChunk,
    StreamResultChunk,
)
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
