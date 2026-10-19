"""The framework-owned eval-resume path: collect, save, checkpoint, grade; and load, grade on retry."""

import json
from collections.abc import AsyncGenerator, Iterator, Mapping
from typing import Any, cast

import pytest

from benchmark_service.base import BenchmarkService
from benchmark_service.context import tenant_scope
from benchmark_service.eval_resume import EvalResumeIdentityError, EvalResumeStore
from benchmark_service.sandbox import Sandbox
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    StreamChunk,
    StreamEvalResumeStateChunk,
    StreamResultChunk,
)
from tests.conftest import StubBenchmark
from tests.test_eval_resume import _FakeS3  # pyright: ignore[reportPrivateUsage]

RESULT = b'{"answer": "2"}'


class _Sandbox:
    id = "sandbox-1"
    labels = {"Id": "run-1"}

    async def download_file(self, remote_path: str) -> bytes:
        return RESULT


class HookBenchmark(StubBenchmark):
    """Grades the bytes its sandbox leaves behind, through the three hooks only."""

    evaluate_instance = BenchmarkService.evaluate_instance  # the framework's default
    grader_version = "rubric-1"
    graded: list[bytes]

    async def collect_grading_input(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> bytes:
        return await sandbox.download_file("/result.json")

    def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
        return {"rubric": self.grader_version}

    async def grade_input(
        self, task_id: str, content: bytes, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        self.graded.append(content)
        answer = json.loads(content)["answer"]
        yield StreamResultChunk(
            type="result", data={"resolved": answer == self.get_dataset(dataset)[task_id]["answer"]}
        )


@pytest.fixture(autouse=True)
def caller_tenant() -> Iterator[None]:
    with tenant_scope("tenant-a"):
        yield


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> _FakeS3:
    monkeypatch.setenv("EVAL_RESUME_BUCKET", "agentic-harness-dev")
    monkeypatch.setenv("EVAL_RESUME_PREFIX", "stub/eval-resume")
    return _FakeS3()


async def _service(s3: _FakeS3) -> HookBenchmark:
    service = await HookBenchmark.create()
    service.graded = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    return service


async def _drain(stream: AsyncGenerator[StreamChunk, None]) -> list[StreamChunk]:
    return [chunk async for chunk in stream]


async def test_default_evaluate_instance_saves_before_it_grades(s3: _FakeS3) -> None:
    service = await _service(s3)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "result"]
    assert chunks[-1].data == {"resolved": True}
    assert list(s3.objects.values()) == [RESULT]
    key = cast(StreamEvalResumeStateChunk, chunks[0]).data["s3_key"]
    assert key.startswith("stub/eval-resume/tenant-a/run-1/task-1/")


async def test_a_failed_save_stops_grading(s3: _FakeS3, monkeypatch: pytest.MonkeyPatch) -> None:
    service = await _service(s3)
    monkeypatch.delenv("EVAL_RESUME_BUCKET")

    with pytest.raises(RuntimeError, match="EVAL_RESUME_BUCKET"):
        await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    assert service.graded == []


async def test_a_fresh_service_grades_the_checkpoint_on_retry(s3: _FakeS3) -> None:
    first = await _service(s3)
    checkpoint = (await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data

    retry = await _service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], checkpoint))
    chunks = await _drain(retry.stream_evaluate_response(request))

    assert [chunk.type for chunk in chunks] == ["result"]
    assert chunks[0].data == {"resolved": True}
    assert retry.graded == [RESULT]


async def test_retry_refuses_a_checkpoint_saved_under_another_grader(s3: _FakeS3) -> None:
    first = await _service(s3)
    checkpoint = (await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data

    retry = await _service(s3)
    retry.grader_version = "rubric-2"
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], checkpoint))
    with pytest.raises(EvalResumeIdentityError):
        await _drain(retry.stream_evaluate_response(request))
    assert retry.graded == []


async def test_retry_refuses_a_task_the_dataset_does_not_have(s3: _FakeS3) -> None:
    first = await _service(s3)
    checkpoint = (await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data

    retry = await _service(s3)
    request = EvaluateResponseRequest(task_id="task-404", eval_resume_state=cast(dict[str, Any], checkpoint))
    with pytest.raises(ValueError, match="task-404"):
        await _drain(retry.stream_evaluate_response(request))
    assert retry.graded == []


async def test_a_response_is_still_graded_by_evaluate_response(s3: _FakeS3) -> None:
    service = await _service(s3)

    chunks = await _drain(service.stream_evaluate_response(EvaluateResponseRequest(task_id="task-1", response="2")))

    assert chunks[0].data == {"resolved": True}
    assert service.graded == []


def test_the_grading_hooks_come_as_a_set() -> None:
    with pytest.raises(TypeError, match="collect_grading_input, grader_identity and grade_input"):

        class _Partial(StubBenchmark):  # pyright: ignore[reportUnusedClass]
            def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
                return {"rubric": "1"}


async def test_a_service_without_evaluate_instance_or_hooks_cannot_be_created() -> None:
    class _NoGrading(StubBenchmark):
        evaluate_instance = BenchmarkService.evaluate_instance

    with pytest.raises(TypeError, match="or collect_grading_input"):
        await _NoGrading.create()


def test_the_store_defaults_to_the_registry_env_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EVAL_RESUME_PREFIX", "/snap/eval-resume/")

    store = EvalResumeStore()

    assert (store.bucket_env, store.object_prefix) == ("EVAL_RESUME_BUCKET", "snap/eval-resume")


def test_the_store_requires_a_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EVAL_RESUME_PREFIX", raising=False)

    with pytest.raises(RuntimeError, match="EVAL_RESUME_PREFIX"):
        EvalResumeStore()


def test_the_store_accepts_the_registry_form_of_the_prefix() -> None:
    store = EvalResumeStore(object_prefix="snap/eval-resume/*")

    assert store.object_prefix == "snap/eval-resume"


def test_adopters_can_bind_a_tenant_from_the_package_root() -> None:
    import benchmark_service

    with benchmark_service.tenant_scope("tenant-b"):
        assert benchmark_service.current_tenant() == "tenant-b"
