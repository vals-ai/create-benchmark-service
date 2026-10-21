"""The framework-owned eval-resume path: collect, save, checkpoint, grade; and load, grade on retry."""

import asyncio
import json
from contextlib import aclosing
from collections.abc import AsyncGenerator, Iterator, Mapping
from typing import Any, cast

import pytest

from benchmark_service import base
from benchmark_service.base import BenchmarkService
from benchmark_service.context import sandbox_provider_scope, tenant_scope
from benchmark_service.eval_resume import EvalResumeIdentityError, EvalResumeStore, GradingInput, NotGradeable
from benchmark_service.sandbox import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxProvider,
    VolumeMount,
)
from benchmark_service.schemas import (
    EvalSandboxSpec,
    EvaluateResponseRequest,
    RetrieveTaskResponse,
    StreamChunk,
    StreamErrorChunk,
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

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        pass


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
    ) -> AsyncGenerator[StreamChunk | GradingInput, None]:
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


async def test_the_default_evaluate_instance_needs_all_three_hooks() -> None:
    class _Partial(StubBenchmark):
        evaluate_instance = BenchmarkService.evaluate_instance

        def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
            return {"rubric": "1"}

    with pytest.raises(TypeError, match="_Partial implements grader_identity; .*collect_grading_input"):
        await _Partial.create()


async def test_a_partial_hook_set_is_refused_beside_a_custom_evaluate_instance() -> None:
    class _OwnGrader(StubBenchmark):
        def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
            return {"rubric": "1"}

    with pytest.raises(TypeError, match="_OwnGrader implements grader_identity; .*collect_grading_input"):
        await _OwnGrader.create()


async def test_a_service_with_its_own_evaluate_instance_and_no_hooks_is_created() -> None:
    service = await StubBenchmark.create()

    chunks = await _drain(service.stream_evaluate_response(EvaluateResponseRequest(task_id="task-1", response="2")))
    assert chunks[0].data == {"resolved": True}


async def test_an_intermediate_base_may_supply_one_hook_for_its_subclass(s3: _FakeS3) -> None:
    class _Base(StubBenchmark):
        evaluate_instance = BenchmarkService.evaluate_instance

        def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
            return {"rubric": "1"}

    class _Concrete(_Base):
        collect_grading_input = HookBenchmark.collect_grading_input
        grade_input = HookBenchmark.grade_input

    service = await _Concrete.create()
    service.graded = []  # pyright: ignore[reportAttributeAccessIssue]
    service.eval_resume_store = EvalResumeStore(s3_client=s3)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "result"]


class _CleanupBenchmark(HookBenchmark):
    """Releases a grading resource after its result, the way a grader closes a session."""

    cleaned: list[str]

    async def grade_input(
        self, task_id: str, content: bytes, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        try:
            yield StreamResultChunk(type="result", data={"resolved": True})
            yield StreamResultChunk(type="result", data={"unreachable": True})
        finally:
            await asyncio.sleep(0)
            self.cleaned.append(task_id)


async def _cleanup_service(s3: _FakeS3) -> _CleanupBenchmark:
    service = await _CleanupBenchmark.create()
    service.graded = []
    service.cleaned = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    return service


async def _close_after_result(stream: AsyncGenerator[StreamChunk, None]) -> None:
    async for chunk in stream:
        if chunk.type == "result":
            break
    await stream.aclose()


async def test_closing_evaluate_instance_after_the_result_finishes_the_graders_cleanup(s3: _FakeS3) -> None:
    service = await _cleanup_service(s3)

    await _close_after_result(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert service.cleaned == ["task-1"]


async def test_closing_the_retry_stream_after_the_result_finishes_the_graders_cleanup(s3: _FakeS3) -> None:
    first = await _service(s3)
    checkpoint = (await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data
    retry = await _cleanup_service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], checkpoint))

    await _close_after_result(retry.stream_evaluate_response(request))

    assert retry.cleaned == ["task-1"]


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


class _ProgressBenchmark(HookBenchmark):
    """Saves its progress after each judge call, the way a judge-graded benchmark resumes mid-rubric."""

    saved_before_resume: list[int]
    store_objects: dict[tuple[str, str], bytes]

    async def grade_input(
        self, task_id: str, content: bytes, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk | GradingInput, None]:
        self.graded.append(content)
        progress = json.loads(content)
        for call in range(progress.get("calls", 0), 2):
            progress["calls"] = call + 1
            yield GradingInput(json.dumps(progress).encode())
            self.saved_before_resume.append(len(self.store_objects))
        yield StreamResultChunk(type="result", data={"calls": progress["calls"]})


async def _progress_service(s3: _FakeS3) -> _ProgressBenchmark:
    service = await _ProgressBenchmark.create()
    service.graded = []
    service.saved_before_resume = []
    service.store_objects = s3.objects
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    return service


async def test_grading_input_is_saved_and_streamed_before_grading_resumes(s3: _FakeS3) -> None:
    service = await _progress_service(s3)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "eval_resume_state", "eval_resume_state", "result"]
    assert service.saved_before_resume == [2, 3]
    keys = [cast(StreamEvalResumeStateChunk, chunk).data["s3_key"] for chunk in chunks[:3]]
    assert all(key.startswith("stub/eval-resume/tenant-a/run-1/task-1/") for key in keys)


async def test_a_retry_resumes_from_the_latest_progress_and_saves_under_the_same_run(s3: _FakeS3) -> None:
    first = await _progress_service(s3)
    chunks = await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    after_one_call = chunks[1].data

    retry = await _progress_service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], after_one_call))
    resumed = await _drain(retry.stream_evaluate_response(request))

    assert json.loads(retry.graded[0]) == {"answer": "2", "calls": 1}
    assert [chunk.type for chunk in resumed] == ["eval_resume_state", "result"]
    assert cast(StreamEvalResumeStateChunk, resumed[0]).data["benchmark_id"] == "run-1"
    assert resumed[-1].data == {"calls": 2}


class _SavedHookBenchmark(HookBenchmark):
    released: list[str]

    async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
        self.released.append(sandbox.id)


async def _saved_service(s3: _FakeS3, cls: type[_SavedHookBenchmark] = _SavedHookBenchmark) -> _SavedHookBenchmark:
    service = await cls.create()
    service.graded, service.released = [], []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    return service


async def test_grading_input_saved_runs_once_after_the_first_save_and_before_grading(s3: _FakeS3) -> None:
    events: list[str] = []

    class _Ordered(HookBenchmark):
        async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
            events.append(f"released {sandbox.id}")

        async def grade_input(
            self, task_id: str, content: bytes, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk | GradingInput, None]:
            events.append("grading")
            yield GradingInput(b"{}")
            yield StreamResultChunk(type="result", data={})

    service = await _Ordered.create()
    service.eval_resume_store = EvalResumeStore(s3_client=s3)

    async for chunk in service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())):
        events.append(chunk.type)

    assert events == ["eval_resume_state", "released sandbox-1", "grading", "eval_resume_state", "result"]


async def test_grading_input_saved_does_not_run_when_the_save_fails(
    s3: _FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = await _saved_service(s3)
    monkeypatch.delenv("EVAL_RESUME_BUCKET")

    with pytest.raises(RuntimeError, match="EVAL_RESUME_BUCKET"):
        await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    assert service.released == []


async def test_load_grading_input_returns_the_checked_bytes(s3: _FakeS3) -> None:
    first = await _service(s3)
    checkpoint = (await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], checkpoint))

    retry = await _service(s3)
    assert await retry.load_grading_input(request) == RESULT

    retry.grader_version = "rubric-2"
    with pytest.raises(EvalResumeIdentityError):
        await retry.load_grading_input(request)


async def test_an_async_grader_identity_is_awaited(s3: _FakeS3) -> None:
    class _AsyncIdentity(StubBenchmark):
        evaluate_instance = BenchmarkService.evaluate_instance
        collect_grading_input = HookBenchmark.collect_grading_input
        grade_input = HookBenchmark.grade_input
        graded: list[bytes]

        async def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
            await asyncio.sleep(0)
            return {"verifier_image": "sha256:abc"}

    service = await _AsyncIdentity.create()
    service.graded = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    checkpoint = (await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))[0].data

    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], checkpoint))
    assert [chunk.type for chunk in await _drain(service.stream_evaluate_response(request))] == ["result"]


class _GradingSandbox:
    def __init__(self, sandbox_id: str) -> None:
        self.id = sandbox_id
        self.labels: dict[str, str] = {}
        self.files: dict[str, bytes] = {}

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.files[remote_path] = content


class _Provider:
    def __init__(self) -> None:
        self.created: list[SandboxCreateRequest] = []
        self.deleted: list[str] = []
        self.borrowed = _GradingSandbox("caller-owned")

    async def create_sandbox(self, request: SandboxCreateRequest) -> _GradingSandbox:
        self.created.append(request)
        return _GradingSandbox(f"fresh-{len(self.created)}")

    async def get_sandbox(self, instance_id: str) -> _GradingSandbox:
        assert instance_id == self.borrowed.id
        return self.borrowed

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleted.append(instance_id)


class _SandboxGrader(StubBenchmark):
    """Restores the saved bytes into whichever sandbox it is handed, then grades there."""

    evaluate_instance = BenchmarkService.evaluate_instance
    graded_in: list[str]
    fail = False

    async def collect_grading_input(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> bytes:
        return await sandbox.download_file("/result.json")

    def grader_identity(self, task_id: str, dataset: str | None = None) -> Mapping[str, Any]:
        return {"grader_image": "sha256:def"}

    async def grade_in_sandbox(
        self, task_id: str, content: bytes, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk | GradingInput, None]:
        await sandbox.upload_file("/submission", content)
        self.graded_in.append(sandbox.id)
        if self.fail:
            raise RuntimeError("grader crashed")
        yield StreamResultChunk(type="result", data={"resolved": True})


async def _sandbox_service(s3: _FakeS3, cls: type[_SandboxGrader] = _SandboxGrader) -> _SandboxGrader:
    service = await cls.create()
    service.graded_in = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)
    return service


async def _sandbox_checkpoint(s3: _FakeS3) -> dict[str, Any]:
    first = await _sandbox_service(s3)
    chunks = await _drain(first.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    assert first.graded_in == ["sandbox-1"]
    return cast(dict[str, Any], chunks[0].data)


async def test_grade_in_sandbox_retries_in_a_fresh_sandbox_and_deletes_it(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        chunks = await _drain(retry.stream_evaluate_response(request))

    assert chunks[-1].data == {"resolved": True}
    assert retry.graded_in == ["fresh-1"]
    created = provider.created[0]
    assert (created.source, created.resources, created.auto_stop_interval, created.create_timeout) == (
        ImageSource(image="python:3.12-slim"),
        Resources(vcpu=2, memory=4, disk=10),
        30,
        600,
    )
    assert (created.labels["Benchmark"], created.labels["Id"], created.labels["Task"]) == (
        "_SandboxGrader",
        "run-1",
        "task-1",
    )
    assert provider.deleted == ["fresh-1"]


async def test_the_fresh_sandbox_mounts_the_tasks_volumes(s3: _FakeS3) -> None:
    fixture = VolumeMount(name="fixtures", mount_path="/fixtures", read_only=True)

    class _WithVolume(_SandboxGrader):
        async def retrieve_task(
            self, task_id: str, skip_validation: bool = False, dataset: str | None = None
        ) -> RetrieveTaskResponse:
            task = await super().retrieve_task(task_id, skip_validation, dataset)
            return task.model_copy(update={"volumes": [fixture]})

    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _WithVolume), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert provider.created[0].volumes == [fixture]


async def test_the_fresh_sandbox_is_deleted_when_grading_fails(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _Provider()
    retry.fail = True
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)), pytest.raises(RuntimeError, match="grader crashed"):
        await _drain(retry.stream_evaluate_response(request))
    assert provider.deleted == ["fresh-1"]


async def test_a_caller_owned_sandbox_is_borrowed_not_deleted(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint, instance_id="caller-owned")

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert retry.graded_in == ["caller-owned"]
    assert (provider.created, provider.deleted) == ([], [])


async def test_grade_in_sandbox_retry_needs_a_sandbox_provider(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry = await _sandbox_service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with pytest.raises(ValueError, match="sandbox_provider"):
        await _drain(retry.stream_evaluate_response(request))
    assert retry.graded_in == []


async def test_a_service_may_not_implement_both_graders() -> None:
    class _Both(_SandboxGrader):
        grade_input = HookBenchmark.grade_input

    with pytest.raises(TypeError, match="either grade_input or grade_in_sandbox"):
        await _Both.create()


async def test_an_adopter_may_define_its_own_private_grade_helper(s3: _FakeS3) -> None:
    class _OwnHelper(HookBenchmark):
        async def _grade(self, conductor_result: dict[str, Any], test: object, condition: str) -> None:
            raise AssertionError("the framework must not call an adopter's own _grade")

    service = await _OwnHelper.create()
    service.graded = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=cast(dict[str, Any], chunks[0].data))
    assert [chunk.type for chunk in await _drain(service.stream_evaluate_response(request))] == ["result"]


async def test_each_checkpoint_reaches_the_caller_before_the_grader_resumes(s3: _FakeS3) -> None:
    events: list[str] = []

    class _Ordered(HookBenchmark):
        async def grade_input(
            self, task_id: str, content: bytes, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk | GradingInput, None]:
            for call in range(2):
                yield GradingInput(json.dumps({"calls": call + 1}).encode())
                events.append("resumed")
            yield StreamResultChunk(type="result", data={})

    service = await _Ordered.create()
    service.eval_resume_store = EvalResumeStore(s3_client=s3)

    async for chunk in service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())):
        events.append(chunk.type)

    assert events == ["eval_resume_state", "eval_resume_state", "resumed", "eval_resume_state", "resumed", "result"]


async def test_a_task_that_declares_a_caller_owned_resume_sandbox_needs_its_instance_id(s3: _FakeS3) -> None:
    class _CallerOwned(_SandboxGrader):
        async def retrieve_task(
            self, task_id: str, skip_validation: bool = False, dataset: str | None = None
        ) -> RetrieveTaskResponse:
            task = await super().retrieve_task(task_id, skip_validation, dataset)
            return task.model_copy(update={"resume_sandbox": EvalSandboxSpec()})

    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _CallerOwned), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)), pytest.raises(ValueError, match="instance_id"):
        await _drain(retry.stream_evaluate_response(request))
    assert (provider.created, retry.graded_in) == ([], [])


@pytest.mark.parametrize("cancels", [1, 2])
async def test_cancelling_a_retry_while_its_sandbox_is_created_still_deletes_it(s3: _FakeS3, cancels: int) -> None:
    class _SlowProvider(_Provider):
        def __init__(self) -> None:
            super().__init__()
            self.creating = asyncio.Event()
            self.release = asyncio.Event()

        async def create_sandbox(self, request: SandboxCreateRequest) -> _GradingSandbox:
            self.creating.set()
            await self.release.wait()
            return await super().create_sandbox(request)

    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _SlowProvider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        task = asyncio.create_task(_drain(retry.stream_evaluate_response(request)))
        await provider.creating.wait()
        for _ in range(cancels):
            task.cancel()
            await asyncio.sleep(0)
        provider.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert (retry.graded_in, provider.deleted) == ([], ["fresh-1"])


class _BlockingDeleteProvider(_Provider):
    def __init__(self, fail: bool = False) -> None:
        super().__init__()
        self.fail = fail
        self.deleting = asyncio.Event()
        self.release = asyncio.Event()

    async def delete_sandbox(self, instance_id: str) -> None:
        self.deleting.set()
        await self.release.wait()
        if self.fail:
            raise RuntimeError("delete failed")
        await super().delete_sandbox(instance_id)


async def _cancel_while_deleting(s3: _FakeS3, provider: _BlockingDeleteProvider, cancels: int = 1) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry = await _sandbox_service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        task = asyncio.create_task(_drain(retry.stream_evaluate_response(request)))
        await provider.deleting.wait()
        for _ in range(cancels):
            task.cancel()
            await asyncio.sleep(0)
        provider.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("cancels", [1, 2])
async def test_cancelling_a_retry_while_its_sandbox_is_deleted_lets_the_delete_finish(
    s3: _FakeS3, cancels: int
) -> None:
    provider = _BlockingDeleteProvider()
    await _cancel_while_deleting(s3, provider, cancels)
    assert provider.deleted == ["fresh-1"]


@pytest.mark.parametrize("cancels", [1, 2])
async def test_a_failed_delete_during_cancellation_is_logged_and_the_cancellation_kept(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture, cancels: int
) -> None:
    await _cancel_while_deleting(s3, _BlockingDeleteProvider(fail=True), cancels)
    assert "Failed to delete eval-resume sandbox fresh-1" in caplog.text


async def test_a_failed_delete_is_logged_and_the_grade_still_streams(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture
) -> None:
    class _FailingDelete(_Provider):
        async def delete_sandbox(self, instance_id: str) -> None:
            raise RuntimeError("delete failed")

    checkpoint = await _sandbox_checkpoint(s3)
    retry = await _sandbox_service(s3)
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, _FailingDelete())):
        chunks = await _drain(retry.stream_evaluate_response(request))

    assert chunks[-1].data == {"resolved": True}
    assert "Failed to delete eval-resume sandbox fresh-1" in caplog.text


async def test_an_ungradeable_input_streams_its_error_and_saves_nothing(s3: _FakeS3) -> None:
    class _Ungradeable(HookBenchmark):
        async def collect_grading_input(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> bytes:
            raise NotGradeable("generation did not finish")

    service = await _Ungradeable.create()
    service.graded = []
    service.eval_resume_store = EvalResumeStore(s3_client=s3)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert chunks == [StreamErrorChunk(type="error", data="generation did not finish")]
    assert (s3.objects, service.graded) == ({}, [])


async def test_grading_input_saved_runs_when_the_caller_hangs_up_at_the_checkpoint(s3: _FakeS3) -> None:
    service = await _saved_service(s3)

    async with aclosing(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))) as chunks:
        assert (await anext(chunks)).type == "eval_resume_state"

    assert (service.released, service.graded) == (["sandbox-1"], [])


class _BlockingRelease(_SavedHookBenchmark):
    releasing: asyncio.Event
    finish: asyncio.Event
    fail = False

    async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
        self.releasing.set()
        await self.finish.wait()
        if self.fail:
            raise RuntimeError("release failed")
        self.released.append(sandbox.id)


async def _cancel_while_releasing(s3: _FakeS3, fail: bool = False, cancels: int = 1) -> _BlockingRelease:
    service = cast(_BlockingRelease, await _saved_service(s3, _BlockingRelease))
    service.releasing, service.finish, service.fail = asyncio.Event(), asyncio.Event(), fail

    task = asyncio.create_task(_drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))
    await service.releasing.wait()
    for _ in range(cancels):
        task.cancel()
        await asyncio.sleep(0)
    service.finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    return service


@pytest.mark.parametrize("cancels", [1, 2])
async def test_grading_input_saved_finishes_when_the_stream_is_cancelled_during_it(s3: _FakeS3, cancels: int) -> None:
    service = await _cancel_while_releasing(s3, cancels=cancels)
    assert (service.released, service.graded) == (["sandbox-1"], [])


async def test_a_failed_grading_input_saved_during_cancellation_is_logged_and_the_cancellation_kept(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture
) -> None:
    await _cancel_while_releasing(s3, fail=True)
    assert "grading_input_saved failed for task-1" in caplog.text


async def test_a_failed_grading_input_saved_is_logged_and_grading_continues(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture
) -> None:
    class _FailingRelease(_SavedHookBenchmark):
        async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
            raise RuntimeError("release failed")

    service = await _saved_service(s3, _FailingRelease)

    chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "result"]
    assert "grading_input_saved failed for task-1" in caplog.text


class _VerifierGrader(_SandboxGrader):
    spec = EvalSandboxSpec(source=ImageSource(image="verifier:sha256-abc"), timeout_s=3600)
    resume_sandbox: EvalSandboxSpec | None = None

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        task = await super().retrieve_task(task_id, skip_validation, dataset)
        return task.model_copy(update={"eval_sandbox": self.spec, "resume_sandbox": self.resume_sandbox})


async def test_a_tasks_eval_sandbox_grades_the_first_attempt_in_its_own_sandbox(s3: _FakeS3) -> None:
    service, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        chunks = await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "result"]
    assert service.graded_in == ["fresh-1"]
    created = provider.created[0]
    assert (created.source, created.resources, created.network_block_all, created.auto_stop_interval) == (
        ImageSource(image="verifier:sha256-abc"),
        Resources(vcpu=2, memory=4, disk=10),
        True,
        70,
    )
    assert provider.deleted == ["fresh-1"]


async def test_a_tasks_eval_sandbox_grades_the_retry_in_the_same_kind_of_sandbox(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert retry.graded_in == ["fresh-1"]
    assert (provider.created[0].source, provider.created[0].network_block_all) == (
        ImageSource(image="verifier:sha256-abc"),
        True,
    )
    assert provider.deleted == ["fresh-1"]


async def test_a_tasks_eval_sandbox_needs_a_sandbox_provider_on_the_first_attempt(s3: _FakeS3) -> None:
    service = await _sandbox_service(s3, _VerifierGrader)

    with pytest.raises(ValueError, match="sandbox_provider"):
        await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))
    assert service.graded_in == []


async def test_a_hung_grading_input_saved_is_abandoned_after_the_teardown_bound(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _HungRelease(_SavedHookBenchmark):
        async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(base, "TEARDOWN_TIMEOUT_S", 0.01)
    service = await _saved_service(s3, _HungRelease)

    grading = asyncio.create_task(_drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))
    done, _ = await asyncio.wait({grading}, timeout=5)
    assert done == {grading}
    chunks = grading.result()

    assert [chunk.type for chunk in chunks] == ["eval_resume_state", "result"]
    assert "grading_input_saved failed for task-1" in caplog.text


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (
            EvalSandboxSpec(timeout_s=3601),
            (ImageSource(image="python:3.12-slim"), Resources(vcpu=2, memory=4, disk=10), True, 71),
        ),
        (
            EvalSandboxSpec(resources=Resources(vcpu=8, memory=16, disk=20), network_block_all=False),
            (ImageSource(image="python:3.12-slim"), Resources(vcpu=8, memory=16, disk=20), False, 40),
        ),
    ],
)
async def test_an_eval_sandbox_falls_back_to_the_task_and_follows_its_own_fields(
    s3: _FakeS3, spec: EvalSandboxSpec, expected: tuple[object, ...]
) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()
    cast(_VerifierGrader, retry).spec = spec
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    created = provider.created[0]
    assert (created.source, created.resources, created.network_block_all, created.auto_stop_interval) == expected


async def test_a_retry_without_eval_sandbox_sets_no_network_policy(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert provider.created[0].network_block_all is False


async def test_a_retry_naming_a_caller_owned_sandbox_borrows_it_even_with_eval_sandbox(s3: _FakeS3) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint, instance_id="caller-owned")

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert (retry.graded_in, provider.created, provider.deleted) == (["caller-owned"], [], [])


async def test_a_first_attempt_with_eval_sandbox_ignores_the_caller_owned_resume_sandbox(s3: _FakeS3) -> None:
    service, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()
    cast(_VerifierGrader, service).resume_sandbox = EvalSandboxSpec()

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox())))

    assert service.graded_in == ["fresh-1"]


async def test_a_v1_grading_sandbox_is_graded_in_rather_than_replaced(s3: _FakeS3) -> None:
    class _V1GradingSandbox(_Sandbox):
        labels = {"Id": "run-1", "vals-eval": "grade-instance"}

    service, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(service.evaluate_instance("task-1", cast(Sandbox, _V1GradingSandbox())))

    assert (service.graded_in, provider.created) == (["sandbox-1"], [])


async def test_a_retry_with_eval_sandbox_grades_fresh_even_when_the_task_declares_resume_sandbox(
    s3: _FakeS3,
) -> None:
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3, _VerifierGrader), _Provider()
    cast(_VerifierGrader, retry).resume_sandbox = EvalSandboxSpec()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        await _drain(retry.stream_evaluate_response(request))

    assert (retry.graded_in, provider.deleted) == (["fresh-1"], ["fresh-1"])


async def test_a_cancelled_create_slower_than_the_teardown_bound_is_still_deleted(
    s3: _FakeS3, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _SlowCreate(_Provider):
        def __init__(self) -> None:
            super().__init__()
            self.creating = asyncio.Event()

        async def create_sandbox(self, request: SandboxCreateRequest) -> _GradingSandbox:
            self.creating.set()
            await asyncio.sleep(0.05)
            return await super().create_sandbox(request)

    monkeypatch.setattr(base, "TEARDOWN_TIMEOUT_S", 0.01)
    checkpoint = await _sandbox_checkpoint(s3)
    retry, provider = await _sandbox_service(s3), _SlowCreate()
    request = EvaluateResponseRequest(task_id="task-1", eval_resume_state=checkpoint)

    with sandbox_provider_scope(cast(SandboxProvider, provider)):
        task = asyncio.create_task(_drain(retry.stream_evaluate_response(request)))
        await provider.creating.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert provider.deleted == ["fresh-1"]


async def test_a_grading_input_saved_that_holds_its_cancellation_is_still_left_behind_at_the_bound(
    s3: _FakeS3, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = asyncio.Event()

    class _StubbornRelease(_SavedHookBenchmark):
        async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

    monkeypatch.setattr(base, "TEARDOWN_TIMEOUT_S", 0.01)
    service = await _saved_service(s3, _StubbornRelease)

    grading = asyncio.create_task(_drain(service.evaluate_instance("task-1", cast(Sandbox, _Sandbox()))))
    done, _ = await asyncio.wait({grading}, timeout=5)
    release.set()

    assert done == {grading}
    assert [chunk.type for chunk in grading.result()] == ["eval_resume_state", "result"]
    assert "grading_input_saved failed for task-1" in caplog.text
