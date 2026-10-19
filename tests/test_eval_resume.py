import asyncio
import hashlib
import threading
from typing import Any

import pytest

from benchmark_service.eval_resume import (
    EvalResumeCheckpoint,
    EvalResumeError,
    EvalResumeIdentityError,
    EvalResumeIntegrityError,
    EvalResumeStore,
    benchmark_id_from_sandbox,
    judge_identity,
)

BUCKET_ENV = "TEST_EVAL_STATE_BUCKET"
RUN_ID = "2ecbe3f6-e545-475f-81e1-00408dc02c6f"
JUDGE = judge_identity({"model": "anthropic/claude-opus-4-7", "test_revision": "rev-1"})


class _Body:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.closed = False
        self.read_sizes: list[int | None] = []

    def read(self, amt: int | None = None) -> bytes:
        self.read_sizes.append(amt)
        return self._content if amt is None else self._content[:amt]

    def close(self) -> None:
        self.closed = True


class _FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[dict[str, Any]] = []
        self.bodies: list[_Body] = []

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> object:
        self.puts.append({"Bucket": Bucket, "Key": Key, "ContentType": ContentType})
        self.objects[(Bucket, Key)] = Body
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> Any:
        content = self.objects[(Bucket, Key)]
        body = _Body(content)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(content)}


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> _FakeS3:
    monkeypatch.setenv(BUCKET_ENV, "agentic-harness-dev")
    return _FakeS3()


@pytest.fixture
def store(s3: _FakeS3) -> EvalResumeStore:
    return EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume/", s3_client=s3)


async def _save(store: EvalResumeStore, content: bytes = b'{"messages": []}') -> EvalResumeCheckpoint:
    return await store.save(
        benchmark_id=RUN_ID, task_id="both__t1", dataset="test", content=content, judge_identity=JUDGE
    )


async def test_save_writes_bytes_under_the_prefix_and_returns_their_pointer(
    store: EvalResumeStore, s3: _FakeS3
) -> None:
    content = b'{"messages": [], "auditor_aggregated_metadata": {"cost": {"total": 0.2}}}'
    checkpoint = await _save(store, content)

    sha256 = hashlib.sha256(content).hexdigest()
    assert checkpoint.model_dump(mode="json") == {
        "version": 1,
        "benchmark_id": RUN_ID,
        "task_id": "both__t1",
        "dataset": "test",
        "s3_key": f"snap/eval-resume/{RUN_ID}/both__t1/{sha256}",
        "sha256": sha256,
        "size_bytes": len(content),
        "judge_identity": JUDGE,
    }
    assert s3.objects[("agentic-harness-dev", checkpoint.s3_key)] == content
    assert s3.puts[0]["ContentType"] == "application/octet-stream"


async def test_load_returns_saved_bytes_from_the_json_state(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store, b"grading input")

    content = await store.load(
        checkpoint.model_dump(mode="json"), task_id="both__t1", dataset="test", judge_identity=JUDGE
    )

    assert content == b"grading input"
    assert s3.bodies[0].read_sizes == [len(b"grading input") + 1]
    assert s3.bodies[0].closed


async def test_load_closes_the_body_when_the_caller_is_cancelled_mid_get(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store, b"grading input")
    started, release = threading.Event(), threading.Event()
    get_object = s3.get_object

    def slow_get_object(*, Bucket: str, Key: str) -> Any:
        started.set()
        release.wait(5)
        return get_object(Bucket=Bucket, Key=Key)

    s3.get_object = slow_get_object  # type: ignore[method-assign]
    load = asyncio.create_task(store.load(checkpoint, task_id="both__t1", dataset="test", judge_identity=JUDGE))
    await asyncio.to_thread(started.wait, 5)
    load.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await load
    assert s3.bodies[0].closed


async def test_default_client_is_built_off_the_event_loop(monkeypatch: pytest.MonkeyPatch, s3: _FakeS3) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    built_on_loop: list[bool] = []

    def slow_default_client() -> _FakeS3:
        try:
            asyncio.get_running_loop()
            built_on_loop.append(True)
        except RuntimeError:
            built_on_loop.append(False)
        return s3

    monkeypatch.setattr("benchmark_service.eval_resume._default_s3_client", slow_default_client)
    checkpoint = await _save(EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume"), b"grading input")
    # A retry runs in a fresh process, so its store builds its own client on the first load.
    retry_store = EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume")
    loaded = await retry_store.load(checkpoint, task_id="both__t1", dataset="test", judge_identity=JUDGE)
    assert loaded == b"grading input"
    assert built_on_loop == [False, False]


async def test_load_refuses_a_checkpoint_saved_under_another_judge(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store)
    new_judge = judge_identity({"model": "openai/gpt-6", "test_revision": "rev-1"})

    with pytest.raises(EvalResumeIdentityError, match="different judge"):
        await store.load(checkpoint, task_id="both__t1", dataset="test", judge_identity=new_judge)
    assert s3.bodies == []


@pytest.mark.parametrize(("task_id", "dataset"), [("both__t2", "test"), ("both__t1", "val")])
async def test_load_refuses_a_checkpoint_for_another_task_or_dataset(
    store: EvalResumeStore, task_id: str, dataset: str
) -> None:
    checkpoint = await _save(store)

    with pytest.raises(EvalResumeIdentityError, match="not task"):
        await store.load(checkpoint, task_id=task_id, dataset=dataset, judge_identity=JUDGE)


async def test_load_detects_changed_bytes_of_the_same_length(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store, b"original")
    s3.objects[("agentic-harness-dev", checkpoint.s3_key)] = b"tampered"

    with pytest.raises(EvalResumeIntegrityError, match="SHA-256"):
        await store.load(checkpoint, task_id="both__t1", dataset="test", judge_identity=JUDGE)


@pytest.mark.parametrize("stored", [b"short", b"original plus more"])
async def test_load_detects_a_size_mismatch(store: EvalResumeStore, s3: _FakeS3, stored: bytes) -> None:
    checkpoint = await _save(store, b"original")
    s3.objects[("agentic-harness-dev", checkpoint.s3_key)] = stored

    with pytest.raises(EvalResumeIntegrityError):
        await store.load(checkpoint, task_id="both__t1", dataset="test", judge_identity=JUDGE)


async def test_org_style_ids_round_trip_under_a_digest_key(store: EvalResumeStore, s3: _FakeS3) -> None:
    task_id, dataset = "terminal-bench-science/tb-1", "terminal-bench-science/tb@sha256:abc"
    checkpoint = await store.save(
        benchmark_id=RUN_ID, task_id=task_id, dataset=dataset, content=b"archive", judge_identity=JUDGE
    )

    digest = hashlib.sha256(task_id.encode()).hexdigest()[:32]
    assert checkpoint.task_id == task_id and checkpoint.dataset == dataset
    assert checkpoint.s3_key == f"snap/eval-resume/{RUN_ID}/{digest}/{checkpoint.sha256}"
    state = checkpoint.model_dump(mode="json")
    assert await store.load(state, task_id=task_id, dataset=dataset, judge_identity=JUDGE) == b"archive"
    with pytest.raises(EvalResumeError, match="outside"):
        store.parse(state | {"task_id": "../escape"})


async def test_parse_rejects_a_pointer_outside_the_prefix(store: EvalResumeStore) -> None:
    checkpoint = await _save(store)
    state = checkpoint.model_dump(mode="json") | {"s3_key": "other-bench/secret"}

    with pytest.raises(EvalResumeError, match="outside"):
        store.parse(state)


@pytest.mark.parametrize(
    "change",
    [
        {"benchmark_id": "../escape"},
        {"task_id": ""},
        {"dataset": ""},
        {"sha256": "ABC"},
        {"judge_identity": "not-a-digest"},
        {"size_bytes": 1.0},
        {"version": 2},
        {"extra": "field"},
    ],
)
async def test_parse_rejects_malformed_state(store: EvalResumeStore, change: dict[str, Any]) -> None:
    checkpoint = await _save(store)

    with pytest.raises(EvalResumeError, match="not a checkpoint"):
        store.parse(checkpoint.model_dump(mode="json") | change)


async def test_save_refuses_content_over_the_limit(s3: _FakeS3) -> None:
    store = EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume", max_bytes=4, s3_client=s3)

    with pytest.raises(ValueError, match="over the 4-byte"):
        await _save(store, b"12345")
    assert s3.puts == []


async def test_save_fails_loudly_without_a_bucket(monkeypatch: pytest.MonkeyPatch, s3: _FakeS3) -> None:
    monkeypatch.delenv(BUCKET_ENV)
    store = EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume", s3_client=s3)

    with pytest.raises(RuntimeError, match=BUCKET_ENV):
        await _save(store)


def test_judge_identity_ignores_key_order_and_tracks_values() -> None:
    assert judge_identity({"a": 1, "b": [2]}) == judge_identity({"b": [2], "a": 1})
    assert judge_identity({"a": 1}) != judge_identity({"a": 2})


class _LabelledSandbox:
    def __init__(self, sandbox_id: str, labels: dict[str, str] | None) -> None:
        self.id = sandbox_id
        self.labels = labels


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        ({"Id": RUN_ID}, RUN_ID),
        (None, "sandbox-1"),
        ({"Id": " "}, "sandbox-1"),
    ],
)
def test_benchmark_id_prefers_the_run_label(labels: dict[str, str] | None, expected: str) -> None:
    assert benchmark_id_from_sandbox(_LabelledSandbox("sandbox-1", labels)) == expected  # pyright: ignore[reportArgumentType]


def test_benchmark_id_hashes_an_unsafe_label() -> None:
    benchmark_id = benchmark_id_from_sandbox(_LabelledSandbox("sandbox-1", {"Id": "run/1"}))  # pyright: ignore[reportArgumentType]

    assert benchmark_id == hashlib.sha256(b"run/1").hexdigest()[:32]


async def test_default_client_requires_a_region(monkeypatch: pytest.MonkeyPatch, s3: _FakeS3) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    store = EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume")
    with pytest.raises(RuntimeError, match="AWS_REGION is not set"):
        await _save(store)
