import asyncio
import hashlib
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from benchmark_service.context import tenant_scope
from benchmark_service.eval_resume import (
    EvalResumeCheckpoint,
    EvalResumeError,
    EvalResumeIdentityError,
    EvalResumeIntegrityError,
    EvalResumeStore,
    benchmark_id_from_sandbox,
    grader_identity,
)

BUCKET_ENV = "TEST_EVAL_STATE_BUCKET"
RUN_ID = "2ecbe3f6-e545-475f-81e1-00408dc02c6f"
GRADER = grader_identity({"model": "anthropic/claude-opus-4-7", "test_revision": "rev-1"})
TENANT = "tenant-a"


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


@pytest.fixture(autouse=True)
def caller_tenant() -> Iterator[None]:
    with tenant_scope(TENANT):
        yield


def _binding(task_id: str = "both__t1", dataset: str = "test", grader: str = GRADER) -> str:
    fields = f'{{"dataset":"{dataset}","grader_identity":"{grader}","task_id":"{task_id}"}}'
    return hashlib.sha256(fields.encode()).hexdigest()[:32]


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> _FakeS3:
    monkeypatch.setenv(BUCKET_ENV, "agentic-harness-dev")
    return _FakeS3()


@pytest.fixture
def store(s3: _FakeS3) -> EvalResumeStore:
    return EvalResumeStore(bucket_env=BUCKET_ENV, object_prefix="snap/eval-resume/", s3_client=s3)


async def _save(store: EvalResumeStore, content: bytes = b'{"messages": []}') -> EvalResumeCheckpoint:
    return await store.save(
        benchmark_id=RUN_ID, task_id="both__t1", dataset="test", content=content, grader_identity=GRADER
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
        "s3_key": f"snap/eval-resume/{TENANT}/{RUN_ID}/both__t1/{_binding()}/{sha256}",
        "sha256": sha256,
        "size_bytes": len(content),
        "grader_identity": GRADER,
    }
    assert s3.objects[("agentic-harness-dev", checkpoint.s3_key)] == content
    assert s3.puts[0]["ContentType"] == "application/octet-stream"


async def test_load_returns_saved_bytes_from_the_json_state(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store, b"grading input")

    content = await store.load(
        checkpoint.model_dump(mode="json"), task_id="both__t1", dataset="test", grader_identity=GRADER
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
    load = asyncio.create_task(store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER))
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
    loaded = await retry_store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER)
    assert loaded == b"grading input"
    assert built_on_loop == [False, False]


async def test_load_refuses_a_checkpoint_saved_under_another_grader(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store)
    new_grader = grader_identity({"model": "openai/gpt-6", "test_revision": "rev-1"})

    with pytest.raises(EvalResumeIdentityError, match="different grader"):
        await store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=new_grader)
    assert s3.bodies == []


@pytest.mark.parametrize(
    "fields",
    [
        {"dataset": "test", "grader_identity": grader_identity({"model": "openai/gpt-6", "test_revision": "rev-2"})},
        {"dataset": "val", "grader_identity": GRADER},
    ],
)
async def test_load_refuses_a_checkpoint_whose_grader_or_dataset_was_edited(
    store: EvalResumeStore, s3: _FakeS3, fields: dict[str, str]
) -> None:
    """An edited grader or dataset no longer derives the saved key, so the old bytes never load under it."""
    checkpoint = await _save(store)
    state = checkpoint.model_dump(mode="json") | fields

    with pytest.raises(EvalResumeError, match="does not match"):
        await store.load(state, task_id="both__t1", **fields)
    rekeyed = state | {"s3_key": store.key(benchmark_id=RUN_ID, task_id="both__t1", sha256=checkpoint.sha256, **fields)}
    assert rekeyed["s3_key"] != checkpoint.s3_key
    with pytest.raises(KeyError):  # the fake's NoSuchKey: nothing was ever saved under the edited fields
        await store.load(rekeyed, task_id="both__t1", **fields)
    assert s3.bodies == []


async def test_load_refuses_a_checkpoint_saved_by_another_tenant(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store)

    with tenant_scope("tenant-b"):
        with pytest.raises(EvalResumeIdentityError, match="another tenant"):
            await store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER)
        own_key = checkpoint.s3_key.replace(f"/{TENANT}/", "/tenant-b/", 1)
        with pytest.raises(KeyError):  # rewriting the tenant segment points at tenant-b's own, empty namespace
            await store.load(
                checkpoint.model_dump(mode="json") | {"s3_key": own_key},
                task_id="both__t1",
                dataset="test",
                grader_identity=GRADER,
            )
    assert s3.bodies == []


async def test_a_task_id_edited_to_its_key_digest_is_refused(store: EvalResumeStore, s3: _FakeS3) -> None:
    """``org/name`` is stored under its digest; a task literally named that digest must not reach it."""
    task_id = "terminal-bench-science/tb-1"
    checkpoint = await store.save(
        benchmark_id=RUN_ID, task_id=task_id, dataset="test", content=b"archive", grader_identity=GRADER
    )
    alias = hashlib.sha256(task_id.encode()).hexdigest()[:32]

    with pytest.raises(EvalResumeError, match="does not match"):
        await store.load(
            checkpoint.model_dump(mode="json") | {"task_id": alias},
            task_id=alias,
            dataset="test",
            grader_identity=GRADER,
        )
    assert s3.bodies == []


async def test_tenant_segments_follow_the_submission_artifact_rule(store: EvalResumeStore, s3: _FakeS3) -> None:
    """The same tenants are valid here as in ``submission_artifacts``: ``KEY_SEGMENT_PATTERN``."""
    with tenant_scope("acme+team"):
        checkpoint = await _save(store)
        assert checkpoint.s3_key.startswith("snap/eval-resume/acme+team/")
    for tenant in (".hidden", "x" * 129):
        with tenant_scope(tenant), pytest.raises(EvalResumeError, match="tenant is not a valid"):
            await _save(store)
    assert len(s3.puts) == 1


async def test_an_unsafe_tenant_is_refused_rather_than_aliased(store: EvalResumeStore, s3: _FakeS3) -> None:
    """Digesting ``org/name`` would share a namespace with a tenant whose id is that digest."""
    with tenant_scope("org/name"):
        with pytest.raises(EvalResumeError, match="tenant is not a valid object-key segment"):
            await _save(store)
    assert s3.puts == []


async def test_save_and_load_require_an_authenticated_tenant(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store)

    with tenant_scope(None):
        with pytest.raises(RuntimeError, match="no authenticated tenant"):
            await _save(store)
        with pytest.raises(RuntimeError, match="no authenticated tenant"):
            await store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER)
    assert len(s3.puts) == 1 and s3.bodies == []


@pytest.mark.parametrize(("task_id", "dataset"), [("both__t2", "test"), ("both__t1", "val")])
async def test_load_refuses_a_checkpoint_for_another_task_or_dataset(
    store: EvalResumeStore, task_id: str, dataset: str
) -> None:
    checkpoint = await _save(store)

    with pytest.raises(EvalResumeIdentityError, match="not task"):
        await store.load(checkpoint, task_id=task_id, dataset=dataset, grader_identity=GRADER)


async def test_load_detects_changed_bytes_of_the_same_length(store: EvalResumeStore, s3: _FakeS3) -> None:
    checkpoint = await _save(store, b"original")
    s3.objects[("agentic-harness-dev", checkpoint.s3_key)] = b"tampered"

    with pytest.raises(EvalResumeIntegrityError, match="SHA-256"):
        await store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER)


@pytest.mark.parametrize("stored", [b"short", b"original plus more"])
async def test_load_detects_a_size_mismatch(store: EvalResumeStore, s3: _FakeS3, stored: bytes) -> None:
    checkpoint = await _save(store, b"original")
    s3.objects[("agentic-harness-dev", checkpoint.s3_key)] = stored

    with pytest.raises(EvalResumeIntegrityError):
        await store.load(checkpoint, task_id="both__t1", dataset="test", grader_identity=GRADER)


async def test_org_style_ids_round_trip_under_a_digest_key(store: EvalResumeStore, s3: _FakeS3) -> None:
    task_id, dataset = "terminal-bench-science/tb-1", "terminal-bench-science/tb@sha256:abc"
    checkpoint = await store.save(
        benchmark_id=RUN_ID, task_id=task_id, dataset=dataset, content=b"archive", grader_identity=GRADER
    )

    digest = hashlib.sha256(task_id.encode()).hexdigest()[:32]
    assert checkpoint.task_id == task_id and checkpoint.dataset == dataset
    assert (
        checkpoint.s3_key
        == f"snap/eval-resume/{TENANT}/{RUN_ID}/{digest}/{_binding(task_id, dataset)}/{checkpoint.sha256}"
    )
    state = checkpoint.model_dump(mode="json")
    assert await store.load(state, task_id=task_id, dataset=dataset, grader_identity=GRADER) == b"archive"
    with pytest.raises(EvalResumeError, match="does not match"):
        store.parse(state | {"task_id": "../escape"})


async def test_parse_rejects_a_pointer_outside_the_prefix(store: EvalResumeStore) -> None:
    checkpoint = await _save(store)
    state = checkpoint.model_dump(mode="json") | {"s3_key": "other-bench/secret"}

    with pytest.raises(EvalResumeError, match="another tenant"):
        store.parse(state)


@pytest.mark.parametrize(
    "change",
    [
        {"benchmark_id": "../escape"},
        {"task_id": ""},
        {"dataset": ""},
        {"sha256": "ABC"},
        {"grader_identity": "not-a-digest"},
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


def test_grader_identity_ignores_key_order_and_tracks_values() -> None:
    assert grader_identity({"a": 1, "b": [2]}) == grader_identity({"b": [2], "a": 1})
    assert grader_identity({"a": 1}) != grader_identity({"a": 2})


def test_grader_identity_refuses_an_empty_mapping() -> None:
    """An empty identity hashes to one constant, so a changed grader would still pass the retry check."""
    with pytest.raises(ValueError, match="at least one"):
        grader_identity({})


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
