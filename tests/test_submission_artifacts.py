from collections.abc import Generator

import pytest
from botocore.exceptions import ClientError

from benchmark_service import submission_artifacts


@pytest.fixture(autouse=True)
def clear_s3_client_cache() -> Generator[None, None, None]:
    submission_artifacts._s3_client.cache_clear()  # pyright: ignore[reportPrivateUsage]
    yield
    submission_artifacts._s3_client.cache_clear()  # pyright: ignore[reportPrivateUsage]


def test_submission_key_is_namespaced_by_tenant_dataset_run_and_task() -> None:
    key = submission_artifacts.submission_key(
        tenant="acme",
        dataset="default",
        run_id="run-1",
        task_id="task-9",
        filename="submission.xlsx",
    )
    assert key == "submission-artifacts/acme/default/run-1/task-9/submission.xlsx"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant", "acme corp"),
        ("run_id", "run/1"),
        ("task_id", ".."),
        ("filename", "../submission.xlsx"),
        ("filename", "a" * 200),
    ],
)
def test_submission_key_rejects_invalid_segments(field: str, value: str) -> None:
    segments = {
        "tenant": "acme",
        "dataset": "default",
        "run_id": "run-1",
        "task_id": "task-9",
        "filename": "submission.xlsx",
        field: value,
    }
    with pytest.raises(ValueError, match=field):
        submission_artifacts.submission_key(**segments)


def test_presigned_put_url_uses_configured_bucket_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    captured: dict[str, object] = {}

    class _FakeS3:
        def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str:
            captured.update(op=op, Params=Params, ExpiresIn=ExpiresIn)
            return "https://signed.example/put"

    def fake_client(service: str, **kwargs: object) -> _FakeS3:
        assert service == "s3"
        captured.update(client_kwargs=kwargs)
        return _FakeS3()

    monkeypatch.setattr(submission_artifacts.boto3, "client", fake_client)
    url = submission_artifacts.presigned_put_url("submission-artifacts/run-1/task-9/submission.xlsx")
    assert url == "https://signed.example/put"
    assert captured["op"] == "put_object"
    assert captured["Params"] == {
        "Bucket": "vals-submission-artifacts",
        "Key": "submission-artifacts/run-1/task-9/submission.xlsx",
    }
    client_kwargs = captured["client_kwargs"]
    assert isinstance(client_kwargs, dict)
    assert client_kwargs["region_name"] == "us-east-1"
    assert client_kwargs["config"].signature_version == "s3v4"  # pyright: ignore[reportUnknownMemberType]


def test_presigned_put_url_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="SUBMISSION_ARTIFACT_BUCKET"):
        submission_artifacts.presigned_put_url("k")


def test_require_configured_rejects_bucket_without_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.delenv("AWS_REGION", raising=False)
    with pytest.raises(RuntimeError, match="AWS_REGION"):
        submission_artifacts.require_configured()


def test_require_configured_allows_fully_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    submission_artifacts.require_configured()


_TENANT_KEY = "submission-artifacts/acme/default/run-1/task-9/submission.xlsx"


class _FakeBody:
    def read(self) -> bytes:
        return b"artifact-bytes"


class _FakeS3:
    def __init__(self, *, content_length: int = 14, missing: bool = False) -> None:
        self.content_length = content_length
        self.missing = missing
        self.calls: dict[str, object] = {}

    def _respond(self, op: str, Bucket: str, Key: str) -> dict[str, object]:
        self.calls.update(op=op, Bucket=Bucket, Key=Key)
        if self.missing:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, op)
        return {"Body": _FakeBody(), "ContentLength": self.content_length}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return self._respond("GetObject", Bucket, Key)

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return self._respond("HeadObject", Bucket, Key)


def _install_fake_s3(monkeypatch: pytest.MonkeyPatch, fake: _FakeS3) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setattr(submission_artifacts, "_s3_client", lambda: fake)


async def test_download_returns_object_bytes_for_own_tenant_key(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeS3()
    _install_fake_s3(monkeypatch, fake)
    body = await submission_artifacts.download(_TENANT_KEY, tenant="acme")
    assert body == b"artifact-bytes"
    assert fake.calls == {"op": "GetObject", "Bucket": "vals-submission-artifacts", "Key": _TENANT_KEY}


@pytest.mark.parametrize(
    "key",
    [
        "submission-artifacts/tenant-b/default/run-1/task-9/submission.xlsx",
        "other-prefix/acme/default/run-1/task-9/submission.xlsx",
        "submission-artifacts/acme/../escape",
        "submission-artifacts/acme",
    ],
)
async def test_download_rejects_keys_outside_tenant_namespace(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    fake = _FakeS3()
    _install_fake_s3(monkeypatch, fake)
    with pytest.raises(ValueError, match="tenant"):
        await submission_artifacts.download(key, tenant="acme")
    assert fake.calls == {}


async def test_download_maps_missing_object_to_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(missing=True))
    with pytest.raises(submission_artifacts.SubmissionArtifactNotFound, match="upload-url"):
        await submission_artifacts.download(_TENANT_KEY, tenant="acme")


async def test_download_rejects_oversized_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(content_length=1024))
    monkeypatch.setenv(submission_artifacts.MAX_DOWNLOAD_BYTES_ENV, "100")
    with pytest.raises(submission_artifacts.SubmissionArtifactTooLarge, match="1024"):
        await submission_artifacts.download(_TENANT_KEY, tenant="acme")


async def test_stat_returns_size_without_fetching(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeS3(content_length=777)
    _install_fake_s3(monkeypatch, fake)
    size = await submission_artifacts.stat(_TENANT_KEY, tenant="acme")
    assert size == 777
    assert fake.calls["op"] == "HeadObject"
