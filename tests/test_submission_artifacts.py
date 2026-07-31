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


@pytest.mark.parametrize("limit", ["0", "-1", "not-a-number"])
def test_require_configured_rejects_invalid_download_limit(monkeypatch: pytest.MonkeyPatch, limit: str) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv(submission_artifacts.MAX_DOWNLOAD_BYTES_ENV, limit)
    with pytest.raises(RuntimeError, match=submission_artifacts.MAX_DOWNLOAD_BYTES_ENV):
        submission_artifacts.require_configured()


def test_require_configured_allows_fully_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    submission_artifacts.require_configured()


_TENANT_KEY = "submission-artifacts/acme/default/run-1/task-9/submission.xlsx"


class _FakeBody:
    def __init__(self, data: bytes = b"artifact-bytes", *, read_error: Exception | None = None) -> None:
        self.data = data
        self.read_error = read_error
        self.read_called = False
        self.closed = False

    def read(self) -> bytes:
        self.read_called = True
        if self.read_error is not None:
            raise self.read_error
        return self.data

    def close(self) -> None:
        self.closed = True


class _FakeS3:
    def __init__(
        self,
        *,
        content_length: int = 14,
        etag: str = '"etag-1"',
        error_code: str | None = None,
        body: _FakeBody | None = None,
    ) -> None:
        self.content_length = content_length
        self.etag = etag
        self.error_code = error_code
        self.body = body if body is not None else _FakeBody()
        self.calls: list[dict[str, object]] = []

    def _raise_error(self, op: str) -> None:
        if self.error_code is not None:
            raise ClientError({"Error": {"Code": self.error_code, "Message": "S3 error"}}, op)

    def get_object(self, *, Bucket: str, Key: str, IfMatch: str) -> dict[str, object]:
        self.calls.append({"op": "GetObject", "Bucket": Bucket, "Key": Key, "IfMatch": IfMatch})
        self._raise_error("GetObject")
        if IfMatch != self.etag:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "ETag changed"}}, "GetObject")
        return {"Body": self.body, "ContentLength": self.content_length, "ETag": self.etag}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        self.calls.append({"op": "HeadObject", "Bucket": Bucket, "Key": Key})
        self._raise_error("HeadObject")
        return {"ContentLength": self.content_length, "ETag": self.etag}


def _install_fake_s3(monkeypatch: pytest.MonkeyPatch, fake: _FakeS3) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    monkeypatch.setattr(submission_artifacts, "_s3_client", lambda: fake)


def _reference(
    *,
    key: str = _TENANT_KEY,
    size_bytes: int = 14,
    etag: str = '"etag-1"',
) -> submission_artifacts.SubmissionArtifactReference:
    return submission_artifacts.SubmissionArtifactReference(key=key, size_bytes=size_bytes, etag=etag)


async def test_download_returns_object_bytes_for_own_tenant_key(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeS3()
    _install_fake_s3(monkeypatch, fake)
    monkeypatch.setenv(submission_artifacts.MAX_DOWNLOAD_BYTES_ENV, "100")
    contents = await submission_artifacts.download(_reference(), tenant="acme")
    assert contents == b"artifact-bytes"
    assert fake.calls == [
        {
            "op": "GetObject",
            "Bucket": "vals-submission-artifacts",
            "Key": _TENANT_KEY,
            "IfMatch": '"etag-1"',
        }
    ]
    assert fake.body.closed


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
        await submission_artifacts.download(_reference(key=key), tenant="acme")
    assert fake.calls == []


@pytest.mark.parametrize(
    "key",
    [
        "submission-artifacts/other/default/run-1/task-9/submission.xlsx",
        "submission-artifacts/acme/other/run-1/task-9/submission.xlsx",
        "submission-artifacts/acme/default/other/task-9/submission.xlsx",
        "submission-artifacts/acme/default/run-1/other/submission.xlsx",
    ],
)
def test_validate_submission_key_binds_the_authenticated_evaluation(key: str) -> None:
    with pytest.raises(ValueError):
        submission_artifacts.validate_submission_key(
            key,
            tenant="acme",
            dataset="default",
            run_id="run-1",
            task_id="task-9",
        )


def test_validate_submission_key_accepts_the_minted_evaluation_key() -> None:
    submission_artifacts.validate_submission_key(
        _TENANT_KEY,
        tenant="acme",
        dataset="default",
        run_id="run-1",
        task_id="task-9",
    )


async def test_stat_maps_missing_object_to_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(error_code="404"))
    with pytest.raises(submission_artifacts.SubmissionArtifactNotFound, match="upload-url"):
        await submission_artifacts.stat(_TENANT_KEY, tenant="acme")


async def test_stat_preserves_forbidden_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(error_code="403"))
    with pytest.raises(ClientError):
        await submission_artifacts.stat(_TENANT_KEY, tenant="acme")


async def test_stat_requires_immutable_object_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(etag=""))
    with pytest.raises(RuntimeError, match="ETag"):
        await submission_artifacts.stat(_TENANT_KEY, tenant="acme")


async def test_stat_rejects_oversized_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_s3(monkeypatch, _FakeS3(content_length=1024))
    monkeypatch.setenv(submission_artifacts.MAX_DOWNLOAD_BYTES_ENV, "100")
    with pytest.raises(submission_artifacts.SubmissionArtifactTooLarge, match="1024"):
        await submission_artifacts.stat(_TENANT_KEY, tenant="acme")


async def test_download_rejects_oversized_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeS3(content_length=1024)
    _install_fake_s3(monkeypatch, fake)
    monkeypatch.setenv(submission_artifacts.MAX_DOWNLOAD_BYTES_ENV, "100")
    with pytest.raises(submission_artifacts.SubmissionArtifactTooLarge, match="1024"):
        await submission_artifacts.download(_reference(), tenant="acme")
    assert not fake.body.read_called
    assert fake.body.closed


async def test_download_closes_body_when_read_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _FakeBody(read_error=OSError("stream failed"))
    fake = _FakeS3(body=body)
    _install_fake_s3(monkeypatch, fake)
    with pytest.raises(OSError, match="stream failed"):
        await submission_artifacts.download(_reference(), tenant="acme")
    assert body.closed


async def test_download_rejects_an_artifact_replaced_after_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeS3(content_length=777)
    _install_fake_s3(monkeypatch, fake)
    reference = await submission_artifacts.stat(_TENANT_KEY, tenant="acme")
    assert reference == submission_artifacts.SubmissionArtifactReference(
        key=_TENANT_KEY,
        size_bytes=777,
        etag='"etag-1"',
    )

    fake.etag = '"etag-2"'
    with pytest.raises(submission_artifacts.SubmissionArtifactChanged, match="changed"):
        await submission_artifacts.download(reference, tenant="acme")
    assert fake.calls[-1]["IfMatch"] == '"etag-1"'
