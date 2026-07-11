import pytest

from benchmark_service import submission_artifacts


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
    submission_artifacts._s3_client.cache_clear()  # pyright: ignore[reportPrivateUsage]
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
    try:
        url = submission_artifacts.presigned_put_url("submission-artifacts/run-1/task-9/submission.xlsx")
    finally:
        submission_artifacts._s3_client.cache_clear()  # pyright: ignore[reportPrivateUsage]
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
