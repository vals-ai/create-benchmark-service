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


def test_submission_key_encodes_non_separator_segments() -> None:
    key = submission_artifacts.submission_key(
        tenant="acme corp",
        dataset="default",
        run_id="run 1",
        task_id="task/9",
        filename="submission #1.xlsx",
    )
    assert key == "submission-artifacts/acme%20corp/default/run%201/task%2F9/submission%20%231.xlsx"


def test_submission_key_rejects_path_like_filename() -> None:
    with pytest.raises(ValueError, match="filename"):
        submission_artifacts.submission_key(
            tenant="acme",
            dataset="default",
            run_id="run-1",
            task_id="task-9",
            filename="../submission.xlsx",
        )


def test_presigned_put_url_uses_configured_bucket_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "vals-submission-artifacts")
    captured: dict[str, object] = {}

    class _FakeS3:
        def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str:
            captured.update(op=op, Params=Params, ExpiresIn=ExpiresIn)
            return "https://signed.example/put"

    def fake_client(service: str) -> _FakeS3:
        assert service == "s3"
        return _FakeS3()

    monkeypatch.setattr(submission_artifacts.boto3, "client", fake_client)
    url = submission_artifacts.presigned_put_url("submission-artifacts/run-1/task-9/submission.xlsx")
    assert url == "https://signed.example/put"
    assert captured["op"] == "put_object"
    assert captured["Params"] == {
        "Bucket": "vals-submission-artifacts",
        "Key": "submission-artifacts/run-1/task-9/submission.xlsx",
    }


def test_presigned_put_url_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBMISSION_ARTIFACT_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="SUBMISSION_ARTIFACT_BUCKET"):
        submission_artifacts.presigned_put_url("k")
