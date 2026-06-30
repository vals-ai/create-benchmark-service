import pytest

from benchmark_service import lab_artifacts


def test_submission_key_is_namespaced_by_run_and_task() -> None:
    key = lab_artifacts.submission_key("run-1", "task-9", "submission.xlsx")
    assert key == "lab-submissions/run-1/task-9/submission.xlsx"


def test_presigned_put_url_uses_configured_bucket_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAB_ARTIFACT_BUCKET", "vals-lab-artifacts")
    captured: dict[str, object] = {}

    class _FakeS3:
        def generate_presigned_url(self, op: str, *, Params: dict, ExpiresIn: int) -> str:
            captured.update(op=op, Params=Params, ExpiresIn=ExpiresIn)
            return "https://signed.example/put"

    monkeypatch.setattr(lab_artifacts.boto3, "client", lambda service: _FakeS3())
    url = lab_artifacts.presigned_put_url("lab-submissions/run-1/task-9/submission.xlsx")
    assert url == "https://signed.example/put"
    assert captured["op"] == "put_object"
    assert captured["Params"] == {
        "Bucket": "vals-lab-artifacts",
        "Key": "lab-submissions/run-1/task-9/submission.xlsx",
    }


def test_presigned_put_url_requires_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAB_ARTIFACT_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="LAB_ARTIFACT_BUCKET"):
        lab_artifacts.presigned_put_url("k")


def test_download_reads_from_configured_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAB_ARTIFACT_BUCKET", "vals-lab-artifacts")

    class _Body:
        def read(self) -> bytes:
            return b"DATA"

    class _FakeS3:
        def get_object(self, *, Bucket: str, Key: str) -> dict:
            assert Bucket == "vals-lab-artifacts" and Key == "k"
            return {"Body": _Body()}

    monkeypatch.setattr(lab_artifacts.boto3, "client", lambda service: _FakeS3())
    assert lab_artifacts.download("k") == b"DATA"
