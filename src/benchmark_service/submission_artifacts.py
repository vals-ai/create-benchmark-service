"""Submission artifact object-storage helpers."""

import os
from pathlib import PurePosixPath
from typing import Protocol, cast
from urllib.parse import quote

import boto3

DEFAULT_UPLOAD_EXPIRY_S = 3600


class _S3Client(Protocol):
    def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str: ...


def _artifact_bucket() -> str:
    bucket = os.environ.get("LAB_ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("LAB_ARTIFACT_BUCKET is not set; the deployment must configure the submission artifact bucket")
    return bucket


def _s3_client() -> _S3Client:
    return cast(_S3Client, boto3.client("s3"))  # pyright: ignore[reportUnknownMemberType]


def _key_segment(value: str, field: str) -> str:
    if not value or value in {".", ".."}:
        raise ValueError(f"{field} must be a non-empty path segment")
    return quote(value, safe="._-")


def _filename_segment(filename: str) -> str:
    if PurePosixPath(filename).name != filename or "\\" in filename:
        raise ValueError("filename must not contain path separators")
    return _key_segment(filename, "filename")


def submission_key(*, tenant: str, dataset: str, run_id: str, task_id: str, filename: str) -> str:
    """Object key for a submitted artifact."""
    return "/".join(
        [
            "lab-submissions",
            _key_segment(tenant, "tenant"),
            _key_segment(dataset, "dataset"),
            _key_segment(run_id, "run_id"),
            _key_segment(task_id, "task_id"),
            _filename_segment(filename),
        ]
    )


def presigned_put_url(key: str, *, expires_in: int = DEFAULT_UPLOAD_EXPIRY_S) -> str:
    url: str = _s3_client().generate_presigned_url(
        "put_object",
        Params={"Bucket": _artifact_bucket(), "Key": key},
        ExpiresIn=expires_in,
    )
    return url
