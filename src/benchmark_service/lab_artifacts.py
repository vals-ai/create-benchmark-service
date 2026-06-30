"""Lab artifact object-storage helpers (presigned uploads for decoupled eval)."""

import os
from typing import Any

import boto3

DEFAULT_UPLOAD_EXPIRY_S = 3600


def _artifact_bucket() -> str:
    bucket = os.environ.get("LAB_ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("LAB_ARTIFACT_BUCKET is not set; the deployment must configure the lab artifact bucket")
    return bucket


def _s3_client() -> Any:
    return boto3.client("s3")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]


def submission_key(run_id: str, task_id: str, filename: str) -> str:
    """Object key for a submitted artifact, namespaced by run and task."""
    return f"lab-submissions/{run_id}/{task_id}/{filename}"


def presigned_put_url(key: str, *, expires_in: int = DEFAULT_UPLOAD_EXPIRY_S) -> str:
    url: str = _s3_client().generate_presigned_url(
        "put_object",
        Params={"Bucket": _artifact_bucket(), "Key": key},
        ExpiresIn=expires_in,
    )
    return url


def download(key: str) -> bytes:
    """Read an uploaded artifact's bytes from the lab artifact bucket (grader read-back)."""
    data: bytes = _s3_client().get_object(Bucket=_artifact_bucket(), Key=key)["Body"].read()
    return data
