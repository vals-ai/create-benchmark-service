"""Submission artifact object-storage helpers."""

import os
import re
from functools import lru_cache
from typing import Protocol, cast

import boto3
from botocore.config import Config

from benchmark_service.v1_schemas import KEY_SEGMENT_PATTERN

DEFAULT_UPLOAD_EXPIRY_S = 3600
SUBMISSION_ARTIFACT_BUCKET_ENV = "SUBMISSION_ARTIFACT_BUCKET"
SUBMISSION_ARTIFACT_REGION_ENV = "AWS_REGION"
SUBMISSION_ARTIFACT_KEY_PREFIX = "submission-artifacts"

_KEY_SEGMENT_RE = re.compile(KEY_SEGMENT_PATTERN)


class _S3Client(Protocol):
    def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str: ...


def is_configured() -> bool:
    return bool(os.environ.get(SUBMISSION_ARTIFACT_BUCKET_ENV))


def require_configured() -> None:
    """Fail fast on a half-configured deployment: presigning is pure local
    signing, so a bucket without a region mints URLs that only fail at the
    caller's PUT (SigV2 / wrong-region 400s), never at mint time."""
    if is_configured() and not os.environ.get(SUBMISSION_ARTIFACT_REGION_ENV):
        raise RuntimeError(
            f"{SUBMISSION_ARTIFACT_BUCKET_ENV} is set but {SUBMISSION_ARTIFACT_REGION_ENV} is not; "
            "presigned upload URLs must be signed for the bucket's region"
        )


def _artifact_bucket() -> str:
    bucket = os.environ.get(SUBMISSION_ARTIFACT_BUCKET_ENV)
    if not bucket:
        raise RuntimeError(
            f"{SUBMISSION_ARTIFACT_BUCKET_ENV} is not set; the deployment must configure the submission artifact bucket"
        )
    return bucket


@lru_cache(maxsize=1)
def _s3_client() -> _S3Client:
    region = os.environ.get(SUBMISSION_ARTIFACT_REGION_ENV)
    if not region:
        raise RuntimeError(
            f"{SUBMISSION_ARTIFACT_REGION_ENV} is not set; presigned upload URLs must be signed for the bucket's region"
        )
    return cast(
        _S3Client,
        boto3.client("s3", region_name=region, config=Config(signature_version="s3v4")),  # pyright: ignore[reportUnknownMemberType]
    )


def submission_key(*, tenant: str, dataset: str, run_id: str, task_id: str, filename: str) -> str:
    """Object key for a submitted artifact.

    Request-sourced fields are already pattern-validated at the schema
    boundary (KeySegment); re-checking every segment here keeps the key safe
    for callers that don't go through the request schema (tenant comes from
    auth state, not the request body).
    """
    segments = {"tenant": tenant, "dataset": dataset, "run_id": run_id, "task_id": task_id, "filename": filename}
    for field, value in segments.items():
        if not _KEY_SEGMENT_RE.fullmatch(value):
            raise ValueError(f"{field} is not a valid object-key segment")
    return "/".join([SUBMISSION_ARTIFACT_KEY_PREFIX, *segments.values()])


def presigned_put_url(key: str, *, expires_in: int = DEFAULT_UPLOAD_EXPIRY_S) -> str:
    bucket = _artifact_bucket()
    url: str = _s3_client().generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
    return url
