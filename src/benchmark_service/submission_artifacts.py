"""Submission artifact object-storage helpers."""

import asyncio
import os
import re
from functools import lru_cache
from typing import Protocol, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from benchmark_service.v1_schemas import KEY_SEGMENT_PATTERN

DEFAULT_UPLOAD_EXPIRY_S = 3600
SUBMISSION_ARTIFACT_BUCKET_ENV = "SUBMISSION_ARTIFACT_BUCKET"
SUBMISSION_ARTIFACT_REGION_ENV = "AWS_REGION"
SUBMISSION_ARTIFACT_KEY_PREFIX = "submission-artifacts"
# Presigned PUTs cannot bound object size, so the read side must: refuse
# anything larger than this before it enters service memory. Downloads are
# buffered fully in RAM and up to GRADING_MAX_CONCURRENCY can be in flight,
# so the cap bounds worst-case memory at cap × concurrency.
MAX_DOWNLOAD_BYTES_ENV = "SUBMISSION_ARTIFACT_MAX_DOWNLOAD_BYTES"
DEFAULT_MAX_DOWNLOAD_BYTES = 256 * 1024**2

_KEY_SEGMENT_RE = re.compile(KEY_SEGMENT_PATTERN)


class SubmissionArtifactNotFound(Exception):
    """The submission key has no uploaded object behind it."""


class SubmissionArtifactTooLarge(Exception):
    """The uploaded object exceeds the configured download size limit."""


class _StreamingBody(Protocol):
    def read(self) -> bytes: ...


class _S3Client(Protocol):
    def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str: ...
    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]: ...
    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]: ...


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
            f"{SUBMISSION_ARTIFACT_REGION_ENV} is not set; submission-artifact S3 access must be "
            "configured for the bucket's region"
        )
    return cast(
        _S3Client,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            region_name=region,
            config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=60),
        ),
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


def _require_tenant_key(key: str, tenant: str) -> None:
    """Reject keys outside the tenant's own submission namespace.

    Keys arrive from caller-supplied request data; the upload side only ever
    mints `{prefix}/{tenant}/...` keys with pattern-valid segments, so any
    other shape is either a cross-tenant probe or corruption.
    """
    if not _KEY_SEGMENT_RE.fullmatch(tenant):
        raise ValueError("tenant is not a valid object-key segment")
    segments = key.split("/")
    if (
        len(segments) < 3
        or segments[0] != SUBMISSION_ARTIFACT_KEY_PREFIX
        or segments[1] != tenant
        or not all(_KEY_SEGMENT_RE.fullmatch(s) for s in segments[1:])
    ):
        raise ValueError("key does not name an artifact submitted by this tenant")


def _max_download_bytes() -> int:
    return int(os.environ.get(MAX_DOWNLOAD_BYTES_ENV) or DEFAULT_MAX_DOWNLOAD_BYTES)


def _require_size_within_limit(size: object, key: str) -> None:
    limit = _max_download_bytes()
    if isinstance(size, int) and size > limit:
        raise SubmissionArtifactTooLarge(
            f"artifact {key} is {size} bytes, over the {limit}-byte download limit"
        )


def _raise_if_missing_object(exc: ClientError, key: str) -> None:
    error = cast(dict[str, str], exc.response.get("Error") or {})  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    code = error.get("Code", "")
    if code in {"404", "NoSuchKey", "NotFound"}:
        raise SubmissionArtifactNotFound(
            f"no artifact was uploaded for key {key}; upload it via /v1/submissions/upload-url first"
        ) from exc


def _stat_sync(key: str, tenant: str) -> int:
    _require_tenant_key(key, tenant)
    bucket = _artifact_bucket()
    try:
        response = _s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        _raise_if_missing_object(exc, key)
        raise
    size = response.get("ContentLength")
    _require_size_within_limit(size, key)
    return size if isinstance(size, int) else 0


def _download_sync(key: str, tenant: str) -> bytes:
    _require_tenant_key(key, tenant)
    bucket = _artifact_bucket()
    try:
        response = _s3_client().get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        _raise_if_missing_object(exc, key)
        raise
    _require_size_within_limit(response.get("ContentLength"), key)
    body = cast(_StreamingBody, response["Body"])
    return body.read()


async def stat(key: str, *, tenant: str) -> int:
    """Return the artifact's size in bytes without fetching it.

    Cheap existence/size check for request admission, before any expensive
    grading work is provisioned.
    """
    return await asyncio.to_thread(_stat_sync, key, tenant)


async def download(key: str, *, tenant: str) -> bytes:
    """Fetch an uploaded artifact's bytes for server-side grading."""
    return await asyncio.to_thread(_download_sync, key, tenant)
