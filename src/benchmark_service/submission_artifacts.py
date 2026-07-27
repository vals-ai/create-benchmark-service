"""Submission artifact object-storage helpers."""

import asyncio
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import Protocol, TypeVar, cast

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from benchmark_service.key_segments import KEY_SEGMENT_PATTERN

DEFAULT_UPLOAD_EXPIRY_S = 3600
SUBMISSION_ARTIFACT_BUCKET_ENV = "SUBMISSION_ARTIFACT_BUCKET"
SUBMISSION_ARTIFACT_REGION_ENV = "AWS_REGION"
SUBMISSION_ARTIFACT_KEY_PREFIX = "submission-artifacts"
MAX_DOWNLOAD_BYTES_ENV = "SUBMISSION_ARTIFACT_MAX_DOWNLOAD_BYTES"
DEFAULT_MAX_DOWNLOAD_BYTES = 64 * 1024**2

_KEY_SEGMENT_RE = re.compile(KEY_SEGMENT_PATTERN)
_T = TypeVar("_T")


class SubmissionArtifactNotFound(Exception):
    """The submission key has no uploaded object behind it.

    S3 can identify this case only when the service role has s3:ListBucket.
    """


class SubmissionArtifactChanged(Exception):
    """The uploaded object changed after admission."""


class SubmissionArtifactTooLarge(Exception):
    """The uploaded object exceeds the configured download size limit."""


@dataclass(frozen=True, slots=True)
class SubmissionArtifactReference:
    """Immutable identity captured when a submission artifact is admitted."""

    key: str
    size_bytes: int
    etag: str


class _StreamingBody(Protocol):
    def read(self) -> bytes: ...
    def close(self) -> None: ...


class _S3Client(Protocol):
    def generate_presigned_url(self, op: str, *, Params: dict[str, str], ExpiresIn: int) -> str: ...
    def get_object(self, *, Bucket: str, Key: str, IfMatch: str) -> dict[str, object]: ...
    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]: ...


def is_configured() -> bool:
    return bool(os.environ.get(SUBMISSION_ARTIFACT_BUCKET_ENV))


def require_configured() -> None:
    """Validate submission-artifact settings at service startup."""
    if not is_configured():
        return
    if not os.environ.get(SUBMISSION_ARTIFACT_REGION_ENV):
        raise RuntimeError(
            f"{SUBMISSION_ARTIFACT_BUCKET_ENV} is set but {SUBMISSION_ARTIFACT_REGION_ENV} is not; "
            "configure the submission artifact bucket's region before starting the service"
        )
    _max_download_bytes()


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
    raw_limit = os.environ.get(MAX_DOWNLOAD_BYTES_ENV)
    if raw_limit is None:
        return DEFAULT_MAX_DOWNLOAD_BYTES
    try:
        limit = int(raw_limit)
    except ValueError as exc:
        raise RuntimeError(f"{MAX_DOWNLOAD_BYTES_ENV} must be a positive integer number of bytes") from exc
    if limit <= 0:
        raise RuntimeError(f"{MAX_DOWNLOAD_BYTES_ENV} must be a positive integer number of bytes")
    return limit


def _require_size_within_limit(size: object, key: str, limit: int) -> int:
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise RuntimeError(f"S3 returned an invalid size for artifact {key}")
    if size > limit:
        raise SubmissionArtifactTooLarge(
            f"artifact {key} is {size} bytes, over the {limit}-byte download limit"
        )
    return size


def _require_etag(etag: object, key: str) -> str:
    if not isinstance(etag, str) or not etag:
        raise RuntimeError(f"S3 did not return an ETag for artifact {key}")
    return etag


def _raise_for_artifact_error(exc: ClientError, key: str) -> None:
    error = cast(dict[str, str], exc.response.get("Error") or {})  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    code = error.get("Code", "")
    if code in {"404", "NoSuchKey", "NotFound"}:
        raise SubmissionArtifactNotFound(
            f"no artifact was uploaded for key {key}; upload it via /v1/submissions/upload-url first"
        ) from exc
    if code in {"412", "PreconditionFailed"}:
        raise SubmissionArtifactChanged(
            f"artifact {key} changed after it was accepted; submit it again before grading"
        ) from exc


def _stat_sync(key: str, tenant: str) -> SubmissionArtifactReference:
    _require_tenant_key(key, tenant)
    bucket = _artifact_bucket()
    try:
        response = _s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        _raise_for_artifact_error(exc, key)
        raise
    size_bytes = _require_size_within_limit(response.get("ContentLength"), key, _max_download_bytes())
    etag = _require_etag(response.get("ETag"), key)
    return SubmissionArtifactReference(key=key, size_bytes=size_bytes, etag=etag)


def _download_sync(reference: SubmissionArtifactReference, tenant: str) -> bytes:
    _require_tenant_key(reference.key, tenant)
    limit = _max_download_bytes()
    _require_size_within_limit(reference.size_bytes, reference.key, limit)
    _require_etag(reference.etag, reference.key)
    bucket = _artifact_bucket()
    try:
        response = _s3_client().get_object(Bucket=bucket, Key=reference.key, IfMatch=reference.etag)
    except ClientError as exc:
        _raise_for_artifact_error(exc, reference.key)
        raise
    body = cast(_StreamingBody, response["Body"])
    try:
        _require_size_within_limit(response.get("ContentLength"), reference.key, limit)
        return body.read()
    finally:
        body.close()


async def _run_in_thread(operation: Callable[[], _T]) -> _T:
    """Keep cancellation from detaching blocking storage work from its caller."""
    worker = asyncio.create_task(asyncio.to_thread(operation))
    cancellation: asyncio.CancelledError | None = None
    while not worker.done():
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except Exception:  # noqa: BLE001
            break

    try:
        result = worker.result()
    except BaseException:  # noqa: BLE001
        if cancellation is not None:
            raise cancellation from None
        raise

    if cancellation is not None:
        raise cancellation
    return result


async def stat(key: str, *, tenant: str) -> SubmissionArtifactReference:
    """Capture the artifact's immutable identity without fetching it.

    This admission check runs before any expensive grading work is provisioned.
    """
    return await _run_in_thread(partial(_stat_sync, key, tenant))


async def download(reference: SubmissionArtifactReference, *, tenant: str) -> bytes:
    """Fetch the admitted artifact version for server-side grading."""
    return await _run_in_thread(partial(_download_sync, reference, tenant))
