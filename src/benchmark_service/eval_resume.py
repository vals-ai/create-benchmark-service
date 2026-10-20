"""Durable grading input for eval-only retry.

A benchmark that grades after its agent finishes can lose the grade to a grader
error or a dropped connection. Before grading, the service saves the exact bytes
it is about to grade to S3 and streams a small pointer to them as
``eval_resume_state``. On ``/ws/evaluate-response`` the service loads those
bytes back, checks their size and SHA-256 against the pointer, and refuses the
retry when the grader it would use now is not the grader the pointer was saved
under, so a retry grades what the first attempt would have graded.

Every object key names the authenticated tenant that saved it and a digest of
the checkpoint's task, dataset and grader, so a checkpoint replayed by another tenant,
or edited to claim the current grader, never resolves to the saved bytes. The
store reads the tenant the framework binds for each request
(``benchmark_service.context.current_tenant``); call ``save`` and ``load`` from
inside ``evaluate_instance`` or ``stream_evaluate_response``.

The bucket comes from the environment variable that the registry's
``EvalResumeStorageConfig.bucket_env`` names for the service; the store's
``object_prefix`` matches that config's ``object_prefix`` without the ``/*``.

Usage::

    STORE = EvalResumeStore(bucket_env="MY_EVAL_STATE_BUCKET", object_prefix="my-bench/eval-resume")

    # evaluate_instance, before grading
    checkpoint = await STORE.save(
        benchmark_id=benchmark_id_from_sandbox(sandbox),
        task_id=task_id,
        dataset=dataset_name,
        content=grading_input,
        grader_identity=grader_identity({"model": eval_model, "test_revision": test_revision}),
    )
    yield StreamEvalResumeStateChunk(type="eval_resume_state", data=checkpoint.model_dump(mode="json"))

    # stream_evaluate_response, when request.eval_resume_state is set
    grading_input = await STORE.load(
        request.eval_resume_state, task_id=request.task_id, dataset=dataset_name, grader_identity=current_identity
    )
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from typing import Any, Literal, NotRequired, Protocol, TypedDict, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from benchmark_service.blocking import run_blocking
from benchmark_service.context import current_tenant
from benchmark_service.sandbox import Sandbox
from benchmark_service.v1_schemas import KEY_SEGMENT_PATTERN

REGION_ENV = "AWS_REGION"
DEFAULT_MAX_BYTES = 256 * 1024**2
RUN_ID_LABEL = "Id"

_SAFE_COMPONENT = re.compile(r"^(?!\.{1,2}$)[A-Za-z0-9_.-]+$")
_TENANT_SEGMENT = re.compile(KEY_SEGMENT_PATTERN)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class EvalResumeError(ValueError):
    """A checkpoint cannot be resumed; the task needs a fresh run instead."""


class EvalResumeIdentityError(EvalResumeError):
    """The checkpoint was saved for a different task, dataset, or grader."""


class EvalResumeIntegrityError(EvalResumeError):
    """The stored bytes do not match the size or SHA-256 the checkpoint recorded."""


class EvalResumeCheckpoint(BaseModel):
    """The ``eval_resume_state`` a benchmark streams: a pointer to its saved grading input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[1] = 1
    benchmark_id: str
    task_id: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    s3_key: str
    sha256: str
    size_bytes: int = Field(ge=0)
    grader_identity: str

    @field_validator("version", "size_bytes", mode="before")
    @classmethod
    def _exact_integer(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("checkpoint integer fields must be exact JSON integers")
        return value

    @field_validator("benchmark_id")
    @classmethod
    def _safe_component(cls, value: str) -> str:
        if not _SAFE_COMPONENT.fullmatch(value):
            raise ValueError("benchmark_id may contain only letters, numbers, '.', '_', and '-'")
        return value

    @field_validator("sha256", "grader_identity")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("checkpoint digests must be lowercase SHA-256 hex")
        return value


def grader_identity(parts: Mapping[str, Any]) -> str:
    """SHA-256 of everything besides the agent's output that decides a grade.

    A judge-graded benchmark passes the judge model and the rubric revision. A
    deterministic grader (tests run against the agent's output) passes the test or
    rubric revision, the grader image digest, and a hash of the grading code. Include
    anything else whose change should make a saved checkpoint ungradeable. Values must
    be JSON serializable; key order does not matter.

    Raises ``ValueError`` for an empty mapping: it hashes to one constant, so a
    changed grader would still pass the retry check.
    """
    if not parts:
        raise ValueError("grader_identity needs at least one part that identifies the grader")
    encoded = json.dumps(parts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _key_component(value: str) -> str:
    """``value`` when it is a safe S3 path segment, else a digest of it (an org-style ``org/name`` id)."""
    if _SAFE_COMPONENT.fullmatch(value):
        return value
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def _caller_tenant() -> str:
    tenant = current_tenant()
    if tenant is None:
        raise RuntimeError(
            "no authenticated tenant is bound; call eval-resume save and load from inside a service request"
        )
    # The rule submission_artifacts._require_tenant_key applies. Rejected rather than digested:
    # a digested tenant could share a namespace with a tenant whose id is that digest.
    if not _TENANT_SEGMENT.fullmatch(tenant):
        raise EvalResumeError("tenant is not a valid object-key segment")
    return tenant


def _binding(task_id: str, dataset: str, grader_identity: str) -> str:
    """Digest of the fields ``load`` checks but the key's readable segments do not pin.

    The task id is included because its key segment may be a digest of it, which a task
    literally named that digest would share."""
    fields = {"task_id": task_id, "dataset": dataset, "grader_identity": grader_identity}
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()[:32]


def benchmark_id_from_sandbox(sandbox: Sandbox) -> str:
    """The Valkyrie run id the sandbox is labelled with, else the sandbox's own id.

    The tracker labels every task sandbox ``{"Benchmark": name, "Id": benchmark id, "Task": task id}``
    (Valkyrie ``services/tracker/src/tracker/sandbox.py``)."""
    return _key_component(((sandbox.labels or {}).get(RUN_ID_LABEL) or "").strip() or sandbox.id)


class _StreamingBody(Protocol):
    def read(self, amt: int | None = None) -> bytes: ...
    def close(self) -> None: ...


class _GetObjectResponse(TypedDict):
    Body: _StreamingBody
    ContentLength: NotRequired[int]


class S3Client(Protocol):
    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> object: ...
    def get_object(self, *, Bucket: str, Key: str) -> _GetObjectResponse: ...


def _default_s3_client() -> S3Client:
    """Mirrors ``submission_artifacts._s3_client``: an explicit region and bounded timeouts, so a
    cancelled caller waits at most one timeout for the worker thread to return."""
    import boto3
    from botocore.config import Config

    region = os.environ.get(REGION_ENV)
    if not region:
        raise RuntimeError(f"{REGION_ENV} is not set; eval-resume S3 access must be configured for the bucket's region")
    return cast(
        S3Client,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            region_name=region,
            config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=60),
        ),
    )


class EvalResumeStore:
    """Saves and loads one benchmark's grading input under one S3 prefix."""

    def __init__(
        self,
        *,
        bucket_env: str,
        object_prefix: str,
        max_bytes: int = DEFAULT_MAX_BYTES,
        content_type: str = "application/octet-stream",
        s3_client: S3Client | None = None,
    ) -> None:
        prefix = object_prefix.strip("/")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.bucket_env = bucket_env
        self.object_prefix = prefix
        self.max_bytes = max_bytes
        self.content_type = content_type
        self._s3_client = s3_client

    def key(self, *, benchmark_id: str, task_id: str, dataset: str, grader_identity: str, sha256: str) -> str:
        """The only key a checkpoint with these fields may point at, for the current tenant.

        Layout: ``<prefix>/<tenant>/<benchmark_id>/<task_id>/<digest of task, dataset and grader>/<sha256>``.
        A task id that is not a safe path segment (``org/name``) is stored under a digest of it;
        the checkpoint keeps it as is. A tenant that is not a safe path segment is refused.
        """
        tenant = _caller_tenant()
        binding = _binding(task_id, dataset, grader_identity)
        return f"{self.object_prefix}/{tenant}/{benchmark_id}/{_key_component(task_id)}/{binding}/{sha256}"

    async def save(
        self,
        *,
        benchmark_id: str,
        task_id: str,
        dataset: str,
        content: bytes,
        grader_identity: str,
    ) -> EvalResumeCheckpoint:
        """Write ``content`` to S3 and return the checkpoint that points at it."""
        if len(content) > self.max_bytes:
            raise ValueError(f"grading input is {len(content)} bytes, over the {self.max_bytes}-byte checkpoint limit")
        sha256 = hashlib.sha256(content).hexdigest()
        checkpoint = EvalResumeCheckpoint(
            benchmark_id=benchmark_id,
            task_id=task_id,
            dataset=dataset,
            s3_key=self.key(
                benchmark_id=benchmark_id,
                task_id=task_id,
                dataset=dataset,
                grader_identity=grader_identity,
                sha256=sha256,
            ),
            sha256=sha256,
            size_bytes=len(content),
            grader_identity=grader_identity,
        )
        bucket = self._bucket()

        # The client is built inside the blocking call: its first creation can fetch
        # role credentials synchronously, which must not stall the event loop.
        def put() -> None:
            self._client().put_object(
                Bucket=bucket,
                Key=checkpoint.s3_key,
                Body=content,
                ContentType=self.content_type,
            )

        await run_blocking(put)
        return checkpoint

    async def load(
        self,
        state: Mapping[str, Any] | EvalResumeCheckpoint,
        *,
        task_id: str,
        dataset: str,
        grader_identity: str,
    ) -> bytes:
        """Return the saved grading input after checking whose it is and that it is intact.

        Raises ``EvalResumeIdentityError`` when the checkpoint belongs to another
        tenant, task, dataset, or grader, and ``EvalResumeIntegrityError`` when the stored
        bytes differ from what was saved.
        """
        checkpoint = self.parse(state)
        if checkpoint.task_id != task_id or checkpoint.dataset != dataset:
            raise EvalResumeIdentityError(
                f"checkpoint is for task {checkpoint.task_id} in dataset {checkpoint.dataset}, "
                f"not task {task_id} in dataset {dataset}"
            )
        if checkpoint.grader_identity != grader_identity:
            raise EvalResumeIdentityError(
                f"checkpoint for task {task_id} was saved under a different grader; "
                "re-grade it with the service version that produced it, or retry the task from scratch"
            )
        content = await self._get(checkpoint.s3_key, checkpoint.size_bytes)
        if len(content) != checkpoint.size_bytes:
            raise EvalResumeIntegrityError(
                f"stored grading input for task {task_id} is {len(content)} bytes, "
                f"the checkpoint recorded {checkpoint.size_bytes}"
            )
        if hashlib.sha256(content).hexdigest() != checkpoint.sha256:
            raise EvalResumeIntegrityError(f"stored grading input for task {task_id} failed its SHA-256 check")
        return content

    def parse(self, state: Mapping[str, Any] | EvalResumeCheckpoint) -> EvalResumeCheckpoint:
        """Validate ``eval_resume_state`` as a checkpoint this store wrote for the current tenant."""
        try:
            checkpoint = (
                state if isinstance(state, EvalResumeCheckpoint) else EvalResumeCheckpoint.model_validate(dict(state))
            )
        except ValidationError as exc:
            raise EvalResumeError(f"eval_resume_state is not a checkpoint: {exc}") from exc
        # Mirrors submission_artifacts._require_tenant_key: the tenant segment is the caller's own.
        tenant = _caller_tenant()
        if not checkpoint.s3_key.startswith(f"{self.object_prefix}/{tenant}/"):
            raise EvalResumeIdentityError("checkpoint was saved by another tenant or outside this benchmark's prefix")
        expected = self.key(
            benchmark_id=checkpoint.benchmark_id,
            task_id=checkpoint.task_id,
            dataset=checkpoint.dataset,
            grader_identity=checkpoint.grader_identity,
            sha256=checkpoint.sha256,
        )
        if checkpoint.s3_key != expected:
            raise EvalResumeError("checkpoint s3_key does not match the key its fields derive")
        if checkpoint.size_bytes > self.max_bytes:
            raise EvalResumeError(
                f"checkpoint records {checkpoint.size_bytes} bytes, over the {self.max_bytes}-byte limit"
            )
        return checkpoint

    async def _get(self, key: str, expected_size: int) -> bytes:
        bucket = self._bucket()

        # Client creation, get, read and close run in one blocking call: creating the
        # client can fetch credentials synchronously, and a cancelled caller cannot drop
        # the response before its body is closed.
        def get() -> bytes:
            response = self._client().get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                declared = response.get("ContentLength")
                if declared is not None and declared > expected_size:
                    raise EvalResumeIntegrityError(f"stored object {key} is larger than its checkpoint records")
                # One byte past the expected size is enough to detect a longer object
                # without reading an unbounded body.
                return body.read(expected_size + 1)
            finally:
                body.close()

        return await run_blocking(get)

    def _bucket(self) -> str:
        bucket = os.environ.get(self.bucket_env)
        if not bucket:
            raise RuntimeError(
                f"{self.bucket_env} is not set; the deployment must configure eval-resume storage "
                "(benchmark-services-registry EvalResumeStorageConfig)"
            )
        return bucket

    def _client(self) -> S3Client:
        if self._s3_client is None:
            self._s3_client = _default_s3_client()
        return self._s3_client


__all__ = [
    "DEFAULT_MAX_BYTES",
    "EvalResumeCheckpoint",
    "EvalResumeError",
    "EvalResumeIdentityError",
    "EvalResumeIntegrityError",
    "EvalResumeStore",
    "S3Client",
    "benchmark_id_from_sandbox",
    "grader_identity",
]
