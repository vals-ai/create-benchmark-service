"""Durable grading input for eval-only retry.

A benchmark that grades after its agent finishes can lose the grade to a judge
error or a dropped connection. Before grading, the service saves the exact bytes
it is about to grade to S3 and streams a small pointer to them as
``eval_resume_state``. On ``/ws/evaluate-response`` the service loads those
bytes back, checks their size and SHA-256 against the pointer, and refuses the
retry when the judge it would use now is not the judge the pointer was saved
under, so a retry grades what the first attempt would have graded.

The bucket comes from the environment variable that the registry's
``EvalResumeStorageConfig.bucket_env`` names for the service; the store's
``object_prefix`` matches that config's ``object_prefix`` without the ``/*``.

Usage::

    STORE = EvalResumeStore(bucket_env="MY_EVAL_STATE_BUCKET", object_prefix="my-bench/eval-resume")

    # evaluate_instance, before the judge call
    checkpoint = await STORE.save(
        benchmark_id=benchmark_id_from_sandbox(sandbox),
        task_id=task_id,
        dataset=dataset_name,
        content=grading_input,
        judge_identity=judge_identity({"model": eval_model, "test_revision": test_revision}),
    )
    yield StreamEvalResumeStateChunk(type="eval_resume_state", data=checkpoint.model_dump(mode="json"))

    # stream_evaluate_response, when request.eval_resume_state is set
    grading_input = await STORE.load(
        request.eval_resume_state, task_id=request.task_id, dataset=dataset_name, judge_identity=current_identity
    )
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from functools import partial
from typing import Any, Literal, NotRequired, Protocol, TypedDict, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from benchmark_service.blocking import run_blocking
from benchmark_service.sandbox import Sandbox

REGION_ENV = "AWS_REGION"
DEFAULT_MAX_BYTES = 256 * 1024**2
RUN_ID_LABEL = "Id"

_SAFE_COMPONENT = re.compile(r"^(?!\.{1,2}$)[A-Za-z0-9_.-]+$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class EvalResumeError(ValueError):
    """A checkpoint cannot be resumed; the task needs a fresh run instead."""


class EvalResumeIdentityError(EvalResumeError):
    """The checkpoint was saved for a different task, dataset, or judge."""


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
    judge_identity: str

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

    @field_validator("sha256", "judge_identity")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("checkpoint digests must be lowercase SHA-256 hex")
        return value


def judge_identity(parts: Mapping[str, Any]) -> str:
    """SHA-256 of everything besides the agent's output that decides a grade.

    Pass the judge model, the test or rubric revision, and anything else whose
    change should make a saved checkpoint ungradeable. Values must be JSON
    serializable; key order does not matter.
    """
    encoded = json.dumps(parts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _key_component(value: str) -> str:
    """``value`` when it is a safe S3 path segment, else a digest of it (an org-style ``org/name`` id)."""
    if _SAFE_COMPONENT.fullmatch(value):
        return value
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def benchmark_id_from_sandbox(sandbox: Sandbox) -> str:
    """The Valkyrie run id the sandbox is labelled with, else the sandbox's own id."""
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
    import boto3

    return cast(
        S3Client,
        boto3.client("s3", region_name=os.environ.get(REGION_ENV)),  # pyright: ignore[reportUnknownMemberType]
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
        if not all(_SAFE_COMPONENT.fullmatch(segment) for segment in prefix.split("/")):
            raise ValueError(f"object_prefix {object_prefix!r} must be '/'-separated safe path segments")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.bucket_env = bucket_env
        self.object_prefix = prefix
        self.max_bytes = max_bytes
        self.content_type = content_type
        self._s3_client = s3_client

    def key(self, benchmark_id: str, task_id: str, sha256: str) -> str:
        """The only key a checkpoint with these fields may point at. A task id that is not a
        safe path segment (``org/name``) is stored under a digest of it; the checkpoint keeps it as is."""
        return f"{self.object_prefix}/{benchmark_id}/{_key_component(task_id)}/{sha256}"

    async def save(
        self,
        *,
        benchmark_id: str,
        task_id: str,
        dataset: str,
        content: bytes,
        judge_identity: str,
    ) -> EvalResumeCheckpoint:
        """Write ``content`` to S3 and return the checkpoint that points at it."""
        if len(content) > self.max_bytes:
            raise ValueError(f"grading input is {len(content)} bytes, over the {self.max_bytes}-byte checkpoint limit")
        sha256 = hashlib.sha256(content).hexdigest()
        checkpoint = EvalResumeCheckpoint(
            benchmark_id=benchmark_id,
            task_id=task_id,
            dataset=dataset,
            s3_key=self.key(benchmark_id, task_id, sha256),
            sha256=sha256,
            size_bytes=len(content),
            judge_identity=judge_identity,
        )
        bucket = self._bucket()
        client = self._client()
        await run_blocking(
            partial(
                client.put_object,
                Bucket=bucket,
                Key=checkpoint.s3_key,
                Body=content,
                ContentType=self.content_type,
            )
        )
        return checkpoint

    async def load(
        self,
        state: Mapping[str, Any] | EvalResumeCheckpoint,
        *,
        task_id: str,
        dataset: str,
        judge_identity: str,
    ) -> bytes:
        """Return the saved grading input after checking whose it is and that it is intact.

        Raises ``EvalResumeIdentityError`` when the checkpoint belongs to another
        task, dataset, or judge, and ``EvalResumeIntegrityError`` when the stored
        bytes differ from what was saved.
        """
        checkpoint = self.parse(state)
        if checkpoint.task_id != task_id or checkpoint.dataset != dataset:
            raise EvalResumeIdentityError(
                f"checkpoint is for task {checkpoint.task_id} in dataset {checkpoint.dataset}, "
                f"not task {task_id} in dataset {dataset}"
            )
        if checkpoint.judge_identity != judge_identity:
            raise EvalResumeIdentityError(
                f"checkpoint for task {task_id} was saved under a different judge; "
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
        """Validate ``eval_resume_state`` as a checkpoint this store wrote."""
        try:
            checkpoint = (
                state if isinstance(state, EvalResumeCheckpoint) else EvalResumeCheckpoint.model_validate(dict(state))
            )
        except ValidationError as exc:
            raise EvalResumeError(f"eval_resume_state is not a checkpoint: {exc}") from exc
        if checkpoint.s3_key != self.key(checkpoint.benchmark_id, checkpoint.task_id, checkpoint.sha256):
            raise EvalResumeError("checkpoint s3_key is outside this benchmark's eval-resume prefix")
        if checkpoint.size_bytes > self.max_bytes:
            raise EvalResumeError(
                f"checkpoint records {checkpoint.size_bytes} bytes, over the {self.max_bytes}-byte limit"
            )
        return checkpoint

    async def _get(self, key: str, expected_size: int) -> bytes:
        bucket = self._bucket()
        client = self._client()

        # Get, read and close run in one blocking call, so a cancelled caller cannot
        # drop the response before its body is closed.
        def get() -> bytes:
            response = client.get_object(Bucket=bucket, Key=key)
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
    "judge_identity",
]
