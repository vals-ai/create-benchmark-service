"""Decoupled sandbox grading for eval_mode() == SANDBOX benchmarks.

The module is organized around one request's worth of context (_GradeRun):

    grade_instance         never-raises boundary: hook guard, outer timeout,
                           exception→ERROR mapping (the /v1 path)
    grade_instance_stream  the same lifecycle as a chunk stream (the ws path)
    _graded                retrieve task → resolve spec → sandbox → collapse
    _GradeRun.sandbox      provision + bounded best-effort delete
    _GradeRun.chunks       materialize artifact → hook → evaluate_instance
    _collapse              fold the chunk stream into one V1EvalResponse
"""

import asyncio
import hashlib
import logging
import math
import re
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi.encoders import jsonable_encoder

from benchmark_service import submission_artifacts
from benchmark_service.base import BenchmarkService
from benchmark_service.sandbox import Sandbox, SandboxCreateRequest, SandboxProvider
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    RetrieveTaskResponse,
    StreamChunk,
    StreamErrorChunk,
    StreamMessageChunk,
    StreamResultChunk,
)
from benchmark_service.v1_schemas import V1EvalResponse, V1EvalStatus

logger = logging.getLogger(__name__)

DEFAULT_GRADE_TIMEOUT_S = 1800.0
# Where the framework materializes an artifact-reference submission inside the
# grading sandbox before the benchmark's prepare_grading_sandbox hook runs.
SUBMISSION_ARTIFACT_SANDBOX_PATH = "/tmp/submission-artifact"
_GRADING_CREATE_TIMEOUT_S = 600
_TEARDOWN_TIMEOUT_S = 120.0


def _auto_stop_minutes(timeout_s: float) -> int:
    """Backstop that must exceed the grade timeout so it never stops a sandbox
    mid-grade; the explicit delete in _GradeRun.sandbox is the real teardown,
    and stopped sandboxes auto-delete (auto_delete_interval=0)."""
    return math.ceil(timeout_s / 60) + 10


def _sandbox_name(run_id: str, task_id: str) -> str:
    """Deterministic per (run_id, task_id) so a create retry after a lost
    response reconciles by name instead of colliding. run_id is caller-supplied
    and sandbox names have a restricted charset, so slug + hash rather than raw
    interpolation."""
    digest = hashlib.sha256(f"{run_id}/{task_id}".encode()).hexdigest()[:10]
    slug = re.sub(r"[^A-Za-z0-9-]+", "-", f"{run_id}-{task_id}").strip("-").lower()[:40]
    return f"grade-{slug}-{digest}" if slug else f"grade-{digest}"


def grading_hook_missing_message(service: BenchmarkService) -> str | None:
    if type(service).prepare_grading_sandbox is not BenchmarkService.prepare_grading_sandbox:
        return None
    return f"{type(service).__name__}.prepare_grading_sandbox must be implemented for eval_mode() == SANDBOX"


@dataclass(frozen=True)
class _ResolvedSpec:
    """The task's eval-sandbox spec with generation-value fallbacks applied."""

    source: Any
    resources: Any
    network_block_all: bool
    env_vars: dict[str, str]
    grade_timeout: float | None


def _resolve_grading_spec(task: RetrieveTaskResponse) -> _ResolvedSpec:
    spec = task.eval_sandbox
    if spec is None:
        return _ResolvedSpec(task.source, task.resources, True, {}, None)
    return _ResolvedSpec(
        source=spec.source if spec.source is not None else task.source,
        resources=spec.resources if spec.resources is not None else task.resources,
        network_block_all=spec.network_block_all,
        env_vars=dict(spec.env_vars),
        grade_timeout=spec.timeout_s,
    )


async def _materialize_artifact(sandbox: Sandbox, key: str, tenant: str) -> None:
    """Download + upload in one frame so the artifact bytes (up to the download
    cap) are released as soon as they land in the sandbox."""
    tarball = await submission_artifacts.download(key, tenant=tenant)
    await sandbox.upload_file(SUBMISSION_ARTIFACT_SANDBOX_PATH, tarball)


@dataclass(frozen=True)
class _GradeRun:
    """One grading request's shared context."""

    service: BenchmarkService
    run_id: str
    tenant: str
    request: EvaluateResponseRequest
    provider: SandboxProvider
    dataset: str | None
    evaluator_version: str | None = None
    artifact_key: str | None = None
    timeout_s: float = DEFAULT_GRADE_TIMEOUT_S
    labels: dict[str, str] | None = None

    @property
    def task_id(self) -> str:
        return self.request.task_id

    def error(self, message: str) -> V1EvalResponse:
        return V1EvalResponse(
            run_id=self.run_id,
            task_id=self.task_id,
            status=V1EvalStatus.ERROR,
            evaluator_version=self.evaluator_version,
            errors=[message],
        )

    def evaluated(self, result: dict[str, Any]) -> V1EvalResponse:
        return V1EvalResponse(
            run_id=self.run_id,
            task_id=self.task_id,
            status=V1EvalStatus.EVALUATED,
            evaluator_version=self.evaluator_version,
            result=jsonable_encoder(result),
            errors=[],
        )

    def create_request(self, spec: _ResolvedSpec) -> SandboxCreateRequest:
        return SandboxCreateRequest(
            source=spec.source,
            resources=spec.resources,
            name=_sandbox_name(self.run_id, self.task_id),
            labels={
                "Benchmark": type(self.service).__name__,
                "Task": self.task_id,
                "vals-eval": "grade-instance",
                "tenant": self.tenant,
                "run-id": self.run_id,
                **(self.labels or {}),
            },
            env_vars=spec.env_vars,
            auto_stop_interval=_auto_stop_minutes(self.timeout_s),
            create_timeout=_GRADING_CREATE_TIMEOUT_S,
            network_block_all=spec.network_block_all,
            reuse=False,
        )

    @asynccontextmanager
    async def sandbox(self, spec: _ResolvedSpec) -> AsyncGenerator[Sandbox, None]:
        """Provision the grading sandbox; always attempt a bounded delete.

        The delete runs on success, on error, and while unwinding an outer
        wait_for cancellation (asyncio delivers cancellation once, then awaits
        in finally blocks complete normally). It must catch Exception, not
        BaseException, so a propagating CancelledError is never swallowed.
        """
        sandbox = await self.provider.create_sandbox(self.create_request(spec))
        try:
            yield sandbox
        finally:
            try:
                await asyncio.wait_for(self.provider.delete_sandbox(sandbox.id), _TEARDOWN_TIMEOUT_S)
            except Exception:
                logger.exception(
                    "failed to delete grading sandbox run_id=%s task_id=%s", self.run_id, self.task_id
                )

    async def chunks(self, sandbox: Sandbox) -> AsyncGenerator[StreamChunk, None]:
        """The in-sandbox phase: materialize the submission, run the hook, then
        pass evaluate_instance's chunks through."""
        if self.artifact_key is not None:
            await _materialize_artifact(sandbox, self.artifact_key, self.tenant)
        await self.service.prepare_grading_sandbox(sandbox, self.request, dataset=self.dataset)
        async with aclosing(self.service.evaluate_instance(self.task_id, sandbox, dataset=self.dataset)) as chunks:
            async for chunk in chunks:
                yield chunk


async def _collapse(run: _GradeRun, sandbox: Sandbox) -> V1EvalResponse:
    """Fold the chunk stream into a single V1EvalResponse."""
    result: dict[str, Any] | None = None
    async with aclosing(run.chunks(sandbox)) as chunks:
        async for chunk in chunks:
            if isinstance(chunk, StreamErrorChunk):
                return run.error(chunk.data)
            if isinstance(chunk, StreamResultChunk):
                result = chunk.data
            elif isinstance(chunk, StreamMessageChunk):
                logger.info("grading run_id=%s task_id=%s: %s", run.run_id, run.task_id, chunk.data)
            # eval-resume-state checkpoints serve the ws resume path; the
            # synchronous v1 path has no resume to feed them into.
    if result is None:
        return run.error("evaluate_instance completed without a result chunk")
    return run.evaluated(result)


async def _graded(run: _GradeRun) -> V1EvalResponse:
    task = await run.service.retrieve_task(run.task_id, dataset=run.dataset)
    spec = _resolve_grading_spec(task)
    async with run.sandbox(spec) as sandbox:
        if spec.grade_timeout:
            try:
                return await asyncio.wait_for(_collapse(run, sandbox), spec.grade_timeout)
            except TimeoutError:
                return run.error(f"grading exceeded the task's {spec.grade_timeout}s eval-sandbox timeout")
        return await _collapse(run, sandbox)


async def grade_instance(
    *,
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    request: EvaluateResponseRequest,
    provider: SandboxProvider,
    evaluator_version: str | None,
    dataset: str | None,
    artifact_key: str | None = None,
    timeout_s: float = DEFAULT_GRADE_TIMEOUT_S,
    labels: dict[str, str] | None = None,
) -> V1EvalResponse:
    """Grade one submission in a fresh sandbox; never raises.

    The whole operation — retrieve_task, sandbox create (and its retries),
    artifact materialization, hook, grade — runs inside the timeout; sandbox
    deletion is bounded separately inside _GradeRun.sandbox.
    """
    run = _GradeRun(
        service=service,
        run_id=run_id,
        tenant=tenant,
        request=request,
        provider=provider,
        dataset=dataset,
        evaluator_version=evaluator_version,
        artifact_key=artifact_key,
        timeout_s=timeout_s,
        labels=labels,
    )
    missing_hook = grading_hook_missing_message(service)
    if missing_hook is not None:
        return run.error(missing_hook)
    try:
        return await asyncio.wait_for(_graded(run), timeout_s)
    except TimeoutError:
        logger.warning("grade_instance timed out run_id=%s task_id=%s", run_id, request.task_id)
        return run.error(f"grading exceeded {timeout_s}s timeout")
    except (submission_artifacts.SubmissionArtifactNotFound, submission_artifacts.SubmissionArtifactTooLarge) as exc:
        return run.error(str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("grade_instance failed run_id=%s task_id=%s", run_id, request.task_id)
        return run.error(str(exc))


async def grade_instance_stream(
    *,
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    request: EvaluateResponseRequest,
    provider: SandboxProvider,
    dataset: str | None,
    artifact_key: str | None = None,
    timeout_s: float = DEFAULT_GRADE_TIMEOUT_S,
    labels: dict[str, str] | None = None,
) -> AsyncGenerator[StreamChunk, None]:
    """Sandbox-grading lifecycle as a chunk stream (websocket path).

    No wall-clock bound is applied here — the ws consumer owns pacing, and an
    abandoned stream is reclaimed by the sandbox's auto-stop backstop. The
    task's eval-spec grade timeout is likewise not enforced on this path.
    """
    run = _GradeRun(
        service=service,
        run_id=run_id,
        tenant=tenant,
        request=request,
        provider=provider,
        dataset=dataset,
        artifact_key=artifact_key,
        timeout_s=timeout_s,
        labels=labels,
    )
    task = await run.service.retrieve_task(run.task_id, dataset=run.dataset)
    spec = _resolve_grading_spec(task)
    async with run.sandbox(spec) as sandbox:
        async with aclosing(run.chunks(sandbox)) as chunks:
            async for chunk in chunks:
                yield chunk
