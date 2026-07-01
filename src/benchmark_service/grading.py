"""Decoupled sandbox grading for eval_mode() == SANDBOX benchmarks.

grade_instance provisions a fresh sandbox per the task's eval-sandbox spec,
materializes the submitted artifact when the submission is an object-store
reference, runs the benchmark's prepare_grading_sandbox hook and
evaluate_instance, collapses the stream into a V1EvalResponse, and deletes the
sandbox. It never raises: error chunks, exceptions, and timeouts all map to
status=ERROR. grade_instance_stream exposes the same lifecycle as a chunk
stream for the websocket path; an async-job wrapper can later reuse either.
"""

import asyncio
import hashlib
import logging
import math
import re
from collections.abc import AsyncGenerator
from contextlib import aclosing

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
    mid-grade; the explicit delete in the caller's finally is the real
    teardown, and stopped sandboxes auto-delete (auto_delete_interval=0)."""
    return math.ceil(timeout_s / 60) + 10


def _sandbox_name(run_id: str, task_id: str) -> str:
    """Deterministic per (run_id, task_id) so a create retry after a lost
    response reconciles by name instead of colliding. run_id is caller-supplied
    and sandbox names have a restricted charset, so slug + hash rather than raw
    interpolation."""
    digest = hashlib.sha256(f"{run_id}/{task_id}".encode()).hexdigest()[:10]
    slug = re.sub(r"[^A-Za-z0-9-]+", "-", f"{run_id}-{task_id}").strip("-").lower()[:40]
    return f"grade-{slug}-{digest}" if slug else f"grade-{digest}"


def _error_response(run_id: str, task_id: str, evaluator_version: str | None, message: str) -> V1EvalResponse:
    return V1EvalResponse(
        run_id=run_id,
        task_id=task_id,
        status=V1EvalStatus.ERROR,
        evaluator_version=evaluator_version,
        errors=[message],
    )


def _missing_grading_hook_message(service: BenchmarkService) -> str | None:
    if type(service).prepare_grading_sandbox is not BenchmarkService.prepare_grading_sandbox:
        return None
    return f"{type(service).__name__}.prepare_grading_sandbox must be implemented for eval_mode() == SANDBOX"


def _create_request(
    service: BenchmarkService,
    task: RetrieveTaskResponse,
    run_id: str,
    task_id: str,
    tenant: str,
    timeout_s: float,
    labels: dict[str, str] | None,
) -> SandboxCreateRequest:
    spec = task.eval_sandbox
    return SandboxCreateRequest(
        source=spec.source if spec is not None and spec.source is not None else task.source,
        resources=spec.resources if spec is not None and spec.resources is not None else task.resources,
        name=_sandbox_name(run_id, task_id),
        labels={
            "Benchmark": type(service).__name__,
            "Task": task_id,
            "vals-eval": "grade-instance",
            "tenant": tenant,
            "run-id": run_id,
            **(labels or {}),
        },
        env_vars=dict(spec.env_vars) if spec is not None else {},
        auto_stop_interval=_auto_stop_minutes(timeout_s),
        create_timeout=_GRADING_CREATE_TIMEOUT_S,
        network_block_all=spec.network_block_all if spec is not None else True,
        reuse=False,
    )


async def _grade_chunks(
    service: BenchmarkService,
    sandbox: Sandbox,
    request: EvaluateResponseRequest,
    dataset: str | None,
    artifact_key: str | None,
    tenant: str,
) -> AsyncGenerator[StreamChunk, None]:
    if artifact_key is not None:
        tarball = await submission_artifacts.download(artifact_key, tenant=tenant)
        await sandbox.upload_file(SUBMISSION_ARTIFACT_SANDBOX_PATH, tarball)
    await service.prepare_grading_sandbox(sandbox, request, dataset=dataset)
    async with aclosing(service.evaluate_instance(request.task_id, sandbox, dataset=dataset)) as chunks:
        async for chunk in chunks:
            yield chunk


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

    Deletion is best-effort here: if the consumer abandons the stream, the
    sandbox's auto-stop backstop (which auto-deletes on stop) reclaims it.
    """
    task = await service.retrieve_task(request.task_id, dataset=dataset)
    sandbox = await provider.create_sandbox(_create_request(service, task, run_id, request.task_id, tenant, timeout_s, labels))
    try:
        async with aclosing(_grade_chunks(service, sandbox, request, dataset, artifact_key, tenant)) as chunks:
            async for chunk in chunks:
                yield chunk
    finally:
        try:
            await asyncio.wait_for(provider.delete_sandbox(sandbox.id), _TEARDOWN_TIMEOUT_S)
        except Exception:
            logger.exception("failed to delete grading sandbox run_id=%s task_id=%s", run_id, request.task_id)


async def _provision_and_collapse(
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    request: EvaluateResponseRequest,
    provider: SandboxProvider,
    dataset: str | None,
    artifact_key: str | None,
    timeout_s: float,
    labels: dict[str, str] | None,
    evaluator_version: str | None,
    sandbox_ids: list[str],
) -> V1EvalResponse:
    task = await service.retrieve_task(request.task_id, dataset=dataset)
    sandbox = await provider.create_sandbox(_create_request(service, task, run_id, request.task_id, tenant, timeout_s, labels))
    sandbox_ids.append(sandbox.id)

    grade_timeout = task.eval_sandbox.timeout_s if task.eval_sandbox is not None and task.eval_sandbox.timeout_s else None
    result: dict[str, object] | None = None

    async def _collapse() -> V1EvalResponse | None:
        nonlocal result
        async with aclosing(_grade_chunks(service, sandbox, request, dataset, artifact_key, tenant)) as chunks:
            async for chunk in chunks:
                if isinstance(chunk, StreamErrorChunk):
                    return _error_response(run_id, request.task_id, evaluator_version, chunk.data)
                if isinstance(chunk, StreamResultChunk):
                    result = chunk.data
                elif isinstance(chunk, StreamMessageChunk):
                    logger.info("grading run_id=%s task_id=%s: %s", run_id, request.task_id, chunk.data)
                # eval-resume-state checkpoints serve the ws resume path; the
                # synchronous v1 path has no resume to feed them into.
        return None

    if grade_timeout:
        try:
            early = await asyncio.wait_for(_collapse(), grade_timeout)
        except TimeoutError:
            return _error_response(
                run_id,
                request.task_id,
                evaluator_version,
                f"grading exceeded the task's {grade_timeout}s eval-sandbox timeout",
            )
    else:
        early = await _collapse()
    if early is not None:
        return early
    if result is None:
        return _error_response(
            run_id,
            request.task_id,
            evaluator_version,
            "evaluate_instance completed without a result chunk",
        )
    return V1EvalResponse(
        run_id=run_id,
        task_id=request.task_id,
        status=V1EvalStatus.EVALUATED,
        evaluator_version=evaluator_version,
        result=jsonable_encoder(result),
        errors=[],
    )


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
    missing_hook_message = _missing_grading_hook_message(service)
    if missing_hook_message is not None:
        return _error_response(run_id, request.task_id, evaluator_version, missing_hook_message)

    sandbox_ids: list[str] = []
    try:
        # The whole operation — retrieve_task, sandbox create (and its
        # retries), artifact materialization, prepare, grade — is bounded;
        # only teardown runs outside, under its own bound.
        return await asyncio.wait_for(
            _provision_and_collapse(
                service,
                run_id,
                tenant,
                request,
                provider,
                dataset,
                artifact_key,
                timeout_s,
                labels,
                evaluator_version,
                sandbox_ids,
            ),
            timeout_s,
        )
    except TimeoutError:
        logger.warning("grade_instance timed out run_id=%s task_id=%s", run_id, request.task_id)
        return _error_response(run_id, request.task_id, evaluator_version, f"grading exceeded {timeout_s}s timeout")
    except (submission_artifacts.SubmissionArtifactNotFound, submission_artifacts.SubmissionArtifactTooLarge) as exc:
        return _error_response(run_id, request.task_id, evaluator_version, str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("grade_instance failed run_id=%s task_id=%s", run_id, request.task_id)
        return _error_response(run_id, request.task_id, evaluator_version, str(exc))
    finally:
        for sandbox_id in sandbox_ids:
            try:
                await asyncio.wait_for(provider.delete_sandbox(sandbox_id), _TEARDOWN_TIMEOUT_S)
            except Exception:
                logger.exception(
                    "failed to delete grading sandbox run_id=%s task_id=%s", run_id, request.task_id
                )
