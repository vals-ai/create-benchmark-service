"""Submission evaluation for /v1: sandbox grading and the text fold.

evaluate_submission is the one-call entry point: pick the stream for the
benchmark's eval mode (the sandbox engine for SANDBOX, the in-process
evaluate_response adapter for TEXT), fold it into one V1EvalResponse.

Chunk grammar, enforced by the engine once:

    (message | eval_resume_state)*  (result | error)   — exactly one terminal, then end

grade_instance_stream is the only sandbox-grading lifecycle: retrieve task →
resolve spec → provision → materialize artifact → hook → evaluate_instance.
It never raises, always ends with exactly one terminal chunk, separately
bounds preparation and evaluation, and tears the sandbox down under its own
bound. collapse_stream folds any chunk stream — sandbox or in-process text —
into one V1EvalResponse, so every consumer shares identical terminal
semantics. An async-job wrapper can later reuse all of these.
"""

import asyncio
import logging
import math
import re
import time
import uuid
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass
from typing import Any

from fastapi.encoders import jsonable_encoder

from benchmark_service import submission_artifacts
from benchmark_service.base import BenchmarkService
from benchmark_service.sandbox import (
    ComposeSource,
    Resources,
    Sandbox,
    SandboxCreateRequest,
    SandboxProvider,
    SnapshotSource,
)
from benchmark_service.sandbox.types import BaseSandboxSource
from benchmark_service.schemas import (
    ArtifactGradingSubmission,
    EvalMode,
    EvaluateResponseRequest,
    GradingSubmission,
    RetrieveTaskResponse,
    StreamChunk,
    StreamErrorChunk,
    StreamMessageChunk,
    StreamResultChunk,
    TextGradingSubmission,
)
from benchmark_service.v1_schemas import V1EvalResponse, V1EvalStatus

logger = logging.getLogger(__name__)

DEFAULT_GRADE_TIMEOUT_S = 1800.0
# Where the framework materializes an artifact-reference submission inside the
# grading sandbox before the benchmark's prepare_grading_sandbox hook runs.
SUBMISSION_ARTIFACT_SANDBOX_PATH = "/tmp/submission-artifact"
_GRADING_CREATE_TIMEOUT_S = 600
_PREPARATION_TIMEOUT_S = 600.0
_TEARDOWN_TIMEOUT_S = 120.0
_STREAM_CLOSE_TIMEOUT_S = 10.0
_COLLAPSE_CLOSE_TIMEOUT_S = _STREAM_CLOSE_TIMEOUT_S


def _discard_future_result(future: asyncio.Future[Any]) -> None:
    try:
        future.result()
    except BaseException:  # noqa: BLE001
        pass


def _cancel_in_background(future: asyncio.Future[Any]) -> None:
    future.cancel()
    future.add_done_callback(_discard_future_result)


async def _future_finished(future: asyncio.Future[Any], timeout_s: float) -> bool:
    done, _ = await asyncio.wait({future}, timeout=timeout_s)
    return future in done


async def _run_bounded_cleanup(
    action: Awaitable[None],
    *,
    timeout_s: float,
    action_name: str,
    run_id: str,
    task_id: str,
) -> str | None:
    future = asyncio.ensure_future(action)
    try:
        if not await _future_finished(future, timeout_s):
            _cancel_in_background(future)
            logger.warning(
                "%s exceeded %.1fs run_id=%s task_id=%s",
                action_name,
                timeout_s,
                run_id,
                task_id,
            )
            return f"{action_name} exceeded {timeout_s:g}s"
    except BaseException:  # noqa: BLE001
        _cancel_in_background(future)
        raise

    try:
        future.result()
    except asyncio.CancelledError:
        logger.warning("%s was cancelled run_id=%s task_id=%s", action_name, run_id, task_id)
        return f"{action_name} was cancelled"
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s failed run_id=%s task_id=%s", action_name, run_id, task_id)
        return str(exc)
    return None


async def _stop_pending_evaluation(
    task: asyncio.Task[StreamChunk],
    *,
    run_id: str,
    task_id: str,
) -> bool:
    task.cancel()
    try:
        if not await _future_finished(task, _STREAM_CLOSE_TIMEOUT_S):
            _cancel_in_background(task)
            logger.warning(
                "evaluation stream cancellation exceeded %.1fs run_id=%s task_id=%s",
                _STREAM_CLOSE_TIMEOUT_S,
                run_id,
                task_id,
            )
            return False
    except BaseException:  # noqa: BLE001
        _cancel_in_background(task)
        raise

    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:  # noqa: BLE001
        logger.exception("evaluation stream failed while closing run_id=%s task_id=%s", run_id, task_id)
    return True


async def _next_stream_chunk(stream: AsyncGenerator[StreamChunk, None]) -> StreamChunk:
    return await anext(stream)


def _auto_stop_minutes(budget_s: float) -> int:
    """Backstop derived from the grading deadline so it never stops a sandbox
    mid-grade; the explicit delete in grade_instance_stream is the real
    teardown, and stopped sandboxes auto-delete (auto_delete_interval=0)."""
    return math.ceil(budget_s / 60) + 10


def _sandbox_name(run_id: str, task_id: str) -> str:
    """Unique per grading request: the create-conflict recovery adopts by name,
    so a reused name could hand this request another tenant's live sandbox or
    a dirty one that survived a failed delete. run_id/task_id appear only as a
    sanitized slug for operators; attribution lives in the labels."""
    slug = re.sub(r"[^A-Za-z0-9-]+", "-", f"{run_id}-{task_id}").strip("-").lower()[:40]
    suffix = uuid.uuid4().hex[:10]
    return f"grade-{slug}-{suffix}" if slug else f"grade-{suffix}"


@dataclass(frozen=True)
class _ResolvedSpec:
    """The task's eval-sandbox spec with generation-value fallbacks applied."""

    source: BaseSandboxSource
    resources: Resources
    network_block_all: bool
    grade_timeout: float


def _resolve_grading_spec(task: RetrieveTaskResponse) -> _ResolvedSpec:
    spec = task.eval_sandbox
    source = spec.source if spec is not None and spec.source is not None else task.source
    if isinstance(source, ComposeSource):
        raise ValueError(
            "sandbox grading does not support ComposeSource; set eval_sandbox.source to an image or snapshot"
        )
    if spec is None:
        return _ResolvedSpec(source, task.resources, True, DEFAULT_GRADE_TIMEOUT_S)

    if spec.resources is not None and isinstance(source, SnapshotSource):
        raise ValueError(
            "eval_sandbox.resources cannot override a snapshot-backed sandbox; use an image source with explicit resources"
        )
    return _ResolvedSpec(
        source=source,
        resources=spec.resources if spec.resources is not None else task.resources,
        network_block_all=spec.network_block_all,
        grade_timeout=spec.timeout_s if spec.timeout_s is not None else DEFAULT_GRADE_TIMEOUT_S,
    )


async def _materialize_artifact(
    sandbox: Sandbox,
    submission: ArtifactGradingSubmission,
    tenant: str,
) -> None:
    """Download + upload in one frame so the artifact bytes (up to the download
    cap) are released as soon as they land in the sandbox."""
    tarball = await submission_artifacts.download(submission.artifact_reference, tenant=tenant)
    await sandbox.upload_file(submission.sandbox_path, tarball)


@dataclass(frozen=True)
class _GradeRun:
    """One grading request's shared context."""

    service: BenchmarkService
    run_id: str
    tenant: str
    submission: GradingSubmission
    provider: SandboxProvider
    dataset: str | None
    labels: dict[str, str] | None = None

    @property
    def task_id(self) -> str:
        return self.submission.task_id

    def create_request(self, spec: _ResolvedSpec, budget_s: float) -> SandboxCreateRequest:
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
            env_vars={},
            auto_stop_interval=_auto_stop_minutes(budget_s),
            create_timeout=_GRADING_CREATE_TIMEOUT_S,
            network_block_all=spec.network_block_all,
        )

    async def create_sandbox(self, spec: _ResolvedSpec, budget_s: float) -> Sandbox:
        return await self.provider.create_sandbox(self.create_request(spec, budget_s))

    async def prepare_sandbox(self, sandbox: Sandbox) -> None:
        if isinstance(self.submission, ArtifactGradingSubmission):
            await _materialize_artifact(sandbox, self.submission, self.tenant)
        await self.service.prepare_grading_sandbox(sandbox, self.submission, dataset=self.dataset)

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        """Best-effort bounded teardown; the auto-stop backstop reclaims what
        this misses."""
        await _run_bounded_cleanup(
            self.provider.delete_sandbox(sandbox.id),
            timeout_s=_TEARDOWN_TIMEOUT_S,
            action_name="grading sandbox deletion",
            run_id=self.run_id,
            task_id=self.task_id,
        )


async def _cleanup_grade_run(
    run: _GradeRun,
    sandbox: Sandbox | None,
    evaluation: AsyncGenerator[StreamChunk, None] | None,
    evaluation_task: asyncio.Task[StreamChunk] | None,
) -> None:
    evaluation_stopped = True
    try:
        if evaluation_task is not None:
            evaluation_stopped = await _stop_pending_evaluation(
                evaluation_task,
                run_id=run.run_id,
                task_id=run.task_id,
            )
        if evaluation is not None and evaluation_stopped:
            await _run_bounded_cleanup(
                evaluation.aclose(),
                timeout_s=_STREAM_CLOSE_TIMEOUT_S,
                action_name="evaluation stream close",
                run_id=run.run_id,
                task_id=run.task_id,
            )
    finally:
        if sandbox is not None:
            await run.delete_sandbox(sandbox)


async def _finish_grade_cleanup(
    run: _GradeRun,
    sandbox: Sandbox | None,
    evaluation: AsyncGenerator[StreamChunk, None] | None,
    evaluation_task: asyncio.Task[StreamChunk] | None,
) -> None:
    cleanup_task = asyncio.create_task(_cleanup_grade_run(run, sandbox, evaluation, evaluation_task))
    cancellation: asyncio.CancelledError | None = None
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except Exception:  # noqa: BLE001
            break

    try:
        cleanup_task.result()
    except asyncio.CancelledError:
        if cancellation is None:
            raise
    except Exception:  # noqa: BLE001
        logger.exception("grading cleanup failed run_id=%s task_id=%s", run.run_id, run.task_id)
    if cancellation is not None:
        raise cancellation


async def grade_instance_stream(
    *,
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    submission: GradingSubmission,
    provider: SandboxProvider,
    dataset: str | None,
    labels: dict[str, str] | None = None,
) -> AsyncGenerator[StreamChunk, None]:
    """The only sandbox-grading lifecycle. Never raises; yields exactly one
    terminal (result | error) chunk and nothing after it.

    Task retrieval, provisioning, materialization, and the hook share a
    preparation timeout. evaluate_instance gets a fresh task-declared grade
    timeout. Teardown runs outside both under its own bound.
    """
    run = _GradeRun(
        service=service,
        run_id=run_id,
        tenant=tenant,
        submission=submission,
        provider=provider,
        dataset=dataset,
        labels=labels,
    )
    sandbox: Sandbox | None = None
    evaluation: AsyncGenerator[StreamChunk, None] | None = None
    evaluation_task: asyncio.Task[StreamChunk] | None = None
    terminal: StreamResultChunk | StreamErrorChunk | None = None
    try:
        try:
            async with asyncio.timeout(_PREPARATION_TIMEOUT_S):
                task = await run.service.retrieve_task(run.task_id, dataset=run.dataset)
                spec = _resolve_grading_spec(task)
                budget_s = _PREPARATION_TIMEOUT_S + spec.grade_timeout
                sandbox = await run.create_sandbox(spec, budget_s)
                await run.prepare_sandbox(sandbox)
        except TimeoutError:
            logger.warning("grading preparation timed out run_id=%s task_id=%s", run_id, run.task_id)
            terminal = StreamErrorChunk(
                type="error",
                data=f"grading preparation exceeded {_PREPARATION_TIMEOUT_S:g}s",
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("grading preparation failed run_id=%s task_id=%s", run_id, run.task_id)
            terminal = StreamErrorChunk(type="error", data=str(exc))
        else:
            evaluation = run.service.evaluate_instance(run.task_id, sandbox, dataset=run.dataset)
            deadline = time.monotonic() + spec.grade_timeout
            timeout_message = f"grading exceeded the {spec.grade_timeout:g}s grade timeout"
            while terminal is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning("grading timed out run_id=%s task_id=%s", run_id, run.task_id)
                    terminal = StreamErrorChunk(type="error", data=timeout_message)
                    break
                evaluation_task = asyncio.create_task(_next_stream_chunk(evaluation))
                try:
                    if not await _future_finished(evaluation_task, remaining):
                        logger.warning("grading timed out run_id=%s task_id=%s", run_id, run.task_id)
                        terminal = StreamErrorChunk(type="error", data=timeout_message)
                        break
                    completed_task = evaluation_task
                    evaluation_task = None
                    chunk = completed_task.result()
                except StopAsyncIteration:
                    terminal = StreamErrorChunk(
                        type="error", data="evaluate_instance completed without a result chunk"
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    logger.exception("grading failed run_id=%s task_id=%s", run_id, run.task_id)
                    terminal = StreamErrorChunk(type="error", data=str(exc))
                    break
                if isinstance(chunk, StreamResultChunk | StreamErrorChunk):
                    terminal = chunk
                    break
                yield chunk
    finally:
        await _finish_grade_cleanup(run, sandbox, evaluation, evaluation_task)

    yield terminal


async def collapse_stream(
    stream: AsyncGenerator[StreamChunk, None],
    *,
    run_id: str,
    task_id: str,
    evaluator_version: str | None,
) -> V1EvalResponse:
    """Fold a chunk stream into one V1EvalResponse; first terminal wins."""

    def error(message: str) -> V1EvalResponse:
        return V1EvalResponse(
            run_id=run_id,
            task_id=task_id,
            status=V1EvalStatus.ERROR,
            evaluator_version=evaluator_version,
            errors=[message],
        )

    response: V1EvalResponse | None = None
    try:
        async for chunk in stream:
            if isinstance(chunk, StreamErrorChunk):
                response = error(chunk.data)
                break
            if isinstance(chunk, StreamResultChunk):
                response = V1EvalResponse(
                    run_id=run_id,
                    task_id=task_id,
                    status=V1EvalStatus.EVALUATED,
                    evaluator_version=evaluator_version,
                    result=jsonable_encoder(chunk.data),
                    errors=[],
                )
                break
            if isinstance(chunk, StreamMessageChunk):
                logger.info("evaluation run_id=%s task_id=%s: %s", run_id, task_id, chunk.data)
            # eval_resume_state checkpoints need a store to be useful; a
            # synchronous collapse has none, so they are skipped.
    except Exception as exc:  # noqa: BLE001
        logger.exception("evaluation failed run_id=%s task_id=%s", run_id, task_id)
        response = error(str(exc))

    cleanup_error = await _run_bounded_cleanup(
        stream.aclose(),
        timeout_s=_COLLAPSE_CLOSE_TIMEOUT_S,
        action_name="evaluation response stream close",
        run_id=run_id,
        task_id=task_id,
    )
    if response is None and cleanup_error is not None:
        response = error(cleanup_error)

    return response or error("evaluation completed without a result chunk")


async def evaluate_submission(
    *,
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    submission: GradingSubmission,
    provider: SandboxProvider | None,
    evaluator_version: str | None,
    dataset: str | None,
    labels: dict[str, str] | None = None,
) -> V1EvalResponse:
    """Evaluate one typed submission per the benchmark's eval mode."""
    if service.eval_mode == EvalMode.SANDBOX:
        if provider is None:
            raise ValueError("eval_mode == EvalMode.SANDBOX requires a sandbox provider")
        stream = grade_instance_stream(
            service=service,
            run_id=run_id,
            tenant=tenant,
            submission=submission,
            provider=provider,
            dataset=dataset,
            labels=labels,
        )
    else:
        if not isinstance(submission, TextGradingSubmission):
            raise ValueError("text evaluation requires a text submission")
        request = EvaluateResponseRequest(
            task_id=submission.task_id,
            response=submission.text,
            dataset=dataset,
        )
        stream = service.stream_evaluate_response(request, dataset=dataset)
    return await collapse_stream(
        stream,
        run_id=run_id,
        task_id=submission.task_id,
        evaluator_version=evaluator_version,
    )
