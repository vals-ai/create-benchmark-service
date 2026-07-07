"""Decoupled sandbox grading for eval_mode == SANDBOX benchmarks.

Chunk grammar, enforced by the engine once:

    (message | eval_resume_state)*  (result | error)   — exactly one terminal, then end

grade_instance_stream is the only sandbox-grading lifecycle: retrieve task →
resolve spec → provision → materialize artifact → hook → evaluate_instance.
It never raises, always ends with exactly one terminal chunk, bounds
everything under one spec-derived deadline, and tears the sandbox down
outside that deadline under its own bound. collapse_stream folds any chunk
stream — sandbox or in-process text — into one V1EvalResponse, so every
consumer shares identical terminal semantics. An async-job wrapper can later
reuse both.
"""

import asyncio
import logging
import math
import re
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import aclosing
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
# Extra deadline allowance for provisioning and artifact materialization, so
# the task's grade timeout budgets the grade itself, not sandbox startup.
_PROVISIONING_ALLOWANCE_S = 600.0
_TEARDOWN_TIMEOUT_S = 120.0


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

    source: Any
    resources: Any
    network_block_all: bool
    env_vars: dict[str, str]
    grade_timeout: float


def _resolve_grading_spec(task: RetrieveTaskResponse) -> _ResolvedSpec:
    spec = task.eval_sandbox
    if spec is None:
        return _ResolvedSpec(task.source, task.resources, True, {}, DEFAULT_GRADE_TIMEOUT_S)
    return _ResolvedSpec(
        source=spec.source if spec.source is not None else task.source,
        resources=spec.resources if spec.resources is not None else task.resources,
        network_block_all=spec.network_block_all,
        env_vars=dict(spec.env_vars),
        grade_timeout=spec.timeout_s if spec.timeout_s is not None else DEFAULT_GRADE_TIMEOUT_S,
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
    artifact_key: str | None = None
    labels: dict[str, str] | None = None

    @property
    def task_id(self) -> str:
        return self.request.task_id

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
            env_vars=spec.env_vars,
            auto_stop_interval=_auto_stop_minutes(budget_s),
            create_timeout=_GRADING_CREATE_TIMEOUT_S,
            network_block_all=spec.network_block_all,
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

    async def delete_sandbox(self, sandbox: Sandbox) -> None:
        """Best-effort bounded teardown; the auto-stop backstop reclaims what
        this misses."""
        try:
            await asyncio.wait_for(self.provider.delete_sandbox(sandbox.id), _TEARDOWN_TIMEOUT_S)
        except Exception:
            logger.exception(
                "failed to delete grading sandbox run_id=%s task_id=%s", self.run_id, self.task_id
            )


async def grade_instance_stream(
    *,
    service: BenchmarkService,
    run_id: str,
    tenant: str,
    request: EvaluateResponseRequest,
    provider: SandboxProvider,
    dataset: str | None,
    artifact_key: str | None = None,
    labels: dict[str, str] | None = None,
) -> AsyncGenerator[StreamChunk, None]:
    """The only sandbox-grading lifecycle. Never raises; yields exactly one
    terminal (result | error) chunk and nothing after it.

    Provisioning, materialization, the hook, and the grade all run under one
    deadline derived from the task's eval-sandbox spec (timeout_s or the
    default, plus a provisioning allowance). Teardown runs outside the
    deadline under its own bound, so a grade that completes near the deadline
    keeps its result and its delete.
    """
    run = _GradeRun(
        service=service,
        run_id=run_id,
        tenant=tenant,
        request=request,
        provider=provider,
        dataset=dataset,
        artifact_key=artifact_key,
        labels=labels,
    )
    try:
        task = await run.service.retrieve_task(run.task_id, dataset=run.dataset)
        spec = _resolve_grading_spec(task)
    except Exception as exc:  # noqa: BLE001
        logger.exception("grading failed to resolve task run_id=%s task_id=%s", run_id, run.task_id)
        yield StreamErrorChunk(type="error", data=str(exc))
        return

    budget_s = spec.grade_timeout + _PROVISIONING_ALLOWANCE_S
    deadline = time.monotonic() + budget_s
    timeout_message = (
        f"grading exceeded the {spec.grade_timeout}s grade timeout "
        f"(+{_PROVISIONING_ALLOWANCE_S}s provisioning allowance)"
    )

    try:
        sandbox = await asyncio.wait_for(
            run.provider.create_sandbox(run.create_request(spec, budget_s)),
            deadline - time.monotonic(),
        )
    except TimeoutError:
        logger.warning("grading sandbox create timed out run_id=%s task_id=%s", run_id, run.task_id)
        yield StreamErrorChunk(type="error", data=timeout_message)
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception("grading sandbox create failed run_id=%s task_id=%s", run_id, run.task_id)
        yield StreamErrorChunk(type="error", data=str(exc))
        return

    try:
        async with aclosing(run.chunks(sandbox)) as chunks:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    yield StreamErrorChunk(type="error", data=timeout_message)
                    return
                try:
                    chunk = await asyncio.wait_for(anext(chunks), remaining)
                except StopAsyncIteration:
                    yield StreamErrorChunk(
                        type="error", data="evaluate_instance completed without a result chunk"
                    )
                    return
                except TimeoutError:
                    logger.warning("grading timed out run_id=%s task_id=%s", run_id, run.task_id)
                    yield StreamErrorChunk(type="error", data=timeout_message)
                    return
                except Exception as exc:  # noqa: BLE001
                    logger.exception("grading failed run_id=%s task_id=%s", run_id, run.task_id)
                    yield StreamErrorChunk(type="error", data=str(exc))
                    return
                yield chunk
                if isinstance(chunk, StreamResultChunk | StreamErrorChunk):
                    return
    finally:
        # Runs on completion, on consumer abandonment (GeneratorExit), and on
        # outer cancellation — and never inside the grading deadline.
        await run.delete_sandbox(sandbox)


async def collapse_stream(
    stream: AsyncGenerator[StreamChunk, None],
    *,
    run_id: str,
    task_id: str,
    evaluator_version: str | None,
) -> V1EvalResponse:
    """Fold a chunk stream into one V1EvalResponse: the first terminal chunk
    (result | error) wins and the stream is closed. Never raises."""

    def error(message: str) -> V1EvalResponse:
        return V1EvalResponse(
            run_id=run_id,
            task_id=task_id,
            status=V1EvalStatus.ERROR,
            evaluator_version=evaluator_version,
            errors=[message],
        )

    try:
        async with aclosing(stream) as chunks:
            async for chunk in chunks:
                if isinstance(chunk, StreamErrorChunk):
                    return error(chunk.data)
                if isinstance(chunk, StreamResultChunk):
                    return V1EvalResponse(
                        run_id=run_id,
                        task_id=task_id,
                        status=V1EvalStatus.EVALUATED,
                        evaluator_version=evaluator_version,
                        result=jsonable_encoder(chunk.data),
                        errors=[],
                    )
                if isinstance(chunk, StreamMessageChunk):
                    logger.info("evaluation run_id=%s task_id=%s: %s", run_id, task_id, chunk.data)
                # eval_resume_state checkpoints need a store to be useful; a
                # synchronous collapse has none, so they are skipped.
    except Exception as exc:  # noqa: BLE001
        logger.exception("evaluation failed run_id=%s task_id=%s", run_id, task_id)
        return error(str(exc))
    return error("evaluation completed without a result chunk")


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
    labels: dict[str, str] | None = None,
) -> V1EvalResponse:
    """Grade one submission in a fresh sandbox and fold the stream; never raises."""
    return await collapse_stream(
        grade_instance_stream(
            service=service,
            run_id=run_id,
            tenant=tenant,
            request=request,
            provider=provider,
            dataset=dataset,
            artifact_key=artifact_key,
            labels=labels,
        ),
        run_id=run_id,
        task_id=request.task_id,
        evaluator_version=evaluator_version,
    )
