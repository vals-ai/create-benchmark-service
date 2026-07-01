"""Decoupled sandbox grading for the /v1/evaluate SANDBOX path.

grade_instance provisions a fresh, network-isolated sandbox from the task's
image, injects the submitted artifact via the benchmark's prepare_grading_sandbox
hook, runs evaluate_instance, collapses the stream into a V1EvalResponse, and
deletes the sandbox. It never raises: error chunks, exceptions, and timeouts all
map to status=ERROR. The async-job model (B) can later wrap this same function.
"""

import asyncio
import logging
from typing import Any

from fastapi.encoders import jsonable_encoder

from benchmark_service.base import BenchmarkService
from benchmark_service.sandbox import SandboxCreateRequest, SandboxProviderConfig
from benchmark_service.schemas import EvaluateResponseRequest, StreamErrorChunk, StreamResultChunk
from benchmark_service.v1_schemas import V1EvalResponse, V1EvalStatus

logger = logging.getLogger(__name__)

DEFAULT_GRADE_TIMEOUT_S = 1800.0
# Backstop auto-stop in MINUTES; must exceed the grade timeout so it never stops a
# sandbox mid-grade. Teardown is explicit (delete in finally); this only matters if
# that delete fails.
_GRADING_AUTO_STOP_MINUTES = 60
_GRADING_CREATE_TIMEOUT_S = 600


def _error_response(run_id: str, task_id: str, evaluator_version: str | None, message: str) -> V1EvalResponse:
    return V1EvalResponse(
        run_id=run_id,
        task_id=task_id,
        status=V1EvalStatus.ERROR,
        evaluator_version=evaluator_version,
        errors=[message],
    )


async def _prepare_and_grade(
    service: BenchmarkService,
    sandbox: Any,
    run_id: str,
    request: EvaluateResponseRequest,
    evaluator_version: str | None,
    dataset: str | None,
) -> V1EvalResponse:
    await service.prepare_grading_sandbox(sandbox, request, dataset=dataset)
    result: dict[str, Any] | None = None
    async for chunk in service.evaluate_instance(request.task_id, sandbox, dataset=dataset):
        if isinstance(chunk, StreamErrorChunk):
            return _error_response(run_id, request.task_id, evaluator_version, chunk.data)
        if isinstance(chunk, StreamResultChunk):
            result = chunk.data
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
    request: EvaluateResponseRequest,
    sandbox_config: SandboxProviderConfig,
    evaluator_version: str | None,
    dataset: str | None,
    timeout_s: float = DEFAULT_GRADE_TIMEOUT_S,
) -> V1EvalResponse:
    try:
        task = await service.retrieve_task(request.task_id, dataset=dataset)
        create_request = SandboxCreateRequest(
            source=task.source,
            resources=task.resources,
            name=f"grade-{run_id}-{request.task_id}",
            labels={"vals-eval": "grade-instance", "run-id": run_id},
            env_vars={},
            auto_stop_interval=_GRADING_AUTO_STOP_MINUTES,
            create_timeout=_GRADING_CREATE_TIMEOUT_S,
            network_block_all=True,
        )
        async with sandbox_config.create_provider() as provider:
            sandbox = await provider.create_sandbox(create_request)
            try:
                return await asyncio.wait_for(
                    _prepare_and_grade(service, sandbox, run_id, request, evaluator_version, dataset),
                    timeout_s,
                )
            finally:
                try:
                    await provider.delete_sandbox(sandbox.id)
                except Exception:
                    logger.exception("failed to delete grading sandbox run_id=%s task_id=%s", run_id, request.task_id)
    except TimeoutError:
        logger.warning("grade_instance timed out run_id=%s task_id=%s", run_id, request.task_id)
        return _error_response(run_id, request.task_id, evaluator_version, f"grading exceeded {timeout_s}s timeout")
    except Exception as exc:  # noqa: BLE001
        logger.exception("grade_instance failed run_id=%s task_id=%s", run_id, request.task_id)
        return _error_response(run_id, request.task_id, evaluator_version, str(exc))
