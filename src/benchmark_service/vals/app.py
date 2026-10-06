"""Vals provider-facing API and hosted policy built on the shared benchmark app."""

import asyncio
import logging
import os
import re
from collections import Counter
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import HTTPException, Request, WebSocket
from fastapi.encoders import jsonable_encoder

from benchmark_service import submission_artifacts
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL, is_auth_required
from benchmark_service.grading import SUBMISSION_ARTIFACT_SANDBOX_PATH, collapse_stream, evaluate_submission
from benchmark_service.inflight import InflightMiddleware
from benchmark_service.sandbox import (
    DaytonaProviderConfig,
    ModalProviderConfig,
    SandboxProvider,
    SandboxProviderConfig,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.schemas import ArtifactGradingSubmission, EvalMode, GradingSubmission, TextGradingSubmission
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalRequest,
    V1EvalStatus,
    V1EvalResponse,
    V1PayloadType,
    V1ScoreItem,
    V1ScoreRequest,
    V1ScoreResponse,
    V1UploadUrlRequest,
    V1UploadUrlResponse,
)

from benchmark_service.vals import evaluation_quota
from benchmark_service.vals.auth import (
    clear_request_tenant_config,
    close_catalog_client,
    get_tenant_config,
    load_allowlist,
)
from benchmark_service.vals.base import ValsBenchmarkService
from benchmark_service.vals.trial import sanitize_v1_dataset_tasks_response, sanitize_v1_eval_response, sanitize_v1_score_response

logger = logging.getLogger(__name__)

_EVALUATION_QUOTA_UNAVAILABLE_DETAIL = "Evaluation quota enforcement is temporarily unavailable; try again later."


def _is_trial_tenant(tenant: str | None) -> bool:
    if tenant is None:
        return False
    cfg = get_tenant_config(tenant)
    return cfg is not None and cfg.trial_mode


def _require_descope_tenant(tenant: str | None) -> None:
    """Raise 403 when authentication is required and the request has no Descope tenant identity."""
    if is_auth_required() and tenant == UNAUTHENTICATED_TENANT_SENTINEL:
        raise HTTPException(
            status_code=403,
            detail="The /v1/ surface requires Descope authentication.",
        )


# Lab-facing /v1 endpoints a trial tenant may reach. Deny-by-default: add new
# trial-accessible endpoints here so they don't silently auto-expose.
# submissions/upload-url is deliberately absent: minting is unmetered and a
# presigned PUT can't cap object size or upload count, so trial tenants don't
# get it until quota/one-shot semantics exist.
_TRIAL_ALLOWED_PATH = re.compile(r"/v1/(?:evaluate|score|datasets/[^/]+/tasks)")


def _trial_tenant_may_access_path(path: str) -> bool:
    return path == "/resolve-dataset" or _TRIAL_ALLOWED_PATH.fullmatch(path) is not None


GRADING_SANDBOX_PROVIDER_ENV = "GRADING_SANDBOX_PROVIDER"


def _grading_provider_config() -> SandboxProviderConfig:
    """Resolve the boot-time grading provider from the environment.

    GRADING_SANDBOX_PROVIDER selects the provider type (default "daytona");
    Daytona credentials come from the DAYTONA_* variables. Non-Daytona types
    resolve through the provider-config union so grading is not Daytona-only
    by construction.
    """
    provider_type = os.environ.get(GRADING_SANDBOX_PROVIDER_ENV) or "daytona"
    if provider_type == "daytona":
        return DaytonaProviderConfig.from_env()

    if provider_type == "daytona-byoc":
        return sandbox_provider_config_from_mapping(
            {**DaytonaProviderConfig.from_env().model_dump(), "type": provider_type}
        )

    if provider_type == "modal":
        return ModalProviderConfig.from_env()

    return sandbox_provider_config_from_mapping({"type": provider_type})


def _grading_max_concurrency() -> int:
    limit = int(os.environ.get("GRADING_MAX_CONCURRENCY") or 4)
    if limit < 1:
        # Semaphore(0) has no permits and none are ever released, so every
        # sandbox evaluation would hang forever with no error or log.
        raise ValueError("GRADING_MAX_CONCURRENCY must be >= 1")
    return limit


def _grading_nonnegative_int(name: str, default: int) -> int:
    value = int(os.environ.get(name) or default)
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def _grading_positive_float(name: str, default: float) -> float:
    value = float(os.environ.get(name) or default)
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


class _DuplicateGradingRequest(Exception):
    pass


class _GradingCapacityExceeded(Exception):
    pass


class _GradingAdmission:
    """Bound active and queued sandbox grades for one service process."""

    def __init__(
        self,
        *,
        max_concurrency: int,
        max_queued: int,
        max_admitted_per_tenant: int,
        queue_timeout_s: float,
    ) -> None:
        if max_admitted_per_tenant < 1:
            raise ValueError("GRADING_MAX_ADMITTED_PER_TENANT must be >= 1")
        self._active = asyncio.Semaphore(max_concurrency)
        self._max_admitted = max_concurrency + max_queued
        self._max_admitted_per_tenant = max_admitted_per_tenant
        self._queue_timeout_s = queue_timeout_s
        self._admitted: set[tuple[str, str, str]] = set()
        self._admitted_by_tenant: Counter[str] = Counter()

    @classmethod
    def from_env(cls) -> "_GradingAdmission":
        max_concurrency = _grading_max_concurrency()
        return cls(
            max_concurrency=max_concurrency,
            max_queued=_grading_nonnegative_int("GRADING_MAX_QUEUED", max_concurrency),
            max_admitted_per_tenant=_grading_nonnegative_int("GRADING_MAX_ADMITTED_PER_TENANT", max_concurrency),
            queue_timeout_s=_grading_positive_float("GRADING_QUEUE_TIMEOUT_S", 30.0),
        )

    @asynccontextmanager
    async def reserve(self, key: tuple[str, str, str]) -> AsyncGenerator[None, None]:
        tenant, run_id, task_id = key
        if key in self._admitted:
            raise _DuplicateGradingRequest(f"an evaluation for run {run_id} task {task_id} is already in progress")
        if (
            len(self._admitted) >= self._max_admitted
            or self._admitted_by_tenant[tenant] >= self._max_admitted_per_tenant
        ):
            raise _GradingCapacityExceeded("The benchmark service is at grading capacity; retry this evaluation later.")

        self._admitted.add(key)
        self._admitted_by_tenant[tenant] += 1
        try:
            yield
        finally:
            self._admitted.discard(key)
            self._admitted_by_tenant[tenant] -= 1
            if self._admitted_by_tenant[tenant] == 0:
                del self._admitted_by_tenant[tenant]

    @asynccontextmanager
    async def acquire_active_slot(self) -> AsyncGenerator[None, None]:
        acquired = False
        try:
            try:
                await asyncio.wait_for(self._active.acquire(), self._queue_timeout_s)
            except TimeoutError as exc:
                raise _GradingCapacityExceeded(
                    "The benchmark service could not start grading in time; retry this evaluation later."
                ) from exc
            acquired = True
            yield
        finally:
            if acquired:
                self._active.release()

    @asynccontextmanager
    async def acquire(self, key: tuple[str, str, str]) -> AsyncGenerator[None, None]:
        async with self.reserve(key):
            async with self.acquire_active_slot():
                yield


def _v1_score_item_to_eval_result(_task_id: str, item: V1ScoreItem | None) -> Any | None:
    """One task's evaluation as calculate_final_score receives it on either scoring surface.

    /final-score/ forwards the grader payload verbatim, so /v1/score does too: benchmarks
    implement one hook and must not have to ask which endpoint the caller used. This wrapped
    the payload as {task_id, status, result} instead, and the cost showed up as three private
    unwrappers -- emb's _score_payload, code-migration's _grader_result, this suite's own
    _score_item_resolved -- and one benchmark that did not write a fourth and silently scored
    every run zero.

    A task that did not reach a verdict becomes None, which is what every implementation
    already reads as incomplete. Its `errors` go no further: nothing consumes them in
    scoring, and the alternative is smuggling a framework key into a grader's own payload.
    """
    if item is None or item.status != V1EvalStatus.EVALUATED:
        return None
    return jsonable_encoder(item.result)


class ValsBenchmarkServiceApp(BenchmarkServiceApp):
    """Extend the shared benchmark routes with the Vals provider API and access policy."""

    def __init__(self, service_cls: type[ValsBenchmarkService]) -> None:
        self._grading_provider: SandboxProvider | None = None
        self._grading_admission = _GradingAdmission.from_env()
        super().__init__(service_cls)

    def _sentry_enabled(self) -> bool:
        return bool(os.getenv("SENTRY_DSN"))

    def _register_routes(self) -> None:
        self.add_middleware(
            InflightMiddleware,
            service_name=self._deployment_name,
            metric_namespace="Vals/BenchmarkServices",
        )
        super()._register_routes()
        self.add_api_route("/v1/evaluate", self._v1_evaluate, methods=["POST"], response_model=V1EvalResponse)
        self.add_api_route("/v1/score", self._v1_score, methods=["POST"], response_model=V1ScoreResponse)
        self.add_api_route(
            "/v1/submissions/upload-url",
            self._v1_submission_upload_url,
            methods=["POST"],
            response_model=V1UploadUrlResponse,
        )
        self.add_api_route(
            "/v1/datasets/{dataset}/tasks",
            self._v1_list_dataset_tasks,
            methods=["GET"],
            response_model=V1DatasetTasksResponse,
        )

    @asynccontextmanager
    async def service_lifespan(self) -> AsyncGenerator[None, None]:
        try:
            if is_auth_required():
                allowlist = load_allowlist()
                if not os.getenv("BENCHMARK_CATALOG_API_URL", "").strip():
                    evaluation_quota.require_configured(
                        allowlist,
                        service_name=os.getenv(evaluation_quota.SERVICE_NAME_ENV, "").strip(),
                    )
            submission_artifacts.require_configured()
            if not submission_artifacts.is_configured():
                logger.warning(
                    f"{submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} is not set; "
                    "POST /v1/submissions/upload-url will return 503"
                )
            async with super().service_lifespan():
                if (
                    self.service.eval_mode
                    in {
                        EvalMode.IN_PROCESS_ARTIFACT,
                        EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT,
                        EvalMode.SANDBOX,
                    }
                    and self.service.accepted_submission_schemas.get(V1PayloadType.ARTIFACT)
                    and not submission_artifacts.is_configured()
                ):
                    raise RuntimeError(
                        "artifact grading requires submission storage; set "
                        f"{submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} and "
                        f"{submission_artifacts.SUBMISSION_ARTIFACT_REGION_ENV}"
                    )
                if self.service.eval_mode == EvalMode.SANDBOX:
                    async with _grading_provider_config().create_provider() as provider:
                        self._grading_provider = provider
                        yield
                        return
                yield
        finally:
            await close_catalog_client()

    def clear_request_context(self) -> None:
        clear_request_tenant_config()

    def authorize_request(self, tenant: str, path: str) -> None:
        if _is_trial_tenant(tenant) and not _trial_tenant_may_access_path(path):
            raise HTTPException(
                status_code=403,
                detail="Trial tenants may only access approved /v1 endpoints (/v1/*)",
            )

    async def consume_evaluation_request(self, tenant: str) -> None:
        try:
            await evaluation_quota.consume_evaluation_request(
                service_name=self._deployment_name,
                tenant=tenant,
            )
        except evaluation_quota.EvaluationQuotaExceeded as exc:
            raise HTTPException(
                status_code=429,
                detail=str(exc),
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except evaluation_quota.EvaluationQuotaUnavailable as exc:
            logger.warning(
                "Evaluation quota storage unavailable for service %s tenant %s",
                self._deployment_name,
                tenant,
                exc_info=True,
            )
            raise HTTPException(status_code=503, detail=_EVALUATION_QUOTA_UNAVAILABLE_DETAIL) from exc

    async def _admit_websocket_evaluation(self, websocket: WebSocket, tenant: str) -> bool:
        try:
            await self.consume_evaluation_request(tenant)
        except HTTPException as exc:
            reason = str(exc.detail)
            if isinstance(exc.__cause__, evaluation_quota.EvaluationQuotaExceeded):
                reset_timestamp = exc.__cause__.reset_at.isoformat(timespec="seconds").replace("+00:00", "Z")
                reason = f"Evaluation quota reached; retry after {reset_timestamp}."
            await websocket.close(code=1008 if exc.status_code < 500 else 1011, reason=reason)
            return False
        return True

    def project_eval_response(self, tenant: str, response: V1EvalResponse) -> V1EvalResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_eval_response(response, cast(ValsBenchmarkService, self.service).project_trial_result)
        return response

    def project_score_response(self, tenant: str, response: V1ScoreResponse) -> V1ScoreResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_score_response(response)
        return response

    def project_dataset_tasks_response(self, tenant: str, response: V1DatasetTasksResponse) -> V1DatasetTasksResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_dataset_tasks_response(response)
        return response

    async def _v1_evaluate(self, request: Request, body: V1EvalRequest) -> V1EvalResponse:
        _require_descope_tenant(request.state.tenant)
        async with self._dataset_scope(request, request.state.tenant, body.dataset):
            return await self._v1_evaluate_in_scope(request, body)

    async def _v1_evaluate_in_scope(self, request: Request, body: V1EvalRequest) -> V1EvalResponse:
        if self._sentry is not None:
            self._sentry.bind_request_context(request.headers, run_id=body.run_id, task_id=body.task_id, dataset=body.dataset)
        tenant = cast(str, request.state.tenant)
        try:
            await self.service.validate_task_ids([body.task_id], dataset=body.dataset)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=f"Task not found: {body.task_id}") from exc

        if self.service.eval_mode in {
            EvalMode.IN_PROCESS_ARTIFACT,
            EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT,
            EvalMode.SANDBOX,
        }:
            accepted_schemas = self.service.accepted_submission_schemas.get(body.payload.type)
            if not accepted_schemas:
                raise HTTPException(
                    status_code=400,
                    detail=f"This benchmark does not accept {body.payload.type.value} submissions.",
                )
            if body.payload.schema_id not in accepted_schemas:
                choices = ", ".join(sorted(accepted_schemas))
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"This benchmark does not accept {body.payload.type.value} schema "
                        f"{body.payload.schema_id}. Use one of: {choices}."
                    ),
                )

            if body.payload.type == V1PayloadType.ARTIFACT and not body.payload.data:
                raise HTTPException(
                    status_code=400,
                    detail="Artifact submissions must include the uploaded object's key in payload.data.",
                )

            if body.payload.type == V1PayloadType.ARTIFACT:
                try:
                    submission_artifacts.validate_submission_key(
                        body.payload.data,
                        tenant=tenant,
                        dataset=body.dataset or "default",
                        run_id=body.run_id,
                        task_id=body.task_id,
                    )
                except ValueError as exc:
                    raise HTTPException(
                        status_code=404,
                        detail="Submission artifact not found for this evaluation.",
                    ) from exc

            try:
                async with self._grading_admission.reserve((tenant, body.run_id, body.task_id)):
                    await self.consume_evaluation_request(tenant)
                    async with self._grading_admission.acquire_active_slot():
                        artifact_reference: submission_artifacts.SubmissionArtifactReference | None = None
                        submission: GradingSubmission
                        if body.payload.type == V1PayloadType.ARTIFACT:
                            try:
                                artifact_reference = await submission_artifacts.stat(body.payload.data, tenant=tenant)
                            except submission_artifacts.SubmissionArtifactNotFound as exc:
                                raise HTTPException(status_code=404, detail=str(exc)) from exc
                            except submission_artifacts.SubmissionArtifactTooLarge as exc:
                                raise HTTPException(status_code=413, detail=str(exc)) from exc
                            submission = ArtifactGradingSubmission(
                                task_id=body.task_id,
                                schema_id=body.payload.schema_id,
                                artifact_reference=artifact_reference,
                                sandbox_path=SUBMISSION_ARTIFACT_SANDBOX_PATH,
                            )
                        else:
                            submission = TextGradingSubmission(
                                task_id=body.task_id,
                                schema_id=body.payload.schema_id,
                                text=body.payload.data,
                            )

                        if self.service.eval_mode == EvalMode.SANDBOX:
                            provider = self._grading_provider
                            if provider is None:
                                raise HTTPException(
                                    status_code=503,
                                    detail="Grading sandbox is not configured; contact the benchmark service owner.",
                                )
                            response = await evaluate_submission(
                                service=self.service,
                                run_id=body.run_id,
                                tenant=tenant,
                                submission=submission,
                                provider=provider,
                                evaluator_version=self._service_version,
                                dataset=body.dataset,
                            )
                        elif self.service.eval_mode == EvalMode.IN_PROCESS_ARTIFACT:
                            if artifact_reference is None:
                                raise RuntimeError("in-process artifact evaluation requires an admitted artifact")
                            try:
                                artifact = await submission_artifacts.download(artifact_reference, tenant=tenant)
                            except submission_artifacts.SubmissionArtifactNotFound as exc:
                                raise HTTPException(status_code=404, detail=str(exc)) from exc
                            except submission_artifacts.SubmissionArtifactChanged as exc:
                                raise HTTPException(status_code=409, detail=str(exc)) from exc
                            except submission_artifacts.SubmissionArtifactTooLarge as exc:
                                raise HTTPException(status_code=413, detail=str(exc)) from exc
                            response = await collapse_stream(
                                self.service.evaluate_artifact(
                                    run_id=body.run_id,
                                    task_id=body.task_id,
                                    schema_id=body.payload.schema_id,
                                    artifact=artifact,
                                    dataset=body.dataset,
                                ),
                                run_id=body.run_id,
                                task_id=body.task_id,
                                evaluator_version=self._service_version,
                            )
                        else:
                            if artifact_reference is None:
                                raise RuntimeError("materialized artifact evaluation requires an admitted artifact")
                            try:
                                async with submission_artifacts.materialize(
                                    artifact_reference,
                                    tenant=tenant,
                                ) as artifact:
                                    response = await collapse_stream(
                                        self.service.evaluate_materialized_artifact(
                                            tenant=tenant,
                                            run_id=body.run_id,
                                            task_id=body.task_id,
                                            schema_id=body.payload.schema_id,
                                            artifact=artifact,
                                            dataset=body.dataset,
                                        ),
                                        run_id=body.run_id,
                                        task_id=body.task_id,
                                        evaluator_version=self._service_version,
                                    )
                            except submission_artifacts.SubmissionArtifactNotFound as exc:
                                raise HTTPException(status_code=404, detail=str(exc)) from exc
                            except submission_artifacts.SubmissionArtifactChanged as exc:
                                raise HTTPException(status_code=409, detail=str(exc)) from exc
                            except submission_artifacts.SubmissionArtifactTooLarge as exc:
                                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except _DuplicateGradingRequest as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except _GradingCapacityExceeded as exc:
                raise HTTPException(status_code=429, detail=str(exc)) from exc
        else:
            if body.payload.type == V1PayloadType.ARTIFACT:
                raise HTTPException(
                    status_code=400,
                    detail="This benchmark does not accept artifact submissions.",
                )
            submission = TextGradingSubmission(
                task_id=body.task_id,
                schema_id=body.payload.schema_id,
                text=body.payload.data,
            )
            await self.consume_evaluation_request(tenant)
            response = await evaluate_submission(
                service=self.service,
                run_id=body.run_id,
                tenant=tenant,
                submission=submission,
                provider=None,
                evaluator_version=self._service_version,
                dataset=body.dataset,
            )
        return self.project_eval_response(tenant, response)

    async def _v1_submission_upload_url(self, request: Request, body: V1UploadUrlRequest) -> V1UploadUrlResponse:
        _require_descope_tenant(request.state.tenant)
        async with self._dataset_scope(request, request.state.tenant, body.dataset):
            if self._sentry is not None:
                self._sentry.bind_request_context(request.headers, run_id=body.run_id, task_id=body.task_id, dataset=body.dataset)
            tenant = cast(str, request.state.tenant)
            if not submission_artifacts.is_configured():
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Submission uploads are not configured on this deployment; "
                        f"set {submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} and "
                        f"{submission_artifacts.SUBMISSION_ARTIFACT_REGION_ENV}"
                    ),
                )
            await self.service.validate_task_ids([body.task_id], dataset=body.dataset)
            key = submission_artifacts.submission_key(
                tenant=tenant,
                dataset=body.dataset or "default",
                run_id=body.run_id,
                task_id=body.task_id,
                filename=body.filename,
            )
            return V1UploadUrlResponse(
                key=key,
                url=submission_artifacts.presigned_put_url(key),
                expires_in=submission_artifacts.DEFAULT_UPLOAD_EXPIRY_S,
            )

    async def _v1_score(self, request: Request, body: V1ScoreRequest) -> V1ScoreResponse:
        _require_descope_tenant(request.state.tenant)
        async with self._dataset_scope(request, request.state.tenant, body.dataset):
            if self._sentry is not None:
                self._sentry.bind_request_context(request.headers, run_id=body.run_id, dataset=body.dataset)

            normalized_results = {
                task_id: _v1_score_item_to_eval_result(task_id, item) for task_id, item in body.evaluation_results.items()
            }
            tasks_evaluated = await self.service.validate_task_ids(list(normalized_results.keys()), dataset=body.dataset)
            result = await self.service.calculate_final_score(normalized_results, dataset=body.dataset)
            response = V1ScoreResponse(
                run_id=body.run_id,
                tasks_evaluated=tasks_evaluated,
                final_score=result.score,
                metadata=result.metadata,
            )
            return self.project_score_response(request.state.tenant, response)

    async def _v1_list_dataset_tasks(self, request: Request, dataset: str) -> V1DatasetTasksResponse:
        _require_descope_tenant(request.state.tenant)
        async with self._dataset_scope(request, request.state.tenant, dataset):
            if self._sentry is not None:
                self._sentry.bind_request_context(request.headers, dataset=dataset)
            try:
                self.service.get_dataset(dataset)
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset}") from exc
            try:
                tasks = await self.service.list_tasks(dataset=dataset)
            except NotImplementedError as exc:
                raise HTTPException(status_code=501, detail=str(exc)) from exc
            response = V1DatasetTasksResponse(
                dataset=dataset,
                dataset_version=(
                    request.state.dataset_version.label
                    if hasattr(request.state, "dataset_version")
                    else self.service.get_dataset_version(dataset)
                ),
                tasks=tasks,
            )
            return self.project_dataset_tasks_response(request.state.tenant, response)
