"""FastAPI application for benchmark services."""

import asyncio
import importlib.metadata
import logging
import os
import re
import traceback
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager, suppress
from typing import Any, cast

from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect
from uvicorn.protocols.utils import ClientDisconnected
from websockets.exceptions import ConnectionClosed

from benchmark_service._version import __version__ as _framework_version
from benchmark_service.auth import LEGACY_TENANT_SENTINEL, get_auth_settings, get_tenant_config, load_allowlist
from benchmark_service.base import BenchmarkService
from benchmark_service.grading import evaluate_submission
from benchmark_service.inflight import InflightMiddleware
from benchmark_service.schemas import (
    EvalMode,
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    FinalScoreRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskRequest,
    StreamChunk,
    StreamErrorChunk,
    TaskFilter,
    VerifyTaskIdsResponse,
    VersionResponse,
)
from benchmark_service.sandbox import SandboxProvider, SandboxProviderConfig, sandbox_provider_config_from_mapping
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.trial import (
    sanitize_v1_dataset_tasks_response,
    sanitize_v1_eval_response,
    sanitize_v1_score_response,
)
from benchmark_service import submission_artifacts
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalRequest,
    V1EvalResponse,
    V1PayloadType,
    V1ScoreItem,
    V1ScoreRequest,
    V1ScoreResponse,
    V1UploadUrlRequest,
    V1UploadUrlResponse,
)

logger = logging.getLogger(__name__)


async def send_json_if_connected(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await websocket.send_json(payload)
        return True
    except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed, RuntimeError):
        return False


async def _forward_stream(
    websocket: WebSocket,
    stream: AsyncGenerator[StreamChunk, None],
    *,
    endpoint: str,
) -> None:
    """Forward a chunk stream to a websocket, closing the stream if the client
    disconnects mid-way."""
    async with aclosing(stream) as chunks:
        async for chunk in chunks:
            if not await send_json_if_connected(websocket, chunk.model_dump()):
                logger.warning("%s websocket disconnected before benchmark service completed", endpoint)
                return


_PUBLIC_PATHS = frozenset({"/health", "/version"})


def _require_descope_tenant(tenant: str | None) -> None:
    """Raise 403 when the resolved tenant is the legacy bearer sentinel.

    The /v1/ surface requires Descope auth; legacy bearer callers resolve to
    LEGACY_TENANT_SENTINEL and must be rejected even though they are technically
    authenticated against the legacy key.
    """
    if tenant == LEGACY_TENANT_SENTINEL:
        raise HTTPException(
            status_code=403,
            detail="The /v1/ surface requires Descope authentication; legacy bearer auth is not accepted.",
        )


def _is_trial_tenant(tenant: str | None) -> bool:
    if tenant is None:
        return False
    cfg = get_tenant_config(tenant)
    return cfg is not None and cfg.trial_mode


# Lab-facing /v1 endpoints a trial tenant may reach. Deny-by-default: add new
# trial-accessible endpoints here so they don't silently auto-expose.
# submissions/upload-url is deliberately absent: minting is unmetered and a
# presigned PUT can't cap object size or upload count, so trial tenants don't
# get it until quota/one-shot semantics exist.
_TRIAL_ALLOWED_PATH = re.compile(r"/v1/(?:evaluate|score|datasets/[^/]+/tasks)")


def _trial_tenant_may_access_path(path: str) -> bool:
    return _TRIAL_ALLOWED_PATH.fullmatch(path) is not None


def _request_sandbox_provider_config(
    request: SetupTaskRequest | EvaluateInstanceRequest,
    websocket: WebSocket,
) -> SandboxProviderConfig:
    return request.sandbox_provider or DaytonaProviderConfig.from_headers(websocket.headers)


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
    return sandbox_provider_config_from_mapping({"type": provider_type})


def _grading_max_concurrency() -> int:
    limit = int(os.environ.get("GRADING_MAX_CONCURRENCY") or 4)
    if limit < 1:
        # Semaphore(0) has no permits and none are ever released, so every
        # sandbox evaluation would hang forever with no error or log.
        raise ValueError("GRADING_MAX_CONCURRENCY must be >= 1")
    return limit


def _get_service_metadata(service_cls: type[BenchmarkService]) -> tuple[str | None, str | None]:
    """Resolve (distribution_name, version) for the installed package containing service_cls.

    Returns (None, None) when the subclass's top-level module does not map to an installed
    distribution (e.g., running from source, or defined inline in main.py).
    """
    top_level_pkg = service_cls.__module__.split(".")[0]
    distributions = importlib.metadata.packages_distributions().get(top_level_pkg, [])
    if not distributions:
        return None, None
    dist_name = distributions[0]
    try:
        return dist_name, importlib.metadata.version(dist_name)
    except importlib.metadata.PackageNotFoundError:
        return None, None


def _v1_score_item_to_eval_result(task_id: str, item: V1ScoreItem | None) -> dict[str, Any] | None:
    if item is None:
        return None

    result: dict[str, Any] = {
        "task_id": task_id,
        "status": item.status.value,
        "result": jsonable_encoder(item.result),
    }
    if item.errors:
        # Runner-side EvalResult.error is str | None, so v1's error list collapses here.
        result["error"] = "; ".join(item.errors)
    return result


class BenchmarkServiceApp(FastAPI):
    """FastAPI application backed by a BenchmarkService subclass."""

    service: BenchmarkService

    def __init__(self, service_cls: type[BenchmarkService]) -> None:
        self._service_name, self._service_version = _get_service_metadata(service_cls)

        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
            if get_auth_settings().auth_required:
                load_allowlist()
            submission_artifacts.require_configured()
            if not submission_artifacts.is_configured():
                logger.warning(
                    f"{submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} is not set; "
                    "POST /v1/submissions/upload-url will return 503"
                )
            self.service = await service_cls.create()
            if self.service.eval_mode == EvalMode.SANDBOX:
                # Sandbox grading needs both the grading-sandbox credentials and
                # artifact storage; a deployment missing either must fail at
                # boot, not per evaluation after provisioning a sandbox.
                if not submission_artifacts.is_configured():
                    raise RuntimeError(
                        "eval_mode == EvalMode.SANDBOX requires submission-artifact storage; set "
                        f"{submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} and "
                        f"{submission_artifacts.SUBMISSION_ARTIFACT_REGION_ENV}"
                    )
                async with _grading_provider_config().create_provider() as provider:
                    self._grading_provider = provider
                    yield
                    return
            yield

        super().__init__(title=service_cls.__name__, lifespan=lifespan)
        self._grading_provider: SandboxProvider | None = None
        self._grading_semaphore = asyncio.Semaphore(_grading_max_concurrency())
        self._grading_in_flight: set[tuple[str, str, str]] = set()
        service_name = os.getenv("SERVICE_NAME", service_cls.__name__)
        self.add_middleware(InflightMiddleware, service_name=service_name)
        self._register_routes()

    def _register_routes(self) -> None:
        @self.middleware("http")
        async def _check_auth(request: Request, call_next):  # type: ignore[no-untyped-def]
            if request.url.path in _PUBLIC_PATHS:
                return await call_next(request)  # type: ignore[reportUnknownVariableType]
            tenant = await self.service.resolve_tenant(dict(request.headers))
            if tenant is None:
                return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            request.state.tenant = tenant
            if _is_trial_tenant(tenant) and not _trial_tenant_may_access_path(request.url.path):
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Trial tenants may only access approved /v1 endpoints (/v1/*)"},
                )
            return await call_next(request)  # type: ignore[reportUnknownVariableType]

        self.add_exception_handler(ValueError, self._value_error_handler)
        self.add_exception_handler(Exception, self._exception_handler)
        self.add_api_route("/health", self._health_check, methods=["GET"])
        self.add_api_route("/version", self._version, methods=["GET"])
        self.add_api_route("/verify-task-ids", self._verify_task_ids, methods=["GET"])
        self.add_api_route("/retrieve-task/", self._retrieve_task, methods=["GET"])
        self.add_api_websocket_route("/ws/setup-task", self._setup_task)
        self.add_api_route("/evaluate-response/", self._evaluate_response, methods=["POST"])
        self.add_api_websocket_route("/ws/evaluate-response", self._evaluate_response_stream)
        self.add_api_websocket_route("/ws/evaluate-instance", self._evaluate_instance)
        self.add_api_route("/final-score/", self._final_score, methods=["POST"])
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

    async def _value_error_handler(self, _request: Request, exc: Exception) -> Response:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def _exception_handler(self, _request: Request, exc: Exception) -> Response:
        logger.error(f"Error: {exc}")
        logger.error(traceback.format_exc())
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    async def _health_check(self) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    def _current_service_version(self) -> str | None:
        service_override = self.service.get_service_version()
        return service_override or self._service_version

    async def _version(self, dataset: str | None = None) -> VersionResponse:
        return VersionResponse(
            framework_version=_framework_version,
            service_name=self._service_name,
            service_version=self._current_service_version(),
            dataset_version=self.service.get_dataset_version(dataset),
            eval_mode=self.service.eval_mode,
        )

    async def _authorize_websocket(self, websocket: WebSocket) -> str | None:
        """Authenticate a WebSocket caller. Returns tenant id, or None after closing 1008."""
        tenant = await self.service.resolve_tenant(dict(websocket.headers))
        if tenant is None:
            await websocket.close(code=1008, reason="Unauthorized")
            return None
        if _is_trial_tenant(tenant):
            await websocket.close(code=1008, reason="Trial tenants may only access /v1/*")
            return None
        websocket.state.tenant = tenant
        return tenant

    async def _verify_task_ids(
        self,
        request: Request,
        task_ids: list[str] | None = Query(default=None, description="List of task IDs to verify"),
        slice: str | None = Query(default=None, description="Slice of dataset (e.g., '3:10:1', '1:10:2')"),
        dataset: str | None = Query(default=None, description="Dataset name to use (defaults to 'default')"),
    ) -> VerifyTaskIdsResponse:
        if not await self.service.check_dataset_access(request.state.tenant, dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")

        task_filter = TaskFilter()

        if task_ids:
            task_filter.task_ids = list(dict.fromkeys(task_ids))

        if slice:
            task_filter.slice_str = slice

        filtered_task_ids = await self.service.filter_tasks(task_filter, dataset=dataset)

        return VerifyTaskIdsResponse(task_ids=filtered_task_ids)

    async def _retrieve_task(
        self,
        request: Request,
        task_id: str = Query(..., description="Task ID to retrieve"),
        skip_validation: bool = Query(False, description="Skip validation of task existence"),
        dataset: str | None = Query(default=None, description="Dataset name to use (defaults to 'default')"),
    ) -> RetrieveTaskResponse:
        if not await self.service.check_dataset_access(request.state.tenant, dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")
        return await self.service.retrieve_task(task_id, skip_validation, dataset=dataset)

    async def _setup_task(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            request = SetupTaskRequest(**await websocket.receive_json())
            sandbox_config = _request_sandbox_provider_config(request, websocket)

            if not await self.service.check_dataset_access(tenant, request.dataset):
                await websocket.close(code=1008, reason="Dataset not allowed")
                return

            async with sandbox_config.create_provider() as provider:
                sandbox = await provider.get_sandbox(request.instance_id)

                await _forward_stream(
                    websocket,
                    self.service.setup_task(request.task_id, sandbox, dataset=request.dataset),
                    endpoint="setup-task",
                )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("setup-task websocket disconnected")
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("setup-task websocket disconnected before error chunk could be sent")
        finally:
            with suppress(RuntimeError):
                await websocket.close()

    async def _evaluate_response(self, request: Request, body: EvaluateResponseRequest) -> Any:
        if not await self.service.check_dataset_access(request.state.tenant, body.dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")
        return await self.service.evaluate_response(body, dataset=body.dataset)

    async def _evaluate_response_stream(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            data = await websocket.receive_json()
            request = EvaluateResponseRequest(**data)

            if not await self.service.check_dataset_access(tenant, request.dataset):
                await websocket.close(code=1008, reason="Dataset not allowed")
                return

            await _forward_stream(
                websocket,
                self.service.stream_evaluate_response(request, dataset=request.dataset),
                endpoint="evaluate-response",
            )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("evaluate-response websocket disconnected")
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("evaluate-response websocket disconnected before error chunk could be sent")
        finally:
            with suppress(RuntimeError):
                await websocket.close()

    async def _evaluate_instance(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            request = EvaluateInstanceRequest(**await websocket.receive_json())
            sandbox_config = _request_sandbox_provider_config(request, websocket)

            if not await self.service.check_dataset_access(tenant, request.dataset):
                await websocket.close(code=1008, reason="Dataset not allowed")
                return

            async with sandbox_config.create_provider() as provider:
                sandbox = await provider.get_sandbox(request.instance_id)

                await _forward_stream(
                    websocket,
                    self.service.evaluate_instance(request.task_id, sandbox, dataset=request.dataset),
                    endpoint="evaluate-instance",
                )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("evaluate-instance websocket disconnected")
        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("evaluate-instance websocket disconnected before error chunk could be sent")
        finally:
            with suppress(RuntimeError):
                await websocket.close()

    async def _v1_evaluate(self, request: Request, body: V1EvalRequest) -> V1EvalResponse:
        tenant = cast(str, request.state.tenant)
        _require_descope_tenant(tenant)
        if body.payload.type not in (V1PayloadType.TEXT, V1PayloadType.ARTIFACT):
            raise HTTPException(
                status_code=400,
                detail=f"payload.type={body.payload.type.value} not supported",
            )
        if not await self.service.check_dataset_access(tenant, body.dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")

        artifact_key: str | None = None
        if body.payload.type == V1PayloadType.ARTIFACT:
            if self.service.eval_mode != EvalMode.SANDBOX:
                raise HTTPException(
                    status_code=400,
                    detail="artifact payloads require a sandbox-grading benchmark (eval_mode == SANDBOX)",
                )
            if not body.payload.data:
                raise HTTPException(
                    status_code=400,
                    detail="artifact payloads must carry the submission's object key in payload.data",
                )
            artifact_key = body.payload.data
            try:
                await submission_artifacts.stat(artifact_key, tenant=tenant)
            except submission_artifacts.SubmissionArtifactNotFound as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except submission_artifacts.SubmissionArtifactTooLarge as exc:
                raise HTTPException(status_code=413, detail=str(exc)) from exc

        internal_req = EvaluateResponseRequest(
            task_id=body.task_id,
            response=body.payload.data,
            dataset=body.dataset,
        )
        if self.service.eval_mode == EvalMode.SANDBOX:
            provider = self._grading_provider
            if provider is None:
                raise HTTPException(
                    status_code=503,
                    detail="Grading sandbox is not configured; contact the benchmark service owner.",
                )
            in_flight_key = (tenant, body.run_id, body.task_id)
            if in_flight_key in self._grading_in_flight:
                raise HTTPException(
                    status_code=409,
                    detail=f"an evaluation for run {body.run_id} task {body.task_id} is already in progress",
                )
            self._grading_in_flight.add(in_flight_key)
            try:
                async with self._grading_semaphore:
                    response = await evaluate_submission(
                        service=self.service,
                        run_id=body.run_id,
                        tenant=tenant,
                        request=internal_req,
                        provider=provider,
                        evaluator_version=self._service_version,
                        dataset=body.dataset,
                        artifact_key=artifact_key,
                    )
            finally:
                self._grading_in_flight.discard(in_flight_key)
        else:
            response = await evaluate_submission(
                service=self.service,
                run_id=body.run_id,
                tenant=tenant,
                request=internal_req,
                provider=None,
                evaluator_version=self._service_version,
                dataset=body.dataset,
            )
        if _is_trial_tenant(request.state.tenant):
            return sanitize_v1_eval_response(response, self.service.project_trial_result)
        return response

    async def _v1_submission_upload_url(
        self, request: Request, body: V1UploadUrlRequest
    ) -> V1UploadUrlResponse:
        tenant = cast(str, request.state.tenant)
        _require_descope_tenant(tenant)
        if not submission_artifacts.is_configured():
            raise HTTPException(
                status_code=503,
                detail=(
                    "Submission uploads are not configured on this deployment; "
                    f"set {submission_artifacts.SUBMISSION_ARTIFACT_BUCKET_ENV} and "
                    f"{submission_artifacts.SUBMISSION_ARTIFACT_REGION_ENV}"
                ),
            )
        if not await self.service.check_dataset_access(tenant, body.dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")
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
        if not await self.service.check_dataset_access(request.state.tenant, body.dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")

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
        if _is_trial_tenant(request.state.tenant):
            return sanitize_v1_score_response(response)
        return response

    async def _v1_list_dataset_tasks(self, request: Request, dataset: str) -> V1DatasetTasksResponse:
        _require_descope_tenant(request.state.tenant)
        if not await self.service.check_dataset_access(request.state.tenant, dataset):
            raise HTTPException(status_code=403, detail=f"Dataset={dataset} access not allowed")
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
            dataset_version=self.service.get_dataset_version(dataset),
            tasks=tasks,
        )
        if _is_trial_tenant(request.state.tenant):
            return sanitize_v1_dataset_tasks_response(response)
        return response

    async def _final_score(self, request: Request, body: FinalScoreRequest) -> FinalScoreResponse:
        if not await self.service.check_dataset_access(request.state.tenant, body.dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")

        tasks_evaluated = list(body.evaluation_results.keys())
        validated_task_ids = await self.service.validate_task_ids(tasks_evaluated, dataset=body.dataset)
        result = await self.service.calculate_final_score(body.evaluation_results, dataset=body.dataset)

        return FinalScoreResponse(
            tasks_evaluated=validated_task_ids,
            final_score=result.score,
            metadata=result.metadata,
        )
