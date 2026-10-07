"""FastAPI application for benchmark services."""

import importlib.metadata
import logging
import os
import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager, nullcontext, suppress
from typing import Any, cast

from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter, ValidationError
from starlette.middleware.base import RequestResponseEndpoint
from starlette.websockets import WebSocketDisconnect
from uvicorn.protocols.utils import ClientDisconnected
from websockets.exceptions import ConnectionClosed

from benchmark_service._version import __version__ as _framework_version
from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL, is_auth_required
from benchmark_service.base import BenchmarkService
from benchmark_service.context import sandbox_provider_scope
from benchmark_service.schemas import (
    DATASET_VERSION_HEADER,
    DatasetVersion,
    DatasetVersionId,
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    FinalScoreRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    ResolveDatasetRequest,
    ResolveDatasetResponse,
    SandboxProviderName,
    SetupTaskRequest,
    StreamChunk,
    StreamDatasetVersionChunk,
    StreamErrorChunk,
    TaskFilter,
    VerifyTaskIdsResponse,
    VersionResponse,
)
from benchmark_service.sandbox import (
    DaytonaProviderConfig,
    DockerProviderConfig,
    SandboxProviderConfig,
)

logger = logging.getLogger(__name__)
_dataset_version_id: TypeAdapter[str] = TypeAdapter(DatasetVersionId)

class _DatasetVersionSelectionError(HTTPException):
    pass


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


async def _send_dataset_version_error(websocket: WebSocket, exc: _DatasetVersionSelectionError) -> None:
    chunk = StreamErrorChunk(type="error", data=str(exc.detail), status_code=exc.status_code)
    await send_json_if_connected(websocket, chunk.model_dump())


_PUBLIC_PATHS = frozenset({"/health", "/version"})


def _request_sandbox_provider_config(
    request: SetupTaskRequest | EvaluateInstanceRequest,
    websocket: WebSocket,
) -> SandboxProviderConfig:
    return request.sandbox_provider or DaytonaProviderConfig.from_headers(websocket.headers)


DOCKER_ENABLED_ENV = "CBS_DOCKER_ENABLED"
_DOCKER_DISABLED_REASON = "Docker sandboxes are disabled on this service"


def _request_may_use_provider(config: SandboxProviderConfig | None) -> bool:
    """Docker needs no caller credentials and drives this host's daemon, so only an opted-in service accepts it."""
    return not isinstance(config, DockerProviderConfig) or os.environ.get(DOCKER_ENABLED_ENV, "").lower() == "true"


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


class BenchmarkServiceApp(FastAPI):
    """FastAPI application backed by a BenchmarkService subclass."""

    service: BenchmarkService

    def __init__(self, service_cls: type[BenchmarkService]) -> None:
        if service_cls.sandbox_providers and service_cls.default_sandbox_provider not in service_cls.sandbox_providers:
            raise ValueError(
                f"{service_cls.__name__}.default_sandbox_provider must be one of {service_cls.sandbox_providers}"
            )
        self._service_cls = service_cls
        self._service_name, self._service_version = _get_service_metadata(service_cls)
        configured_deployment_name = os.getenv("SERVICE_NAME", "").strip()
        deployment_name = configured_deployment_name or service_cls.__name__
        sentry = None
        if self._sentry_enabled():
            from benchmark_service import sentry

            sentry.init_sentry()

        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
            with sentry.isolation_scope() if sentry is not None else nullcontext():
                self._bind_service_context(self._service_version)
                try:
                    async with self.service_lifespan():
                        yield
                except Exception as exc:
                    if sentry is not None:
                        sentry.capture_exception(exc)
                    raise

        super().__init__(title=service_cls.__name__, lifespan=lifespan)
        self._deployment_name = deployment_name
        self._sentry = sentry
        self._register_routes()

    def _sentry_enabled(self) -> bool:
        """Let app subclasses opt into the optional Sentry integration."""
        return False

    def _bind_service_context(self, service_version: str | None) -> None:
        if self._sentry is None:
            return
        self._sentry.bind_service_context(
            service_name=self._deployment_name,
            framework_version=_framework_version,
            service_version=service_version,
        )

    def _register_routes(self) -> None:
        @self.middleware("http")
        async def _check_auth(request: Request, call_next: RequestResponseEndpoint) -> Response:  # pyright: ignore[reportUnusedFunction]
            self.clear_request_context()
            if self._sentry is not None:
                self._bind_service_context(self._current_service_version())
                self._sentry.bind_request_context(request.headers)
            try:
                if request.url.path in _PUBLIC_PATHS:
                    return await call_next(request)  # type: ignore[reportUnknownVariableType]
                tenant = UNAUTHENTICATED_TENANT_SENTINEL
                if is_auth_required():
                    tenant = await self.service.resolve_tenant(dict(request.headers))
                if tenant is None:
                    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
                request.state.tenant = tenant
                try:
                    self.authorize_request(tenant, request.url.path)
                except HTTPException as exc:
                    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)
                response = await call_next(request)  # type: ignore[reportUnknownVariableType]
                version = getattr(request.state, "dataset_version", None)
                if version is not None and 200 <= response.status_code < 300:
                    response.headers[DATASET_VERSION_HEADER] = version.id
                return response
            finally:
                self.clear_request_context()

        self.add_exception_handler(ValueError, self._value_error_handler)
        self.add_exception_handler(Exception, self._exception_handler)
        self.add_api_route("/health", self._health_check, methods=["GET"])
        self.add_api_route("/version", self._version, methods=["GET"])
        self.add_api_route("/resolve-dataset", self._resolve_dataset, methods=["POST"])
        self.add_api_route("/verify-task-ids", self._verify_task_ids, methods=["GET"])
        self.add_api_route("/retrieve-task/", self._retrieve_task, methods=["GET"])
        self.add_api_websocket_route("/ws/setup-task", self._setup_task)
        self.add_api_route("/evaluate-response/", self._evaluate_response, methods=["POST"])
        self.add_api_websocket_route("/ws/evaluate-response", self._evaluate_response_stream)
        self.add_api_websocket_route("/ws/evaluate-instance", self._evaluate_instance)
        self.add_api_route("/final-score/", self._final_score, methods=["POST"])

    async def _value_error_handler(self, _request: Request, exc: Exception) -> Response:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def _exception_handler(self, _request: Request, exc: Exception) -> Response:
        if self._sentry is not None:
            self._sentry.capture_http_exception(exc)
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
            dataset_version_selection=self.service.supports_dataset_version_selection(dataset or "default"),
            eval_mode=self.service.eval_mode,
            sandbox_providers=list(self.service.sandbox_providers),
            default_sandbox_provider=self.service.default_sandbox_provider,
        )

    @asynccontextmanager
    async def _open_dataset(
        self, tenant: str, dataset: str | None, version: str | None
    ) -> AsyncGenerator[DatasetVersion | None, None]:
        if not await self.service.check_dataset_access(tenant, dataset):
            raise HTTPException(status_code=403, detail="Dataset not allowed")
        name = dataset or "default"
        if not self.service.supports_dataset_version_selection(name):
            if version is not None:
                raise HTTPException(status_code=400, detail="Dataset version selection is not supported")
            yield None
            return
        async with self.service.open_dataset_version(name, version) as resolved:
            yield resolved

    @asynccontextmanager
    async def _dataset_scope(
        self, connection: Request | WebSocket, tenant: str, dataset: str | None
    ) -> AsyncGenerator[None, None]:
        values = connection.headers.getlist(DATASET_VERSION_HEADER)
        if len(values) > 1:
            raise _DatasetVersionSelectionError(400, f"Supply {DATASET_VERSION_HEADER} only once")
        try:
            version = _dataset_version_id.validate_python(values[0]) if values else None
        except ValidationError as exc:
            raise _DatasetVersionSelectionError(400, f"Invalid {DATASET_VERSION_HEADER}") from exc

        entered = False
        try:
            async with self._open_dataset(tenant, dataset, version) as resolved:
                if version is not None:
                    if resolved is None or resolved.id != version:
                        raise HTTPException(status_code=409, detail="The service could not honor the dataset version")
                    if isinstance(connection, WebSocket):
                        await connection.send_json(StreamDatasetVersionChunk(data=resolved).model_dump())
                    else:
                        connection.state.dataset_version = resolved
                entered = True
                yield
        except HTTPException as exc:
            if isinstance(connection, WebSocket) and exc.status_code == 403:
                await connection.close(code=1008, reason="Dataset not allowed")
                raise WebSocketDisconnect(code=1008) from exc
            if (
                isinstance(connection, WebSocket)
                and version is not None
                and not entered
                and exc.status_code in {400, 404, 409, 503}
            ):
                raise _DatasetVersionSelectionError(exc.status_code, str(exc.detail)) from exc
            raise

    async def _resolve_dataset(self, request: Request, body: ResolveDatasetRequest) -> ResolveDatasetResponse:
        if request.headers.getlist(DATASET_VERSION_HEADER):
            raise HTTPException(status_code=400, detail="Use the request body to select the dataset version")
        async with self._open_dataset(request.state.tenant, body.dataset, body.version) as resolved:
            if resolved is None:
                raise HTTPException(status_code=400, detail="Dataset version selection is not supported")
            return ResolveDatasetResponse(dataset=body.dataset, version=resolved)

    async def _authorize_websocket(self, websocket: WebSocket) -> str | None:
        """Authenticate a WebSocket caller. Returns tenant id, or None after closing 1008."""
        self.clear_request_context()
        self._bind_service_context(self._current_service_version())
        tenant = UNAUTHENTICATED_TENANT_SENTINEL
        if is_auth_required():
            tenant = await self.service.resolve_tenant(dict(websocket.headers))
        if tenant is None:
            await websocket.close(code=1008, reason="Unauthorized")
            return None
        try:
            self.authorize_request(tenant, websocket.url.path)
        except HTTPException as exc:
            await websocket.close(code=1008, reason=str(exc.detail))
            return None
        websocket.state.tenant = tenant
        return tenant

    @asynccontextmanager
    async def service_lifespan(self) -> AsyncGenerator[None, None]:
        """Initialize the benchmark for the application lifetime."""
        self.service = await self._service_cls.create()
        self._bind_service_context(self._current_service_version())
        yield

    def clear_request_context(self) -> None:
        """Reset deployment-specific request state before and after requests."""

    def authorize_request(self, tenant: str, path: str) -> None:
        """Raise HTTPException when deployment policy denies an authenticated route."""

    async def consume_evaluation_request(self, tenant: str) -> None:
        """Apply deployment admission policy before evaluation begins."""

    async def _admit_websocket_evaluation(self, websocket: WebSocket, tenant: str) -> bool:
        try:
            await self.consume_evaluation_request(tenant)
        except HTTPException as exc:
            await websocket.close(code=1008 if exc.status_code < 500 else 1011, reason=str(exc.detail))
            return False
        return True

    async def _verify_task_ids(
        self,
        request: Request,
        task_ids: list[str] | None = Query(default=None, description="List of task IDs to verify"),
        slice: str | None = Query(default=None, description="Slice of dataset (e.g., '3:10:1', '1:10:2')"),
        dataset: str | None = Query(default=None, description="Dataset name to use (defaults to 'default')"),
    ) -> VerifyTaskIdsResponse:
        if self._sentry is not None:
            self._sentry.bind_request_context(request.headers, dataset=dataset)
        async with self._dataset_scope(request, request.state.tenant, dataset):
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
        sandbox_provider: SandboxProviderName | None = Query(
            default=None, description="Sandbox provider the run uses (defaults to the service default)"
        ),
    ) -> RetrieveTaskResponse:
        if self._sentry is not None:
            self._sentry.bind_request_context(request.headers, task_id=task_id, dataset=dataset)
        providers = self.service.sandbox_providers
        provider = sandbox_provider or self.service.default_sandbox_provider
        if providers and provider not in providers:
            raise HTTPException(
                status_code=400,
                detail=f"Sandbox provider '{provider}' is not supported; this service supports {', '.join(providers)}",
            )
        async with self._dataset_scope(request, request.state.tenant, dataset):
            if not providers:
                return await self.service.retrieve_task(task_id, skip_validation, dataset=dataset)
            retrieve_task = cast(Callable[..., Awaitable[RetrieveTaskResponse]], self.service.retrieve_task)
            return await retrieve_task(task_id, skip_validation, dataset=dataset, sandbox_provider=provider)

    async def _setup_task(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            request = SetupTaskRequest(**await websocket.receive_json())
            if self._sentry is not None:
                self._sentry.bind_request_context(
                    websocket.headers,
                    task_id=request.task_id,
                    dataset=request.dataset,
                    sandbox_id=request.instance_id,
                )
            sandbox_config = _request_sandbox_provider_config(request, websocket)
            if not _request_may_use_provider(sandbox_config):
                await websocket.close(code=1008, reason=_DOCKER_DISABLED_REASON)
                return

            async with self._dataset_scope(websocket, tenant, request.dataset):
                async with sandbox_config.create_provider() as provider:
                    sandbox = await provider.get_sandbox(request.instance_id)

                    await _forward_stream(
                        websocket,
                        self.service.setup_task(request.task_id, sandbox, dataset=request.dataset),
                        endpoint="setup-task",
                    )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("setup-task websocket disconnected")
        except _DatasetVersionSelectionError as exc:
            await _send_dataset_version_error(websocket, exc)
        except Exception as e:
            if self._sentry is not None:
                self._sentry.capture_exception(e)
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("setup-task websocket disconnected before error chunk could be sent")
        finally:
            self.clear_request_context()
            with suppress(RuntimeError):
                await websocket.close()

    async def _evaluate_response(self, request: Request, body: EvaluateResponseRequest) -> Any:
        if self._sentry is not None:
            self._sentry.bind_request_context(request.headers, task_id=body.task_id, dataset=body.dataset)
        if not _request_may_use_provider(body.sandbox_provider):
            raise HTTPException(status_code=403, detail=_DOCKER_DISABLED_REASON)
        async with self._dataset_scope(request, request.state.tenant, body.dataset):
            await self.consume_evaluation_request(cast(str, request.state.tenant))
            return await self.service.evaluate_response(body, dataset=body.dataset)

    async def _evaluate_response_stream(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            data = await websocket.receive_json()
            request = EvaluateResponseRequest(**data)
            if self._sentry is not None:
                self._sentry.bind_request_context(websocket.headers, task_id=request.task_id, dataset=request.dataset)
            if not _request_may_use_provider(request.sandbox_provider):
                await websocket.close(code=1008, reason=_DOCKER_DISABLED_REASON)
                return

            async with self._dataset_scope(websocket, tenant, request.dataset):
                if not await self._admit_websocket_evaluation(websocket, tenant):
                    return

                await _forward_stream(
                    websocket,
                    self.service.stream_evaluate_response(request, dataset=request.dataset),
                    endpoint="evaluate-response",
                )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("evaluate-response websocket disconnected")
        except _DatasetVersionSelectionError as exc:
            await _send_dataset_version_error(websocket, exc)
        except Exception as e:
            if self._sentry is not None:
                self._sentry.capture_exception(e)
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("evaluate-response websocket disconnected before error chunk could be sent")
        finally:
            self.clear_request_context()
            with suppress(RuntimeError):
                await websocket.close()

    async def _evaluate_instance(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            tenant = await self._authorize_websocket(websocket)
            if tenant is None:
                return

            request = EvaluateInstanceRequest(**await websocket.receive_json())
            if self._sentry is not None:
                self._sentry.bind_request_context(
                    websocket.headers,
                    task_id=request.task_id,
                    dataset=request.dataset,
                    sandbox_id=request.instance_id,
                )
            sandbox_config = _request_sandbox_provider_config(request, websocket)
            if not _request_may_use_provider(sandbox_config):
                await websocket.close(code=1008, reason=_DOCKER_DISABLED_REASON)
                return

            async with self._dataset_scope(websocket, tenant, request.dataset):
                if not await self._admit_websocket_evaluation(websocket, tenant):
                    return

                async with sandbox_config.create_provider() as provider:
                    sandbox = await provider.get_sandbox(request.instance_id)

                    # Benchmarks that grade in a second sandbox read the provider
                    # from the request scope; see benchmark_service.context.
                    with sandbox_provider_scope(provider):
                        await _forward_stream(
                            websocket,
                            self.service.evaluate_instance(request.task_id, sandbox, dataset=request.dataset),
                            endpoint="evaluate-instance",
                        )

        except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed):
            logger.warning("evaluate-instance websocket disconnected")
        except _DatasetVersionSelectionError as exc:
            await _send_dataset_version_error(websocket, exc)
        except Exception as e:
            if self._sentry is not None:
                self._sentry.capture_exception(e)
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            if not await send_json_if_connected(websocket, error_chunk.model_dump()):
                logger.warning("evaluate-instance websocket disconnected before error chunk could be sent")
        finally:
            self.clear_request_context()
            with suppress(RuntimeError):
                await websocket.close()

    async def _final_score(self, request: Request, body: FinalScoreRequest) -> FinalScoreResponse:
        if self._sentry is not None:
            self._sentry.bind_request_context(request.headers, dataset=body.dataset)
        async with self._dataset_scope(request, request.state.tenant, body.dataset):
            tasks_evaluated = list(body.evaluation_results.keys())
            validated_task_ids = await self.service.validate_task_ids(tasks_evaluated, dataset=body.dataset)
            result = await self.service.calculate_final_score(body.evaluation_results, dataset=body.dataset)
            return FinalScoreResponse(
                tasks_evaluated=validated_task_ids,
                final_score=result.score,
                metadata=result.metadata,
            )
