"""FastAPI application for benchmark services."""

import importlib.metadata
import logging
import os
import traceback
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect
from uvicorn.protocols.utils import ClientDisconnected
from websockets.exceptions import ConnectionClosed

from benchmark_service._version import __version__ as _framework_version
from benchmark_service.auth import get_auth_settings, load_allowlist
from benchmark_service.base import BenchmarkService
from benchmark_service.inflight import InflightMiddleware
from benchmark_service.sandbox import MissingSandboxConfigError, create_provider, sandbox_config_from_headers
from benchmark_service.schemas import (
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    FinalScoreRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskRequest,
    StreamErrorChunk,
    TaskFilter,
    VerifyTaskIdsResponse,
    VersionResponse,
)

logger = logging.getLogger(__name__)


async def send_json_if_connected(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await websocket.send_json(payload)
        return True
    except (WebSocketDisconnect, ClientDisconnected, ConnectionClosed, RuntimeError):
        return False


_PUBLIC_PATHS = frozenset({"/health", "/version"})


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
        self._service_name, self._service_version = _get_service_metadata(service_cls)

        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
            if get_auth_settings().auth_required:
                load_allowlist()
            self.service = await service_cls.create()
            yield

        super().__init__(title=service_cls.__name__, lifespan=lifespan)
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

    async def _value_error_handler(self, _request: Request, exc: Exception) -> Response:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def _exception_handler(self, _request: Request, exc: Exception) -> Response:
        logger.error(f"Error: {exc}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"{str(exc)}: {traceback.format_exc()}") from exc

    async def _health_check(self) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _version(self) -> VersionResponse:
        return VersionResponse(
            framework_version=_framework_version,
            service_name=self._service_name,
            service_version=self._service_version,
        )

    async def _authorize_websocket(self, websocket: WebSocket) -> str | None:
        """Authenticate a WebSocket caller. Returns tenant id, or None after closing 1008."""
        tenant = await self.service.resolve_tenant(dict(websocket.headers))
        if tenant is None:
            await websocket.close(code=1008, reason="Unauthorized")
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

            try:
                sandbox_config = sandbox_config_from_headers(websocket.headers)
            except MissingSandboxConfigError as e:
                await websocket.close(code=1008, reason=str(e))
                return

            data = await websocket.receive_json()
            request = SetupTaskRequest(**data)

            if not await self.service.check_dataset_access(tenant, request.dataset):
                await websocket.close(code=1008, reason="Dataset not allowed")
                return

            async with create_provider(sandbox_config) as provider:
                sandbox = await provider.get_sandbox(request.instance_id)

                async for message in self.service.setup_task(request.task_id, sandbox, dataset=request.dataset):
                    if not await send_json_if_connected(websocket, message.model_dump()):
                        logger.warning("setup-task websocket disconnected before benchmark service completed")
                        return

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

            async for message in self.service.stream_evaluate_response(request, dataset=request.dataset):
                if not await send_json_if_connected(websocket, message.model_dump()):
                    logger.warning("evaluate-response websocket disconnected before benchmark service completed")
                    return

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

            try:
                sandbox_config = sandbox_config_from_headers(websocket.headers)
            except MissingSandboxConfigError as e:
                await websocket.close(code=1008, reason=str(e))
                return

            data = await websocket.receive_json()
            request = EvaluateInstanceRequest(**data)

            if not await self.service.check_dataset_access(tenant, request.dataset):
                await websocket.close(code=1008, reason="Dataset not allowed")
                return

            async with create_provider(sandbox_config) as provider:
                sandbox = await provider.get_sandbox(request.instance_id)

                async for message in self.service.evaluate_instance(request.task_id, sandbox, dataset=request.dataset):
                    if not await send_json_if_connected(websocket, message.model_dump()):
                        logger.warning("evaluate-instance websocket disconnected before benchmark service completed")
                        return

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
