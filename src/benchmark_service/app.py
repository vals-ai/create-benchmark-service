"""FastAPI application for benchmark services."""

import logging
import traceback
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from daytona import AsyncDaytona, DaytonaConfig
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket

from benchmark_service.base import BenchmarkService
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
)

logger = logging.getLogger(__name__)


class BenchmarkServiceApp(FastAPI):
    """FastAPI application backed by a BenchmarkService subclass."""

    service: BenchmarkService

    def __init__(self, service_cls: type[BenchmarkService]) -> None:
        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
            self.service = await service_cls.create()
            yield

        super().__init__(title=service_cls.__name__, lifespan=lifespan)
        self._register_routes()

    def _register_routes(self) -> None:
        self.add_exception_handler(ValueError, self._value_error_handler)
        self.add_exception_handler(Exception, self._exception_handler)
        self.add_api_route("/health", self._health_check, methods=["GET"])
        self.add_api_route("/verify-task-ids", self._verify_task_ids, methods=["GET"])
        self.add_api_route("/retrieve-task/", self._retrieve_task, methods=["GET"])
        self.add_api_websocket_route("/ws/setup-task", self._setup_task)
        self.add_api_route("/evaluate-response/", self._evaluate_response, methods=["POST"])
        self.add_api_websocket_route("/ws/evaluate-instance", self._evaluate_instance)
        self.add_api_route("/final-score/", self._final_score, methods=["POST"])

    async def _value_error_handler(self, _request: Request, exc: ValueError) -> Response:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def _exception_handler(self, _request: Request, exc: Exception) -> Response:
        logger.error(f"Error: {exc}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"{str(exc)}: {traceback.format_exc()}") from exc

    async def _health_check(self) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _verify_task_ids(
        self,
        task_ids: list[str] | None = Query(default=None, description="List of task IDs to verify"),
        slice: str | None = Query(default=None, description="Slice of dataset (e.g., '3:10:1', '1:10:2')"),
        dataset: str | None = Query(default=None, description="Dataset name to use (defaults to 'default')"),
    ) -> VerifyTaskIdsResponse:
        task_filter = TaskFilter()

        if task_ids:
            task_filter.task_ids = list(dict.fromkeys(task_ids))

        if slice:
            task_filter.slice_str = slice

        filtered_task_ids = await self.service.filter_tasks(task_filter, dataset=dataset)

        return VerifyTaskIdsResponse(task_ids=filtered_task_ids)

    async def _retrieve_task(
        self,
        task_id: str = Query(..., description="Task ID to retrieve"),
        skip_validation: bool = Query(False, description="Skip validation of task existence"),
        dataset: str | None = Query(default=None, description="Dataset name to use (defaults to 'default')"),
    ) -> RetrieveTaskResponse:
        return await self.service.retrieve_task(task_id, skip_validation, dataset=dataset)

    async def _setup_task(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            api_key = websocket.headers.get("x-api-key")
            api_url = websocket.headers.get("x-api-url")
            target = websocket.headers.get("x-target")

            if not api_key or not api_url or not target:
                await websocket.close(code=1008, reason="Missing required headers: x-api-key, x-api-url, x-target")
                return

            data = await websocket.receive_json()
            request = SetupTaskRequest(**data)

            daytona_config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)

            async with AsyncDaytona(config=daytona_config) as daytona:
                sandbox = await daytona.get(request.instance_id)

                async for message in self.service.setup_task(request.task_id, sandbox, dataset=request.dataset):
                    await websocket.send_json(message.model_dump())

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            await websocket.send_json(error_chunk.model_dump())
        finally:
            try:
                await websocket.close()
            except RuntimeError:
                pass

    async def _evaluate_response(self, request: EvaluateResponseRequest) -> Any:
        return await self.service.evaluate_response(request, dataset=request.dataset)

    async def _evaluate_instance(self, websocket: WebSocket) -> None:
        await websocket.accept()

        try:
            api_key = websocket.headers.get("x-api-key")
            api_url = websocket.headers.get("x-api-url")
            target = websocket.headers.get("x-target")

            if not api_key or not api_url or not target:
                await websocket.close(code=1008, reason="Missing required headers: x-api-key, x-api-url, x-target")
                return

            data = await websocket.receive_json()
            request = EvaluateInstanceRequest(**data)

            daytona_config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)

            async with AsyncDaytona(config=daytona_config) as daytona:
                sandbox = await daytona.get(request.instance_id)

                async for message in self.service.evaluate_instance(request.task_id, sandbox, dataset=request.dataset):
                    await websocket.send_json(message.model_dump())

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            await websocket.send_json(error_chunk.model_dump())
        finally:
            try:
                await websocket.close()
            except RuntimeError:
                pass

    async def _final_score(self, request: FinalScoreRequest) -> FinalScoreResponse:
        tasks_evaluated = list(request.evaluation_results.keys())

        validated_task_ids = await self.service.validate_task_ids(tasks_evaluated, dataset=request.dataset)
        result = await self.service.calculate_final_score(request.evaluation_results, dataset=request.dataset)

        return FinalScoreResponse(
            tasks_evaluated=validated_task_ids,
            final_score=result.score,
            metadata=result.metadata,
        )
