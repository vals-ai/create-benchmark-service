"""
FastAPI application factory for benchmark services.

This module provides a complete FastAPI implementation.
Import create_app and pass your BenchmarkService implementation.
"""

import logging
import traceback
from typing import Any

from daytona import AsyncDaytona, DaytonaConfig
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket

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

# pyright: reportUnusedFunction=false

logger = logging.getLogger(__name__)


def create_app(benchmark_service: BenchmarkService) -> FastAPI:
    """
    Create a FastAPI application with the provided benchmark service implementation.

    Args:
        benchmark_service: Your BenchmarkService implementation

    Returns:
        Configured FastAPI application ready to run
    """
    app = FastAPI(title=benchmark_service.__class__.__name__)

    @app.exception_handler(Exception)
    async def exception_handler(_request: Request, exc: Exception):
        """Global exception handler for unhandled errors."""
        logger.error(f"Error: {exc}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"{str(exc)}: {traceback.format_exc()}") from exc

    @app.get("/health")
    def health_check() -> HealthCheckResponse:
        """
        Health check endpoint to verify the service is running.

        Usage:
            curl -X GET http://localhost:8000/health

        Returns:
            {"status": "ok"}
        """
        return HealthCheckResponse(status="ok")

    @app.get("/verify-task-ids")
    def verify_task_ids(
        task_ids: list[str] | None = Query(default=None, description="List of task IDs to verify"),
        slice: str | None = Query(default=None, description="Slice of dataset (e.g., '3:10:1', '1:10:2')"),
    ) -> VerifyTaskIdsResponse:
        """
        Verify that task IDs exist in the benchmark dataset.

        Usage:
            # Verify specific task IDs
            curl -X GET "http://localhost:8000/verify-task-ids?task_ids=task_1&task_ids=task_2"

            # Verify all tasks
            curl -X GET "http://localhost:8000/verify-task-ids"

            # Verify with slice
            curl -X GET "http://localhost:8000/verify-task-ids?slice=0:10:1"

        Returns:
            {"task_ids": ["task_1", "task_2", ...]}
        """
        task_filter = TaskFilter()

        if task_ids:
            task_filter.task_ids = list(dict.fromkeys(task_ids))  # Remove duplicates

        if slice:
            task_filter.slice_str = slice

        filtered_task_ids = benchmark_service.filter_tasks(task_filter)

        return VerifyTaskIdsResponse(task_ids=filtered_task_ids)

    @app.get("/retrieve-task/")
    async def retrieve_task(
        task_id: str = Query(..., description="Task ID to retrieve"),
        skip_validation: bool = Query(False, description="Skip validation of task existence"),
    ) -> RetrieveTaskResponse:
        """
        Retrieve task metadata including environment specification and problem statement.

        Usage:
            curl -X GET "http://localhost:8000/retrieve-task/?task_id=task_1"

        Returns:
            {
                "docker_image": "your-registry/benchmark-task:latest",
                "problem_statement": "Solve this problem...",
                "request_setup": true,
                "cwd": "/workspace",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10}
            }
        """
        return benchmark_service.retrieve_task(task_id, skip_validation)

    @app.websocket("/ws/setup-task")
    async def setup_task(websocket: WebSocket):
        """
        Setup a task in a sandbox environment (WebSocket endpoint).

        This endpoint is called when a task requires setup before evaluation
        (e.g., installing dependencies, preparing data, etc.).

        Headers:
            x-api-key: API key for sandbox service (e.g., Daytona)
            x-api-url: API URL for sandbox service
            x-target: Target identifier for sandbox

        Body:
            {"task_id": "task_1", "instance_id": "instance_123"}

        The framework handles:
        1. WebSocket connection management
        2. Daytona client setup
        3. Sandbox retrieval
        4. Streaming yielded messages to client
        """
        await websocket.accept()

        try:
            # Extract headers
            api_key = websocket.headers.get("x-api-key")
            api_url = websocket.headers.get("x-api-url")
            target = websocket.headers.get("x-target")

            if not api_key or not api_url or not target:
                await websocket.close(code=1008, reason="Missing required headers: x-api-key, x-api-url, x-target")
                return

            # Receive request data
            data = await websocket.receive_json()
            request = SetupTaskRequest(**data)

            # Setup Daytona client and get sandbox
            daytona_config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)

            async with AsyncDaytona(config=daytona_config) as daytona:
                sandbox = await daytona.get(request.instance_id)

                # Call benchmark service implementation and stream results
                async for message in benchmark_service.setup_task(request.task_id, sandbox):
                    await websocket.send_json(message.model_dump())

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            await websocket.send_json(error_chunk.model_dump())
        finally:
            await websocket.close()

    @app.post("/evaluate-response/")
    def evaluate_response(request: EvaluateResponseRequest) -> Any:
        """
        Evaluate a text response directly (without sandbox execution).

        Use this endpoint when your benchmark can evaluate responses without
        executing code or running tests (e.g., text generation, QA tasks).

        Usage:
            curl -X POST http://localhost:8000/evaluate-response/ \
                -H "Content-Type: application/json" \
                -d '{"task_id": "task_1", "response": "The answer is..."}'

        Returns:
            Benchmark-specific evaluation result
        """
        return benchmark_service.evaluate_response(request)

    @app.websocket("/ws/evaluate-instance")
    async def evaluate_instance(websocket: WebSocket):
        """
        Evaluate a solution in a sandbox environment (WebSocket endpoint).

        Use this endpoint when your benchmark requires executing code,
        running tests, or otherwise interacting with a sandbox.

        Headers:
            x-api-key: API key for sandbox service
            x-api-url: API URL for sandbox service
            x-target: Target identifier for sandbox

        Body:
            {"task_id": "task_1", "instance_id": "instance_123"}

        The framework handles:
        1. WebSocket connection management
        2. Daytona client setup
        3. Sandbox retrieval
        4. Streaming yielded messages to client
        """
        await websocket.accept()

        try:
            # Extract headers
            api_key = websocket.headers.get("x-api-key")
            api_url = websocket.headers.get("x-api-url")
            target = websocket.headers.get("x-target")

            if not api_key or not api_url or not target:
                await websocket.close(code=1008, reason="Missing required headers: x-api-key, x-api-url, x-target")
                return

            # Receive request data
            data = await websocket.receive_json()
            request = EvaluateInstanceRequest(**data)

            # Setup Daytona client and get sandbox
            daytona_config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)

            async with AsyncDaytona(config=daytona_config) as daytona:
                sandbox = await daytona.get(request.instance_id)

                # Call benchmark service implementation and stream results
                async for message in benchmark_service.evaluate_instance(request.task_id, sandbox):
                    await websocket.send_json(message.model_dump())

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"WebSocket error: {error_msg}")
            error_chunk = StreamErrorChunk(type="error", data=error_msg)
            await websocket.send_json(error_chunk.model_dump())
        finally:
            await websocket.close()

    @app.post("/final-score/")
    async def final_score(request: FinalScoreRequest) -> FinalScoreResponse:
        """
        Calculate final aggregate score from all evaluation results.

        Usage:
            curl -X POST http://localhost:8000/final-score/ \
                -H "Content-Type: application/json" \
                -d '{"evaluation_results": {"task_1": {...}, "task_2": {...}}}'

        Returns:
            {
                "tasks_evaluated": ["task_1", "task_2"],
                "final_score": 75.0,
                "metadata": {...}
            }
        """
        tasks_evaluated = list(request.evaluation_results.keys())

        # Validate task IDs
        validated_task_ids = benchmark_service.validate_task_ids(tasks_evaluated)

        # Calculate final score using benchmark service implementation
        result = benchmark_service.calculate_final_score(request.evaluation_results)

        return FinalScoreResponse(
            tasks_evaluated=validated_task_ids,
            final_score=result.score,
            metadata=result.metadata,
        )

    return app
