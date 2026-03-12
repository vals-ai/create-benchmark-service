"""HTTP/WebSocket client for communicating with a benchmark service."""

from collections.abc import Callable
from typing import Any

import httpx
import websockets
from daytona import AsyncDaytona, DaytonaConfig
from pydantic import BaseModel, TypeAdapter
from websockets.exceptions import ConnectionClosed

from benchmark_service.schemas import (
    EvaluateInstanceRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskRequest,
    SetupTaskResponse,
    StreamChunk,
    VerifyTaskIdsResponse,
)

_stream_chunk_adapter: TypeAdapter[StreamChunk] = TypeAdapter(StreamChunk)


class BenchmarkServiceError(Exception):
    """Exception raised for benchmark service communication errors."""

    pass


class BenchmarkServiceClient:
    """HTTP/WebSocket client for communicating with a benchmark service."""

    _url: str
    _headers: dict[str, str]
    _timeout: int
    _daytona_client: AsyncDaytona | None = None

    def __init__(self, url: str, headers: dict[str, str], timeout: int = 60):
        """Initialize the client.

        Args:
            url: Base URL of the benchmark service.
            headers: Headers to include in all requests.
            timeout: Request timeout in seconds.
        """
        self._url = url
        self._headers = headers
        self._timeout = timeout

    @property
    def daytona_client(self) -> AsyncDaytona:
        """Lazy-initialized Daytona SDK client, built from the same headers used for API requests."""
        if self._daytona_client:
            return self._daytona_client

        self._daytona_client = AsyncDaytona(
            config=DaytonaConfig(
                api_key=self._headers["x-api-key"],
                api_url=self._headers["x-api-url"],
                target=self._headers["x-target"],
            )
        )

        return self._daytona_client

    async def close(self) -> None:
        """Close the Daytona client if it was initialized."""
        if self._daytona_client:
            await self._daytona_client.close()

    @property
    def _ws_url(self) -> str:
        return self._url.replace("http://", "ws://").replace("https://", "wss://")

    async def _websocket_request(
        self, path: str, request: BaseModel, on_message: Callable[[str], None] | None = None
    ) -> dict[str, Any]:
        """Send a request over WebSocket and stream the response.

        Args:
            path: WebSocket endpoint path (appended to /ws/).
            request: Pydantic model to serialize and send as the initial message.
            on_message: Optional callback invoked for each intermediate "message" chunk.

        Returns:
            The data payload from the final "result" chunk.

        Raises:
            BenchmarkServiceError: If an "error" chunk is received or the connection
                closes without a result.
        """
        async with websockets.connect(
            f"{self._ws_url}/ws/{path}",
            additional_headers=self._headers,
            open_timeout=60,
            max_size=10 * 1024 * 1024,  # 10MB
        ) as websocket:
            await websocket.send(request.model_dump_json())

            try:
                async for message in websocket:
                    chunk: StreamChunk = _stream_chunk_adapter.validate_json(message)

                    match chunk.type:
                        case "error":
                            raise BenchmarkServiceError(chunk.data)
                        case "result":
                            return chunk.data
                        case "message":
                            if on_message:
                                on_message(chunk.data)
            except ConnectionClosed:
                pass

        raise BenchmarkServiceError("Exited websocket without returning final result")

    async def health_check(self) -> HealthCheckResponse:
        """Check if the benchmark service is healthy."""
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
            response = await client.get(f"{self._url}/health", headers=self._headers)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}"
            )

        return HealthCheckResponse.model_validate(response.json())

    async def verify_task_ids(self, task_ids: list[str] | None, slice_str: str | None, dataset: str | None = None) -> VerifyTaskIdsResponse:
        """Verify that the given task IDs or slice are valid.

        Args:
            task_ids: List of task IDs to verify, or None.
            slice_str: Slice string to verify, or None.
        """
        params: dict[str, list[str] | str] = {}
        if task_ids is not None:
            params["task_ids"] = task_ids
        if slice_str is not None:
            params["slice"] = slice_str
        if dataset is not None:
            params["dataset"] = dataset

        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
            response = await client.get(f"{self._url}/verify-task-ids", params=params, headers=self._headers)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}"
            )

        return VerifyTaskIdsResponse.model_validate(response.json())

    async def retrieve_task(self, task_id: str, skip_validation: bool = False, dataset: str | None = None) -> RetrieveTaskResponse:
        """Retrieve a task by ID.

        Args:
            task_id: The task to retrieve.
            skip_validation: If True, skip task validation.
        """
        params: dict[str, Any] = {"task_id": task_id, "skip_validation": skip_validation}
        if dataset is not None:
            params["dataset"] = dataset
        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
            response = await client.get(f"{self._url}/retrieve-task/", params=params, headers=self._headers)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve task failed with status code {response.status_code}, response: {response.text}"
            )

        return RetrieveTaskResponse.model_validate(response.json())

    async def setup_task(
        self, task_id: str, instance_id: str, on_message: Callable[[str], None] | None = None, dataset: str | None = None
    ) -> SetupTaskResponse:
        """Set up a task instance via WebSocket.

        Args:
            task_id: The task to set up.
            instance_id: The instance to set up.
            on_message: Optional callback for intermediate progress messages.
        """
        request = SetupTaskRequest(task_id=task_id, instance_id=instance_id, dataset=dataset)
        result = await self._websocket_request("setup-task", request, on_message)
        return SetupTaskResponse.model_validate(result)

    async def evaluate_instance(
        self, task_id: str, instance_id: str, on_message: Callable[[str], None] | None = None, dataset: str | None = None
    ) -> dict[str, Any]:
        """Evaluate a task instance via WebSocket.

        Args:
            task_id: The task to evaluate.
            instance_id: The instance to evaluate.
            on_message: Optional callback for intermediate progress messages.
        """
        request = EvaluateInstanceRequest(task_id=task_id, instance_id=instance_id, dataset=dataset)
        return await self._websocket_request("evaluate-instance", request, on_message)

    async def final_score(self, evaluation_results: dict[str, Any], dataset: str | None = None) -> FinalScoreResponse:
        """Compute the final score from evaluation results.

        Args:
            evaluation_results: Mapping of evaluation results to score.
        """
        body: dict[str, Any] = {"evaluation_results": evaluation_results}
        if dataset is not None:
            body["dataset"] = dataset

        async with httpx.AsyncClient(follow_redirects=True, timeout=self._timeout) as client:
            response = await client.post(
                f"{self._url}/final-score/",
                json=body,
                headers={**self._headers, "Content-Type": "application/json"},
            )

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}"
            )

        return FinalScoreResponse.model_validate(response.json())
