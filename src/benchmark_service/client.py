"""HTTP/WebSocket client for communicating with a benchmark service."""

from collections.abc import Callable
from typing import Any

import httpx
import websockets
from daytona import AsyncDaytona, DaytonaConfig
from pydantic import BaseModel, TypeAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)

from benchmark_service.schemas import (
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskRequest,
    SetupTaskResponse,
    StreamChunk,
    VerifyTaskIdsResponse,
)

_stream_chunk_adapter: TypeAdapter[StreamChunk] = TypeAdapter(StreamChunk)

_retry_http = retry(
    retry=retry_if_exception_type(
        (
            httpx.RemoteProtocolError,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadError,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        )
    ),
    stop=stop_after_attempt(5),
    wait=wait_random(min=1, max=10),
    reraise=True,
)


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
        self._http_client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers=headers,
            limits=httpx.Limits(max_connections=200),
        )

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
                connection_pool_maxsize=None,
            )
        )

        return self._daytona_client

    async def close(self) -> None:
        """Close the HTTP and Daytona clients."""
        await self._http_client.aclose()
        if self._daytona_client:
            await self._daytona_client.close()

    async def __aenter__(self) -> "BenchmarkServiceClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    @property
    def _ws_url(self) -> str:
        return self._url.replace("http://", "ws://").replace("https://", "wss://")

    async def _websocket_request(
        self,
        path: str,
        request: BaseModel,
        on_message: Callable[[str], None] | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
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
            ping_timeout=None,
            max_size=10 * 1024 * 1024,  # 10MB
        ) as websocket:
            await websocket.send(request.model_dump_json(exclude_none=True))

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
                    case "eval_resume_state":
                        if on_eval_resume_state:
                            on_eval_resume_state(chunk.data)

        raise BenchmarkServiceError("Exited websocket without returning final result")

    @_retry_http
    async def health_check(self) -> HealthCheckResponse:
        """Check if the benchmark service is healthy."""
        response = await self._http_client.get(f"{self._url}/health")

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}"
            )

        return HealthCheckResponse.model_validate(response.json())

    @_retry_http
    async def verify_task_ids(
        self, task_ids: list[str] | None, slice_str: str | None, dataset: str | None = None
    ) -> VerifyTaskIdsResponse:
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

        response = await self._http_client.get(f"{self._url}/verify-task-ids", params=params)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}"
            )

        return VerifyTaskIdsResponse.model_validate(response.json())

    @_retry_http
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        """Retrieve a task by ID.

        Args:
            task_id: The task to retrieve.
            skip_validation: If True, skip task validation.
        """
        params: dict[str, Any] = {"task_id": task_id, "skip_validation": skip_validation}
        if dataset is not None:
            params["dataset"] = dataset
        response = await self._http_client.get(f"{self._url}/retrieve-task/", params=params)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve task failed with status code {response.status_code}, response: {response.text}"
            )

        return RetrieveTaskResponse.model_validate(response.json())

    async def setup_task(
        self,
        task_id: str,
        instance_id: str,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
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

    @_retry_http
    async def evaluate_response(
        self,
        task_id: str,
        response: str,
        dataset: str | None = None,
    ) -> Any:
        """Evaluate a text response without a live sandbox.

        Args:
            task_id: The task to evaluate.
            response: The agent's response to evaluate.
            dataset: Optional dataset name.
        """
        request = EvaluateResponseRequest(task_id=task_id, response=response, dataset=dataset)
        body = request.model_dump(exclude_none=True)

        resp = await self._http_client.post(
            f"{self._url}/evaluate-response/",
            json=body,
            timeout=self._timeout,
        )

        if resp.status_code != 200:
            raise BenchmarkServiceError(
                f"Evaluate response failed with status code {resp.status_code}, response: {resp.text}"
            )

        return resp.json()

    async def resume_evaluation(
        self,
        task_id: str,
        eval_resume_state: dict[str, Any],
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Resume evaluation from state previously streamed by the benchmark service."""
        request = EvaluateResponseRequest(task_id=task_id, eval_resume_state=eval_resume_state, dataset=dataset)
        return await self._websocket_request("evaluate-response", request, on_message, on_eval_resume_state)

    async def evaluate_instance(
        self,
        task_id: str,
        instance_id: str,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Evaluate a task instance via WebSocket.

        Args:
            task_id: The task to evaluate.
            instance_id: The instance to evaluate.
            on_message: Optional callback for intermediate progress messages.
        """
        request = EvaluateInstanceRequest(task_id=task_id, instance_id=instance_id, dataset=dataset)
        return await self._websocket_request("evaluate-instance", request, on_message, on_eval_resume_state)

    @_retry_http
    async def final_score(self, evaluation_results: dict[str, Any], dataset: str | None = None) -> FinalScoreResponse:
        """Compute the final score from evaluation results.

        Args:
            evaluation_results: Mapping of evaluation results to score.
        """
        body: dict[str, Any] = {"evaluation_results": evaluation_results}
        if dataset is not None:
            body["dataset"] = dataset

        response = await self._http_client.post(
            f"{self._url}/final-score/",
            json=body,
        )

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}"
            )

        return FinalScoreResponse.model_validate(response.json())
