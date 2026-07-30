"""HTTP/WebSocket client for communicating with a benchmark service."""

from collections.abc import Callable
from typing import Any

import httpx
import websockets
from pydantic import BaseModel, TypeAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)
from websockets.exceptions import ConnectionClosed

from benchmark_service.sandbox import SandboxProvider, SandboxProviderConfig
from benchmark_service.schemas import (
    EvaluateInstanceRequest,
    EvaluateResponseRequest,
    FinalScoreResponse,
    HealthCheckResponse,
    JsonValue,
    RetrieveTaskResponse,
    SetupTaskRequest,
    SetupTaskResponse,
    StreamChunk,
    VerifyTaskIdsResponse,
    VersionResponse,
)
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalRequest,
    V1EvalResponse,
    V1Payload,
    V1PayloadType,
    V1ScoreItem,
    V1ScoreRequest,
    V1ScoreResponse,
    V1UploadUrlRequest,
    V1UploadUrlResponse,
    V1Versions,
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

    status_code: int | None

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class BenchmarkServiceUnauthenticatedError(BenchmarkServiceError):
    """Exception raised when the benchmark service returns 401 — credentials are missing or invalid."""

    def __str__(self) -> str:
        return "Authentication failed: " + super().__str__()


def _unauthenticated_error(response: httpx.Response) -> "BenchmarkServiceUnauthenticatedError":
    """Parse the value from the httpx response"""

    try:
        detail = response.json().get("detail", response.text)
    except Exception:
        detail = response.text

    return BenchmarkServiceUnauthenticatedError(detail, status_code=response.status_code)


class BenchmarkServiceClient:
    """HTTP/WebSocket client for communicating with a benchmark service."""

    _url: str
    _headers: dict[str, str]
    _timeout: int
    _sandbox_providers: dict[str, SandboxProvider]

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
        self._sandbox_providers = {}
        self._http_client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers=headers,
            limits=httpx.Limits(max_connections=200),
        )

    def get_sandbox_provider(self, provider: SandboxProviderConfig) -> SandboxProvider:
        provider_key = provider.model_dump_json()
        if provider_key not in self._sandbox_providers:
            self._sandbox_providers[provider_key] = provider.create_provider()
        return self._sandbox_providers[provider_key]

    async def close(self) -> None:
        """Close the HTTP and sandbox provider clients."""
        await self._http_client.aclose()
        for sandbox_provider in self._sandbox_providers.values():
            await sandbox_provider.close()

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
    ) -> JsonValue:
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
        try:
            async with websockets.connect(
                f"{self._ws_url}/ws/{path}",
                additional_headers=self._headers,
                open_timeout=60,
                ping_timeout=None,
                max_size=10 * 1024 * 1024,  # 10MB
            ) as websocket:
                await websocket.send(request.model_dump_json())

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
        except ConnectionClosed as exc:
            close_frame = exc.rcvd or exc.sent
            if close_frame is None:
                detail = "without a close code"
            else:
                reason = f": {close_frame.reason}" if close_frame.reason else ""
                detail = f"with code {close_frame.code}{reason}"
            raise BenchmarkServiceError(f"WebSocket closed {detail}") from exc

        raise BenchmarkServiceError("Exited websocket without returning final result")

    @_retry_http
    async def health_check(self) -> HealthCheckResponse:
        """Check if the benchmark service is healthy."""
        response = await self._http_client.get(f"{self._url}/health")

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Health check failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return HealthCheckResponse.model_validate(response.json())

    @_retry_http
    async def version(self) -> VersionResponse:
        """Fetch framework and benchmark service version metadata."""
        response = await self._http_client.get(f"{self._url}/version")

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Version check failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return VersionResponse.model_validate(response.json())

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

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Verify task ids failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
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

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Retrieve task failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return RetrieveTaskResponse.model_validate(response.json())

    async def setup_task(
        self,
        task_id: str,
        instance_id: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
    ) -> SetupTaskResponse:
        """Set up a task instance via WebSocket.

        Args:
            task_id: The task to set up.
            instance_id: The instance to set up.
            sandbox_provider: Sandbox provider config for the task sandbox.
            on_message: Optional callback for intermediate progress messages.
        """
        request = SetupTaskRequest(
            task_id=task_id,
            instance_id=instance_id,
            sandbox_provider=sandbox_provider,
            dataset=dataset,
        )
        result = await self._websocket_request("setup-task", request, on_message)
        return SetupTaskResponse.model_validate(result)

    @_retry_http
    async def evaluate_response(
        self,
        task_id: str,
        response: str,
        dataset: str | None = None,
        sandbox_provider: SandboxProviderConfig | None = None,
    ) -> Any:
        """Evaluate a text response without a live sandbox.

        Args:
            task_id: The task to evaluate.
            response: The agent's response to evaluate.
            dataset: Optional dataset name.
            sandbox_provider: Optional request-scoped provider config when evaluation creates a sandbox.
        """
        request = EvaluateResponseRequest(
            task_id=task_id,
            response=response,
            sandbox_provider=sandbox_provider,
            dataset=dataset,
        )
        body = request.model_dump(exclude_none=True)

        resp = await self._http_client.post(
            f"{self._url}/evaluate-response/",
            json=body,
            timeout=self._timeout,
        )

        if resp.status_code == 401:
            raise _unauthenticated_error(resp)

        if resp.status_code != 200:
            raise BenchmarkServiceError(
                f"Evaluate response failed with status code {resp.status_code}, response: {resp.text}",
                status_code=resp.status_code,
            )

        return resp.json()

    async def resume_evaluation(
        self,
        task_id: str,
        eval_resume_state: dict[str, Any],
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
        sandbox_provider: SandboxProviderConfig | None = None,
    ) -> JsonValue:
        """Resume evaluation from state previously streamed by the benchmark service.

        ``sandbox_provider`` is sent only with this request; benchmark-owned
        ``eval_resume_state`` remains independently persistable.
        """
        request = EvaluateResponseRequest(
            task_id=task_id,
            eval_resume_state=eval_resume_state,
            sandbox_provider=sandbox_provider,
            dataset=dataset,
        )
        return await self._websocket_request("evaluate-response", request, on_message, on_eval_resume_state)

    async def evaluate_instance(
        self,
        task_id: str,
        instance_id: str,
        sandbox_provider: SandboxProviderConfig | None = None,
        on_message: Callable[[str], None] | None = None,
        dataset: str | None = None,
        on_eval_resume_state: Callable[[dict[str, Any]], None] | None = None,
    ) -> JsonValue:
        """Evaluate a task instance via WebSocket.

        Args:
            task_id: The task to evaluate.
            instance_id: The instance to evaluate.
            sandbox_provider: Sandbox provider config for the task sandbox.
            on_message: Optional callback for intermediate progress messages.
        """
        request = EvaluateInstanceRequest(
            task_id=task_id,
            instance_id=instance_id,
            sandbox_provider=sandbox_provider,
            dataset=dataset,
        )
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

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"Final score failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return FinalScoreResponse.model_validate(response.json())

    @_retry_http
    async def list_tasks(self, dataset: str) -> V1DatasetTasksResponse:
        """Fetch a dataset's task list via the lab-facing /v1/ surface.

        Auth headers (Descope) are taken from self._headers as set at
        construction. Server returns 403 for legacy bearer or unauthorized
        datasets, 404 for unknown datasets, and 501 when the benchmark has
        not implemented task listing.
        """
        response = await self._http_client.get(f"{self._url}/v1/datasets/{dataset}/tasks")

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"List tasks failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return V1DatasetTasksResponse.model_validate(response.json())

    async def v1_upload_url(
        self,
        run_id: str,
        task_id: str,
        filename: str,
        dataset: str | None = None,
    ) -> V1UploadUrlResponse:
        """Request an upload URL for one generated task artifact."""
        request = V1UploadUrlRequest(
            run_id=run_id,
            task_id=task_id,
            dataset=dataset,
            filename=filename,
        )
        response = await self._http_client.post(
            f"{self._url}/v1/submissions/upload-url",
            json=request.model_dump(mode="json", exclude_none=True),
        )

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"v1 upload URL request failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return V1UploadUrlResponse.model_validate(response.json())

    async def v1_evaluate(
        self,
        run_id: str,
        task_id: str,
        payload_data: str,
        payload_schema: str,
        payload_type: V1PayloadType,
        dataset: str | None = None,
        versions: V1Versions | None = None,
    ) -> V1EvalResponse:
        """Evaluate via the lab-facing /v1/evaluate surface (Descope-authenticated).

        payload_data is either inline text or an uploaded artifact key, as
        selected by payload_type. Transport failures are not retried because
        grading may already have started.
        """
        request = V1EvalRequest(
            run_id=run_id,
            task_id=task_id,
            dataset=dataset,
            payload=V1Payload(type=payload_type, schema=payload_schema, data=payload_data),
            versions=versions or V1Versions(),
        )
        response = await self._http_client.post(
            f"{self._url}/v1/evaluate",
            json=request.model_dump(mode="json", by_alias=True, exclude_none=True),
        )

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"v1 evaluate failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return V1EvalResponse.model_validate(response.json())

    async def v1_score(
        self,
        run_id: str,
        evaluation_results: dict[str, V1ScoreItem | None],
        dataset: str | None = None,
    ) -> V1ScoreResponse:
        """Score via the lab-facing /v1/score surface without retrying a submitted request."""
        request = V1ScoreRequest(run_id=run_id, dataset=dataset, evaluation_results=evaluation_results)
        response = await self._http_client.post(
            f"{self._url}/v1/score",
            json=request.model_dump(mode="json", exclude_none=True),
        )

        if response.status_code == 401:
            raise _unauthenticated_error(response)

        if response.status_code != 200:
            raise BenchmarkServiceError(
                f"v1 score failed with status code {response.status_code}, response: {response.text}",
                status_code=response.status_code,
            )

        return V1ScoreResponse.model_validate(response.json())
