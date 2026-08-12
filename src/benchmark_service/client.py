"""HTTP/WebSocket client for communicating with a benchmark service."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar
from uuid import uuid4

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

from benchmark_service.sandbox import SandboxNotFoundError, SandboxProvider, SandboxProviderConfig
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

logger = logging.getLogger(__name__)

_stream_chunk_adapter: TypeAdapter[StreamChunk] = TypeAdapter(StreamChunk)
_RecoveryResult = TypeVar("_RecoveryResult")

_OUTAGE_ID_ENV = "VALKYRIE_SANDBOX_OUTAGE_ID"
_OUTAGE_STARTED_ENV = "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH"

# Server half of the keepalive contract, set in templates/Dockerfile:
# uvicorn --ws-ping-interval 30 --ws-ping-timeout 10.
_SERVER_PING_INTERVAL_S = 30
_SERVER_PING_TIMEOUT_S = 10
# The client pings on that cadence but never times a pong out: a blocked server event loop runs no
# keepalive of its own, so a client deadline would make it the sole actor and fail evaluations the
# server goes on to complete.
_WS_PING_INTERVAL_S = _SERVER_PING_INTERVAL_S
_WS_PING_TIMEOUT_S = None
# Silence budget for an established stream. Protocol keepalive cannot detect a peer that stops
# producing while the socket stays open (lost host, blackholed connection), which leaves the stream
# blocked forever; this bounds that wait. The budget is generous because a slow evaluation is
# indistinguishable from a dead one from the client side.
_WS_IDLE_TIMEOUT_ENV = "BENCHMARK_SERVICE_WS_IDLE_TIMEOUT_S"
_DEFAULT_WS_IDLE_TIMEOUT_S = 3600.0
# Distinguishes "caller passed None to disable" from "caller said nothing".
_UNSET_WS_IDLE_TIMEOUT: float = -1.0


def _default_ws_idle_timeout_s() -> float | None:
    """Idle budget from the environment; ``0`` or a negative value disables the watchdog."""
    raw = os.environ.get(_WS_IDLE_TIMEOUT_ENV)
    if raw is None:
        return _DEFAULT_WS_IDLE_TIMEOUT_S
    try:
        parsed = float(raw)
    except ValueError:
        logger.warning("Ignoring non-numeric %s=%r", _WS_IDLE_TIMEOUT_ENV, raw)
        return _DEFAULT_WS_IDLE_TIMEOUT_S
    return parsed if parsed > 0 else None


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


class BenchmarkServiceStreamIdleError(BenchmarkServiceError):
    """Raised when an established evaluation WebSocket goes silent past the idle budget.

    The socket was never closed, so keepalive and close handling cannot surface the failure.
    ``health_ok`` records whether the service answered ``/health`` while the stream was silent,
    which separates a wedged or lost task from a service that is down entirely.
    """

    idle_s: float
    health_ok: bool

    def __init__(self, *, idle_s: float, health_ok: bool) -> None:
        health = "service still reports healthy" if health_ok else "service health check also failing"
        super().__init__(f"WebSocket idle for {idle_s:.1f}s without an application message ({health})")
        self.idle_s = idle_s
        self.health_ok = health_ok


class BenchmarkServiceStreamClosedError(BenchmarkServiceError):
    """Raised when an established evaluation WebSocket closes without a terminal chunk.

    ``idle_s`` counts silence since the last application message, or since connect if none arrived.
    """

    close_code: int | None
    close_reason: str | None
    idle_s: float

    def __init__(self, *, close_code: int | None, close_reason: str | None, idle_s: float) -> None:
        if close_code is None:
            detail = "without a close frame"
        else:
            reason = f": {close_reason}" if close_reason else ""
            detail = f"with code {close_code}{reason}"
        super().__init__(f"WebSocket closed {detail} after {idle_s:.1f}s without an application message")
        self.close_code = close_code
        self.close_reason = close_reason
        self.idle_s = idle_s


def _unauthenticated_error(response: httpx.Response) -> "BenchmarkServiceUnauthenticatedError":
    """Parse the value from the httpx response"""

    try:
        detail = response.json().get("detail", response.text)
    except Exception:
        detail = response.text

    return BenchmarkServiceUnauthenticatedError(detail, status_code=response.status_code)


@dataclass(frozen=True)
class SandboxRecoveryAttempt:
    """One bounded invocation of a durable sandbox-backed task.

    ``environment`` is empty for the initial attempt. After a provider-confirmed
    sandbox loss it carries stable outage metadata until the replacement has
    completed benchmark setup and calls ``mark_replacement_ready``. If that
    setup fails, the next replacement receives the same outage identity rather
    than double-crediting one interruption.
    """

    number: int
    outage_id: str | None
    outage_started_epoch: float | None
    _state: _SandboxRecoveryState = field(repr=False, compare=False)

    @property
    def max_attempts(self) -> int:
        """Current overall cap after applying the cached benchmark policy."""
        return self._state.max_attempts

    @property
    def sandbox_loss_retry_available(self) -> bool:
        """Whether this attempt may recover a typed provider sandbox loss."""
        return self._state.recover_sandbox_loss and self.number < self._state.max_attempts

    @property
    def environment(self) -> dict[str, str]:
        """Environment metadata to add to this attempt's sandbox."""
        if self.outage_id is None or self.outage_started_epoch is None:
            return {}
        return {
            _OUTAGE_ID_ENV: self.outage_id,
            _OUTAGE_STARTED_ENV: str(self.outage_started_epoch),
        }

    def mark_replacement_ready(self) -> None:
        """Acknowledge that benchmark setup persisted this outage metadata."""
        self._state.mark_ready(self.outage_id)

    async def retrieve_task(self) -> RetrieveTaskResponse:
        """Load and cache the task payload and its recovery policy."""
        return await self._state.retrieve_task()


@dataclass
class _SandboxRecoveryState:
    run_id: str
    task_id: str
    load_task: Callable[[], Awaitable[RetrieveTaskResponse]] = field(repr=False)
    default_max_attempts: int
    outage_id: str | None = None
    outage_started_epoch: float | None = None
    consecutive_attempt_errors: int = 0
    task: RetrieveTaskResponse | None = None
    recover_sandbox_loss: bool = False
    max_attempts: int = field(init=False)

    def __post_init__(self) -> None:
        self.max_attempts = self.default_max_attempts

    def attempt(self, number: int) -> SandboxRecoveryAttempt:
        return SandboxRecoveryAttempt(
            number=number,
            outage_id=self.outage_id,
            outage_started_epoch=self.outage_started_epoch,
            _state=self,
        )

    async def retrieve_task(self) -> RetrieveTaskResponse:
        if self.task is None:
            self.task = await self.load_task()
            policy = self.task.sandbox_recovery
            self.recover_sandbox_loss = policy is not None
            if policy is not None:
                self.max_attempts = policy.max_sandbox_attempts
        return self.task

    def record_loss(self, attempt_number: int) -> None:
        self.consecutive_attempt_errors = 0
        if self.outage_id is not None:
            return
        self.outage_id = f"{self.run_id}:{self.task_id}:{attempt_number}:{uuid4().hex}"
        self.outage_started_epoch = time.time()

    def record_attempt_error(self) -> None:
        self.consecutive_attempt_errors += 1

    def mark_ready(self, outage_id: str | None) -> None:
        if outage_id is None or outage_id != self.outage_id:
            return
        self.outage_id = None
        self.outage_started_epoch = None


class BenchmarkServiceClient:
    """HTTP/WebSocket client for communicating with a benchmark service."""

    _url: str
    _headers: dict[str, str]
    _timeout: int
    _ws_idle_timeout_s: float | None
    _sandbox_providers: dict[str, SandboxProvider]

    def __init__(
        self,
        url: str,
        headers: dict[str, str],
        timeout: int = 60,
        ws_idle_timeout_s: float | None = _UNSET_WS_IDLE_TIMEOUT,
    ):
        """Initialize the client.

        Args:
            url: Base URL of the benchmark service.
            headers: Headers to include in all requests.
            timeout: Request timeout in seconds.
            ws_idle_timeout_s: Silence budget for an established evaluation stream, after which
                the stream fails with ``BenchmarkServiceStreamIdleError``. ``None`` disables the
                watchdog; omit it to take the value from ``BENCHMARK_SERVICE_WS_IDLE_TIMEOUT_S``.
        """
        self._url = url
        self._headers = headers
        self._timeout = timeout
        idle_timeout = _default_ws_idle_timeout_s() if ws_idle_timeout_s is _UNSET_WS_IDLE_TIMEOUT else ws_idle_timeout_s
        self._ws_idle_timeout_s = idle_timeout if idle_timeout is None or idle_timeout > 0 else None
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
            BenchmarkServiceError: If an "error" chunk is received.
            BenchmarkServiceStreamClosedError: If the established socket closes without a
                terminal chunk. Pre-connection failures (DNS, connect, handshake) stay
                distinguishable: they propagate unwrapped from websockets.connect.
            BenchmarkServiceStreamIdleError: If the socket stays open but produces no
                application message within the idle budget.
        """
        async with websockets.connect(
            f"{self._ws_url}/ws/{path}",
            additional_headers=self._headers,
            open_timeout=60,
            ping_interval=_WS_PING_INTERVAL_S,
            ping_timeout=_WS_PING_TIMEOUT_S,
            max_size=10 * 1024 * 1024,  # 10MB
        ) as websocket:
            last_message_at = time.monotonic()

            try:
                await websocket.send(request.model_dump_json())

                stream = aiter(websocket)
                while True:
                    try:
                        message = await asyncio.wait_for(anext(stream), timeout=self._ws_idle_timeout_s)
                    except StopAsyncIteration:
                        break
                    except TimeoutError:
                        raise BenchmarkServiceStreamIdleError(
                            idle_s=time.monotonic() - last_message_at,
                            health_ok=await self._health_ok(),
                        ) from None

                    last_message_at = time.monotonic()
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
                raise BenchmarkServiceStreamClosedError(
                    close_code=close_frame.code if close_frame else None,
                    close_reason=close_frame.reason if close_frame else None,
                    idle_s=time.monotonic() - last_message_at,
                ) from exc

            # The socket closed cleanly, but a stream without a terminal chunk is still broken.
            raise BenchmarkServiceStreamClosedError(
                close_code=websocket.close_code,
                close_reason=websocket.close_reason,
                idle_s=time.monotonic() - last_message_at,
            )

    async def _health_ok(self) -> bool:
        """Best-effort liveness probe used to describe an idle stream failure."""
        try:
            return (await self.health_check()).status == "ok"
        except Exception:
            return False

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

    async def run_with_sandbox_recovery(
        self,
        task_id: str,
        run_id: str,
        operation: Callable[[SandboxRecoveryAttempt], Awaitable[_RecoveryResult]],
        *,
        dataset: str | None = None,
        retryable_attempt_errors: tuple[type[Exception], ...] = (),
        default_max_attempts: int = 1,
        retry_delay_s: float = 2.0,
        on_retry: Callable[[SandboxRecoveryAttempt, Exception], Awaitable[None] | None] | None = None,
    ) -> _RecoveryResult:
        """Run a task attempt with benchmark-declared sandbox-loss recovery.

        ``SandboxRecoveryAttempt.retrieve_task`` lazily loads and caches the
        task payload so successful durable evaluation resumes need no task
        request. ``SandboxNotFoundError`` is retried only when the benchmark
        explicitly opts in. Callers may
        additionally name setup errors that require a fresh sandbox. These use
        ``default_max_attempts`` both as the fallback total cap for benchmarks
        without a recovery policy and as their consecutive-attempt cap. Every
        attempt still counts against the benchmark's declared overall cap.
        Synchronous and asynchronous ``on_retry`` callbacks are both
        supported and complete before the retry delay begins.

        The final exception is re-raised unchanged when the applicable attempt
        budget is exhausted.
        """
        if not run_id:
            raise ValueError("run_id must be non-empty")
        if not task_id:
            raise ValueError("task_id must be non-empty")
        if not 1 <= default_max_attempts <= 20:
            raise ValueError("default_max_attempts must be between 1 and 20")
        if retry_delay_s < 0:
            raise ValueError("retry_delay_s must be non-negative")
        if any(issubclass(error_type, SandboxNotFoundError) for error_type in retryable_attempt_errors):
            raise ValueError("SandboxNotFoundError is controlled only by SandboxRecoveryPolicy")

        state = _SandboxRecoveryState(
            run_id=run_id,
            task_id=task_id,
            load_task=lambda: self.retrieve_task(task_id=task_id, dataset=dataset),
            default_max_attempts=default_max_attempts,
        )

        attempt_number = 1
        while attempt_number <= state.max_attempts:
            attempt = state.attempt(attempt_number)
            try:
                return await operation(attempt)
            except SandboxNotFoundError as exc:
                if state.task is None:
                    await state.retrieve_task()
                if not attempt.sandbox_loss_retry_available:
                    raise
                state.record_loss(attempt_number)
                retry_error: Exception = exc
            except Exception as exc:
                if (
                    not isinstance(exc, retryable_attempt_errors)
                    or attempt_number >= state.max_attempts
                    or state.consecutive_attempt_errors >= default_max_attempts - 1
                ):
                    raise
                state.record_attempt_error()
                retry_error = exc

            if on_retry is not None:
                callback_result = on_retry(attempt, retry_error)
                if inspect.isawaitable(callback_result):
                    await callback_result
            if retry_delay_s:
                await asyncio.sleep(retry_delay_s)
            attempt_number += 1

        raise AssertionError("sandbox recovery loop exited without a result")

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
