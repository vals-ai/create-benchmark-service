from __future__ import annotations

import asyncio
import re
import secrets
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError
from starlette.middleware.base import RequestResponseEndpoint
from starlette.websockets import WebSocketDisconnect

from benchmark_service.sandbox.kubernetes.control.backend import (
    SandboxConflictError,
    SandboxControlBackend,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandErrorEvent,
    CommandEvent,
    CommandExitEvent,
    CommandRequest,
    ControlErrorDetail,
    ControlErrorResponse,
    EgressRequest,
)
from benchmark_service.sandbox.types import (
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
)

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _request_id(headers: Mapping[str, str]) -> str:
    value = headers.get("x-request-id")
    return value if value and _REQUEST_ID.fullmatch(value) else str(uuid.uuid4())


def _authorized(authorization: str | None, token: str) -> bool:
    if not authorization or not authorization.startswith("Bearer "):
        return False
    return secrets.compare_digest(authorization.removeprefix("Bearer "), token)


def _error_detail(error: SandboxError, request_id: str) -> tuple[int, ControlErrorDetail]:
    if isinstance(error, SandboxNotFoundError):
        return 404, ControlErrorDetail(code="not_found", message=str(error), request_id=request_id)
    if isinstance(error, SandboxConnectionError):
        return 503, ControlErrorDetail(code="unavailable", message=str(error), request_id=request_id)
    if isinstance(error, SandboxConflictError):
        return 409, ControlErrorDetail(code="conflict", message=str(error), request_id=request_id)
    return 500, ControlErrorDetail(code="sandbox_error", message=str(error), request_id=request_id)


def _error_response(status_code: int, detail: ControlErrorDetail) -> JSONResponse:
    body = ControlErrorResponse(error=detail).model_dump(mode="json")
    return JSONResponse(status_code=status_code, content=body)


async def command_events_to_ndjson(
    stream: AsyncGenerator[CommandEvent, None],
    *,
    request_id: str,
    heartbeat_seconds: float,
) -> AsyncGenerator[bytes, None]:
    """Encode command events and keep an idle HTTP stream alive."""
    pending_event: asyncio.Task[CommandEvent] | None = None
    terminal_received = False
    try:
        while True:
            if pending_event is None:
                pending_event = asyncio.create_task(anext(stream))
            completed, _ = await asyncio.wait((pending_event,), timeout=heartbeat_seconds)
            if not completed:
                yield b"\n"
                continue
            try:
                event = pending_event.result()
            except StopAsyncIteration:
                if terminal_received:
                    return
                raise SandboxConnectionError("Command stream ended without a terminal event") from None
            pending_event = None
            terminal_received = isinstance(event, (CommandExitEvent, CommandErrorEvent))
            yield f"{event.model_dump_json()}\n".encode()
            if terminal_received:
                return
    except SandboxError as error:
        _, detail = _error_detail(error, request_id)
        event = CommandErrorEvent(
            type="error",
            code=detail.code,
            message=detail.message,
            request_id=request_id,
        )
        yield f"{event.model_dump_json()}\n".encode()
    finally:
        if pending_event is not None:
            pending_event.cancel()
            await asyncio.gather(pending_event, return_exceptions=True)
        await stream.aclose()


def _parse_labels(values: list[str]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for value in values:
        name, separator, label_value = value.partition("=")
        if not separator or not name or not label_value:
            raise ValueError(f"Malformed label selector: {value}")
        labels[name] = label_value
    return labels


def create_kubernetes_control_app(
    settings: KubernetesControlSettings,
    backend: SandboxControlBackend,
    *,
    readiness: Callable[[], Awaitable[bool]] | None = None,
) -> FastAPI:
    """Create the private sandbox control service without changing cluster state."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await backend.close()

    app = FastAPI(lifespan=lifespan)

    async def add_request_id(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.request_id = _request_id(request.headers)
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    async def handle_sandbox_error(request: Request, error: Exception) -> JSONResponse:
        assert isinstance(error, SandboxError)
        status_code, detail = _error_detail(error, request.state.request_id)
        return _error_response(status_code, detail)

    async def handle_invalid_request(request: Request, error: Exception) -> JSONResponse:
        return _error_response(
            422,
            ControlErrorDetail(
                code="invalid_request",
                message=str(error),
                request_id=request.state.request_id,
            ),
        )

    app.middleware("http")(add_request_id)
    app.add_exception_handler(SandboxError, handle_sandbox_error)
    app.add_exception_handler(ValidationError, handle_invalid_request)
    app.add_exception_handler(ValueError, handle_invalid_request)

    def require_auth(request: Request) -> JSONResponse | None:
        if _authorized(request.headers.get("authorization"), settings.api_token):
            return None
        return _error_response(
            401,
            ControlErrorDetail(
                code="unauthorized",
                message="Authentication required",
                request_id=request.state.request_id,
            ),
        )

    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def ready() -> Response:
        if readiness is None or await readiness():
            return JSONResponse(content={"status": "ready"})
        return JSONResponse(status_code=503, content={"status": "unavailable"})

    async def create(request: Request) -> Response:
        if error := require_auth(request):
            return error
        payload = SandboxCreateRequest.model_validate(await request.json())
        record = await backend.create_sandbox(payload)
        return JSONResponse(status_code=201, content=record.model_dump(mode="json"))

    async def get(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        record = await backend.get_sandbox(instance_id)
        return JSONResponse(content=record.model_dump(mode="json"))

    async def list_sandboxes(request: Request) -> Response:
        if error := require_auth(request):
            return error
        try:
            labels = _parse_labels(request.query_params.getlist("label"))
        except ValueError as error:
            return _error_response(
                422,
                ControlErrorDetail(
                    code="invalid_request",
                    message=str(error),
                    request_id=request.state.request_id,
                ),
            )
        limit = int(request.query_params.get("limit", "10"))
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        page = await backend.list_sandboxes(labels, limit, request.query_params.get("continue_token"))
        return JSONResponse(content=page.model_dump(mode="json"))

    async def delete(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        await backend.delete_sandbox(instance_id)
        return Response(status_code=204)

    async def exec_command(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        payload = CommandRequest.model_validate(await request.json())
        result = await backend.exec(instance_id, payload)
        return JSONResponse(content=result.model_dump(mode="json"))

    async def stream_command(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        payload = CommandRequest.model_validate(await request.json())
        request_id = request.state.request_id

        return StreamingResponse(
            command_events_to_ndjson(
                backend.command(instance_id, payload),
                request_id=request_id,
                heartbeat_seconds=settings.command_heartbeat_seconds,
            ),
            media_type="application/x-ndjson",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    async def upload(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        remote_path = request.query_params.get("path")
        if not remote_path:
            return _error_response(
                422,
                ControlErrorDetail(
                    code="invalid_request",
                    message="path is required",
                    request_id=request.state.request_id,
                ),
            )
        await backend.upload_file(instance_id, remote_path, request.stream())
        return Response(status_code=204)

    async def download(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        remote_path = request.query_params.get("path")
        if not remote_path:
            return _error_response(
                422,
                ControlErrorDetail(
                    code="invalid_request",
                    message="path is required",
                    request_id=request.state.request_id,
                ),
            )
        return StreamingResponse(
            backend.stream_download(instance_id, remote_path),
            media_type="application/octet-stream",
        )

    async def modify_egress(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        payload = EgressRequest.model_validate(await request.json())
        await backend.modify_egress_rules(instance_id, payload.allowed_addresses)
        return Response(status_code=204)

    async def clear_egress(request: Request, instance_id: str) -> Response:
        if error := require_auth(request):
            return error
        await backend.clear_egress_rules(instance_id)
        return Response(status_code=204)

    async def command(websocket: WebSocket, instance_id: str) -> None:
        async def forward_events(payload: CommandRequest) -> None:
            async for event in backend.command(instance_id, payload):
                await websocket.send_json(event.model_dump(mode="json"))

        async def wait_for_disconnect() -> None:
            while (await websocket.receive())["type"] != "websocket.disconnect":
                pass

        request_id = _request_id(websocket.headers)
        if not _authorized(websocket.headers.get("authorization"), settings.api_token):
            await websocket.close(code=1008, reason="Authentication required")
            return
        await websocket.accept(headers=[(b"x-request-id", request_id.encode())])
        try:
            payload = CommandRequest.model_validate(await websocket.receive_json())
            event_task = asyncio.create_task(forward_events(payload))
            disconnect_task = asyncio.create_task(wait_for_disconnect())
            tasks = (event_task, disconnect_task)
            try:
                completed, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in completed:
                    task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        except WebSocketDisconnect:
            pass
        except (SandboxError, ValidationError) as error:
            if isinstance(error, SandboxError):
                _, detail = _error_detail(error, request_id)
            else:
                detail = ControlErrorDetail(
                    code="invalid_request",
                    message=str(error),
                    request_id=request_id,
                )
            event = CommandErrorEvent(
                type="error",
                code=detail.code,
                message=detail.message,
                request_id=request_id,
            )
            await websocket.send_json(event.model_dump(mode="json"))
        finally:
            with suppress(RuntimeError):
                await websocket.close()

    app.add_api_route("/health", health, methods=["GET"])
    app.add_api_route("/ready", ready, methods=["GET"])
    app.add_api_route("/v1/sandboxes", create, methods=["POST"])
    app.add_api_route("/v1/sandboxes", list_sandboxes, methods=["GET"])
    app.add_api_route("/v1/sandboxes/{instance_id}", get, methods=["GET"])
    app.add_api_route("/v1/sandboxes/{instance_id}", delete, methods=["DELETE"])
    app.add_api_route("/v1/sandboxes/{instance_id}/exec", exec_command, methods=["POST"])
    app.add_api_route("/v1/sandboxes/{instance_id}/command", stream_command, methods=["POST"])
    app.add_api_route("/v1/sandboxes/{instance_id}/files", upload, methods=["PUT"])
    app.add_api_route("/v1/sandboxes/{instance_id}/files", download, methods=["GET"])
    app.add_api_route("/v1/sandboxes/{instance_id}/egress", modify_egress, methods=["PUT"])
    app.add_api_route("/v1/sandboxes/{instance_id}/egress", clear_egress, methods=["DELETE"])
    app.add_api_websocket_route("/v1/sandboxes/{instance_id}/command", command)
    return app
