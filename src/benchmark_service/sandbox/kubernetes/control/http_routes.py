"""Register authenticated HTTP endpoints for the Kubernetes control service."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from benchmark_service.sandbox.kubernetes.control.backend import SandboxControlBackend
from benchmark_service.sandbox.kubernetes.control.errors import authorized, error_response
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.control.streaming import command_events_to_ndjson
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandRequest,
    ControlErrorDetail,
    EgressRequest,
)
from benchmark_service.sandbox.types import SandboxCreateRequest


def _parse_labels(values: list[str]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for value in values:
        name, separator, label_value = value.partition("=")
        if not separator or not name or not label_value:
            raise ValueError(f"Malformed label selector: {value}")
        labels[name] = label_value
    return labels


def create_http_router(
    settings: KubernetesControlSettings,
    backend: SandboxControlBackend,
    readiness: Callable[[], Awaitable[bool]] | None,
) -> APIRouter:
    router = APIRouter()

    def require_auth(request: Request) -> JSONResponse | None:
        if authorized(request.headers.get("authorization"), settings.api_token):
            return None
        return error_response(
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
            return error_response(
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
            return error_response(
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
            return error_response(
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

    router.add_api_route("/health", health, methods=["GET"])
    router.add_api_route("/ready", ready, methods=["GET"])
    router.add_api_route("/v1/sandboxes", create, methods=["POST"])
    router.add_api_route("/v1/sandboxes", list_sandboxes, methods=["GET"])
    router.add_api_route("/v1/sandboxes/{instance_id}", get, methods=["GET"])
    router.add_api_route("/v1/sandboxes/{instance_id}", delete, methods=["DELETE"])
    router.add_api_route("/v1/sandboxes/{instance_id}/exec", exec_command, methods=["POST"])
    router.add_api_route("/v1/sandboxes/{instance_id}/command", stream_command, methods=["POST"])
    router.add_api_route("/v1/sandboxes/{instance_id}/files", upload, methods=["PUT"])
    router.add_api_route("/v1/sandboxes/{instance_id}/files", download, methods=["GET"])
    router.add_api_route("/v1/sandboxes/{instance_id}/egress", modify_egress, methods=["PUT"])
    router.add_api_route("/v1/sandboxes/{instance_id}/egress", clear_egress, methods=["DELETE"])
    return router
