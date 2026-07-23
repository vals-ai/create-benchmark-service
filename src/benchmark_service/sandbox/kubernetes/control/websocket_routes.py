"""Register the compatibility WebSocket command-stream route."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from fastapi import APIRouter, WebSocket
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from benchmark_service.sandbox.kubernetes.control.backend import SandboxControlBackend
from benchmark_service.sandbox.kubernetes.control.errors import (
    authorized,
    error_detail,
    request_id,
)
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.protocol import (
    CommandErrorEvent,
    CommandRequest,
    ControlErrorDetail,
)
from benchmark_service.sandbox.types import SandboxError


def create_websocket_router(
    settings: KubernetesControlSettings,
    backend: SandboxControlBackend,
) -> APIRouter:
    router = APIRouter()

    async def command(websocket: WebSocket, instance_id: str) -> None:
        async def forward_events(payload: CommandRequest) -> None:
            async for event in backend.command(instance_id, payload):
                await websocket.send_json(event.model_dump(mode="json"))

        async def wait_for_disconnect() -> None:
            while (await websocket.receive())["type"] != "websocket.disconnect":
                pass

        command_request_id = request_id(websocket.headers)
        if not authorized(websocket.headers.get("authorization"), settings.api_token):
            await websocket.close(code=1008, reason="Authentication required")
            return
        await websocket.accept(headers=[(b"x-request-id", command_request_id.encode())])
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
                _, detail = error_detail(error, command_request_id)
            else:
                detail = ControlErrorDetail(
                    code="invalid_request",
                    message=str(error),
                    request_id=command_request_id,
                )
            event = CommandErrorEvent(
                type="error",
                code=detail.code,
                message=detail.message,
                request_id=command_request_id,
            )
            await websocket.send_json(event.model_dump(mode="json"))
        finally:
            with suppress(RuntimeError):
                await websocket.close()

    router.add_api_websocket_route("/v1/sandboxes/{instance_id}/command", command)
    return router
