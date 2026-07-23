"""Translate control-service authentication and internal failures into HTTP responses."""

from __future__ import annotations

import re
import secrets
import uuid
from collections.abc import Mapping

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.middleware.base import RequestResponseEndpoint

from benchmark_service.sandbox.kubernetes.control.backend import SandboxConflictError
from benchmark_service.sandbox.kubernetes.protocol import ControlErrorDetail, ControlErrorResponse
from benchmark_service.sandbox.types import (
    SandboxConnectionError,
    SandboxError,
    SandboxNotFoundError,
)

REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def request_id(headers: Mapping[str, str]) -> str:
    value = headers.get("x-request-id")
    return value if value and REQUEST_ID.fullmatch(value) else str(uuid.uuid4())


def authorized(authorization: str | None, token: str) -> bool:
    if not authorization or not authorization.startswith("Bearer "):
        return False
    return secrets.compare_digest(authorization.removeprefix("Bearer "), token)


def error_detail(error: SandboxError, request_id: str) -> tuple[int, ControlErrorDetail]:
    if isinstance(error, SandboxNotFoundError):
        return 404, ControlErrorDetail(code="not_found", message=str(error), request_id=request_id)
    if isinstance(error, SandboxConnectionError):
        return 503, ControlErrorDetail(code="unavailable", message=str(error), request_id=request_id)
    if isinstance(error, SandboxConflictError):
        return 409, ControlErrorDetail(code="conflict", message=str(error), request_id=request_id)
    return 500, ControlErrorDetail(code="sandbox_error", message=str(error), request_id=request_id)


def error_response(status_code: int, detail: ControlErrorDetail) -> JSONResponse:
    body = ControlErrorResponse(error=detail).model_dump(mode="json")
    return JSONResponse(status_code=status_code, content=body)


def install_http_error_handling(app: FastAPI) -> None:
    async def add_request_id(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.request_id = request_id(request.headers)
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    async def handle_sandbox_error(request: Request, error: Exception) -> JSONResponse:
        assert isinstance(error, SandboxError)
        status_code, detail = error_detail(error, request.state.request_id)
        return error_response(status_code, detail)

    async def handle_invalid_request(request: Request, error: Exception) -> JSONResponse:
        return error_response(
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
