"""Assemble the FastAPI application for the Kubernetes control service."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from benchmark_service.sandbox.kubernetes.control.backend import SandboxControlBackend
from benchmark_service.sandbox.kubernetes.control.errors import install_http_error_handling
from benchmark_service.sandbox.kubernetes.control.http_routes import create_http_router
from benchmark_service.sandbox.kubernetes.control.settings import KubernetesControlSettings
from benchmark_service.sandbox.kubernetes.control.websocket_routes import (
    create_websocket_router,
)


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
    install_http_error_handling(app)
    app.include_router(create_http_router(settings, backend, readiness))
    app.include_router(create_websocket_router(settings, backend))
    return app
