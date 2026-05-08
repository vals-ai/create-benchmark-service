"""ASGI middleware that tracks in-flight HTTP and WebSocket scopes.

Emits the current count via CloudWatch Embedded Metric Format (EMF) — a
structured JSON log line that CloudWatch ingests as a custom metric without
any boto3 calls or extra IAM. The metric appears in the namespace defined by
``METRIC_NAMESPACE`` with a single ``ServiceName`` dimension; aggregate with
``Sum`` for fleet-wide concurrency, ``Average`` for per-task concurrency.

``/health`` is excluded so the ALB's 10s health probe does not dominate the
count.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

logger = logging.getLogger(__name__)

METRIC_NAMESPACE = "Vals/BenchmarkServices"
EXCLUDED_PATHS = frozenset({"/health"})

ASGIScope = MutableMapping[str, Any]
ASGIReceive = Callable[[], Awaitable[MutableMapping[str, Any]]]
ASGISend = Callable[[MutableMapping[str, Any]], Awaitable[None]]


class InflightMiddleware:
    """Pure-ASGI middleware (handles both http and websocket scopes)."""

    def __init__(
        self,
        app: Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]],
        service_name: str,
        emit_interval_s: float = 30.0,
    ) -> None:
        self.app = app
        self.service_name = service_name
        self.emit_interval_s = emit_interval_s
        self._inflight = 0
        self._emitter_task: asyncio.Task[None] | None = None

    def current_count(self) -> int:
        return self._inflight

    async def __call__(self, scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in EXCLUDED_PATHS:
            await self.app(scope, receive, send)
            return

        await self._ensure_emitter()
        self._inflight += 1
        try:
            await self.app(scope, receive, send)
        finally:
            self._inflight -= 1

    async def _ensure_emitter(self) -> None:
        if self._emitter_task is None:
            self._emitter_task = asyncio.create_task(self._emit_loop())

    async def _emit_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.emit_interval_s)
                self._emit_once()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("inflight emitter loop error")

    def _emit_once(self) -> None:
        record = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": METRIC_NAMESPACE,
                        "Dimensions": [["ServiceName"]],
                        "Metrics": [{"Name": "InFlightRequests", "Unit": "Count"}],
                    }
                ],
            },
            "ServiceName": self.service_name,
            "InFlightRequests": self._inflight,
        }
        print(json.dumps(record), flush=True)

    async def aclose(self) -> None:
        if self._emitter_task is not None:
            self._emitter_task.cancel()
            try:
                await self._emitter_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._emitter_task = None
