"""Vals hosted policy composed with the reusable benchmark application."""

import logging
import os
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import HTTPException, WebSocket

from benchmark_service.app import BenchmarkServiceApp as CoreBenchmarkServiceApp
from benchmark_service.inflight import InflightMiddleware
from benchmark_service.v1_schemas import V1DatasetTasksResponse, V1EvalResponse, V1ScoreResponse

from . import evaluation_quota
from .auth import (
    clear_request_tenant_config,
    close_catalog_client,
    get_tenant_config,
    load_allowlist,
    require_supported_auth_config,
)
from .base import BenchmarkService
from .trial import sanitize_v1_dataset_tasks_response, sanitize_v1_eval_response, sanitize_v1_score_response

logger = logging.getLogger(__name__)

_EVALUATION_QUOTA_UNAVAILABLE_DETAIL = "Evaluation quota enforcement is temporarily unavailable; try again later."
_TRIAL_ALLOWED_PATH = re.compile(r"/v1/(?:evaluate|score|datasets/[^/]+/tasks)")


def _is_trial_tenant(tenant: str) -> bool:
    config = get_tenant_config(tenant)
    return config is not None and config.trial_mode


class BenchmarkServiceApp(CoreBenchmarkServiceApp):
    """Add catalog access, customer quotas, and trial responses to the shared routes."""

    def __init__(self, service_cls: type[BenchmarkService]) -> None:
        super().__init__(service_cls)
        self.add_middleware(
            InflightMiddleware,
            service_name=self._deployment_name,
            metric_namespace="Vals/BenchmarkServices",
        )

    @asynccontextmanager
    async def service_lifespan(self) -> AsyncGenerator[None, None]:
        try:
            require_supported_auth_config()
            allowlist = load_allowlist()
            if not os.getenv("BENCHMARK_CATALOG_API_URL", "").strip():
                evaluation_quota.require_configured(
                    allowlist,
                    service_name=os.getenv(evaluation_quota.SERVICE_NAME_ENV, "").strip(),
                )
            yield
        finally:
            await close_catalog_client()

    def clear_request_context(self) -> None:
        clear_request_tenant_config()

    def authorize_request(self, tenant: str, path: str) -> None:
        if _is_trial_tenant(tenant) and _TRIAL_ALLOWED_PATH.fullmatch(path) is None:
            raise HTTPException(
                status_code=403,
                detail="Trial tenants may only access approved /v1 endpoints (/v1/*)",
            )

    async def consume_evaluation_request(self, tenant: str) -> None:
        try:
            await evaluation_quota.consume_evaluation_request(
                service_name=self._deployment_name,
                tenant=tenant,
            )
        except evaluation_quota.EvaluationQuotaExceeded as exc:
            raise HTTPException(
                status_code=429,
                detail=str(exc),
                headers={"Retry-After": str(exc.retry_after_seconds)},
            ) from exc
        except evaluation_quota.EvaluationQuotaUnavailable as exc:
            logger.warning(
                "Evaluation quota storage unavailable for service %s tenant %s",
                self._deployment_name,
                tenant,
                exc_info=True,
            )
            raise HTTPException(status_code=503, detail=_EVALUATION_QUOTA_UNAVAILABLE_DETAIL) from exc

    async def _admit_websocket_evaluation(self, websocket: WebSocket, tenant: str) -> bool:
        try:
            await self.consume_evaluation_request(tenant)
        except HTTPException as exc:
            reason = str(exc.detail)
            if isinstance(exc.__cause__, evaluation_quota.EvaluationQuotaExceeded):
                reset_timestamp = exc.__cause__.reset_at.isoformat(timespec="seconds").replace("+00:00", "Z")
                reason = f"Evaluation quota reached; retry after {reset_timestamp}."
            await websocket.close(code=1008 if exc.status_code < 500 else 1011, reason=reason)
            return False
        return True

    def project_eval_response(self, tenant: str, response: V1EvalResponse) -> V1EvalResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_eval_response(response, cast(BenchmarkService, self.service).project_trial_result)
        return response

    def project_score_response(self, tenant: str, response: V1ScoreResponse) -> V1ScoreResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_score_response(response)
        return response

    def project_dataset_tasks_response(self, tenant: str, response: V1DatasetTasksResponse) -> V1DatasetTasksResponse:
        if _is_trial_tenant(tenant):
            return sanitize_v1_dataset_tasks_response(response)
        return response
