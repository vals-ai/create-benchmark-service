"""Per-evaluation Model Gateway credentials.

The caller (the Valkyrie tracker, the Vals app) names the run, task and task
attempt on each evaluation request. For that evaluation the framework mints a
Gateway run token from the service's own key whose claims are those ids plus the
service ``IDENTITY`` with ``agent_name="grader"``. The Gateway stamps the claims
on every request made with the token, so the usage ledger attributes judge spend
to the run and attempt as the grader without grading code attaching anything.

The token is bound request-locally for the whole evaluation: model-library sends
it on judge queries (``gateway_token_scope``), framework grading sandboxes receive
it as ``MODEL_GATEWAY_API_KEY``, and services that grade elsewhere forward
``current_grading_credentials().as_environment()``. It is revoked afterwards.

Without a run id or Gateway settings nothing is minted and the service's own
credentials apply; a run id without ``GRADING_MODELS`` falls back the same way
with a warning. Nothing here invents a run.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass

import httpx

from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL
from benchmark_service.context import current_tenant

logger = logging.getLogger(__name__)

GRADER_AGENT_NAME = "grader"
_TOKEN_TTL_S = 24 * 60 * 60
_MINT_TIMEOUT_S = 30.0
_REVOKE_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class GradingCredentials:
    """Gateway credentials for one evaluation; ``lease_id`` is set when ``api_key`` is a run token."""

    gateway_url: str
    api_key: str
    lease_id: str | None = None

    def as_environment(self) -> dict[str, str]:
        """Gateway settings for a grader process; the token carries the attribution."""
        return {"MODEL_GATEWAY_URL": self.gateway_url, "MODEL_GATEWAY_API_KEY": self.api_key}


_current: ContextVar[GradingCredentials | None] = ContextVar("benchmark_service_grading_credentials", default=None)


def _service_credentials() -> GradingCredentials | None:
    url, api_key = os.environ.get("MODEL_GATEWAY_URL", "").rstrip("/"), os.environ.get("MODEL_GATEWAY_API_KEY", "")
    return GradingCredentials(url, api_key) if url and api_key else None


def current_grading_credentials() -> GradingCredentials | None:
    """The in-flight evaluation's credentials, else the service's own Gateway settings, else None.

    Graders that call the Gateway themselves (custom HTTP clients, judge sandboxes)
    read this instead of ``MODEL_GATEWAY_*`` so they use the evaluation's token.
    """
    return _current.get() or _service_credentials()


def _identity() -> dict[str, object]:
    """The service ``IDENTITY`` as the grader, naming the caller's tenant when one authenticated."""
    try:
        identity = json.loads(os.environ.get("IDENTITY") or "{}")
    except json.JSONDecodeError:
        identity = {}
    tenant = current_tenant()
    return {
        **(identity if isinstance(identity, dict) else {}),
        **({"tenant": tenant} if tenant not in (None, UNAUTHENTICATED_TENANT_SENTINEL) else {}),
        "agent_name": GRADER_AGENT_NAME,
    }


async def _mint(
    service: GradingCredentials, *, run_id: str, task_id: str, attempt_id: str | None, models: list[str]
) -> GradingCredentials:
    body = {
        "run_id": run_id,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "allowed_models": models,
        "allowed_operations": ["query", "tokens_count"],
        "identity": _identity(),
        "ttl_seconds": _TOKEN_TTL_S,
    }
    async with httpx.AsyncClient(timeout=_MINT_TIMEOUT_S) as client:
        response = await client.post(
            f"{service.gateway_url}/service-auth", headers={"Authorization": f"Bearer {service.api_key}"}, json=body
        )
        response.raise_for_status()
    lease = response.json()
    logger.info(
        "minted grading token run_id=%s task_id=%s attempt_id=%s lease=%s",
        run_id,
        task_id,
        attempt_id,
        lease["lease_id"],
    )
    return GradingCredentials(service.gateway_url, lease["token"], lease_id=lease["lease_id"])


async def _revoke(credentials: GradingCredentials, api_key: str) -> None:
    # Teardown must not mask a grading error; the token also expires on its own.
    try:
        async with httpx.AsyncClient(timeout=_REVOKE_TIMEOUT_S) as client:
            response = await client.post(
                f"{credentials.gateway_url}/service-auth/revoke",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"lease_id": credentials.lease_id},
            )
            response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("could not revoke grading token lease=%s: %s", credentials.lease_id, exc)


def _model_library_scope(credentials: GradingCredentials) -> AbstractContextManager[None]:
    if credentials.lease_id is None:
        return nullcontext()
    try:
        module = importlib.import_module("model_library.gateway_credentials")
    except ImportError:
        return nullcontext()
    return module.gateway_token_scope(credentials.api_key)


@asynccontextmanager
async def grading_credentials(
    *, run_id: str | None, task_id: str, attempt_id: str | None = None
) -> AsyncIterator[None]:
    """Mint, bind and finally revoke the Gateway credentials for one evaluation.

    Spans the complete operation, so callers must leave the scope only after every
    task that grades (stream finalizers, cleanup) has finished. A scope entered
    inside another reuses the enclosing credentials.
    """
    service = _service_credentials()
    if _current.get() is not None or service is None:
        yield
        return
    models = [model.strip() for model in os.environ.get("GRADING_MODELS", "").split(",") if model.strip()]
    credentials = service
    if run_id is not None and models:
        credentials = await _mint(service, run_id=run_id, task_id=task_id, attempt_id=attempt_id, models=models)
    elif run_id is not None:
        logger.warning(
            "GRADING_MODELS is not configured: grading of run_id=%s task_id=%s is charged to the service key",
            run_id,
            task_id,
        )
    token = _current.set(credentials)
    try:
        with _model_library_scope(credentials):
            yield
    finally:
        _current.reset(token)
        if credentials.lease_id is not None:
            await _revoke(credentials, service.api_key)
