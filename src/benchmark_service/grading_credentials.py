"""Per-evaluation Model Gateway credentials that carry the grading attribution.

The caller (the Valkyrie tracker, the Vals app) names the run, task and task
attempt on each evaluation request. For the duration of that evaluation the
framework mints a Gateway run token from the service's own key whose claims are
that run, task and attempt plus the service ``IDENTITY`` with
``agent_name="grader"``. The Gateway stamps those claims on every request made
with the token, so the usage ledger attributes judge spend to the run and
attempt as the grader without the benchmark's grading code attaching anything.

The token is bound request-locally: model-library sends it on the queries made
inside the scope (``model_library.gateway_credentials.gateway_token_scope``), framework
grading sandboxes receive it as ``MODEL_GATEWAY_API_KEY``, and services that
grade elsewhere read ``current_grading_credentials()`` and forward it. The
token is revoked when the evaluation finishes.

Without a Valkyrie run id, or without Gateway settings, nothing is minted and
the service's own credentials apply unchanged; a run id without
``GRADING_MODELS`` falls back the same way with a warning, so the gap is
visible rather than attributed to the wrong place. Nothing here invents a run.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass

import httpx

from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL
from benchmark_service.context import current_tenant
from benchmark_service.observability import ATTEMPT_ID_HEADER, RUN_ID_HEADER

logger = logging.getLogger(__name__)

GRADER_AGENT_NAME = "grader"
GRADING_TOKEN_TTL_SECONDS = 24 * 60 * 60
_MINT_TIMEOUT_S = 30.0
_REVOKE_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class RequestAttribution:
    """The run, task and attempt an evaluation request names; unknown values stay None."""

    run_id: str | None
    task_id: str | None
    attempt_id: str | None


def attribution_from_request(headers: Mapping[str, str], *, task_id: str) -> RequestAttribution:
    """Read the caller's run and attempt from the correlation headers the client sends."""
    return RequestAttribution(
        run_id=headers.get(RUN_ID_HEADER), task_id=task_id, attempt_id=headers.get(ATTEMPT_ID_HEADER)
    )


@dataclass(frozen=True)
class GradingCredentials:
    """Gateway credentials for one evaluation, with the attribution they carry."""

    gateway_url: str
    api_key: str
    attribution: RequestAttribution
    lease_id: str | None = None

    def as_environment(self) -> dict[str, str]:
        """Gateway settings for a grader process; the token carries the attribution."""
        return {"MODEL_GATEWAY_URL": self.gateway_url, "MODEL_GATEWAY_API_KEY": self.api_key}


_current: ContextVar[GradingCredentials | None] = ContextVar("benchmark_service_grading_credentials", default=None)


def current_grading_credentials() -> GradingCredentials | None:
    """The credentials of the in-flight evaluation, or None outside one (or without a Gateway)."""
    return _current.get()


def grading_models() -> list[str]:
    """The registry keys a grading token may call, from the service's ``GRADING_MODELS`` config."""
    return [model.strip() for model in os.environ.get("GRADING_MODELS", "").split(",") if model.strip()]


def grading_identity() -> dict[str, object]:
    """The service ``IDENTITY`` reported as the grader, naming the caller's tenant when one authenticated."""
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


async def _mint(url: str, api_key: str, attribution: RequestAttribution, models: list[str]) -> GradingCredentials:
    body: dict[str, object] = {
        "run_id": attribution.run_id,
        "task_id": attribution.task_id,
        "attempt_id": attribution.attempt_id,
        "allowed_models": models,
        "allowed_operations": ["query", "tokens_count"],
        "identity": grading_identity(),
        "ttl_seconds": GRADING_TOKEN_TTL_SECONDS,
    }
    async with httpx.AsyncClient(timeout=_MINT_TIMEOUT_S) as client:
        response = await client.post(f"{url}/service-auth", headers={"Authorization": f"Bearer {api_key}"}, json=body)
        response.raise_for_status()
    lease = response.json()
    logger.info(
        "minted grading token run_id=%s task_id=%s attempt_id=%s lease=%s models=%s",
        attribution.run_id,
        attribution.task_id,
        attribution.attempt_id,
        lease["lease_id"],
        ",".join(models),
    )
    return GradingCredentials(url, lease["token"], attribution, lease_id=lease["lease_id"])


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
async def grading_credentials(attribution: RequestAttribution) -> AsyncIterator[None]:
    """Mint, bind and finally revoke the Gateway credentials for one evaluation.

    Spans the complete operation, so callers must leave the scope only after every
    task that grades (stream finalizers, cleanup) has finished.
    """
    url, api_key = os.environ.get("MODEL_GATEWAY_URL", "").rstrip("/"), os.environ.get("MODEL_GATEWAY_API_KEY", "")
    if not url or not api_key:
        yield
        return
    models = grading_models()
    if attribution.run_id is not None and models:
        credentials = await _mint(url, api_key, attribution, models)
    else:
        if attribution.run_id is not None:
            logger.warning(
                "GRADING_MODELS is not configured: grading of run_id=%s task_id=%s is charged to the service key",
                attribution.run_id,
                attribution.task_id,
            )
        credentials = GradingCredentials(url, api_key, attribution)
    token = _current.set(credentials)
    try:
        with _model_library_scope(credentials):
            yield
    finally:
        _current.reset(token)
        if credentials.lease_id is not None:
            await _revoke(credentials, api_key)
