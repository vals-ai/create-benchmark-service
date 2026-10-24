"""Per-evaluation Model Gateway credentials.

For each evaluation that names a run, task and attempt, mint a Gateway run token
from the service key (claims: those ids, ``IDENTITY`` with ``agent_name="grader"``,
``GRADING_MODELS``), bind it request-locally so judge calls carry it, and revoke it
afterwards. Without a run id or Gateway settings the service's own key applies; a run
id without an attempt id or ``GRADING_MODELS`` fails the evaluation.

The binding is a ``ContextVar``: asyncio tasks and ``asyncio.to_thread`` inherit it,
but plain ``loop.run_in_executor`` and ``ThreadPoolExecutor.submit`` do not, so graders
that hand work to threads must use ``asyncio.to_thread`` or ``contextvars.copy_context().run``.
"""

from __future__ import annotations

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
    url = os.environ.get("MODEL_GATEWAY_URL", "").strip().rstrip("/")
    api_key = os.environ.get("MODEL_GATEWAY_API_KEY", "").strip()
    return GradingCredentials(url, api_key) if url and api_key else None


def current_grading_credentials() -> GradingCredentials | None:
    """The in-flight evaluation's credentials, else None.

    Graders that call the Gateway themselves (custom HTTP clients, judge sandboxes)
    read this instead of ``MODEL_GATEWAY_*`` so they use the evaluation's token.
    """
    return _current.get()


def _identity() -> dict[str, object]:
    """The service ``IDENTITY`` as the grader, naming the caller's tenant when one authenticated."""
    identity: dict[str, object] = json.loads(os.environ.get("IDENTITY") or "{}")
    tenant = current_tenant()
    return {
        **identity,
        **({"tenant": tenant} if tenant not in (None, UNAUTHENTICATED_TENANT_SENTINEL) else {}),
        "agent_name": GRADER_AGENT_NAME,
    }


async def _mint(
    service: GradingCredentials, *, run_id: str, task_id: str, attempt_id: str, models: list[str]
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
    credentials = service
    scope: AbstractContextManager[None] = nullcontext()
    if run_id is not None:
        models = [model.strip() for model in os.environ.get("GRADING_MODELS", "").split(",") if model.strip()]
        if attempt_id is None or not models:
            reason = "GRADING_MODELS is not configured" if not models else "no attempt id"
            raise RuntimeError(f"cannot mint a grading token for run_id={run_id} task_id={task_id}: {reason}")
        from model_library.base.gateway import gateway_token_scope  # pyright: ignore[reportMissingImports, reportUnknownVariableType]

        credentials = await _mint(service, run_id=run_id, task_id=task_id, attempt_id=attempt_id, models=models)
        scope = gateway_token_scope(credentials.api_key)  # pyright: ignore[reportUnknownVariableType]
    token = _current.set(credentials)
    try:
        with scope:
            yield
    finally:
        _current.reset(token)
        if credentials.lease_id is not None:
            await _revoke(credentials, service.api_key)
