"""Tenant allowlist clients and policy models."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Literal
from urllib.parse import quote

import httpx
from cachetools import TTLCache
from pydantic import BaseModel, ConfigDict, Field, PositiveInt

logger = logging.getLogger(__name__)

DESCOPE_API_KEY_HEADER = "x-descope-api-key"
DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS = 300
ALLOWLIST_CACHE_MAX_SIZE = 1024
DEFAULT_CATALOG_REQUEST_TIMEOUT_SECONDS = 5.0


EvaluationQuotaPeriod = Literal["day", "week", "month", "year"]


class EvaluationQuotaConfig(BaseModel):
    """Per-tenant evaluation request quota."""

    model_config = ConfigDict(extra="forbid")

    limit: PositiveInt
    period: EvaluationQuotaPeriod


class TenantConfig(BaseModel):
    """Per-tenant access rules within a benchmark service."""

    model_config = ConfigDict(extra="forbid")

    datasets: list[str] = Field(default_factory=list)
    evaluation_quota: EvaluationQuotaConfig | None = None
    trial_mode: bool = Field(
        default=False,
        description=(
            "If true, responses on /v1/evaluate and /v1/score are sanitized to "
            "score-only fields. Set this on prospects' tenants in allowlist.yaml."
        ),
    )


class _CatalogAllowlistResponse(BaseModel):
    """Strict response contract for the service-specific catalog endpoint."""

    model_config = ConfigDict(extra="ignore", strict=True)

    name: str
    datasets: list[str]
    evaluation_quota: EvaluationQuotaConfig | None = None
    trial_mode: bool


class _NonClosingTransport(httpx.AsyncBaseTransport):
    """Adapt a caller-owned transport without taking ownership of its lifetime."""

    def __init__(self, delegate: httpx.AsyncBaseTransport) -> None:
        self._delegate = delegate

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self._delegate.handle_async_request(request)

    async def aclose(self) -> None:
        # The caller owns the delegated transport and closes it separately.
        return None


class CatalogAllowlistClient:
    """Fetch and cache one service's tenant policy from the catalog API."""

    def __init__(
        self,
        base_url: str,
        service_name: str,
        *,
        timeout: float = DEFAULT_CATALOG_REQUEST_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
        cache_timer: Callable[[], float] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.service_name = service_name
        if not self.base_url:
            raise ValueError("Catalog API base URL must not be empty")
        if not self.service_name:
            raise ValueError("Catalog service name must not be empty")

        client_transport = (
            _NonClosingTransport(transport) if transport is not None else None
        )
        self._client = httpx.AsyncClient(timeout=timeout, transport=client_transport)
        if cache_timer is None:
            self._cache = TTLCache[str, TenantConfig](
                maxsize=ALLOWLIST_CACHE_MAX_SIZE,
                ttl=DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS,
            )
        else:
            self._cache = TTLCache[str, TenantConfig](
                maxsize=ALLOWLIST_CACHE_MAX_SIZE,
                ttl=DEFAULT_ALLOWLIST_CACHE_TTL_SECONDS,
                timer=cache_timer,
            )

    @property
    def endpoint(self) -> str:
        """Return the service-specific catalog endpoint."""
        return f"{self.base_url}/benchmark-services/{quote(self.service_name, safe='')}"

    async def aclose(self) -> None:
        """Close the underlying HTTP client and transport."""
        await self._client.aclose()

    def clear_cache(self) -> None:
        """Clear successful tenant policies."""
        self._cache.clear()

    def cached_tenant_config(self, tenant: str) -> TenantConfig | None:
        """Return a cached policy without making a network request."""
        return self._cache.get(tenant)

    async def get_tenant_config(self, access_key: str, tenant: str) -> TenantConfig | None:
        """Fetch a tenant policy, returning ``None`` for misses or failures."""
        cached = self._cache.get(tenant)
        if cached is not None:
            return cached

        try:
            response = await self._client.get(
                self.endpoint,
                headers={DESCOPE_API_KEY_HEADER: access_key},
            )
        except Exception:
            logger.warning("Failed to fetch tenant policy from benchmark catalog API", exc_info=True)
            return None

        if response.status_code != 200:
            logger.warning("Benchmark catalog API returned status %s", response.status_code)
            return None

        try:
            payload = _CatalogAllowlistResponse.model_validate(response.json())
        except Exception:
            logger.warning("Benchmark catalog API returned a malformed tenant policy", exc_info=True)
            return None

        if payload.name != self.service_name:
            logger.warning("Benchmark catalog API returned policy for unexpected service %s", payload.name)
            return None

        config = TenantConfig(
            datasets=payload.datasets,
            evaluation_quota=payload.evaluation_quota,
            trial_mode=payload.trial_mode,
        )
        self._cache[tenant] = config
        return config
