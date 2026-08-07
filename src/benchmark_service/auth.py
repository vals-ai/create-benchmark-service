"""Authentication helpers for benchmark service requests."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml
from cachetools import TTLCache
from descope.descope_client import DescopeClient
from pydantic import BaseModel, Field


from benchmark_service.allowlist import (
    ALLOWLIST_CACHE_MAX_SIZE,
    DESCOPE_API_KEY_HEADER,
    CatalogAllowlistClient,
    TenantConfig,
)


class AllowlistConfig(BaseModel):
    """Allowlist for a single benchmark service.

    Stored centrally in `benchmark-services-registry/allowlist.yaml`; the per-service
    slice is delivered to each container as JSON via `DESCOPE_TENANT_ALLOWLIST_JSON`,
    or as a YAML file via `DESCOPE_ALLOWLIST_PATH` for local dev.
    """

    tenants: dict[str, TenantConfig] = Field(default_factory=dict)


logger = logging.getLogger(__name__)

DEFAULT_AUTH_CACHE_TTL_SECONDS = 300
AUTH_CACHE_MAX_SIZE = ALLOWLIST_CACHE_MAX_SIZE
LEGACY_TENANT_SENTINEL = "_legacy"


_DESCOPE_ALLOWLIST_JSON_ENV = "DESCOPE_TENANT_ALLOWLIST_JSON"
_DESCOPE_ALLOWLIST_PATH_ENV = "DESCOPE_ALLOWLIST_PATH"
_BENCHMARK_CATALOG_API_URL_ENV = "BENCHMARK_CATALOG_API_URL"
_SERVICE_NAME_ENV = "SERVICE_NAME"


_allowlist_cache: AllowlistConfig | None = None
_allowlist_warned: bool = False
_catalog_client: CatalogAllowlistClient | None = None


def _catalog_api_url() -> str | None:
    value = os.environ.get(_BENCHMARK_CATALOG_API_URL_ENV, "").strip()
    return value.rstrip("/") or None


def _catalog_service_name() -> str | None:
    value = os.environ.get(_SERVICE_NAME_ENV, "").strip()
    return value or None


def clear_allowlist_cache() -> None:
    """Reset module-level legacy and catalog allowlist state. Intended for tests."""
    global _allowlist_cache, _allowlist_warned, _catalog_client
    _allowlist_cache = None
    _allowlist_warned = False
    _catalog_client = None


def _get_catalog_client() -> CatalogAllowlistClient | None:
    global _catalog_client

    api_url = _catalog_api_url()
    service_name = _catalog_service_name()
    if api_url is None or service_name is None:
        logger.warning(
            "%s requires both %s and %s",
            _BENCHMARK_CATALOG_API_URL_ENV,
            _BENCHMARK_CATALOG_API_URL_ENV,
            _SERVICE_NAME_ENV,
        )
        return None

    if _catalog_client is None:
        _catalog_client = CatalogAllowlistClient(api_url, service_name)
    return _catalog_client


async def _fetch_api_tenant_config(access_key: str, tenant: str) -> TenantConfig | None:
    """Fetch and cache one tenant's service policy from the catalog API."""
    client = _get_catalog_client()
    if client is None:
        return None
    return await client.get_tenant_config(access_key, tenant)


def load_allowlist() -> AllowlistConfig:
    """Load the tenant + dataset allowlist for this service.

    Resolution order:
      1. The catalog API when `BENCHMARK_CATALOG_API_URL` is configured.
      2. `DESCOPE_TENANT_ALLOWLIST_JSON` env var (legacy rollout fallback).
      3. `DESCOPE_ALLOWLIST_PATH` env var pointing at a YAML file (local dev).
      4. Empty config (legal: fails closed when AUTH_REQUIRED=true; logs once).

    Raises:
        ValueError: if the configured source exists but is malformed.
    """
    global _allowlist_cache, _allowlist_warned

    # API mode is caller-scoped. Startup validation must not make a caller-less
    # request, and legacy policy must never override an explicitly configured API.
    if _catalog_api_url() is not None:
        return AllowlistConfig()

    if _allowlist_cache is not None:
        return _allowlist_cache

    json_payload = os.environ.get(_DESCOPE_ALLOWLIST_JSON_ENV)
    if json_payload:
        try:
            data = json.loads(json_payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{_DESCOPE_ALLOWLIST_JSON_ENV} is not valid JSON: {exc}") from exc
        try:
            config = AllowlistConfig.model_validate(data)
        except Exception as exc:
            raise ValueError(f"{_DESCOPE_ALLOWLIST_JSON_ENV} failed schema validation: {exc}") from exc
        _allowlist_cache = config
        return config

    file_path = os.environ.get(_DESCOPE_ALLOWLIST_PATH_ENV)
    if file_path:
        path = Path(file_path)
        if not path.exists():
            raise ValueError(f"{_DESCOPE_ALLOWLIST_PATH_ENV} points to missing file: {file_path}")
        try:
            data = yaml.safe_load(path.read_text())
        except yaml.YAMLError as exc:
            raise ValueError(f"{_DESCOPE_ALLOWLIST_PATH_ENV} is not valid YAML: {exc}") from exc
        try:
            config = AllowlistConfig.model_validate(data or {})
        except Exception as exc:
            raise ValueError(f"{_DESCOPE_ALLOWLIST_PATH_ENV} failed schema validation: {exc}") from exc
        _allowlist_cache = config
        return config

    config = AllowlistConfig()
    _allowlist_cache = config
    if not _allowlist_warned:
        logger.warning(
            "No tenant allowlist configured (set %s or %s). All Descope-authenticated "
            "requests will be rejected when AUTH_REQUIRED=true.",
            _DESCOPE_ALLOWLIST_JSON_ENV,
            _DESCOPE_ALLOWLIST_PATH_ENV,
        )
        _allowlist_warned = True
    return config


def get_tenant_config(tenant: str) -> TenantConfig | None:
    """Return the TenantConfig for `tenant`, or None if not allowlisted."""
    if _catalog_api_url() is not None:
        client = _get_catalog_client()
        return client.cached_tenant_config(tenant) if client is not None else None
    return load_allowlist().tenants.get(tenant)


def _initial_cache_ttl_seconds() -> int:
    raw = os.environ.get("DESCOPE_AUTH_CACHE_TTL_SECONDS", str(DEFAULT_AUTH_CACHE_TTL_SECONDS))
    try:
        return max(int(raw), 1)
    except ValueError:
        return DEFAULT_AUTH_CACHE_TTL_SECONDS


_auth_cache: TTLCache[tuple[str, str], str] = TTLCache(
    maxsize=AUTH_CACHE_MAX_SIZE,
    ttl=_initial_cache_ttl_seconds(),
)


@dataclass(frozen=True)
class AuthSettings:
    """Runtime auth settings loaded from environment variables."""

    auth_required: bool
    descope_project_id: str
    benchmark_api_key: str | None


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def get_auth_settings() -> AuthSettings:
    """Load auth settings from the current process environment."""
    return AuthSettings(
        auth_required=_env_bool("AUTH_REQUIRED"),
        descope_project_id=os.environ.get("DESCOPE_PROJECT_ID", ""),
        benchmark_api_key=os.environ.get("BENCHMARK_API_KEY"),
    )


def clear_auth_cache() -> None:
    """Clear cached positive auth checks. Intended for tests and process maintenance."""
    _auth_cache.clear()


@lru_cache(maxsize=1)
def _get_descope_client(project_id: str) -> DescopeClient:
    return DescopeClient(project_id=project_id)


async def _exchange_descope_access_key(project_id: str, access_key: str) -> Mapping[str, Any]:
    return await asyncio.to_thread(_get_descope_client(project_id).exchange_access_key, access_key)  # type: ignore[reportUnknownMemberType,reportUnknownVariableType]


def _check_legacy_benchmark_api_key(headers: Mapping[str, str], settings: AuthSettings) -> bool:
    if not settings.benchmark_api_key:
        return True

    authorization = headers.get("authorization", "")
    expected = f"Bearer {settings.benchmark_api_key}"
    return hmac.compare_digest(authorization, expected)


async def resolve_descope_tenant(headers: Mapping[str, str]) -> str | None:
    """Validate a Descope access key and resolve a single allowlisted tenant."""
    settings = get_auth_settings()
    if not settings.descope_project_id:
        logger.warning("AUTH_REQUIRED is true but DESCOPE_PROJECT_ID is not configured")
        return None

    access_key = headers.get(DESCOPE_API_KEY_HEADER)
    if not access_key:
        return None

    cache_key = (settings.descope_project_id, access_key)
    cached = _auth_cache.get(cache_key)
    if cached is not None:
        if _catalog_api_url() is None:
            return cached
        return cached if await _fetch_api_tenant_config(access_key, cached) is not None else None

    try:
        jwt_response = await _exchange_descope_access_key(settings.descope_project_id, access_key)
        tenants_claim = jwt_response.get("tenants", {})
        tenants = list(tenants_claim.keys())
    except Exception:
        logger.warning("Failed to resolve tenant from Descope access key", exc_info=True)
        return None

    if len(tenants) != 1:
        logger.warning("Descope access key must be scoped to exactly one tenant, got %s", len(tenants))
        return None

    tenant = tenants[0]
    if tenant == LEGACY_TENANT_SENTINEL:
        logger.info("Descope tenant %s is reserved for legacy auth compatibility", tenant)
        return None

    if _catalog_api_url() is not None:
        if await _fetch_api_tenant_config(access_key, tenant) is None:
            logger.info("Descope tenant %s is not in the service allowlist", tenant)
            return None
    else:
        allowlist = load_allowlist()
        if tenant not in allowlist.tenants:
            logger.info("Descope tenant %s is not in the service allowlist", tenant)
            return None

    _auth_cache[cache_key] = tenant
    return tenant


async def resolve_caller_tenant(headers: Mapping[str, str]) -> str | None:
    """Return the caller tenant id, "_legacy" sentinel, or None to reject."""
    settings = get_auth_settings()
    if settings.auth_required:
        return await resolve_descope_tenant(headers)
    return LEGACY_TENANT_SENTINEL if _check_legacy_benchmark_api_key(headers, settings) else None


async def check_benchmark_service_auth(headers: Mapping[str, str]) -> bool:
    """Validate benchmark-service auth headers using the configured auth mode."""
    return await resolve_caller_tenant(headers) is not None
