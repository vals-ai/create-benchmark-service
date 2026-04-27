"""Authentication helpers for benchmark service requests."""

from __future__ import annotations

import hmac
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from descope.descope_client import DescopeClient

logger = logging.getLogger(__name__)

DESCOPE_API_KEY_HEADER = "x-descope-api-key"
DEFAULT_AUTH_CACHE_TTL_SECONDS = 300
AUTH_CACHE_MAX_SIZE = 1024

_auth_cache: dict[tuple[str, str], float] = {}


@dataclass(frozen=True)
class AuthSettings:
    """Runtime auth settings loaded from environment variables."""

    auth_required: bool
    descope_project_id: str
    benchmark_api_key: str | None
    cache_ttl_seconds: int


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def get_auth_settings() -> AuthSettings:
    """Load auth settings from the current process environment."""
    ttl_raw = os.environ.get("DESCOPE_AUTH_CACHE_TTL_SECONDS", str(DEFAULT_AUTH_CACHE_TTL_SECONDS))
    try:
        cache_ttl_seconds = int(ttl_raw)
    except ValueError:
        cache_ttl_seconds = DEFAULT_AUTH_CACHE_TTL_SECONDS

    return AuthSettings(
        auth_required=_env_bool("AUTH_REQUIRED"),
        descope_project_id=os.environ.get("DESCOPE_PROJECT_ID", ""),
        benchmark_api_key=os.environ.get("BENCHMARK_API_KEY"),
        cache_ttl_seconds=max(cache_ttl_seconds, 0),
    )


def clear_auth_cache() -> None:
    """Clear cached positive auth checks. Intended for tests and process maintenance."""
    _auth_cache.clear()


@lru_cache(maxsize=1)
def _get_descope_client(project_id: str) -> DescopeClient:
    return DescopeClient(project_id=project_id)


def _exchange_descope_access_key(project_id: str, access_key: str) -> Mapping[str, Any]:
    return _get_descope_client(project_id).exchange_access_key(access_key)  # type: ignore[reportUnknownMemberType,reportUnknownVariableType]


def _is_cache_hit(project_id: str, access_key: str, now: float) -> bool:
    key = (project_id, access_key)
    expires_at = _auth_cache.get(key)
    if expires_at is None:
        return False
    if expires_at <= now:
        del _auth_cache[key]
        return False
    return True


def _cache_success(project_id: str, access_key: str, now: float, ttl_seconds: int) -> None:
    if ttl_seconds <= 0:
        return
    if len(_auth_cache) >= AUTH_CACHE_MAX_SIZE:
        _auth_cache.pop(next(iter(_auth_cache)))
    _auth_cache[(project_id, access_key)] = now + ttl_seconds


def _check_legacy_benchmark_api_key(headers: Mapping[str, str], settings: AuthSettings) -> bool:
    if not settings.benchmark_api_key:
        return True

    authorization = headers.get("authorization", "")
    expected = f"Bearer {settings.benchmark_api_key}"
    return hmac.compare_digest(authorization, expected)


def _check_descope_access_key(headers: Mapping[str, str], settings: AuthSettings) -> bool:
    if not settings.descope_project_id:
        logger.warning("AUTH_REQUIRED is true but DESCOPE_PROJECT_ID is not configured")
        return False

    access_key = headers.get(DESCOPE_API_KEY_HEADER)
    if not access_key:
        return False

    now = time.monotonic()
    if _is_cache_hit(settings.descope_project_id, access_key, now):
        return True

    try:
        jwt_response = _exchange_descope_access_key(settings.descope_project_id, access_key)
    except Exception:
        logger.warning("Failed to exchange Descope access key", exc_info=True)
        return False

    tenants = list(jwt_response.get("tenants", {}).keys())
    if len(tenants) != 1:
        logger.warning("Descope access key must be scoped to exactly one tenant, got %s", len(tenants))
        return False

    _cache_success(settings.descope_project_id, access_key, now, settings.cache_ttl_seconds)
    return True


def check_benchmark_service_auth(headers: Mapping[str, str]) -> bool:
    """Validate benchmark-service auth headers using the configured auth mode."""
    settings = get_auth_settings()
    if settings.auth_required:
        return _check_descope_access_key(headers, settings)
    return _check_legacy_benchmark_api_key(headers, settings)
