"""Tenant-aware evaluation request quota enforcement."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from functools import lru_cache, partial
from math import ceil
from typing import Protocol, assert_never, cast

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from benchmark_service.auth import (
    AllowlistConfig,
    EvaluationQuotaConfig,
    EvaluationQuotaPeriod,
    get_tenant_config,
)
from benchmark_service.blocking import run_blocking

EVALUATION_QUOTA_TABLE_ENV = "EVALUATION_QUOTA_TABLE_NAME"
SERVICE_NAME_ENV = "SERVICE_NAME"
_EVALUATION_OPERATION = "evaluation"
_DAY = timedelta(days=1)
_WEEK = timedelta(days=7)


class EvaluationQuotaExceeded(Exception):
    """The tenant has consumed its configured evaluation quota."""

    def __init__(
        self,
        *,
        policy: EvaluationQuotaConfig,
        reset_at: datetime,
        retry_after_seconds: int,
    ) -> None:
        self.policy = policy
        self.reset_at = reset_at
        self.retry_after_seconds = retry_after_seconds
        reset_timestamp = reset_at.isoformat(timespec="seconds").replace("+00:00", "Z")
        super().__init__(
            f"Evaluation request limit of {policy.limit:,} per {policy.period} reached; "
            f"try again after {reset_timestamp}."
        )


class EvaluationQuotaUnavailable(Exception):
    """The durable quota counter could not admit or reject the request."""


class _DynamoDBClient(Protocol):
    def update_item(self, **kwargs: object) -> object: ...


def _has_evaluation_quota(allowlist: AllowlistConfig) -> bool:
    return any(config.evaluation_quota is not None for config in allowlist.tenants.values())


def require_configured(allowlist: AllowlistConfig, *, service_name: str) -> None:
    """Fail startup when tenant quotas lack durable counter identity or storage."""
    if not _has_evaluation_quota(allowlist):
        return
    if not service_name:
        raise RuntimeError(f"{SERVICE_NAME_ENV} is required when an allowlisted tenant has an evaluation quota")
    if not os.environ.get(EVALUATION_QUOTA_TABLE_ENV, "").strip():
        raise RuntimeError(
            f"{EVALUATION_QUOTA_TABLE_ENV} is required when an allowlisted tenant has an evaluation quota"
        )


@lru_cache(maxsize=1)
def _dynamodb_client() -> _DynamoDBClient:
    return cast(
        _DynamoDBClient,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "dynamodb",
            config=Config(
                connect_timeout=5,
                read_timeout=10,
                # UpdateItem increments are not idempotent. Retrying can consume multiple quota units.
                retries={"total_max_attempts": 1},
            ),
        ),
    )


def _utc_period(now: datetime, period: EvaluationQuotaPeriod) -> tuple[datetime, datetime]:
    current = now.astimezone(UTC)
    day_start = datetime(current.year, current.month, current.day, tzinfo=UTC)
    match period:
        case "day":
            return day_start, day_start + _DAY
        case "week":
            start = day_start - timedelta(days=current.weekday())
            return start, start + _WEEK
        case "month":
            start = datetime(current.year, current.month, 1, tzinfo=UTC)
            if current.month == 12:
                return start, datetime(current.year + 1, 1, 1, tzinfo=UTC)
            return start, datetime(current.year, current.month + 1, 1, tzinfo=UTC)
        case "year":
            start = datetime(current.year, 1, 1, tzinfo=UTC)
            return start, datetime(current.year + 1, 1, 1, tzinfo=UTC)
    assert_never(period)


def _counter_key(
    service_name: str,
    tenant: str,
    period: EvaluationQuotaPeriod,
    period_start: datetime,
) -> str:
    return json.dumps(
        [service_name, tenant, _EVALUATION_OPERATION, period, period_start.date().isoformat()],
        separators=(",", ":"),
    )


def _consume_sync(
    *,
    table_name: str,
    service_name: str,
    tenant: str,
    policy: EvaluationQuotaConfig,
    period_start: datetime,
    period_end: datetime,
    now: datetime,
) -> None:
    try:
        _dynamodb_client().update_item(
            TableName=table_name,
            Key={
                "quota_key": {
                    "S": _counter_key(service_name, tenant, policy.period, period_start),
                }
            },
            UpdateExpression=(
                "SET #request_count = if_not_exists(#request_count, :zero) + :one, "
                "#expires_at = :expires_at, #service_name = :service_name, "
                "#tenant = :tenant, #operation = :operation, #period = :period, "
                "#period_start = :period_start"
            ),
            ConditionExpression="attribute_not_exists(#request_count) OR #request_count < :limit",
            ExpressionAttributeNames={
                "#request_count": "request_count",
                "#expires_at": "expires_at",
                "#service_name": "service_name",
                "#tenant": "tenant",
                "#operation": "operation",
                "#period": "period",
                "#period_start": "period_start",
            },
            ExpressionAttributeValues={
                ":zero": {"N": "0"},
                ":one": {"N": "1"},
                ":limit": {"N": str(policy.limit)},
                ":expires_at": {"N": str(int(period_end.timestamp()))},
                ":service_name": {"S": service_name},
                ":tenant": {"S": tenant},
                ":operation": {"S": _EVALUATION_OPERATION},
                ":period": {"S": policy.period},
                ":period_start": {"S": period_start.isoformat(timespec="seconds").replace("+00:00", "Z")},
            },
        )
    except ClientError as exc:
        error = cast(
            dict[str, str],
            exc.response.get("Error") or {},  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        )
        if error.get("Code") != "ConditionalCheckFailedException":
            raise EvaluationQuotaUnavailable("Evaluation quota storage request failed") from exc
        retry_after_seconds = max(1, ceil((period_end - now).total_seconds()))
        raise EvaluationQuotaExceeded(
            policy=policy,
            reset_at=period_end,
            retry_after_seconds=retry_after_seconds,
        ) from exc
    except BotoCoreError as exc:
        raise EvaluationQuotaUnavailable("Evaluation quota storage request failed") from exc


async def consume_evaluation_request(
    *,
    service_name: str,
    tenant: str,
    now: datetime | None = None,
) -> None:
    """Atomically consume one tenant evaluation request when a quota is configured."""
    tenant_config = get_tenant_config(tenant)
    if tenant_config is None or tenant_config.evaluation_quota is None:
        return

    table_name = os.environ.get(EVALUATION_QUOTA_TABLE_ENV, "").strip()
    if not table_name:
        raise EvaluationQuotaUnavailable(
            f"{EVALUATION_QUOTA_TABLE_ENV} is required when tenant {tenant!r} has an evaluation quota"
        )

    current = (now or datetime.now(UTC)).astimezone(UTC)
    period_start, period_end = _utc_period(current, tenant_config.evaluation_quota.period)
    await run_blocking(
        partial(
            _consume_sync,
            table_name=table_name,
            service_name=service_name,
            tenant=tenant,
            policy=tenant_config.evaluation_quota,
            period_start=period_start,
            period_end=period_end,
            now=current,
        )
    )
