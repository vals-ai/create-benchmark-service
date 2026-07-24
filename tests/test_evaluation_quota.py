"""Tenant-aware evaluation quota behavior."""

from __future__ import annotations

import json
from collections.abc import Generator
from datetime import UTC, datetime
from typing import cast
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from httpx import Response
from starlette.websockets import WebSocketDisconnect

from benchmark_service import auth as auth_module
from benchmark_service import evaluation_quota
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache, load_allowlist
from tests.conftest import StubBenchmark


class _FakeDynamoDB:
    def __init__(self, successful_updates: int) -> None:
        self.successful_updates = successful_updates
        self.calls: list[dict[str, object]] = []

    def update_item(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if len(self.calls) > self.successful_updates:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ConditionalCheckFailedException",
                        "Message": "quota exhausted",
                    }
                },
                "UpdateItem",
            )
        return {}


def _set_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps(
            {
                "tenants": {
                    "anthropic": {
                        "datasets": ["default"],
                        "evaluation_quota": {"limit": 1, "period": "week"},
                    },
                    "vals.ai": {"datasets": ["default"]},
                }
            }
        ),
    )
    clear_allowlist_cache()


async def test_weekly_counter_is_atomic_and_resets_on_monday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    fake = _FakeDynamoDB(successful_updates=1)
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)
    now = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)

    await evaluation_quota.consume_evaluation_request(
        service_name="legal-research-eevee",
        tenant="anthropic",
        now=now,
    )

    call = fake.calls[0]
    assert call["TableName"] == "evaluation-quotas"
    assert call["Key"] == {"quota_key": {"S": '["legal-research-eevee","anthropic","evaluation","2026-07-20"]'}}
    assert call["ConditionExpression"] == ("attribute_not_exists(#request_count) OR #request_count < :limit")
    values = cast(dict[str, dict[str, str]], call["ExpressionAttributeValues"])
    assert values[":limit"] == {"N": "1"}
    assert values[":expires_at"] == {"N": str(int(datetime(2026, 7, 27, tzinfo=UTC).timestamp()))}

    with pytest.raises(evaluation_quota.EvaluationQuotaExceeded) as exc_info:
        await evaluation_quota.consume_evaluation_request(
            service_name="legal-research-eevee",
            tenant="anthropic",
            now=now,
        )

    assert exc_info.value.reset_at == datetime(2026, 7, 27, tzinfo=UTC)
    assert exc_info.value.retry_after_seconds == 3 * 24 * 60 * 60 - 12 * 60 * 60


async def test_tenant_without_quota_does_not_touch_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    fake = _FakeDynamoDB(successful_updates=0)
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)

    await evaluation_quota.consume_evaluation_request(
        service_name="legal-research-eevee",
        tenant="vals.ai",
    )

    assert fake.calls == []


def test_quota_configuration_requires_counter_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    monkeypatch.delenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, raising=False)

    with pytest.raises(RuntimeError, match=evaluation_quota.EVALUATION_QUOTA_TABLE_ENV):
        evaluation_quota.require_configured(load_allowlist())


@pytest.fixture
def quota_client(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[tuple[TestClient, _FakeDynamoDB], None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv("SERVICE_NAME", "legal-research-eevee")
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    _set_allowlist(monkeypatch)
    fake = _FakeDynamoDB(successful_updates=1)
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)
    app = BenchmarkServiceApp(StubBenchmark)

    async def exchange(
        _project_id: str,
        access_key: str,
    ) -> dict[str, dict[str, dict[str, str]]]:
        tenant = "vals.ai" if access_key == "vals-key" else "anthropic"
        return {"tenants": {tenant: {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", exchange):
        with TestClient(app) as client:
            yield client, fake


def _v1_eval(
    client: TestClient,
    *,
    key: str = "anthropic-key",
    dataset: str = "default",
) -> Response:
    return client.post(
        "/v1/evaluate",
        json={
            "run_id": "run-1",
            "task_id": "task-1",
            "dataset": dataset,
            "payload": {
                "type": "text",
                "schema": "stub.text.v1",
                "data": "2",
            },
        },
        headers={"x-descope-api-key": key},
    )


def test_legacy_and_v1_evaluation_routes_share_tenant_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB],
) -> None:
    client, fake = quota_client

    first = client.post(
        "/evaluate-response/",
        json={"task_id": "task-1", "response": "2", "dataset": "default"},
        headers={"x-descope-api-key": "anthropic-key"},
    )
    exhausted = _v1_eval(client)

    assert first.status_code == 200
    assert exhausted.status_code == 429
    assert int(exhausted.headers["Retry-After"]) > 0
    assert exhausted.json()["detail"].startswith("Evaluation request limit of 1 per week reached")
    assert len(fake.calls) == 2


def test_dataset_rejection_does_not_consume_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB],
) -> None:
    client, fake = quota_client

    rejected = _v1_eval(client, dataset="alt")
    admitted = _v1_eval(client)

    assert rejected.status_code == 403
    assert admitted.status_code == 200
    assert len(fake.calls) == 1


def test_unlimited_tenant_does_not_consume_limited_tenant_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB],
) -> None:
    client, fake = quota_client

    assert _v1_eval(client, key="vals-key").status_code == 200
    assert _v1_eval(client, key="vals-key").status_code == 200
    assert fake.calls == []


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (
            "/ws/evaluate-response",
            {"task_id": "task-1", "response": "2", "dataset": "default"},
        ),
        (
            "/ws/evaluate-instance",
            {
                "task_id": "task-1",
                "instance_id": "instance-1",
                "dataset": "default",
                "sandbox_provider": {
                    "type": "modal",
                    "MODAL_TOKEN_ID": "id",
                    "MODAL_TOKEN_SECRET": "secret",
                },
            },
        ),
    ],
)
def test_websocket_evaluation_routes_cannot_bypass_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB],
    path: str,
    payload: dict[str, object],
) -> None:
    client, fake = quota_client
    assert _v1_eval(client).status_code == 200

    with client.websocket_connect(
        path,
        headers={"x-descope-api-key": "anthropic-key"},
    ) as websocket:
        websocket.send_json(payload)
        with pytest.raises(WebSocketDisconnect) as exc_info:
            websocket.receive_json()

    assert exc_info.value.code == 1008
    assert exc_info.value.reason.startswith("Evaluation request limit of 1 per week reached")
    assert len(fake.calls) == 2
