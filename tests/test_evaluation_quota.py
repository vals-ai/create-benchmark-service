"""Tenant-aware evaluation quota behavior."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from datetime import UTC, datetime
from threading import Event, Lock
from typing import Protocol, cast
from unittest.mock import patch

import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from httpx import Response
from starlette.websockets import WebSocketDisconnect

from benchmark_service import auth as auth_module
from benchmark_service import evaluation_quota
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import clear_allowlist_cache, clear_auth_cache
from benchmark_service.sandbox import SandboxProvider
from benchmark_service.schemas import EvalMode
from benchmark_service.submission_artifacts import SubmissionArtifactNotFound
from benchmark_service.v1_schemas import V1PayloadType
from tests.conftest import StubBenchmark


class _RetryConfig(Protocol):
    retries: dict[str, object]


class _FakeDynamoDB:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.calls: list[dict[str, object]] = []
        self.error_code: str | None = None
        self._lock = Lock()

    def update_item(self, **kwargs: object) -> object:
        with self._lock:
            self.calls.append(kwargs)
            if self.error_code is not None:
                raise _client_error(self.error_code)

            key = cast(dict[str, dict[str, str]], kwargs["Key"])["quota_key"]["S"]
            values = cast(dict[str, dict[str, str]], kwargs["ExpressionAttributeValues"])
            limit = int(values[":limit"]["N"])
            count = self.counts.get(key, 0)
            if count >= limit:
                raise _client_error("ConditionalCheckFailedException")
            self.counts[key] = count + 1
        return {}


def _client_error(code: str) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": code,
                "Message": "quota storage error",
            }
        },
        "UpdateItem",
    )


def _set_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps(
            {
                "tenants": {
                    "limited-tenant": {
                        "datasets": ["default"],
                        "evaluation_quota": {"limit": 1, "period": "week"},
                    },
                    "unlimited-tenant": {"datasets": ["default"]},
                }
            }
        ),
    )
    clear_allowlist_cache()


async def test_weekly_counter_uses_conditional_update_and_resets_on_monday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    fake = _FakeDynamoDB()
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)
    now = datetime(2026, 7, 24, 12, 0, 0, 500_000, tzinfo=UTC)

    await evaluation_quota.consume_evaluation_request(
        service_name="example-benchmark",
        tenant="limited-tenant",
        now=now,
    )

    call = fake.calls[0]
    assert call["TableName"] == "evaluation-quotas"
    assert call["Key"] == {"quota_key": {"S": '["example-benchmark","limited-tenant","evaluation","2026-07-20"]'}}
    assert call["ConditionExpression"] == ("attribute_not_exists(#request_count) OR #request_count < :limit")
    values = cast(dict[str, dict[str, str]], call["ExpressionAttributeValues"])
    assert values[":limit"] == {"N": "1"}
    assert values[":expires_at"] == {"N": str(int(datetime(2026, 7, 27, tzinfo=UTC).timestamp()))}

    with pytest.raises(evaluation_quota.EvaluationQuotaExceeded) as exc_info:
        await evaluation_quota.consume_evaluation_request(
            service_name="example-benchmark",
            tenant="limited-tenant",
            now=now,
        )

    assert exc_info.value.reset_at == datetime(2026, 7, 27, tzinfo=UTC)
    assert exc_info.value.retry_after_seconds == 216_000

    await evaluation_quota.consume_evaluation_request(
        service_name="example-benchmark",
        tenant="limited-tenant",
        now=datetime(2026, 7, 27, tzinfo=UTC),
    )
    assert fake.counts == {
        '["example-benchmark","limited-tenant","evaluation","2026-07-20"]': 1,
        '["example-benchmark","limited-tenant","evaluation","2026-07-27"]': 1,
    }


async def test_tenant_without_quota_does_not_touch_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    fake = _FakeDynamoDB()
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)

    await evaluation_quota.consume_evaluation_request(
        service_name="example-benchmark",
        tenant="unlimited-tenant",
    )

    assert fake.calls == []


def test_dynamodb_client_disables_automatic_counter_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeDynamoDB()
    captured_configs: list[Config] = []

    def create_client(service_name: str, **kwargs: object) -> object:
        assert service_name == "dynamodb"
        config = kwargs["config"]
        assert isinstance(config, Config)
        captured_configs.append(config)
        return fake

    monkeypatch.setattr(evaluation_quota.boto3, "client", create_client)
    evaluation_quota._dynamodb_client.cache_clear()  # pyright: ignore[reportPrivateUsage]
    try:
        assert evaluation_quota._dynamodb_client() is fake  # pyright: ignore[reportPrivateUsage]
        retry_config = cast(_RetryConfig, captured_configs[0])
        assert retry_config.retries == {"total_max_attempts": 1}
    finally:
        evaluation_quota._dynamodb_client.cache_clear()  # pyright: ignore[reportPrivateUsage]


async def test_cancellation_waits_for_counter_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_allowlist(monkeypatch)
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    update_started = Event()
    release_update = Event()

    def blocking_update(**_kwargs: object) -> None:
        update_started.set()
        if not release_update.wait(timeout=1):
            raise RuntimeError("test did not release quota update")

    monkeypatch.setattr(evaluation_quota, "_consume_sync", blocking_update)
    quota_update = asyncio.create_task(
        evaluation_quota.consume_evaluation_request(
            service_name="example-benchmark",
            tenant="limited-tenant",
        )
    )
    try:
        assert await asyncio.to_thread(update_started.wait, 1)
        quota_update.cancel()
        await asyncio.sleep(0)
        assert not quota_update.done()

        release_update.set()
        with pytest.raises(asyncio.CancelledError):
            await quota_update
    finally:
        release_update.set()
        if not quota_update.done():
            quota_update.cancel()
        await asyncio.gather(quota_update, return_exceptions=True)


@pytest.mark.parametrize(
    "missing_env",
    [
        evaluation_quota.EVALUATION_QUOTA_TABLE_ENV,
        evaluation_quota.SERVICE_NAME_ENV,
    ],
)
def test_quota_configuration_requires_durable_counter_settings_at_startup(
    monkeypatch: pytest.MonkeyPatch,
    missing_env: str,
) -> None:
    _set_allowlist(monkeypatch)
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    monkeypatch.setenv(evaluation_quota.SERVICE_NAME_ENV, "example-benchmark")
    monkeypatch.delenv(missing_env)

    with pytest.raises(RuntimeError, match=missing_env):
        with TestClient(BenchmarkServiceApp(StubBenchmark)):
            pass


@pytest.fixture
def quota_client(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp], None, None]:
    clear_allowlist_cache()
    clear_auth_cache()
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(evaluation_quota.SERVICE_NAME_ENV, "example-benchmark")
    monkeypatch.setenv(evaluation_quota.EVALUATION_QUOTA_TABLE_ENV, "evaluation-quotas")
    _set_allowlist(monkeypatch)
    fake = _FakeDynamoDB()
    monkeypatch.setattr(evaluation_quota, "_dynamodb_client", lambda: fake)
    app = BenchmarkServiceApp(StubBenchmark)

    async def exchange(
        _project_id: str,
        access_key: str,
    ) -> dict[str, dict[str, dict[str, str]]]:
        tenant = "unlimited-tenant" if access_key == "unlimited-key" else "limited-tenant"
        return {"tenants": {tenant: {}}}

    with patch.object(auth_module, "_exchange_descope_access_key", exchange):
        with TestClient(app) as client:
            yield client, fake, app


def _v1_eval(
    client: TestClient,
    *,
    key: str = "limited-key",
    dataset: str = "default",
    payload: dict[str, str] | None = None,
) -> Response:
    return client.post(
        "/v1/evaluate",
        json={
            "run_id": "run-1",
            "task_id": "task-1",
            "dataset": dataset,
            "payload": payload
            if payload is not None
            else {
                "type": "text",
                "schema": "stub.text.v1",
                "data": "2",
            },
        },
        headers={"x-descope-api-key": key},
    )


def test_failed_legacy_evaluation_consumes_shared_tenant_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake, _app = quota_client

    async def fail_evaluation(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("evaluation failed")

    monkeypatch.setattr(StubBenchmark, "evaluate_response", fail_evaluation)
    with pytest.raises(RuntimeError, match="evaluation failed"):
        client.post(
            "/evaluate-response/",
            json={"task_id": "task-1", "response": "2", "dataset": "default"},
            headers={"x-descope-api-key": "limited-key"},
        )
    exhausted = _v1_eval(client)

    assert exhausted.status_code == 429
    assert int(exhausted.headers["Retry-After"]) > 0
    assert exhausted.json()["detail"].startswith("Evaluation request limit of 1 per week reached")
    assert len(fake.calls) == 2


def test_dataset_rejection_does_not_consume_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
) -> None:
    client, fake, _app = quota_client

    rejected = _v1_eval(client, dataset="alt")
    admitted = _v1_eval(client)

    assert rejected.status_code == 403
    assert admitted.status_code == 200
    assert len(fake.calls) == 1


def test_quota_storage_failure_returns_retriable_http_error(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
) -> None:
    client, fake, _app = quota_client
    fake.error_code = "InternalServerError"

    response = _v1_eval(client)

    assert response.status_code == 503
    assert response.json() == {"detail": "Evaluation quota enforcement is temporarily unavailable; try again later."}
    assert "InternalServerError" not in response.text


def test_quota_storage_failure_closes_websocket_without_internal_details(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
) -> None:
    client, fake, _app = quota_client
    fake.error_code = "InternalServerError"

    with client.websocket_connect(
        "/ws/evaluate-response",
        headers={"x-descope-api-key": "limited-key"},
    ) as websocket:
        websocket.send_json({"task_id": "task-1", "response": "2", "dataset": "default"})
        with pytest.raises(WebSocketDisconnect) as exc_info:
            websocket.receive_json()

    assert exc_info.value.code == 1011
    assert exc_info.value.reason == "Evaluation quota enforcement is temporarily unavailable; try again later."


def test_unlimited_tenant_does_not_consume_limited_tenant_quota(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
) -> None:
    client, fake, _app = quota_client

    assert _v1_eval(client, key="unlimited-key").status_code == 200
    assert _v1_eval(client, key="unlimited-key").status_code == 200
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
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
    path: str,
    payload: dict[str, object],
) -> None:
    client, fake, _app = quota_client
    assert _v1_eval(client).status_code == 200

    with client.websocket_connect(
        path,
        headers={"x-descope-api-key": "limited-key"},
    ) as websocket:
        websocket.send_json(payload)
        with pytest.raises(WebSocketDisconnect) as exc_info:
            websocket.receive_json()

    assert exc_info.value.code == 1008
    assert exc_info.value.reason.startswith("Evaluation quota reached; retry after ")
    assert len(fake.calls) == 2


def test_sandbox_artifact_storage_failure_consumes_quota_before_storage_access(
    quota_client: tuple[TestClient, _FakeDynamoDB, BenchmarkServiceApp],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake, app = quota_client
    monkeypatch.setattr(StubBenchmark, "eval_mode", EvalMode.SANDBOX)
    monkeypatch.setattr(
        StubBenchmark,
        "accepted_submission_schemas",
        {V1PayloadType.ARTIFACT: frozenset({"stub.artifact.v1"})},
    )
    app._grading_provider = cast(SandboxProvider, object())  # pyright: ignore[reportPrivateUsage]
    stat_calls = 0

    async def missing_stat(_key: str, *, tenant: str) -> object:
        nonlocal stat_calls
        assert tenant == "limited-tenant"
        stat_calls += 1
        raise SubmissionArtifactNotFound("submission artifact was not found")

    monkeypatch.setattr("benchmark_service.submission_artifacts.stat", missing_stat)
    payload = {
        "type": "artifact",
        "schema": "stub.artifact.v1",
        "data": "submission-artifacts/limited-tenant/default/run-1/task-1/answer.bin",
    }

    failed = _v1_eval(client, payload=payload)
    exhausted = _v1_eval(client, payload=payload)

    assert failed.status_code == 404
    assert exhausted.status_code == 429
    assert stat_calls == 1
    assert len(fake.calls) == 2
