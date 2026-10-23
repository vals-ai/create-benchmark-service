"""Per-evaluation Gateway credentials: minted from the caller's ids, bound while grading, revoked after."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL
from benchmark_service.context import tenant_scope
from benchmark_service.grading_credentials import GradingCredentials, current_grading_credentials, grading_credentials
from benchmark_service.observability import ATTEMPT_ID_HEADER, RUN_ID_HEADER, TASK_ID_HEADER
from benchmark_service.schemas import EvaluateResponseRequest
from tests.conftest import StubBenchmark

GATEWAY = "https://gateway.test"
STATIC_KEY = "static-key"
HEADERS = {RUN_ID_HEADER: "run-1", TASK_ID_HEADER: "task-1", ATTEMPT_ID_HEADER: "attempt-1"}
TOKEN = GradingCredentials(GATEWAY, "mgwt_run-1", lease_id="lease-1")
STATIC = GradingCredentials(GATEWAY, STATIC_KEY)


class FakeGateway:
    """Records /service-auth and /service-auth/revoke calls and hands out one token."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        gateway = self

        async def post(self: httpx.AsyncClient, url: str, *, headers: dict[str, str], json: dict[str, Any]) -> Any:
            gateway.calls.append((url, headers["Authorization"], json))
            body = (
                {"token": f"mgwt_{json['run_id']}", "lease_id": "lease-1", "expires_at": 1.0}
                if url.endswith("/service-auth")
                else {"revoked": 1}
            )
            return httpx.Response(200, json=body, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx.AsyncClient, "post", post)

    def paths(self) -> list[str]:
        return [url.removeprefix(GATEWAY) for url, _, _ in self.calls]

    def mint_body(self) -> dict[str, Any]:
        return next(body for url, _, body in self.calls if url.endswith("/service-auth"))


@pytest.fixture
def gateway(monkeypatch: pytest.MonkeyPatch) -> FakeGateway:
    monkeypatch.setenv("MODEL_GATEWAY_URL", GATEWAY)
    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", STATIC_KEY)
    monkeypatch.setenv("IDENTITY", json.dumps({"benchmark_name": "snap", "phase": "grading"}))
    monkeypatch.setenv("GRADING_MODELS", "anthropic/judge, openai/helper")
    fake = FakeGateway()
    fake.install(monkeypatch)
    return fake


def bound() -> Any:
    return grading_credentials(run_id="run-1", task_id="task-1", attempt_id="attempt-1")


async def test_without_gateway_settings_nothing_is_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODEL_GATEWAY_URL", raising=False)
    async with bound():
        assert current_grading_credentials() is None


async def test_run_mints_a_grader_token_and_revokes_it_after(gateway: FakeGateway) -> None:
    async with bound():
        assert current_grading_credentials() == TOKEN
        assert TOKEN.as_environment() == {"MODEL_GATEWAY_URL": GATEWAY, "MODEL_GATEWAY_API_KEY": "mgwt_run-1"}
    assert current_grading_credentials() == STATIC
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]
    assert gateway.mint_body() == {
        "run_id": "run-1",
        "task_id": "task-1",
        "attempt_id": "attempt-1",
        "allowed_models": ["anthropic/judge", "openai/helper"],
        "allowed_operations": ["query", "tokens_count"],
        "identity": {"benchmark_name": "snap", "phase": "grading", "agent_name": "grader"},
        "ttl_seconds": 24 * 60 * 60,
    }
    assert {auth for _, auth, _ in gateway.calls} == {f"Bearer {STATIC_KEY}"}
    assert gateway.calls[1][2] == {"lease_id": "lease-1"}


async def test_token_identity_names_an_authenticated_tenant(gateway: FakeGateway) -> None:
    with tenant_scope("acme"):
        async with bound():
            pass
    assert gateway.mint_body()["identity"] == {
        "benchmark_name": "snap",
        "phase": "grading",
        "tenant": "acme",
        "agent_name": "grader",
    }


async def test_token_identity_omits_the_unauthenticated_tenant(gateway: FakeGateway) -> None:
    with tenant_scope(UNAUTHENTICATED_TENANT_SENTINEL):
        async with bound():
            pass
    assert "tenant" not in gateway.mint_body()["identity"]


async def test_token_is_revoked_even_when_grading_fails(gateway: FakeGateway) -> None:
    with pytest.raises(RuntimeError):
        async with bound():
            raise RuntimeError("judge exploded")
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]
    assert current_grading_credentials() == STATIC


async def test_nested_scope_reuses_the_enclosing_token(gateway: FakeGateway) -> None:
    async with bound():
        async with grading_credentials(run_id="run-1", task_id="task-1"):
            assert current_grading_credentials() == TOKEN
        assert current_grading_credentials() == TOKEN
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]


async def test_without_a_run_the_service_key_is_used_and_nothing_is_minted(gateway: FakeGateway) -> None:
    async with grading_credentials(run_id=None, task_id="task-1"):
        assert current_grading_credentials() == STATIC
    assert gateway.calls == []


def test_outside_an_evaluation_the_service_settings_apply(gateway: FakeGateway) -> None:
    assert current_grading_credentials() == STATIC


async def test_run_without_grading_models_falls_back_loudly(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("GRADING_MODELS")
    async with bound():
        assert current_grading_credentials() == STATIC
    assert gateway.calls == []
    assert "GRADING_MODELS is not configured" in caplog.text


async def test_concurrent_evaluations_keep_their_own_credentials(gateway: FakeGateway) -> None:
    async def observe(run_id: str) -> str | None:
        async with grading_credentials(run_id=run_id, task_id="task-1"):
            await asyncio.sleep(0)
            credentials = current_grading_credentials()
            return credentials.api_key if credentials else None

    assert await asyncio.gather(observe("run-a"), observe("run-b")) == ["mgwt_run-a", "mgwt_run-b"]
    assert current_grading_credentials() == STATIC


async def test_model_library_sends_the_token_while_grading(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []

    @contextmanager
    def gateway_token_scope(token: str) -> Iterator[None]:
        seen.append(token)
        yield

    module = types.ModuleType("model_library.base.gateway")
    module.gateway_token_scope = gateway_token_scope  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "model_library.base.gateway", module)
    async with bound():
        pass
    assert seen == ["mgwt_run-1"]


class CredentialReportingBenchmark(StubBenchmark):
    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        credentials = current_grading_credentials()
        return None if credentials is None else credentials.api_key


@pytest.fixture
def client(gateway: FakeGateway) -> Iterator[TestClient]:
    with TestClient(BenchmarkServiceApp(CredentialReportingBenchmark)) as client:
        yield client


def test_evaluate_response_grades_with_a_token_for_the_callers_run(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"}, headers=HEADERS)
    assert response.status_code == 200, response.text
    assert response.json() == "mgwt_run-1"
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]
    assert gateway.mint_body()["attempt_id"] == "attempt-1"


def test_evaluate_response_without_a_run_uses_the_service_key(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"})
    assert response.json() == STATIC_KEY
    assert gateway.calls == []
