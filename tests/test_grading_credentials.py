"""Gateway credentials for legacy service-key runs and supplied project grader tokens."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from sentry_sdk.types import Event

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import UNAUTHENTICATED_TENANT_SENTINEL
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.context import tenant_scope
from benchmark_service.grading_credentials import (
    EvaluationCaller,
    GradingCredentials,
    current_evaluation_caller,
    current_grading_credentials,
    grading_credentials,
)
from benchmark_service.observability import ATTEMPT_ID_HEADER, EMAIL_HEADER, RUN_ID_HEADER, TASK_ID_HEADER
from benchmark_service.remote_grading import (
    REMOTE_GRADING_GRANT_HEADER,
    REMOTE_GRADING_ISSUER_HEADER,
    RemoteGradingAuthorization,
    current_remote_grading_authorization,
)
from benchmark_service.sentry import _omit_remote_grading_grant  # pyright: ignore[reportPrivateUsage]
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.schemas import EvaluateResponseRequest, StreamChunk, StreamMessageChunk, StreamResultChunk
from tests.conftest import StubBenchmark
from tests.test_draining import serve
from tests.test_sandbox_created import GetByIdClient
from tests.test_sandbox_created import _PROVIDER_BODY, _patch_daytona_provider  # pyright: ignore[reportPrivateUsage]

GATEWAY = "https://gateway.test"
STATIC_KEY = "static-key"
HEADERS = {RUN_ID_HEADER: "run-1", TASK_ID_HEADER: "task-1", ATTEMPT_ID_HEADER: "attempt-1"}
TOKEN = GradingCredentials(GATEWAY, "mgwt_run-1", lease_id="lease-1")
STATIC = GradingCredentials(GATEWAY, STATIC_KEY)


class FakeGateway:
    """Records Gateway calls and hands out one service token."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        gateway = self

        async def post(self: httpx.AsyncClient, url: str, *, headers: dict[str, str], json: dict[str, Any]) -> Any:
            gateway.calls.append((url, headers["Authorization"], json))
            if url.endswith("/models/query"):
                body = {"score": 1.0}
            elif url.endswith("/service-auth"):
                body = {"token": f"mgwt_{json['run_id']}", "lease_id": "lease-1", "expires_at": 1.0}
            else:
                body = {"revoked": 1}
            return httpx.Response(200, json=body, request=httpx.Request("POST", url))

        monkeypatch.setattr(httpx.AsyncClient, "post", post)

    def paths(self) -> list[str]:
        return [url.removeprefix(GATEWAY) for url, _, _ in self.calls]

    def mint_body(self) -> dict[str, Any]:
        return next(body for url, _, body in self.calls if url.endswith("/service-auth"))


def install_model_library(monkeypatch: pytest.MonkeyPatch, **attributes: Any) -> None:
    module = types.ModuleType("model_library.base.gateway")
    vars(module).update(attributes)
    monkeypatch.setitem(sys.modules, "model_library.base.gateway", module)


@pytest.fixture
def scoped_tokens(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    @contextmanager
    def gateway_token_scope(token: str) -> Iterator[None]:
        seen.append(token)
        yield

    install_model_library(monkeypatch, gateway_token_scope=gateway_token_scope)
    return seen


@pytest.fixture
def gateway(monkeypatch: pytest.MonkeyPatch, scoped_tokens: list[str]) -> FakeGateway:
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
    assert current_grading_credentials() is None
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
    assert current_grading_credentials() is None


async def test_nested_scope_reuses_the_enclosing_token(gateway: FakeGateway) -> None:
    async with bound():
        async with grading_credentials(run_id="run-1", task_id="task-1", attempt_id="attempt-1"):
            assert current_grading_credentials() == TOKEN
        assert current_grading_credentials() == TOKEN
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]


async def test_without_a_run_the_service_key_is_used_and_nothing_is_minted(gateway: FakeGateway) -> None:
    async with grading_credentials(run_id=None, task_id="task-1"):
        assert current_grading_credentials() == STATIC
    assert gateway.calls == []


async def test_a_run_without_an_attempt_fails_grading(gateway: FakeGateway) -> None:
    with pytest.raises(RuntimeError, match="no attempt id"):
        async with grading_credentials(run_id="run-1", task_id="task-1"):
            pytest.fail("graded without a token")
    assert gateway.calls == []


def test_outside_an_evaluation_nothing_is_bound(gateway: FakeGateway) -> None:
    assert current_grading_credentials() is None


@pytest.mark.parametrize("run_id", ["run-1", None])
async def test_without_grading_models_nothing_is_bound(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, run_id: str | None
) -> None:
    monkeypatch.delenv("GRADING_MODELS")
    async with grading_credentials(run_id=run_id, task_id="task-1", attempt_id="attempt-1"):
        assert current_grading_credentials() is None
    assert gateway.calls == []


async def test_concurrent_evaluations_keep_their_own_credentials(gateway: FakeGateway) -> None:
    async def observe(run_id: str) -> str | None:
        async with grading_credentials(run_id=run_id, task_id="task-1", attempt_id="attempt-1"):
            await asyncio.sleep(0)
            credentials = current_grading_credentials()
            return credentials.api_key if credentials else None

    assert await asyncio.gather(observe("run-a"), observe("run-b")) == ["mgwt_run-a", "mgwt_run-b"]
    assert current_grading_credentials() is None


async def test_model_library_sends_the_token_while_grading(gateway: FakeGateway, scoped_tokens: list[str]) -> None:
    async with bound():
        pass
    assert scoped_tokens == ["mgwt_run-1"]


async def test_a_model_library_without_token_scopes_fails_grading_before_minting(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_model_library(monkeypatch)
    with pytest.raises(ImportError, match="gateway_token_scope"):
        async with bound():
            pytest.fail("graded without the token reaching model-library")
    assert gateway.calls == []


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
    assert "email" not in gateway.mint_body()["identity"]


def test_evaluate_response_names_the_callers_email_on_the_token(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post(
        "/evaluate-response/",
        json={"task_id": "task-1", "response": "2"},
        headers={**HEADERS, EMAIL_HEADER: "starter@example.com"},
    )
    assert response.status_code == 200, response.text
    assert gateway.mint_body()["identity"]["email"] == "starter@example.com"


def test_evaluate_response_without_a_run_uses_the_service_key(client: TestClient, gateway: FakeGateway) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"})
    assert response.json() == STATIC_KEY
    assert gateway.calls == []


def test_http_response_evaluation_rejects_a_grader_token_instead_of_service_billing(
    client: TestClient, gateway: FakeGateway
) -> None:
    response = client.post(
        "/evaluate-response/",
        json={"task_id": "task-1", "response": "2", "grading_token": "project-grader-token"},
        headers=HEADERS,
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "grading_token is supported only on WebSocket evaluation"}
    assert gateway.calls == []


@pytest.mark.parametrize("grading_models", ["openai/gpt-4o", ""])
async def test_the_callers_run_and_email_are_bound_with_or_without_grading_models(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, grading_models: str
) -> None:
    monkeypatch.setenv("GRADING_MODELS", grading_models)
    async with grading_credentials(run_id="run-1", task_id="task-1", attempt_id="attempt-1", email="a@example.com"):
        assert current_evaluation_caller() == EvaluationCaller("run-1", "task-1", "attempt-1", "a@example.com")
    assert current_evaluation_caller() is None


async def test_without_a_run_no_caller_is_bound(gateway: FakeGateway) -> None:
    async with grading_credentials(run_id=None, task_id="task-1", email="a@example.com"):
        assert current_evaluation_caller() is None


class TaskScopedBenchmark(CredentialReportingBenchmark):
    def grading_requirements(self, task_id: str, dataset: str | None = None) -> list[str]:
        if dataset == "alt":
            return ["openai/alt-judge"]
        return super().grading_requirements(task_id, dataset=dataset)

    async def evaluate_instance(
        self, task_id: str, sandbox: Any, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        credentials = current_grading_credentials()
        yield StreamResultChunk(type="result", data={"gateway_key": credentials.api_key if credentials else None})

    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        assert request.grading_token is None
        credentials = current_grading_credentials()
        yield StreamResultChunk(type="result", data={"gateway_key": credentials.api_key if credentials else None})


def test_requirements_are_authenticated_dataset_scoped_and_task_overridable(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    class AuthenticatedBenchmark(TaskScopedBenchmark):
        async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
            return "tenant" if headers.get("authorization") == "Bearer authorized" else None

        async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
            return dataset != "blocked"

    monkeypatch.setenv("AUTH_REQUIRED", "true")
    with TestClient(BenchmarkServiceApp(AuthenticatedBenchmark)) as client:
        body = {"task_id": "alt-task-1", "dataset": "alt"}
        assert client.post("/grading-requirements", json=body).status_code == 401
        assert client.post(
            "/grading-requirements",
            json={"task_id": "task-1", "dataset": "blocked"},
            headers={"Authorization": "Bearer authorized"},
        ).status_code == 403
        response = client.post("/grading-requirements", json=body, headers={"Authorization": "Bearer authorized"})
        assert response.status_code == 200
        assert response.json() == {"models": ["openai/alt-judge"]}
        response = client.post(
            "/grading-requirements",
            json={"task_id": "task-1", "dataset": None},
            headers={"Authorization": "Bearer authorized"},
        )
        assert response.json() == {"models": ["anthropic/judge", "openai/helper"]}


def test_supplied_grader_token_reaches_instance_and_resume_without_service_mint(
    gateway: FakeGateway, scoped_tokens: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_daytona_provider(monkeypatch, GetByIdClient())
    with TestClient(BenchmarkServiceApp(TaskScopedBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-instance", headers=HEADERS.copy()) as websocket:
            websocket.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "i-1",
                    "sandbox_provider": _PROVIDER_BODY,
                    "grading_token": "project-grader-token",
                }
            )
            assert websocket.receive_json() == {
                "type": "result",
                "data": {"gateway_key": "project-grader-token"},
            }
        with client.websocket_connect("/ws/evaluate-response", headers=HEADERS.copy()) as websocket:
            websocket.send_json(
                {"task_id": "task-1", "eval_resume_state": {"progress": 1}, "grading_token": "project-grader-token"}
            )
            assert websocket.receive_json() == {
                "type": "result",
                "data": {"gateway_key": "project-grader-token"},
            }
    assert gateway.calls == []
    assert scoped_tokens == ["project-grader-token", "project-grader-token"]


@pytest.mark.parametrize("resume", [False, True])
async def test_client_keeps_grading_token_through_server_finalizer(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, resume: bool
) -> None:
    query_started = asyncio.Event()
    post_result_received = asyncio.Event()
    release_query = asyncio.Event()
    gateway_post = httpx.AsyncClient.post

    def on_message(message: str) -> None:
        assert message == "after result"
        post_result_received.set()

    async def pending_query(
        session: httpx.AsyncClient, url: str, *, headers: dict[str, str], json: dict[str, Any]
    ) -> Any:
        if url == f"{GATEWAY}/models/query":
            query_started.set()
            await release_query.wait()
        return await gateway_post(session, url, headers=headers, json=json)

    monkeypatch.setattr(httpx.AsyncClient, "post", pending_query)
    _patch_daytona_provider(monkeypatch, GetByIdClient())

    class FinalizingBenchmark(TaskScopedBenchmark):
        async def _grade(self) -> AsyncGenerator[StreamChunk, None]:
            try:
                yield StreamResultChunk(type="result", data={"score": 1.0})
                yield StreamMessageChunk(type="message", data="after result")
            finally:
                credentials = current_grading_credentials()
                assert credentials is not None
                async with httpx.AsyncClient() as session:
                    response = await session.post(
                        f"{GATEWAY}/models/query",
                        headers={"Authorization": f"Bearer {credentials.api_key}"},
                        json={"model": "anthropic/judge"},
                    )
                assert response.json() == {"score": 1.0}

        async def evaluate_instance(
            self, task_id: str, sandbox: Any, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            async for chunk in self._grade():
                yield chunk

        async def stream_evaluate_response(
            self, request: EvaluateResponseRequest, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            assert request.grading_token is None
            async for chunk in self._grade():
                yield chunk

    async with serve(BenchmarkServiceApp(FinalizingBenchmark)) as url:
        async with BenchmarkServiceClient(url, HEADERS) as client:
            if resume:
                operation = asyncio.create_task(
                    client.resume_evaluation(
                        "task-1",
                        {"progress": 1},
                        on_message=on_message,
                        grading_token="project-grader-token",
                    )
                )
            else:
                operation = asyncio.create_task(
                    client.evaluate_instance(
                        "task-1",
                        "i-1",
                        sandbox_provider=DaytonaProviderConfig.model_validate(_PROVIDER_BODY),
                        on_message=on_message,
                        grading_token="project-grader-token",
                    )
                )
            try:
                await asyncio.wait_for(post_result_received.wait(), 5)
                await asyncio.wait_for(query_started.wait(), 5)
                assert not operation.done()
            finally:
                release_query.set()
            assert await asyncio.wait_for(operation, 5) == {"score": 1.0}

    assert gateway.calls == [
        (f"{GATEWAY}/models/query", "Bearer project-grader-token", {"model": "anthropic/judge"})
    ]


def test_supplied_token_is_not_echoed_on_websocket_exception(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class FailingBenchmark(TaskScopedBenchmark):
        async def evaluate_instance(
            self, task_id: str, sandbox: Any, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            raise RuntimeError("project-grader-token")
            yield StreamResultChunk(type="result", data=None)

    _patch_daytona_provider(monkeypatch, GetByIdClient())
    with TestClient(BenchmarkServiceApp(FailingBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-instance", headers=HEADERS.copy()) as websocket:
            websocket.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "i-1",
                    "sandbox_provider": _PROVIDER_BODY,
                    "grading_token": "project-grader-token",
                }
            )
            assert websocket.receive_json() == {"type": "error", "data": "Evaluation failed"}
    assert "project-grader-token" not in caplog.text
    assert gateway.calls == []


async def test_registered_remote_handoff_binds_setup_evaluation_and_resume_without_service_mint(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_daytona_provider(monkeypatch, GetByIdClient())
    seen: list[tuple[str, RemoteGradingAuthorization | None, GradingCredentials | None, EvaluationCaller | None]] = []

    class RemoteBenchmark(TaskScopedBenchmark):
        remote_grader_service = "vcb"

        def observe(self, stage: str) -> None:
            seen.append((
                stage,
                current_remote_grading_authorization(),
                current_grading_credentials(),
                current_evaluation_caller(),
            ))

        async def setup_task(
            self, task_id: str, sandbox: Any, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            await asyncio.sleep(0)
            self.observe("setup")
            yield StreamResultChunk(type="result", data={"status": "ok"})

        async def evaluate_instance(
            self, task_id: str, sandbox: Any, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            await asyncio.sleep(0)
            self.observe("instance")
            yield StreamResultChunk(type="result", data={"score": 1.0})

        async def stream_evaluate_response(
            self, request: EvaluateResponseRequest, dataset: str | None = None
        ) -> AsyncGenerator[StreamChunk, None]:
            await asyncio.sleep(0)
            self.observe("resume")
            yield StreamResultChunk(type="result", data={"score": 1.0})

    authorization_a = RemoteGradingAuthorization("tracker-a", "private-grant-a")
    authorization_b = RemoteGradingAuthorization("tracker-b", "private-grant-b")
    assert "private-grant-a" not in repr(authorization_a)
    provider = DaytonaProviderConfig.model_validate(_PROVIDER_BODY)

    async with serve(BenchmarkServiceApp(RemoteBenchmark)) as url:
        async with BenchmarkServiceClient(url, HEADERS) as client:
            requirements = await client.grading_requirements_response("task-1")
            assert requirements.remote_grader_service == "vcb"
            assert requirements.models == ["anthropic/judge", "openai/helper"]
            assert await client.grading_requirements("task-1") == requirements.models
            assert (
                await client.setup_task(
                    "task-1", "i-1", sandbox_provider=provider,
                    remote_grading_authorization=authorization_a,
                )
            ).status == "ok"
            assert await asyncio.gather(
                client.evaluate_instance(
                    "task-1", "i-1", sandbox_provider=provider,
                    remote_grading_authorization=authorization_b,
                ),
                client.resume_evaluation(
                    "task-1", {"checkpoint": "saved"},
                    remote_grading_authorization=authorization_a,
                ),
            ) == [{"score": 1.0}, {"score": 1.0}]
            assert await client.evaluate_instance("task-1", "i-1", sandbox_provider=provider) == {"score": 1.0}

    caller = EvaluationCaller("run-1", "task-1", "attempt-1", None)
    assert seen[0] == ("setup", authorization_a, None, None)
    assert set(seen[1:3]) == {
        ("instance", authorization_b, None, caller),
        ("resume", authorization_a, None, caller),
    }
    assert seen[3] == ("instance", None, TOKEN, caller)
    assert gateway.paths() == ["/service-auth", "/service-auth/revoke"]
    assert current_remote_grading_authorization() is None


def test_malformed_remote_handoff_never_falls_back_to_service_credentials(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_daytona_provider(monkeypatch, GetByIdClient())
    with TestClient(BenchmarkServiceApp(TaskScopedBenchmark)) as client:
        with client.websocket_connect(
            "/ws/evaluate-instance", headers={**HEADERS, REMOTE_GRADING_ISSUER_HEADER: "tracker-a"}
        ) as websocket:
            websocket.send_json({"task_id": "task-1", "instance_id": "i-1", "sandbox_provider": _PROVIDER_BODY})
            assert websocket.receive_json()["type"] == "error"
        with client.websocket_connect(
            "/ws/evaluate-instance", headers={**HEADERS, REMOTE_GRADING_GRANT_HEADER: "secret"}
        ) as websocket:
            websocket.send_json({"task_id": "task-1", "instance_id": "i-1", "sandbox_provider": _PROVIDER_BODY})
            error = websocket.receive_json()
            assert error["type"] == "error"
            assert "Supply exactly one remote grading issuer and grant" in error["data"]
            assert "secret" not in error["data"]
    assert gateway.calls == []


def test_sentry_preserves_remote_error_without_capturing_grant_in_frame_locals() -> None:
    remote: Event = {
        "request": {"headers": {"x-vals-remote-grading-grant": "private-grant-a"}},
        "exception": {"values": [{
            "value": "worker failed",
            "stacktrace": {"frames": [
                {
                    "function": "remote_grading_authorization_from_headers",
                    "vars": {
                        "headers": {"x-vals-remote-grading-grant": "'private-grant-a'"},
                        "grants": ["'private-grant-a'"],
                        "issuer": "tracker-a",
                    },
                },
                {"function": "evaluate_instance", "vars": {"task_id": "task-1"}},
            ]},
        }]},
    }
    ordinary: Event = {
        "request": {"headers": {"x-vals-run-id": "run-1"}},
        "exception": {"values": [{
            "stacktrace": {"frames": [{"function": "evaluate_instance", "vars": {"task_id": "task-1"}}]},
        }]},
    }
    _omit_remote_grading_grant(remote, {})
    _omit_remote_grading_grant(ordinary, {})
    assert "private-grant-a" not in repr(remote)
    assert remote["exception"]["values"][0]["value"] == "worker failed"
    frames = remote["exception"]["values"][0]["stacktrace"]["frames"]
    assert frames == [
        {"function": "remote_grading_authorization_from_headers"},
        {"function": "evaluate_instance"},
    ]
    assert ordinary["exception"]["values"][0]["stacktrace"]["frames"][0]["vars"] == {"task_id": "task-1"}
