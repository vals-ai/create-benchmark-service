from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
from daytona.common.errors import DaytonaError
from fastapi.testclient import TestClient

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.context import current_sandbox_provider, sandbox_created_scope
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.types import Sandbox, SandboxProvider
from benchmark_service.schemas import EvaluateInstanceRequest, EvaluateResponseRequest, StreamChunk, StreamResultChunk
from tests.conftest import StubBenchmark
from tests.test_sandbox import DaytonaClient, InnerSandbox, _provider, _request  # pyright: ignore[reportPrivateUsage]


class NumberedCreateClient(DaytonaClient):
    async def create(self, params: object, *_args: object, **_kwargs: object) -> InnerSandbox:
        created = InnerSandbox()
        created.id = f"sandbox-{len(self.created_ids) + 1}"
        self.created_ids.append(created.id)
        return created

    def __init__(self) -> None:
        super().__init__(InnerSandbox())
        self.created_ids: list[str] = []


class GetByIdClient(NumberedCreateClient):
    async def get(self, instance_id: str) -> InnerSandbox:
        return InnerSandbox()


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, provider: str, sandbox_id: str) -> None:
        self.calls.append((provider, sandbox_id))


async def test_each_created_sandbox_is_announced_once() -> None:
    recorder = Recorder()
    provider = _provider(NumberedCreateClient())

    with sandbox_created_scope(recorder):
        await provider.create_sandbox(_request("first"))
        await provider.create_sandbox(_request("second"))

    assert recorder.calls == [("daytona", "sandbox-1"), ("daytona", "sandbox-2")]


async def test_sandbox_recovered_after_a_name_conflict_is_announced() -> None:
    inner = InnerSandbox()

    class ConflictingCreateClient(DaytonaClient):
        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            raise DaytonaError(f"Sandbox with name {inner.name} already exists")

    recorder = Recorder()
    with sandbox_created_scope(recorder):
        await _provider(ConflictingCreateClient(inner)).create_sandbox(_request(inner.name))

    assert recorder.calls == [("daytona", inner.id)]


async def test_failed_create_announces_nothing() -> None:
    class FailingCreateClient(DaytonaClient):
        async def create(self, *_args: object, **_kwargs: object) -> InnerSandbox:
            raise DaytonaError("quota exceeded")

    recorder = Recorder()
    with sandbox_created_scope(recorder), pytest.raises(Exception):
        await _provider(FailingCreateClient(InnerSandbox())).create_sandbox(_request("sandbox-name"))

    assert recorder.calls == []


async def test_create_outside_a_request_returns_the_sandbox_without_a_hook() -> None:
    inner = InnerSandbox()

    sandbox = await _provider(DaytonaClient(inner)).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id


async def test_a_failing_hook_does_not_fail_the_create() -> None:
    async def broken(_provider: str, _sandbox_id: str) -> None:
        raise RuntimeError("socket gone")

    inner = InnerSandbox()
    with sandbox_created_scope(broken):
        sandbox = await _provider(DaytonaClient(inner)).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id


class OwnProviderBenchmark(StubBenchmark):
    """Builds its own provider from the request, as a service with its own grader sandbox does."""

    async def stream_evaluate_response(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        assert request.sandbox_provider is not None
        async with request.sandbox_provider.create_provider() as own:
            await own.create_sandbox(_request("grader-a"))
            await own.create_sandbox(_request("grader-b"))
        yield StreamResultChunk(type="result", data={"ok": True})

    async def evaluate_instance(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        provider = current_sandbox_provider()
        assert provider is not None
        await provider.create_sandbox(_request("verifier"))
        yield StreamResultChunk(type="result", data={"ok": True})


class _NullAsyncContext:
    def __init__(self, provider: SandboxProvider) -> None:
        self._provider = provider

    async def __aenter__(self) -> SandboxProvider:
        return self._provider

    async def __aexit__(self, *_args: object) -> None:
        return None


def _patch_daytona_provider(monkeypatch: pytest.MonkeyPatch, client: DaytonaClient) -> None:
    provider = _provider(client)

    def create_provider(_config: DaytonaProviderConfig) -> SandboxProvider:
        return cast(SandboxProvider, _NullAsyncContext(provider))

    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)


_PROVIDER_BODY: dict[str, Any] = {
    "type": "daytona",
    "DAYTONA_API_KEY": "key",
    "DAYTONA_API_URL": "url",
    "DAYTONA_TARGET": "target",
}


def test_evaluate_response_stream_announces_sandboxes_a_service_creates_from_its_own_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_daytona_provider(monkeypatch, NumberedCreateClient())

    with TestClient(BenchmarkServiceApp(OwnProviderBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json(
                {
                    "task_id": "task-1",
                    "eval_resume_state": {"k": "v"},
                    "sandbox_provider": _PROVIDER_BODY,
                    "announce_sandboxes": True,
                }
            )
            frames = [ws.receive_json() for _ in range(3)]

    assert frames == [
        {"type": "sandbox_created", "data": {"provider": "daytona", "sandbox_id": "sandbox-1"}},
        {"type": "sandbox_created", "data": {"provider": "daytona", "sandbox_id": "sandbox-2"}},
        {"type": "result", "data": {"ok": True}},
    ]


def test_evaluate_instance_stream_announces_sandboxes_created_from_the_request_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_daytona_provider(monkeypatch, GetByIdClient())

    with TestClient(BenchmarkServiceApp(OwnProviderBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-instance") as ws:
            ws.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "i-1",
                    "sandbox_provider": _PROVIDER_BODY,
                    "announce_sandboxes": True,
                }
            )
            frames = [ws.receive_json() for _ in range(2)]

    assert frames == [
        {"type": "sandbox_created", "data": {"provider": "daytona", "sandbox_id": "sandbox-1"}},
        {"type": "result", "data": {"ok": True}},
    ]


_OLD_EVALUATE_RESPONSE: dict[str, Any] = {
    "task_id": "task-1",
    "eval_resume_state": {"k": "v"},
    "sandbox_provider": _PROVIDER_BODY,
}
_OLD_EVALUATE_INSTANCE: dict[str, Any] = {"task_id": "task-1", "instance_id": "i-1", "sandbox_provider": _PROVIDER_BODY}


@pytest.mark.parametrize(
    "body", [{}, {"announce_sandboxes": False}], ids=["old-client-no-field", "new-client-no-callback"]
)
def test_evaluate_response_without_opt_in_streams_only_the_result_when_a_sandbox_is_created(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    _patch_daytona_provider(monkeypatch, NumberedCreateClient())

    with TestClient(BenchmarkServiceApp(OwnProviderBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({**_OLD_EVALUATE_RESPONSE, **body})
            frames = [ws.receive_json()]

    assert frames == [{"type": "result", "data": {"ok": True}}]


@pytest.mark.parametrize(
    "body", [{}, {"announce_sandboxes": False}], ids=["old-client-no-field", "new-client-no-callback"]
)
def test_evaluate_instance_without_opt_in_streams_only_the_result_when_a_sandbox_is_created(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    _patch_daytona_provider(monkeypatch, GetByIdClient())

    with TestClient(BenchmarkServiceApp(OwnProviderBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-instance") as ws:
            ws.send_json({**_OLD_EVALUATE_INSTANCE, **body})
            frames = [ws.receive_json()]

    assert frames == [{"type": "result", "data": {"ok": True}}]


def test_evaluate_requests_omit_the_opt_in_from_the_wire_unless_set() -> None:
    assert "announce_sandboxes" not in EvaluateInstanceRequest(task_id="t", instance_id="i").model_dump_json()
    assert "announce_sandboxes" not in EvaluateResponseRequest(task_id="t", response="r").model_dump_json()
    assert (
        '"announce_sandboxes":true'
        in EvaluateInstanceRequest(task_id="t", instance_id="i", announce_sandboxes=True).model_dump_json()
    )
