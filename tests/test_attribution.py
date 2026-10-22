"""Request-scoped grading attribution: headers in, scope bound, model-library told."""

from __future__ import annotations

import asyncio
import sys
import types
from collections.abc import AsyncGenerator, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from fastapi.testclient import TestClient

from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.attribution import (
    GRADER_AGENT_NAME,
    RequestAttribution,
    attribution_from_request,
    current_request_attribution,
    request_attribution_scope,
)
from benchmark_service.observability import (
    ATTEMPT_ID_HEADER,
    RUN_ID_HEADER,
    TASK_ID_HEADER,
    correlation_scope,
    request_headers,
)
from benchmark_service.sandbox import SandboxProvider
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import Sandbox
from benchmark_service.schemas import EvaluateResponseRequest, StreamChunk, StreamResultChunk
from tests.conftest import StubBenchmark
from tests.test_app import ProviderSelectionProvider

HEADERS = {RUN_ID_HEADER: "run-1", TASK_ID_HEADER: "task-1", ATTEMPT_ID_HEADER: "attempt-1"}
BOUND = RequestAttribution(run_id="run-1", task_id="task-1", attempt_id="attempt-1")


def _as_dict(attribution: RequestAttribution | None) -> dict[str, Any] | None:
    if attribution is None:
        return None
    return {"run_id": attribution.run_id, "task_id": attribution.task_id, "attempt_id": attribution.attempt_id}


class AttributingBenchmark(StubBenchmark):
    """Reports what the grading hooks see bound while they run."""

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        return _as_dict(current_request_attribution())

    async def evaluate_instance(  # type: ignore[override]
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        yield StreamResultChunk(type="result", data=_as_dict(current_request_attribution()))


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    def create_provider(_config: ModalProviderConfig) -> SandboxProvider:
        return ProviderSelectionProvider()

    monkeypatch.setattr(ModalProviderConfig, "create_provider", create_provider)
    with TestClient(BenchmarkServiceApp(AttributingBenchmark)) as c:
        yield c


def test_nothing_is_bound_outside_a_request() -> None:
    assert current_request_attribution() is None


def test_scope_binds_and_restores_even_after_an_error() -> None:
    with pytest.raises(RuntimeError):
        with request_attribution_scope(BOUND):
            assert current_request_attribution() == BOUND
            raise RuntimeError("grader failed")
    assert current_request_attribution() is None


def test_headers_are_read_case_insensitively_and_missing_values_stay_none() -> None:
    assert attribution_from_request(HEADERS, task_id="task-1") == BOUND
    lowered = {name.lower(): value for name, value in HEADERS.items()}
    assert attribution_from_request(lowered, task_id="task-1") == BOUND
    assert attribution_from_request({}, task_id="task-1") == RequestAttribution(
        run_id=None, task_id="task-1", attempt_id=None
    )


def test_explicit_attempt_id_wins_over_the_header() -> None:
    assert attribution_from_request(HEADERS, task_id="task-1", attempt_id="attempt-2").attempt_id == "attempt-2"


def test_environment_carries_only_known_ids() -> None:
    assert BOUND.as_environment() == {"RUN_ID": "run-1", "TASK_ID": "task-1", "ATTEMPT_ID": "attempt-1"}
    assert RequestAttribution(run_id="run-1", task_id="task-1", attempt_id=None).as_environment() == {
        "RUN_ID": "run-1",
        "TASK_ID": "task-1",
    }


async def test_concurrent_requests_keep_their_own_attribution() -> None:
    barrier = asyncio.Barrier(2)

    async def grade(run_id: str) -> RequestAttribution | None:
        with request_attribution_scope(RequestAttribution(run_id=run_id, task_id="task", attempt_id=run_id)):
            await barrier.wait()
            return current_request_attribution()

    first, second = await asyncio.gather(grade("run-a"), grade("run-b"))
    assert (first, second) == (
        RequestAttribution(run_id="run-a", task_id="task", attempt_id="run-a"),
        RequestAttribution(run_id="run-b", task_id="task", attempt_id="run-b"),
    )


def test_scope_attributes_model_library_queries_as_the_grader(monkeypatch: pytest.MonkeyPatch) -> None:
    opened: list[dict[str, str | None]] = []
    closed: list[bool] = []

    @contextmanager
    def attribution_scope(**kwargs: str | None) -> Iterator[None]:
        opened.append(kwargs)
        try:
            yield
        finally:
            closed.append(True)

    stub = types.ModuleType("model_library.attribution")
    stub.attribution_scope = attribution_scope  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "model_library.attribution", stub)

    with request_attribution_scope(BOUND):
        assert closed == []
    assert opened == [
        {"run_id": "run-1", "question_id": "task-1", "attempt_id": "attempt-1", "agent_name": GRADER_AGENT_NAME}
    ]
    assert closed == [True]


def test_scope_works_without_model_library(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "model_library.attribution", None)  # import raises ImportError
    with request_attribution_scope(BOUND):
        assert current_request_attribution() == BOUND


def test_client_correlation_sends_the_attempt_header_only_when_known() -> None:
    with correlation_scope(run_id="run-1", task_id="task-1", attempt_id="attempt-1"):
        assert request_headers({})[ATTEMPT_ID_HEADER] == "attempt-1"
    with correlation_scope(run_id="run-1", task_id="task-1"):
        assert ATTEMPT_ID_HEADER not in request_headers({})


def test_evaluate_response_binds_the_callers_run_and_attempt(client: TestClient) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"}, headers=HEADERS)
    assert response.status_code == 200
    assert response.json() == _as_dict(BOUND)


def test_evaluate_response_without_headers_binds_only_the_task(client: TestClient) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"})
    assert response.json() == {"run_id": None, "task_id": "task-1", "attempt_id": None}


def test_evaluate_response_stream_binds_the_callers_run_and_attempt(client: TestClient) -> None:
    with client.websocket_connect("/ws/evaluate-response", headers=HEADERS) as ws:
        ws.send_json({"task_id": "task-1", "response": "2"})
        assert ws.receive_json() == {"type": "result", "data": _as_dict(BOUND)}


def test_evaluate_instance_binds_the_callers_run_and_attempt(client: TestClient) -> None:
    with client.websocket_connect("/ws/evaluate-instance", headers=HEADERS) as ws:
        ws.send_json(
            {
                "task_id": "task-1",
                "instance_id": "i-1",
                "sandbox_provider": {"type": "modal", "MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"},
            }
        )
        assert ws.receive_json() == {"type": "result", "data": _as_dict(BOUND)}
