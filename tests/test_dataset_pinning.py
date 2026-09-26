"""Version selection through the real HTTP and WebSocket client transports."""

import asyncio
import json
import socket
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, cast

import httpx
import pytest
import uvicorn
import websockets
from fastapi import HTTPException
from fastapi.testclient import TestClient
from websockets.asyncio.server import serve

from benchmark_service import BenchmarkServiceApp, BenchmarkServiceClient, BenchmarkServiceError, DatasetVersion
from benchmark_service.schemas import (
    DATASET_VERSION_HEADER,
    EvaluateResponseRequest,
    StreamChunk,
    StreamErrorChunk,
    StreamResultChunk,
)
from benchmark_service.v1_schemas import V1Task
from conftest import StubBenchmark


class VersionedBenchmark(StubBenchmark):
    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        self.default_version = "v1.0"
        self.selected: ContextVar[str] = ContextVar("selected")
        self.opened: list[str] = []
        self.closed: list[str] = []
        self.concurrent_arrivals = 0
        self.concurrent_ready = asyncio.Event()
        return {
            version: {"task-1": {"answer": version}, f"only-{version}": {"answer": version}}
            for version in ("v1.0", "v1.1")
        }

    async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
        return headers.get("x-test-tenant")

    async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
        return tenant == "reader" and dataset in (None, "default")

    def supports_dataset_version_selection(self, dataset: str) -> bool:
        return dataset == "default"

    def get_dataset_version(self, dataset: str | None = None) -> str | None:
        return "Configured current release"

    @asynccontextmanager
    async def open_dataset_version(self, dataset: str, version: str | None) -> AsyncGenerator[DatasetVersion, None]:
        selected = "v1.0" if version == "Release α" else version or self.default_version
        if selected == "incompatible":
            raise HTTPException(status_code=409, detail="Dataset version is incompatible")
        if selected == "storage-down":
            raise HTTPException(status_code=503, detail="Dataset storage is temporarily unavailable")
        if selected not in self.datasets:
            raise HTTPException(status_code=404, detail="Dataset version unavailable")
        self.opened.append(selected)
        token = self.selected.set(selected)
        try:
            yield DatasetVersion(id=selected, label=f"Display release {selected}")
        finally:
            self.selected.reset(token)
            self.closed.append(selected)

    def get_dataset(self, dataset: str | None = None) -> dict[str, Any]:
        return self.datasets[self.selected.get()]

    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        return [V1Task(id=key, question=task["answer"], timeout=60) for key, task in self.get_dataset(dataset).items()]

    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        if request.eval_resume_state and request.eval_resume_state.get("overlap"):
            self.concurrent_arrivals += 1
            if self.concurrent_arrivals == 2:
                self.concurrent_ready.set()
            await asyncio.wait_for(self.concurrent_ready.wait(), timeout=5)
        if request.eval_resume_state and request.eval_resume_state.get("terminal_type") in {"error", "error_then_result"}:
            yield StreamErrorChunk(type="error", data="Evaluation failed")
        if not request.eval_resume_state or request.eval_resume_state.get("terminal_type") != "error":
            yield StreamResultChunk(
                type="result", data={"version": self.get_dataset(dataset)[request.task_id]["answer"]}
            )


class LegacyContinuingBenchmark(StubBenchmark):
    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        yield StreamErrorChunk(type="error", data="Recoverable warning")
        yield StreamResultChunk(type="result", data={"resolved": True})


@pytest.fixture
async def running_service(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[tuple[str, VersionedBenchmark], None]:
    monkeypatch.setenv("AUTH_DISABLED", "true")
    app = BenchmarkServiceApp(VersionedBenchmark)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical"))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if serving.done():
                        await serving
                    await asyncio.sleep(0.01)
            yield url, cast(VersionedBenchmark, app.service)
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, timeout=10)


async def test_pin_survives_default_switch_on_http_and_websocket(
    running_service: tuple[str, VersionedBenchmark],
) -> None:
    url, service = running_service
    headers = {"X-Test-Tenant": "reader"}
    async with BenchmarkServiceClient(url, headers) as unpinned:
        assert (await unpinned.version("default")).dataset_version_selection
        first = await unpinned.resolve_dataset("default")
        service.default_version = "v1.1"
        second = await unpinned.resolve_dataset("default")
        assert (await unpinned.resolve_dataset("default", version="Release α")).version.id == first.version.id
        assert first.version.id == "v1.0"
        assert second.version.id == "v1.1"

        async def use_version(version: str) -> None:
            async with BenchmarkServiceClient(url, headers, dataset_version=version) as client:
                assert (await client.verify_task_ids(None, None)).task_ids == ["task-1", f"only-{version}"]
                assert (await client.evaluate_response("task-1", version))["resolved"] is True
                assert await client.resume_evaluation("task-1", {"overlap": True}) == {"version": version}
                assert (await client.final_score({f"only-{version}": {"resolved": True}})).final_score == 100
                tasks = await client.list_tasks("default")
                assert [task.question for task in tasks.tasks] == [version, version]
                assert tasks.dataset_version == f"Display release {version}"

        await asyncio.gather(use_version(first.version.id), use_version(second.version.id))
        assert (await unpinned.verify_task_ids(None, None)).task_ids == ["task-1", "only-v1.1"]
        assert (await unpinned.list_tasks("default")).dataset_version == "Configured current release"
    assert sorted(service.opened) == sorted(service.closed)


async def test_authorization_and_bad_pins_never_enter_version_scope(
    running_service: tuple[str, VersionedBenchmark],
) -> None:
    url, service = running_service
    async with httpx.AsyncClient(base_url=url) as client:
        assert (await client.post("/resolve-dataset", json={"dataset": "default", "version": None})).status_code == 401
        assert (
            await client.post(
                "/resolve-dataset", headers={"X-Test-Tenant": "denied"}, json={"dataset": "default", "version": "v1.0"}
            )
        ).status_code == 403
        assert not service.opened

        for version_headers in (
            [(DATASET_VERSION_HEADER, "v1.0"), (DATASET_VERSION_HEADER.lower(), "v1.1")],
            [(DATASET_VERSION_HEADER, "bad version")],
        ):
            response = await client.get("/verify-task-ids", headers=[("X-Test-Tenant", "reader"), *version_headers])
            assert response.status_code == 400
        assert not service.opened
        response = await client.get(
            "/verify-task-ids", headers={"X-Test-Tenant": "reader", DATASET_VERSION_HEADER: "gone"}
        )
        assert response.status_code == 404
        assert DATASET_VERSION_HEADER not in response.headers
        assert not service.opened


def test_trial_resolution_checks_dataset_access_without_grading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_DISABLED", "true")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"reader": {"datasets": ["default"], "trial_mode": True}}}),
    )
    app = BenchmarkServiceApp(VersionedBenchmark)
    with TestClient(app) as client:
        headers = {"X-Test-Tenant": "reader"}
        assert (
            client.post("/resolve-dataset", headers=headers, json={"dataset": "default", "version": None}).status_code
            == 200
        )
        assert (
            client.post("/resolve-dataset", headers=headers, json={"dataset": "held-out", "version": None}).status_code
            == 403
        )
        assert client.get("/verify-task-ids", headers=headers).status_code == 403
        assert cast(VersionedBenchmark, app.service).opened == ["v1.0"]


@pytest.mark.parametrize("terminal_type", ["error_then_result", "error"])
async def test_raw_websocket_error_keeps_scope_until_stream_finishes(
    running_service: tuple[str, VersionedBenchmark], terminal_type: str
) -> None:
    url, service = running_service
    async with websockets.connect(
        url.replace("http", "ws") + "/ws/evaluate-response",
        additional_headers={"X-Test-Tenant": "reader", DATASET_VERSION_HEADER: "v1.0"},
    ) as ws:
        await ws.send(json.dumps({"task_id": "task-1", "eval_resume_state": {"terminal_type": terminal_type}}))
        assert json.loads(await ws.recv()) == {
            "type": "dataset_version",
            "data": {"id": "v1.0", "label": "Display release v1.0"},
        }
        assert json.loads(await ws.recv()) == {"type": "error", "data": "Evaluation failed"}
        if terminal_type == "error_then_result":
            assert json.loads(await ws.recv()) == {"type": "result", "data": {"version": "v1.0"}}
        await asyncio.wait_for(ws.wait_closed(), timeout=5)
    assert service.opened == service.closed == ["v1.0"]


@pytest.mark.parametrize(
    ("headers", "status_code", "detail"),
    [
        ([(DATASET_VERSION_HEADER, "gone")], 404, "Dataset version unavailable"),
        ([(DATASET_VERSION_HEADER, "incompatible")], 409, "Dataset version is incompatible"),
        ([(DATASET_VERSION_HEADER, "storage-down")], 503, "Dataset storage is temporarily unavailable"),
        ([(DATASET_VERSION_HEADER, "bad version")], 400, f"Invalid {DATASET_VERSION_HEADER}"),
        (
            [(DATASET_VERSION_HEADER, "v1.0"), (DATASET_VERSION_HEADER.lower(), "v1.1")],
            400,
            f"Supply {DATASET_VERSION_HEADER} only once",
        ),
    ],
)
async def test_websocket_version_selection_error_is_structured_before_acknowledgement(
    running_service: tuple[str, VersionedBenchmark],
    headers: list[tuple[str, str]],
    status_code: int,
    detail: str,
) -> None:
    url, _ = running_service
    async with websockets.connect(
        url.replace("http", "ws") + "/ws/evaluate-response",
        additional_headers=[("X-Test-Tenant", "reader"), *headers],
    ) as ws:
        await ws.send(json.dumps({"task_id": "task-1", "response": "answer"}))
        assert json.loads(await ws.recv()) == {
            "type": "dataset_version_error",
            "data": {"status_code": status_code, "detail": detail},
        }


@pytest.mark.parametrize(("version", "status_code"), [("gone", 404), ("incompatible", 409), ("storage-down", 503)])
async def test_client_preserves_websocket_version_selection_status(
    running_service: tuple[str, VersionedBenchmark], version: str, status_code: int
) -> None:
    url, _ = running_service
    async with BenchmarkServiceClient(url, {"X-Test-Tenant": "reader"}, dataset_version=version) as client:
        with pytest.raises(BenchmarkServiceError) as failure:
            await client.resume_evaluation("task-1", {})
        assert failure.value.status_code == status_code


def test_legacy_service_requires_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_DISABLED", "true")
    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        assert client.get("/verify-task-ids").json()["task_ids"] == ["task-1", "task-2", "task-3"]
        assert client.get("/verify-task-ids", headers={DATASET_VERSION_HEADER: "v1.0"}).status_code == 400
        assert client.post("/resolve-dataset", json={"dataset": "default", "version": None}).status_code == 400
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {"type": "result", "data": {"resolved": True}}
        with client.websocket_connect(
            "/ws/evaluate-response", headers={DATASET_VERSION_HEADER: "v1.0"}
        ) as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {
                "type": "dataset_version_error",
                "data": {"status_code": 400, "detail": "Dataset version selection is not supported"},
            }


def test_legacy_unpinned_stream_continues_after_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_DISABLED", "true")
    with TestClient(BenchmarkServiceApp(LegacyContinuingBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {"type": "error", "data": "Recoverable warning"}
            assert ws.receive_json() == {"type": "result", "data": {"resolved": True}}


@pytest.mark.parametrize("echo", [None, "v1.1", "v1.0, v1.0"])
async def test_client_rejects_http_without_exact_acknowledgement(echo: str | None) -> None:
    async with BenchmarkServiceClient("http://test", {}, dataset_version="v1.0") as client:

        async def response(request: httpx.Request) -> httpx.Response:
            assert request.headers[DATASET_VERSION_HEADER] == "v1.0"
            return httpx.Response(
                200, json={"task_ids": []}, headers={} if echo is None else {DATASET_VERSION_HEADER: echo}
            )

        original = client._http_client  # pyright: ignore[reportPrivateUsage]
        client._http_client = httpx.AsyncClient(  # pyright: ignore[reportPrivateUsage]
            headers=original.headers, event_hooks=original.event_hooks, transport=httpx.MockTransport(response)
        )
        await original.aclose()
        with pytest.raises(BenchmarkServiceError, match="acknowledge"):
            await client.verify_task_ids(None, None)


@pytest.mark.parametrize(
    "messages",
    [
        [{"type": "result", "data": 1}],
        [{"type": "eval_resume_state", "data": {}}],
        [{"type": "dataset_version", "data": {"id": "v1.1", "label": None}}],
        [{"type": "dataset_version", "data": {"id": "v1.0", "label": None}}] * 2,
    ],
)
async def test_client_rejects_invalid_stream_before_callbacks(messages: list[dict[str, Any]]) -> None:
    async def respond(ws: websockets.ServerConnection) -> None:
        await ws.recv()
        for message in messages:
            await ws.send(json.dumps(message))

    async with serve(respond, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        async with BenchmarkServiceClient(f"http://127.0.0.1:{port}", {}, dataset_version="v1.0") as client:
            checkpoints: list[dict[str, Any]] = []
            with pytest.raises(BenchmarkServiceError, match="acknowledge"):
                await client.resume_evaluation("task-1", {}, on_eval_resume_state=checkpoints.append)
            assert not checkpoints
